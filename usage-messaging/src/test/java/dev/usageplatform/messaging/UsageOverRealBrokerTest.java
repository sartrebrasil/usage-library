package dev.usageplatform.messaging;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.inbox.ConsumerSettings;
import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.Destinations;
import com.example.messaging.inbox.EnvelopeMode;
import com.example.messaging.inbox.EnvelopeReader;
import com.example.messaging.inbox.InboxConsumer;
import com.example.messaging.inbox.Ordering;
import com.example.messaging.inbox.Pipeline;
import com.example.messaging.inbox.RetryQueue;
import com.example.messaging.inbox.StopSignal;
import com.example.messaging.inbox.retry.BackoffSpec;
import com.example.messaging.inbox.retry.RetryPolicy;
import com.example.messaging.rabbitmq.RabbitMessageReceiver;
import com.example.messaging.rabbitmq.RabbitMessageSender;
import com.example.outbox.broker.messaging.MessageSenderPublisher;
import com.example.outbox.core.OutboxConfig;
import com.example.outbox.core.OutboxMessage;
import com.example.outbox.core.OutboxRelay;
import com.example.outbox.spring.JdbcOutboxWriter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import dev.usageplatform.consumer.UsageEventConsumer;
import dev.usageplatform.consumer.UsageEventJson;
import dev.usageplatform.core.Compensation;
import dev.usageplatform.core.InMemoryCounterStore;
import dev.usageplatform.core.PeriodCalculator;
import dev.usageplatform.core.SummaryStore.Summary;
import dev.usageplatform.core.TenantId;
import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.UsageEngine.FailMode;
import dev.usageplatform.core.UsageEvent;
import dev.usageplatform.core.Window;
import dev.usageplatform.postgres.JdbcLedger;
import dev.usageplatform.postgres.JdbcPolicyStore;
import dev.usageplatform.postgres.JdbcSubscriptions;
import dev.usageplatform.postgres.JdbcSummaryStore;
import dev.usageplatform.postgres.UsageSchema;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The whole path against real services: usage is written to the outbox in PostgreSQL, the outbox relay publishes it
 * to RabbitMQ through messaging-library, the messaging-inbox delivers it to {@link UsageInboxHandler}, and the
 * platform counts it in the ledger. Nothing in the path is faked except the counters, which Redis tests cover.
 */
@Testcontainers
class UsageOverRealBrokerTest {
    @Container
    static final GenericContainer<?> RABBIT = new GenericContainer<>("rabbitmq:4.1-management-alpine")
            .withExposedPorts(5672)
            .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1));

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static final String TOPIC = "usage-events";
    static final Duration LEASE = Duration.ofSeconds(5);
    static final Instant ANCHOR = Instant.parse("2020-01-01T00:00:00Z");
    static final Duration PATIENCE = Duration.ofSeconds(60);

    static DataSource ds;
    static com.rabbitmq.client.Connection rabbit;
    static JdbcSummaryStore summaries;
    static OutboxUsageSink sink;
    static JdbcOutboxWriter writer;
    static TransactionTemplate transaction;
    static OutboxRelay relay;
    static InboxConsumer inbox;
    static final List<UsageEventConsumer.Outcome> outcomes = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void start() throws Exception {
        PGSimpleDataSource pg = new PGSimpleDataSource();
        pg.setUrl(POSTGRES.getJdbcUrl());
        pg.setUser(POSTGRES.getUsername());
        pg.setPassword(POSTGRES.getPassword());
        ds = pg;
        Flyway.configure().dataSource(ds).locations("classpath:db/outbox/postgresql").table("outbox_schema_history").load().migrate();
        UsageSchema.migrate(ds, "usage");
        seed();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT.getHost());
        factory.setPort(RABBIT.getMappedPort(5672));
        rabbit = factory.newConnection();
        try (Channel channel = rabbit.createChannel()) {
            for (String queue : List.of(TOPIC, "usage-failed", "usage-dlq")) {
                channel.queueDeclare(queue, true, false, false, Map.of("x-queue-type", "quorum"));
            }
            // retry-queues on RabbitMQ is topology, not code: the wait queue's TTL must equal the configured delay and
            // dead-letter back to the main queue (messaging-inbox README, "RabbitMQ com retry-queues")
            channel.queueDeclare("usage-retry", true, false, false, Map.of("x-queue-type", "quorum",
                    "x-message-ttl", (int) LEASE.toMillis(), "x-dead-letter-exchange", "", "x-dead-letter-routing-key", TOPIC));
        }

        // producer: the outbox writer, in a transaction of its own per message, then the relay to RabbitMQ
        OutboxConfig config = OutboxConfig.defaults();
        var metrics = new SimpleMeterRegistry();
        writer = new JdbcOutboxWriter(config, metrics, JdbcClient.create(ds), payload -> {
            throw new UnsupportedOperationException("the sink writes ready JSON");
        });
        transaction = new TransactionTemplate(new DataSourceTransactionManager(ds));
        sink = OutboxUsageSink.ownTransaction(writer, TOPIC, Executors.newVirtualThreadPerTaskExecutor(),
                work -> transaction.executeWithoutResult(status -> work.run()));
        relay = new OutboxRelay(ds, config, metrics, Map.of(TOPIC,
                new MessageSenderPublisher(RabbitMessageSender.forQueue(rabbit, TOPIC, RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES))));
        relay.start();

        // consumer: the platform behind the messaging-inbox, wired the way the library's own contract tests do
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        Clock clock = Clock.systemUTC();
        summaries = new JdbcSummaryStore(ds, executor, "usage");
        var engine = new UsageEngine(new JdbcPolicyStore(ds, executor, clock, "usage"), new InMemoryCounterStore(clock),
                new JdbcLedger(ds, executor, "usage"), sink, clock, FailMode.CLOSED, Duration.ofMinutes(15));
        var consumer = new UsageEventConsumer(engine, Set.of("API_REQUESTS")::contains, clock, Duration.ofMinutes(5), Duration.ofDays(90));
        var handler = new UsageInboxHandler<String>(consumer, Duration.ofSeconds(1), Duration.ofSeconds(20), outcomes::add);

        ConsumerSettings settings = new ConsumerSettings("usage", TOPIC, 1, 10, Duration.ofSeconds(1), LEASE.multipliedBy(6),
                Duration.ofSeconds(5), Ordering.RELAXED, DelayedStrategy.RETRY_QUEUES, true, 1, LEASE, 100);
        RetryPolicy policy = new RetryPolicy(new BackoffSpec(2, Duration.ofMillis(10), 1.0, Duration.ofMillis(10), 0),
                new BackoffSpec(5, LEASE, 1.0, LEASE, 0), true, 0, List.of(IllegalArgumentException.class), List.of(), List.of());
        StopSignal stop = new StopSignal();
        var pipeline = new Pipeline<>(settings.consumer(), new EnvelopeReader(EnvelopeMode.AUTO), (m, t) -> m.payloadAsString(),
                String.class, handler, policy, stop, () -> 0.0);
        MessageSender dlq = queue("usage-dlq");
        var destinations = new Destinations(() -> new RabbitMessageReceiver(rabbit, TOPIC, LEASE, dlq), queue("usage-failed"), dlq,
                List.of(new RetryQueue(LEASE, queue("usage-retry"))));
        inbox = new InboxConsumer(settings, pipeline, destinations, stop, clock, () -> 0.0);
        inbox.start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (inbox != null) inbox.stop();
        if (relay != null) relay.close();
        if (rabbit != null) rabbit.close();
    }

    static MessageSender queue(String name) {
        return RabbitMessageSender.forQueue(rabbit, name, RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES);
    }

    static void seed() throws Exception {
        try (java.sql.Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.execute("""
                    INSERT INTO usage.metric (id, code, unit, count_trigger) VALUES (gen_random_uuid(), 'API_REQUESTS', 'REQUEST', 'ON_REQUEST');
                    INSERT INTO usage.plan (id, code, name, version) VALUES (gen_random_uuid(), 'E2E', 'End to end', 1);
                    INSERT INTO usage.plan_limit (id, plan_id, metric_id, limit_type, limit_value, window_seconds, enforcement)
                      SELECT gen_random_uuid(), p.id, m.id, 'QUOTA', 1000000, 86400, 'REJECT'
                        FROM usage.plan p, usage.metric m WHERE p.code = 'E2E' AND m.code = 'API_REQUESTS';
                    """);
            for (String tenant : List.of("t-flow", "t-dup", "t-order", "t-poison", "t-outage")) {
                s.execute("INSERT INTO usage.tenant (id, name) VALUES ('" + tenant + "', '" + tenant + "')");
            }
        }
        var subscriptions = new JdbcSubscriptions(ds, "usage");
        for (String tenant : List.of("t-flow", "t-dup", "t-order", "t-poison", "t-outage")) {
            subscriptions.subscribe(new TenantId(tenant), "E2E", 1, ANCHOR);
        }
    }

    // ---- helpers ------------------------------------------------------------------------------------------

    /** One instant for the whole class: events and the period the assertions read must not straddle midnight UTC. */
    static final Instant AT = Instant.now();

    static UsageEvent usage(String tenant, long quantity) {
        return new UsageEvent(UUID.randomUUID(), new TenantId(tenant), "API_REQUESTS", quantity, AT);
    }

    static Summary summary(String tenant) {
        Instant day = PeriodCalculator.periodOf(Window.fixed(Duration.ofDays(1)), ANCHOR, AT).start();
        return summaries.find(new TenantId(tenant), "API_REQUESTS", "86400", day).toCompletableFuture().join().orElse(null);
    }

    static long total(String tenant) {
        Summary s = summary(tenant);
        return s == null ? -1 : s.total();
    }

    static void eventually(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + PATIENCE.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(200);
        }
        fail("timed out waiting for: " + what);
    }

    // ---- the path -----------------------------------------------------------------------------------------

    @Test
    void aUsageEventTravelsFromTheOutboxThroughRabbitMqToTheLedger() throws Exception {
        sink.publish(usage("t-flow", 5)).toCompletableFuture().join();
        eventually("5 counted for t-flow", () -> total("t-flow") == 5);
    }

    @Test
    void theSameEventDeliveredTwiceIsCountedOnce() throws Exception {
        UsageEvent e = usage("t-dup", 7);
        sink.publish(e).toCompletableFuture().join();
        // the very same event again, through the sink: it must not collide in the outbox, and the ledger must not add it twice
        sink.publish(e).toCompletableFuture().join();
        eventually("both deliveries consumed", () -> outcomes.stream().filter(o -> o.reason().equals("duplicate")).count() >= 1);
        assertEquals(7, total("t-dup"));
    }

    @Test
    void aCompensationThatArrivesBeforeItsUsageWaitsInTheRetryLevelThenApplies() throws Exception {
        UsageEvent original = usage("t-order", 10);
        Compensation reversal = Compensation.of(original.eventId(), original.tenant(), "API_REQUESTS", 4, Instant.now(), "RESULT_REVERTED");
        sink.publish(reversal).toCompletableFuture().join(); // the reversal goes out first
        eventually("the inbox saw the reversal early", () -> outcomes.stream().anyMatch(o -> o.reason().equals("original_not_found")));

        sink.publish(original).toCompletableFuture().join();
        eventually("10 counted, then 4 reversed", () -> total("t-order") == 6);
        Summary s = summary("t-order");
        assertEquals(6, s.total());
    }

    @Test
    void aMessageThatCanNeverBeCountedEndsInTheFailedQueueWithoutBlockingTheRest() throws Exception {
        transaction.executeWithoutResult(status -> writer.enqueue(new OutboxMessage(UUID.randomUUID(), "tenant", "t-poison", null,
                OutboxUsageSink.USAGE_TYPE, "{\"eventType\":\"USAGE\"}", Map.of(), TOPIC, Instant.now()))); // valid JSON, no tenant or metric
        sink.publish(usage("t-poison", 2)).toCompletableFuture().join(); // right behind it

        eventually("the good message behind the poison one was counted", () -> total("t-poison") == 2);
        eventually("the poison message reached the failed queue", () -> {
            try (Channel channel = rabbit.createChannel()) {
                GetResponse response = channel.basicGet("usage-failed", true);
                return response != null && new String(response.getBody()).contains("USAGE");
            } catch (Exception e) {
                return false;
            }
        });
    }

    @Test
    void usageWrittenWhileTheBrokerIsDownIsNotLostAndArrivesWhenItReturns() throws Exception {
        var docker = RABBIT.getDockerClient();
        docker.pauseContainerCmd(RABBIT.getContainerId()).exec(); // the broker stops answering
        try {
            for (int i = 0; i < 3; i++) sink.publish(usage("t-outage", 2)).toCompletableFuture().join(); // the outbox only needs PostgreSQL
            Thread.sleep(2_000);
            assertEquals(-1, total("t-outage")); // nothing could be delivered meanwhile
        } finally {
            docker.unpauseContainerCmd(RABBIT.getContainerId()).exec();
        }
        eventually("all three counted after the broker came back", () -> total("t-outage") == 6);
    }
}
