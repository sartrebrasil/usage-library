package dev.usageplatform.spring;

import com.example.messaging.inbox.InboxResultHandler;
import com.example.messaging.rabbitmq.RabbitMessageSender;
import com.example.outbox.broker.messaging.MessageSenderPublisher;
import com.example.outbox.core.MessagePublisher;
import com.example.outbox.core.OutboxWriter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import dev.usageplatform.core.model.PeriodCalculator;
import dev.usageplatform.core.model.TenantId;
import dev.usageplatform.core.model.UsageEvent;
import dev.usageplatform.core.model.Window;
import dev.usageplatform.core.spi.SummaryStore;
import dev.usageplatform.core.spi.UsageSink;
import dev.usageplatform.messaging.OutboxUsageSink;
import dev.usageplatform.messaging.UsageInboxHandler;
import dev.usageplatform.postgres.JdbcSubscriptions;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.json.JsonMapper;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The starter wired by properties alone over the real messaging and outbox starters: a usage event written through the
 * auto-configured sink travels outbox, relay, RabbitMQ, messaging-inbox and reaches the ledger through the auto-registered
 * handler. This is also what proves the handler is found by the inbox under the receiver's name.
 */
@Testcontainers
class StarterBrokerTest {
    @Container
    static final GenericContainer<?> RABBIT = new GenericContainer<>("rabbitmq:4.1-management-alpine")
            .withExposedPorts(5672)
            .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1));

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static final Instant ANCHOR = Instant.parse("2020-01-01T00:00:00Z");

    static DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        return ds;
    }

    /** What the host application declares; everything else comes from the starters. */
    @Configuration(proxyBeanMethods = false)
    static class HostBeans {
        @Bean DataSource dataSource() { return StarterBrokerTest.dataSource(); }

        @Bean DataSourceTransactionManager transactionManager(DataSource ds) { return new DataSourceTransactionManager(ds); }

        @Bean JsonMapper jsonMapper() { return JsonMapper.builder().build(); }

        @Bean ConnectionFactory rabbitConnectionFactory() {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(RABBIT.getHost());
            factory.setPort(RABBIT.getMappedPort(5672));
            return factory;
        }

        /** The outbox relay publishes destination "usage-events" through this publisher. */
        @Bean("usage-events")
        MessagePublisher usageEventsPublisher(ConnectionFactory factory) throws Exception {
            return new MessageSenderPublisher(RabbitMessageSender.forQueue(factory.newConnection("usage-publisher"), "usage-events",
                    RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES));
        }
    }

    static void declareQueues() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT.getHost());
        factory.setPort(RABBIT.getMappedPort(5672));
        try (var connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            for (String queue : List.of("usage-events", "usage-failed", "usage-dlq")) {
                channel.queueDeclare(queue, true, false, false, Map.of("x-queue-type", "quorum"));
            }
            channel.queueDeclare("usage-retry", true, false, false, Map.of("x-queue-type", "quorum",
                    "x-message-ttl", 5000, "x-dead-letter-exchange", "", "x-dead-letter-routing-key", "usage-events"));
        }
    }

    static void eventually(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(200);
        }
        fail("timed out waiting for: " + what);
    }

    @Test
    void theStarterWiresTheWholePathFromPropertiesAlone() throws Exception {
        declareQueues();
        Flyway.configure().dataSource(dataSource()).locations("classpath:db/outbox/postgresql").table("outbox_schema_history").load().migrate();

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class,
                        com.example.outbox.spring.OutboxAutoConfiguration.class,
                        com.example.messaging.spring.MessagingAutoConfiguration.class,
                        com.example.messaging.inbox.spring.InboxAutoConfiguration.class))
                .withUserConfiguration(HostBeans.class)
                .withPropertyValues(
                        "usage.counters.type=memory", "usage.postgres.migrate=true",
                        "usage.consumer.enabled=true", "usage.consumer.known-metrics=API_REQUESTS",
                        "usage.messaging.outbox.destination=usage-events",
                        "usage.messaging.inbox.enabled=true", "usage.messaging.inbox.original-not-found-delay=1s",
                        "messaging.providers.rabbit.type=rabbitmq",
                        "messaging.receivers.usage-events.destination=usage-events",
                        "messaging.receivers.usage-events.failed=usage-failed",
                        "messaging.receivers.usage-events.dead-letter=usage-dlq",
                        "messaging.receivers.usage-events.max-messages-per-poll=10",
                        "messaging.receivers.usage-events.poll-wait=1s",
                        "messaging.receivers.usage-events.retry.delayed.strategy=retry-queues",
                        "messaging.receivers.usage-events.retry.delayed.max-attempts=5",
                        "messaging.receivers.usage-events.retry.delayed.queues[0].delay=5s",
                        "messaging.receivers.usage-events.retry.delayed.queues[0].destination=usage-retry")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    // the starter built both ends from properties
                    assertThat(ctx.getBean(UsageSink.class)).isInstanceOf(OutboxUsageSink.class);
                    assertThat(ctx.getBean("usageEvents")).isInstanceOf(UsageInboxHandler.class);
                    assertThat(ctx.getBeansOfType(InboxResultHandler.class)).hasSize(1);

                    try (Connection c = dataSource().getConnection(); Statement s = c.createStatement()) {
                        s.execute("""
                                INSERT INTO usage.tenant (id, name) VALUES ('wired', 'Wired');
                                INSERT INTO usage.metric (id, code, unit, count_trigger) VALUES (gen_random_uuid(), 'API_REQUESTS', 'REQUEST', 'ON_REQUEST');
                                INSERT INTO usage.plan (id, code, name, version) VALUES (gen_random_uuid(), 'WIRED', 'Wired', 1);
                                INSERT INTO usage.plan_limit (id, plan_id, metric_id, limit_type, limit_value, window_seconds, enforcement)
                                  SELECT gen_random_uuid(), p.id, m.id, 'QUOTA', 1000000, 86400, 'REJECT'
                                    FROM usage.plan p, usage.metric m WHERE p.code = 'WIRED' AND m.code = 'API_REQUESTS';
                                """);
                    }
                    TenantId tenant = new TenantId("wired");
                    ctx.getBean(JdbcSubscriptions.class).subscribe(tenant, "WIRED", 1, ANCHOR);

                    Instant at = Instant.now();
                    ctx.getBean(UsageSink.class).publish(new UsageEvent(UUID.randomUUID(), tenant, "API_REQUESTS", 9, at))
                            .toCompletableFuture().join();

                    Instant day = PeriodCalculator.periodOf(Window.fixed(Duration.ofDays(1)), ANCHOR, at).start();
                    SummaryStore summaries = ctx.getBean(SummaryStore.class);
                    eventually("9 counted through outbox, RabbitMQ and the inbox handler the starter registered", () ->
                            summaries.find(tenant, "API_REQUESTS", "86400", day).toCompletableFuture().join()
                                    .map(s -> s.total() == 9).orElse(false));
                });
    }

    // ---- configuration mistakes fail at startup, with the property to fix ------------------------------------

    static final ApplicationContextRunner LIGHT = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
            .withUserConfiguration(UsageAutoConfigurationTest.HostStores.class)
            .withPropertyValues("usage.counters.type=memory");

    @Test
    void theInboxHandlerNeedsTheConsumer() {
        LIGHT.withPropertyValues("usage.messaging.inbox.enabled=true")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage.consumer.enabled=true"));
    }

    static OutboxWriter stubWriter() {
        return new OutboxWriter() {
            @Override public void enqueue(com.example.outbox.core.OutboxMessage message) { }
            @Override public void enqueue(String a, String b, Long c, String d, Object e, String f) { }
        };
    }

    static final org.springframework.boot.test.context.runner.ApplicationContextRunner GATEWAY_APP = LIGHT
            .withBean(OutboxWriter.class, StarterBrokerTest::stubWriter)
            .withBean(dev.usageplatform.core.spi.TenantResolver.class, () -> (dev.usageplatform.core.spi.TenantResolver<org.springframework.web.server.ServerWebExchange>)
                    ex -> java.util.concurrent.CompletableFuture.completedFuture(java.util.Optional.of(new TenantId("acme"))))
            .withPropertyValues("usage.messaging.outbox.destination=usage-events", "usage.gateway.enabled=true",
                    "usage.gateway.metrics[0].code=API_REQUESTS");

    @Test
    void theOutboxSinkInItsOwnTransactionNeedsATransactionManager() {
        LIGHT.withBean(OutboxWriter.class, StarterBrokerTest::stubWriter)
                .withPropertyValues("usage.messaging.outbox.destination=usage-events")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("PlatformTransactionManager"));
        // in the caller's transaction there is nothing to open, so none is needed
        LIGHT.withBean(OutboxWriter.class, StarterBrokerTest::stubWriter)
                .withPropertyValues("usage.messaging.outbox.destination=usage-events", "usage.messaging.outbox.transaction=CALLER")
                .run(ctx -> assertThat(ctx).hasNotFailed().getBean(UsageSink.class).isInstanceOf(OutboxUsageSink.class));
    }

    @Test
    void theGatewayCannotUseTheCallersTransactionBecauseItHasNone() {
        GATEWAY_APP.withPropertyValues("usage.messaging.outbox.transaction=CALLER")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("cannot be used with usage.gateway.enabled=true"));
    }

    @Test
    void theTransactionModeFollowsTheRoleOfTheApplicationWhenNotSet() {
        // a processor joins the transaction of its Persist: no transaction manager needed, nothing to configure
        LIGHT.withBean(OutboxWriter.class, StarterBrokerTest::stubWriter)
                .withPropertyValues("usage.messaging.outbox.destination=usage-events",
                        "usage.processor.metric=ITEMS_PROCESSED", "usage.processor.countable-statuses=COMPLETED")
                .run(ctx -> assertThat(ctx).hasNotFailed().hasSingleBean(UsageSink.class));
        // a gateway that is also a processor has two answers, so it must pick
        GATEWAY_APP.withPropertyValues("usage.processor.metric=ITEMS_PROCESSED", "usage.processor.countable-statuses=COMPLETED")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage.messaging.outbox.transaction must be set"));
    }

    @Test
    void aPropertyThatNeedsAMissingJarFailsTheStartupInsteadOfBeingIgnored() {
        var hostBeans = LIGHT.withUserConfiguration(UsageAutoConfigurationTest.HostTenants.class);
        hostBeans.withClassLoader(new org.springframework.boot.test.context.FilteredClassLoader(dev.usageplatform.gateway.UsageGatewayFilter.class))
                .withPropertyValues("usage.gateway.enabled=true", "usage.gateway.metrics[0].code=API_REQUESTS")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage-gateway jar"));
        hostBeans.withClassLoader(new org.springframework.boot.test.context.FilteredClassLoader(dev.usageplatform.consumer.UsageEventConsumer.class))
                .withPropertyValues("usage.consumer.enabled=true", "usage.consumer.known-metrics=API_REQUESTS")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage-consumer jar"));
        hostBeans.withClassLoader(new org.springframework.boot.test.context.FilteredClassLoader(dev.usageplatform.processor.QueueUsageAdapter.class))
                .withPropertyValues("usage.processor.metric=ITEMS_PROCESSED", "usage.processor.countable-statuses=COMPLETED")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage-processor jar"));
        hostBeans.withClassLoader(new org.springframework.boot.test.context.FilteredClassLoader(OutboxUsageSink.class))
                .withPropertyValues("usage.messaging.outbox.destination=usage-events")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage-messaging jar"));
        hostBeans.withClassLoader(new org.springframework.boot.test.context.FilteredClassLoader(UsageInboxHandler.class))
                .withPropertyValues("usage.messaging.inbox.enabled=true")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage-messaging jar"));
    }

    @Test
    void aSinkOfTheHostReplacesTheOutboxOne() {
        UsageSink mine = message -> java.util.concurrent.CompletableFuture.completedFuture(null);
        LIGHT.withBean(UsageSink.class, () -> mine)
                .withBean(OutboxWriter.class, () -> new OutboxWriter() {
                    @Override public void enqueue(com.example.outbox.core.OutboxMessage message) { }
                    @Override public void enqueue(String a, String b, Long c, String d, Object e, String f) { }
                })
                .withPropertyValues("usage.messaging.outbox.destination=usage-events", "usage.messaging.outbox.transaction=CALLER")
                .run(ctx -> assertThat(ctx).hasSingleBean(UsageSink.class).getBean(UsageSink.class).isSameAs(mine));
    }
}
