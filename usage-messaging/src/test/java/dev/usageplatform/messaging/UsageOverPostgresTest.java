package dev.usageplatform.messaging;

import com.example.outbox.core.OutboxConfig;
import com.example.outbox.spring.JdbcOutboxWriter;
import dev.usageplatform.consumer.UsageEventConsumer;
import dev.usageplatform.consumer.UsageEventConsumer.Disposition;
import dev.usageplatform.consumer.UsageEventJson;
import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.UsageEngine.FailMode;
import dev.usageplatform.core.memory.InMemoryCounterStore;
import dev.usageplatform.core.model.TenantId;
import dev.usageplatform.core.model.UsageEvent;
import dev.usageplatform.postgres.JdbcLedger;
import dev.usageplatform.postgres.JdbcPolicyStore;
import dev.usageplatform.postgres.JdbcSubscriptions;
import dev.usageplatform.postgres.UsageSchema;
import dev.usageplatform.processor.QueueUsageAdapter;
import dev.usageplatform.processor.QueueUsageAdapter.Done;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two behaviours that only a real PostgreSQL can confirm: a processor's result and its usage event share one
 * transaction (a rollback takes both), and the database's own refusals (SQLSTATE class 22; class 23 shares the prefix check, but no input reaches a constraint the schema enforces) are dead-lettered
 * instead of retried forever.
 */
@Testcontainers
class UsageOverPostgresTest {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static final Instant ANCHOR = Instant.parse("2020-01-01T00:00:00Z");
    static DataSource ds;
    static TransactionTemplate transaction;
    static JdbcOutboxWriter writer;
    static UsageEngine engine;
    static UsageEventConsumer consumer;

    @BeforeAll
    static void start() throws Exception {
        PGSimpleDataSource pg = new PGSimpleDataSource();
        pg.setUrl(POSTGRES.getJdbcUrl());
        pg.setUser(POSTGRES.getUsername());
        pg.setPassword(POSTGRES.getPassword());
        ds = pg;
        Flyway.configure().dataSource(ds).locations("classpath:db/outbox/postgresql").table("outbox_schema_history").load().migrate();
        UsageSchema.migrate(ds, "usage");
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.execute("""
                    INSERT INTO usage.metric (id, code, unit, count_trigger) VALUES (gen_random_uuid(), 'ITEMS_PROCESSED', 'ITEM', 'ON_FINAL_STATUS');
                    INSERT INTO usage.plan (id, code, name, version) VALUES (gen_random_uuid(), 'P', 'P', 1);
                    INSERT INTO usage.plan_limit (id, plan_id, metric_id, limit_type, limit_value, window_seconds, enforcement)
                      SELECT gen_random_uuid(), p.id, m.id, 'QUOTA', 1000000, 86400, 'REJECT'
                        FROM usage.plan p, usage.metric m WHERE p.code = 'P' AND m.code = 'ITEMS_PROCESSED';
                    INSERT INTO usage.tenant (id, name) VALUES ('acme', 'acme');
                    CREATE TABLE results (item TEXT PRIMARY KEY);
                    """);
        }
        new JdbcSubscriptions(ds, "usage").subscribe(new TenantId("acme"), "P", 1, ANCHOR);

        var executor = Executors.newVirtualThreadPerTaskExecutor();
        Clock clock = Clock.systemUTC();
        engine = new UsageEngine(new JdbcPolicyStore(ds, executor, clock, "usage"), new InMemoryCounterStore(clock),
                new JdbcLedger(ds, executor, "usage"), m -> CompletableFuture.completedFuture(null), clock, FailMode.CLOSED, Duration.ofMinutes(15));
        consumer = new UsageEventConsumer(engine, Set.of("ITEMS_PROCESSED")::contains, clock, Duration.ofMinutes(5), Duration.ofDays(90));
        writer = new JdbcOutboxWriter(OutboxConfig.defaults(), new SimpleMeterRegistry(), JdbcClient.create(ds), payload -> {
            throw new UnsupportedOperationException("the sink writes ready JSON");
        });
        transaction = new TransactionTemplate(new DataSourceTransactionManager(ds));
    }

    static long count(String sql) throws Exception {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement(); var rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** The processor's persistence: the result row and the usage event, in the caller's transaction. */
    static QueueUsageAdapter.Persist<String> persist(boolean failAfterWriting) {
        var sink = OutboxUsageSink.sameTransaction(writer, "usage-events");
        return (item, usage) -> {
            try {
                transaction.executeWithoutResult(status -> {
                    JdbcClient.create(ds).sql("INSERT INTO results (item) VALUES (?)").param(item).update();
                    usage.ifPresent(e -> sink.publish(e).toCompletableFuture().join());
                    if (failAfterWriting) throw new IllegalStateException("the commit fails");
                });
                return CompletableFuture.completedFuture(null);
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        };
    }

    static CompletionStage<Done<String>> finished(String item) {
        return CompletableFuture.completedFuture(new Done<>(item, "COMPLETED", Instant.now()));
    }

    @Test
    void aRollbackTakesTheResultAndItsUsageEventTogether() throws Exception {
        var adapter = new QueueUsageAdapter(engine, "ITEMS_PROCESSED", Set.of("COMPLETED"), false, "x-tenant-id", Runnable::run);
        Map<String, String> headers = Map.of("x-tenant-id", "acme");

        var failed = adapter.<String>handle(headers, "doc-1", () -> finished("doc-1"), persist(true)).toCompletableFuture();
        assertThrows(java.util.concurrent.CompletionException.class, failed::join);
        assertEquals(0, count("SELECT count(*) FROM results"), "the result rolled back");
        assertEquals(0, count("SELECT count(*) FROM outbox_message"), "and so did the usage event: nothing to bill for work that did not stick");

        // the broker redelivers; this time it commits, and both are there, once
        var ok = adapter.<String>handle(headers, "doc-1", () -> finished("doc-1"), persist(false)).toCompletableFuture().join();
        assertEquals(QueueUsageAdapter.Verdict.COMPLETED, ok.verdict());
        assertEquals(1, count("SELECT count(*) FROM results"));
        assertEquals(1, count("SELECT count(*) FROM outbox_message"));
    }

    static UsageEvent event(String tenant, String metric) {
        return new UsageEvent(UUID.randomUUID(), new TenantId(tenant), metric, 1, Instant.now());
    }

    @Test
    void textPostgresCannotStoreIsDeadLetteredNotRetried() {
        // a NUL character in the tenant id: SQLSTATE 22021, class 22
        var outcome = consumer.handle(UsageEventJson.toJson(event("ac\u0000me", "ITEMS_PROCESSED"))).toCompletableFuture().join();
        assertEquals(Disposition.DEAD_LETTER, outcome.disposition());
    }

    @Test
    void aDatabaseOutageIsStillARetry() {
        // contrast: a connection failure is not a data problem and must keep being retried
        PGSimpleDataSource dead = new PGSimpleDataSource();
        dead.setUrl("jdbc:postgresql://localhost:1/none");
        var ex = Executors.newVirtualThreadPerTaskExecutor();
        var down = new UsageEngine(new JdbcPolicyStore(ds, ex, Clock.systemUTC(), "usage"), new InMemoryCounterStore(Clock.systemUTC()),
                new JdbcLedger(dead, ex, "usage"), m -> CompletableFuture.completedFuture(null), Clock.systemUTC(), FailMode.CLOSED, Duration.ofMinutes(15));
        var c = new UsageEventConsumer(down, Set.of("ITEMS_PROCESSED")::contains, Clock.systemUTC(), Duration.ofMinutes(5), Duration.ofDays(90));
        assertEquals(Disposition.RETRY, c.handle(UsageEventJson.toJson(event("acme", "ITEMS_PROCESSED"))).toCompletableFuture().join().disposition());
    }
}
