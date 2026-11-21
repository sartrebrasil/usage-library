package dev.usageplatform.spring;

import dev.usageplatform.consumer.UsageEventConsumer;
import dev.usageplatform.consumer.UsageEventConsumer.Disposition;
import dev.usageplatform.consumer.UsageEventJson;
import dev.usageplatform.core.MaintenanceScheduler;
import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.model.Decision;
import dev.usageplatform.core.model.PeriodCalculator;
import dev.usageplatform.core.model.TenantId;
import dev.usageplatform.core.model.UsageEvent;
import dev.usageplatform.core.model.Window;
import dev.usageplatform.core.spi.SummaryStore;
import dev.usageplatform.postgres.JdbcSubscriptions;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

/** The starter wired by properties only, against a real Redis and a real PostgreSQL. */
@Testcontainers
class StarterEndToEndTest {
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static <X> X join(CompletionStage<X> s) { return s.toCompletableFuture().join(); }

    static DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        return ds;
    }

    static void seed(DataSource ds) throws Exception {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.execute("""
                    INSERT INTO usage.tenant (id, name) VALUES ('acme', 'Acme');
                    INSERT INTO usage.metric (id, code, unit, count_trigger) VALUES (gen_random_uuid(), 'API_REQUESTS', 'REQUEST', 'ON_REQUEST');
                    INSERT INTO usage.plan (id, code, name, version) VALUES (gen_random_uuid(), 'E2E', 'End to end', 1);
                    INSERT INTO usage.plan_limit (id, plan_id, metric_id, limit_type, limit_value, window_seconds, enforcement)
                      SELECT gen_random_uuid(), p.id, m.id, 'QUOTA', 2, 86400, 'REJECT' FROM usage.plan p, usage.metric m
                       WHERE p.code = 'E2E' AND m.code = 'API_REQUESTS';
                    """);
        }
    }

    @Test
    void theStarterWiresRealRedisAndPostgresFromPropertiesAlone() {
        DataSource ds = dataSource();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
                .withBean(DataSource.class, () -> ds)
                .withPropertyValues(
                        "usage.redis.uri=redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                        "usage.postgres.migrate=true",
                        "usage.fail-mode=CLOSED",
                        "usage.consumer.enabled=true",
                        "usage.consumer.known-metrics=API_REQUESTS",
                        "usage.maintenance.enabled=true",
                        "usage.maintenance.initial-delay=1h")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    seedIfNeeded(ds); // the migrations ran when the context started
                    Instant anchor = Instant.parse("2020-01-01T00:00:00Z");
                    ctx.getBean(JdbcSubscriptions.class).subscribe(new TenantId("acme"), "E2E", 1, anchor);

                    // policy from PostgreSQL, counters in Redis: a quota of 2 per day
                    UsageEngine engine = ctx.getBean(UsageEngine.class);
                    TenantId acme = new TenantId("acme");
                    assertThat(join(engine.check(acme, "API_REQUESTS", 1)).allowed()).isTrue();
                    assertThat(join(engine.check(acme, "API_REQUESTS", 1)).allowed()).isTrue();
                    assertThat(join(engine.check(acme, "API_REQUESTS", 1)).reason()).isEqualTo("quota_exceeded");
                    assertThat(join(engine.check(new TenantId("stranger"), "API_REQUESTS", 1)).reason()).isEqualTo("no_policy");

                    // the central consumer persists to the ledger and the summary; a redelivery changes nothing
                    Instant now = Instant.now();
                    String message = UsageEventJson.toJson(new UsageEvent(java.util.UUID.randomUUID(), acme, "API_REQUESTS", 1, now));
                    UsageEventConsumer consumer = ctx.getBean(UsageEventConsumer.class);
                    assertThat(join(consumer.handle(message)).reason()).isEqualTo("applied");
                    assertThat(join(consumer.handle(message)).reason()).isEqualTo("duplicate");
                    assertThat(join(consumer.handle("not json")).disposition()).isEqualTo(Disposition.DEAD_LETTER);

                    Instant dayStart = PeriodCalculator.periodOf(Window.fixed(Duration.ofDays(1)), anchor, now).start();
                    SummaryStore.Summary summary = join(ctx.getBean(SummaryStore.class).find(acme, "API_REQUESTS", "86400", dayStart)).orElseThrow();
                    assertThat(summary.total()).isEqualTo(1);

                    // maintenance goes through the real advisory lock
                    assertThat(join(ctx.getBean(MaintenanceScheduler.class).runOnce()))
                            .isInstanceOf(MaintenanceScheduler.RunOutcome.Completed.class);
                });
    }

    @Test
    void theMigrationFlagIsTheOnlyThingThatCreatesTheSchema() throws Exception {
        DataSource ds = dataSource();
        String redis = "usage.redis.uri=redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
        for (boolean migrate : new boolean[]{false, true}) {
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
                    .withBean(DataSource.class, () -> ds)
                    .withPropertyValues(redis, "usage.postgres.migrate=" + migrate, "usage.postgres.schema=" + (migrate ? "flagon" : "flagoff"))
                    .run(ctx -> assertThat(ctx).hasNotFailed().hasSingleBean(UsageEngine.class));
        }
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            var rs = st.executeQuery("SELECT to_regclass('flagoff.usage_summary') IS NULL AS off_empty, to_regclass('flagon.usage_summary') IS NOT NULL AS on_created");
            rs.next();
            assertThat(rs.getBoolean("off_empty")).as("without the flag nothing is created").isTrue();
            assertThat(rs.getBoolean("on_created")).as("with the flag the migrations ran, in the schema asked for").isTrue();
        }
    }

    @Test
    void anInvalidSchemaNameIsRefusedAtStartup() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
                .withBean(DataSource.class, StarterEndToEndTest::dataSource)
                .withPropertyValues("usage.redis.uri=redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379), "usage.postgres.schema=Usage; DROP TABLE x")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("lowercase identifier"));
    }

    @Test
    void aStalledRedisMakesTheCheckFailFastInsteadOfHangingTheRequest() throws Exception {
        DataSource ds = dataSource();
        var ctxHolder = new java.util.concurrent.atomic.AtomicReference<org.springframework.context.ConfigurableApplicationContext>();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
                .withBean(DataSource.class, () -> ds)
                .withPropertyValues("usage.redis.uri=redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                        "usage.postgres.migrate=true", "usage.fail-mode=CLOSED", "usage.redis.command-timeout=400ms")
                .run(ctx -> {
                    seedIfNeeded(ds);
                    ctx.getBean(JdbcSubscriptions.class).subscribe(new TenantId("slowco"), "E2E", 1, Instant.parse("2020-01-01T00:00:00Z"));
                    UsageEngine engine = ctx.getBean(UsageEngine.class);
                    TenantId t = new TenantId("slowco");
                    assertThat(join(engine.check(t, "API_REQUESTS", 1)).allowed()).isTrue(); // healthy, and the policy is cached now

                    var docker = REDIS.getDockerClient();
                    docker.pauseContainerCmd(REDIS.getContainerId()).exec(); // Redis stops answering but the socket stays open
                    try {
                        long started = System.nanoTime();
                        Decision d = join(engine.check(t, "API_REQUESTS", 1));
                        long millis = java.time.Duration.ofNanos(System.nanoTime() - started).toMillis();
                        assertThat(d.allowed()).isFalse();
                        assertThat(d.reason()).isEqualTo("store_unavailable");
                        assertThat(millis).as("the request must not wait for Lettuce's 60 s default").isLessThan(5_000);
                    } finally {
                        docker.unpauseContainerCmd(REDIS.getContainerId()).exec();
                    }
                });
    }

    private static void seedIfNeeded(DataSource ds) throws Exception {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            var rs = s.executeQuery("SELECT count(*) FROM usage.metric WHERE code = 'API_REQUESTS'");
            rs.next();
            if (rs.getLong(1) == 0) seed(ds);
            s.execute("INSERT INTO usage.tenant (id, name) VALUES ('slowco', 'Slow Co') ON CONFLICT DO NOTHING");
        }
    }
}
