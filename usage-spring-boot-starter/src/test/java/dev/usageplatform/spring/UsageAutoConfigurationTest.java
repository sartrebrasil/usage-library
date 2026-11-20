package dev.usageplatform.spring;

import dev.usageplatform.consumer.UsageEventConsumer;
import dev.usageplatform.core.CounterStore;
import dev.usageplatform.core.InMemoryCounterStore;
import dev.usageplatform.core.InMemoryJobLock;
import dev.usageplatform.core.InMemoryLedger;
import dev.usageplatform.core.JobLock;
import dev.usageplatform.core.Limit;
import dev.usageplatform.core.Limit.Enforcement;
import dev.usageplatform.core.Limit.Type;
import dev.usageplatform.core.MaintenanceScheduler;
import dev.usageplatform.core.Metric;
import dev.usageplatform.core.Metric.Mode;
import dev.usageplatform.core.Policy;
import dev.usageplatform.core.PolicyStore;
import dev.usageplatform.core.TenantId;
import dev.usageplatform.core.TenantResolver;
import dev.usageplatform.core.UsageSink;
import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.gateway.UsageGatewayFilter;
import dev.usageplatform.processor.QueueUsageAdapter;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UsageAutoConfigurationTest {
    static final Metric API = new Metric("API_REQUESTS", Mode.CONSUME_ON_CHECK);

    /** What a real application supplies besides the starter: here, in-memory stand-ins and a quota of 2 per day. */
    @Configuration(proxyBeanMethods = false)
    static class HostStores {
        @Bean PolicyStore policies() {
            Policy policy = new Policy(Instant.parse("2020-01-01T00:00:00Z"),
                    List.of(new Limit(API, Type.QUOTA, 2, Duration.ofDays(1), Enforcement.REJECT)));
            return tenant -> CompletableFuture.completedFuture(Optional.of(policy));
        }

        @Bean InMemoryLedger ledger() { return new InMemoryLedger(); } // is both the Ledger and the SummaryStore

        @Bean JobLock jobLock() { return new InMemoryJobLock(); }
    }

    @Configuration(proxyBeanMethods = false)
    static class HostTenants {
        @Bean TenantResolver<ServerWebExchange> tenants() {
            return ex -> CompletableFuture.completedFuture(Optional.of(new TenantId("acme")));
        }

        @Bean UsageSink sink() { return message -> CompletableFuture.completedFuture(null); }
    }

    final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
            .withUserConfiguration(HostStores.class)
            .withPropertyValues("usage.counters.type=memory");

    @Test
    void theEngineIsWiredFromTheHostsStoresAndEnforcesQuotas() {
        runner.run(ctx -> {
            UsageEngine engine = ctx.getBean(UsageEngine.class);
            TenantId t = new TenantId("acme");
            assertThat(engine.check(t, "API_REQUESTS", 1).toCompletableFuture().join().allowed()).isTrue();
            assertThat(engine.check(t, "API_REQUESTS", 1).toCompletableFuture().join().allowed()).isTrue();
            assertThat(engine.check(t, "API_REQUESTS", 1).toCompletableFuture().join().reason()).isEqualTo("quota_exceeded");
            assertThat(ctx).hasSingleBean(CounterStore.class);
            assertThat(ctx.getBean(CounterStore.class)).isInstanceOf(InMemoryCounterStore.class);
        });
    }

    @Test
    void failModeComesFromPropertiesAndReachesTheEngine() {
        CounterStore down = new CounterStore() {
            public java.util.concurrent.CompletionStage<Result> add(String k, long d, long c, Duration t) { return CompletableFuture.failedFuture(new IllegalStateException("down")); }
            public java.util.concurrent.CompletionStage<Long> get(String k) { return CompletableFuture.failedFuture(new IllegalStateException("down")); }
        };
        TenantId t = new TenantId("acme");
        runner.withBean(CounterStore.class, () -> down).withPropertyValues("usage.fail-mode=CLOSED").run(ctx -> {
            var d = ctx.getBean(UsageEngine.class).check(t, "API_REQUESTS", 1).toCompletableFuture().join();
            assertThat(d.allowed()).isFalse();
            assertThat(d.reason()).isEqualTo("store_unavailable");
        });
        runner.withBean(CounterStore.class, () -> down).withPropertyValues("usage.fail-mode=OPEN").run(ctx ->
                assertThat(ctx.getBean(UsageEngine.class).check(t, "API_REQUESTS", 1).toCompletableFuture().join().allowed()).isTrue());
    }

    @Test
    void concurrencyLeaseComesFromPropertiesAndReachesTheEngine() {
        var now = new java.util.concurrent.atomic.AtomicReference<>(Instant.parse("2026-11-19T12:00:00Z"));
        java.time.Clock moving = new java.time.Clock() {
            public java.time.ZoneId getZone() { return java.time.ZoneOffset.UTC; }
            public java.time.Clock withZone(java.time.ZoneId z) { return this; }
            public Instant instant() { return now.get(); }
        };
        Metric items = new Metric("ITEMS_PROCESSED", Mode.REPORT_ONLY);
        PolicyStore oneAtATime = tenant -> CompletableFuture.completedFuture(Optional.of(
                new Policy(Instant.parse("2020-01-01T00:00:00Z"), List.of(Limit.concurrency(items, 1)))));
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
                .withBean(PolicyStore.class, () -> oneAtATime).withBean(InMemoryLedger.class, InMemoryLedger::new)
                .withBean(java.time.Clock.class, () -> moving)
                .withPropertyValues("usage.counters.type=memory", "usage.concurrency-lease=2m")
                .run(ctx -> {
                    UsageEngine engine = ctx.getBean(UsageEngine.class);
                    TenantId t = new TenantId("acme");
                    assertThat(engine.acquire(t, "ITEMS_PROCESSED", 1).toCompletableFuture().join().decision().allowed()).isTrue();
                    now.set(Instant.parse("2026-11-19T12:01:50Z")); // 1m50 later: inside the 2m lease, the slot is still taken
                    assertThat(engine.acquire(t, "ITEMS_PROCESSED", 1).toCompletableFuture().join().decision().allowed()).isFalse();
                    now.set(Instant.parse("2026-11-19T12:02:10Z")); // past 2m: the holder that never released is forgotten
                    assertThat(engine.acquire(t, "ITEMS_PROCESSED", 1).toCompletableFuture().join().decision().allowed()).isTrue();
                });
    }

    @Test
    void withoutAPolicyStoreAndLedgerTheContextFailsAndSaysWhatIsMissing() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
                .withPropertyValues("usage.counters.type=memory")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasMessageContaining("PolicyStore"));
    }

    @Test
    void redisCountersNeedAnUri() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
                .withUserConfiguration(HostStores.class)
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage.redis.uri is required"));
    }

    @Test
    void anEngineDefinedByTheHostReplacesTheDefault() {
        UsageEngine mine = new UsageEngine(t -> CompletableFuture.completedFuture(Optional.empty()), new InMemoryCounterStore(java.time.Clock.systemUTC()),
                new InMemoryLedger(), e -> CompletableFuture.completedFuture(null), java.time.Clock.systemUTC(), UsageEngine.FailMode.OPEN, Duration.ofMinutes(1));
        runner.withBean(UsageEngine.class, () -> mine)
                .run(ctx -> assertThat(ctx).hasSingleBean(UsageEngine.class).getBean(UsageEngine.class).isSameAs(mine));
    }

    @Test
    void theGatewayFilterIsOffUnlessEnabledAndNeedsMetrics() {
        runner.withUserConfiguration(HostTenants.class)
                .run(ctx -> assertThat(ctx).doesNotHaveBean(UsageGatewayFilter.class));
        runner.withUserConfiguration(HostTenants.class).withPropertyValues("usage.gateway.enabled=true")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage.gateway.metrics must list at least one metric"));
    }

    @Test
    void theGatewayFilterReadsItsBatchCostFromTheConfiguredHeader() {
        runner.withUserConfiguration(HostTenants.class)
                .withPropertyValues("usage.gateway.enabled=true",
                        "usage.gateway.metrics[0].code=API_REQUESTS", "usage.gateway.metrics[0].cost-header=X-Item-Count")
                .run(ctx -> {
                    UsageGatewayFilter filter = ctx.getBean(UsageGatewayFilter.class);
                    var tooBig = MockServerWebExchange.from(MockServerHttpRequest.get("/x").header("X-Item-Count", "3").build());
                    filter.filter(tooBig, ex -> Mono.empty()).block();
                    assertEquals(HttpStatus.TOO_MANY_REQUESTS, tooBig.getResponse().getStatusCode()); // 3 > the quota of 2

                    var fits = MockServerWebExchange.from(MockServerHttpRequest.get("/x").header("X-Item-Count", "2").build());
                    var reached = new boolean[1];
                    filter.filter(fits, ex -> { reached[0] = true; return Mono.empty(); }).block();
                    assertThat(reached[0]).isTrue();

                    // a malformed, repeated or absurd cost is refused with 400, never counted as 1
                    for (var bad : List.of(
                            MockServerHttpRequest.get("/x").header("X-Item-Count", "many").build(),
                            MockServerHttpRequest.get("/x").header("X-Item-Count", "1", "1").build(),
                            MockServerHttpRequest.get("/x").header("X-Item-Count", "0").build(),
                            MockServerHttpRequest.get("/x").header("X-Item-Count", String.valueOf(Long.MAX_VALUE)).build())) {
                        var refused = MockServerWebExchange.from(bad);
                        var forwarded = new boolean[1];
                        filter.filter(refused, ex -> { forwarded[0] = true; return Mono.empty(); }).block();
                        assertEquals(HttpStatus.BAD_REQUEST, refused.getResponse().getStatusCode());
                        assertThat(forwarded[0]).isFalse();
                    }
                });
    }

    @Test
    void theGatewayRefusesToStartWithoutADurableSink() {
        runner.withUserConfiguration(HostTenants.class)
                .withBean("sink2", UsageSink.class, () -> message -> CompletableFuture.completedFuture(null)) // two sinks: ambiguous
                .withPropertyValues("usage.gateway.enabled=true", "usage.gateway.metrics[0].code=API_REQUESTS")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("needs exactly one UsageSink"));
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
                .withUserConfiguration(HostStores.class)
                .withBean(TenantResolver.class, () -> (TenantResolver<ServerWebExchange>) ex -> CompletableFuture.completedFuture(Optional.of(new TenantId("acme"))))
                .withPropertyValues("usage.counters.type=memory", "usage.gateway.enabled=true", "usage.gateway.metrics[0].code=API_REQUESTS")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("needs exactly one UsageSink"));
        // an application that only consumes events never publishes, so it does not need a sink
        runner.withPropertyValues("usage.consumer.enabled=true", "usage.consumer.known-metrics=API_REQUESTS")
                .run(ctx -> assertThat(ctx).hasNotFailed());
    }

    @Test
    void aCounterStoreOfTheHostMeansNoRedisClientAndNoUri() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
                .withUserConfiguration(HostStores.class)
                .withBean(CounterStore.class, () -> new InMemoryCounterStore(java.time.Clock.systemUTC())) // default properties: type=redis, no uri
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed().hasSingleBean(UsageEngine.class);
                    assertThat(ctx).doesNotHaveBean(io.lettuce.core.RedisClient.class);
                });
    }

    @Test
    void theProcessorAdapterIsBuiltFromProperties() {
        runner.withPropertyValues("usage.processor.metric=ITEMS_PROCESSED", "usage.processor.countable-statuses=COMPLETED,APPROVED")
                .run(ctx -> assertThat(ctx).hasSingleBean(QueueUsageAdapter.class));
        runner.withPropertyValues("usage.processor.metric=ITEMS_PROCESSED")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("countable-statuses"));
        runner.run(ctx -> assertThat(ctx).doesNotHaveBean(QueueUsageAdapter.class));
    }

    @Test
    void theCentralConsumerNeedsItsKnownMetrics() {
        runner.withPropertyValues("usage.consumer.enabled=true", "usage.consumer.known-metrics=API_REQUESTS,ITEMS_PROCESSED")
                .run(ctx -> assertThat(ctx).hasSingleBean(UsageEventConsumer.class));
        runner.withPropertyValues("usage.consumer.enabled=true")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("known-metrics"));
    }

    @Test
    void maintenanceStartsWithTheContextAndStopsWithIt() {
        var lifecycle = new java.util.concurrent.atomic.AtomicReference<SmartLifecycle>();
        runner.withPropertyValues("usage.maintenance.enabled=true", "usage.maintenance.initial-delay=1h")
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(MaintenanceScheduler.class);
                    lifecycle.set(ctx.getBean("usageMaintenanceLifecycle", SmartLifecycle.class));
                    assertThat(lifecycle.get().isRunning()).isTrue();
                });
        assertThat(lifecycle.get().isRunning()).isFalse(); // the context is closed now: the scheduler was stopped with it
        runner.run(ctx -> assertThat(ctx).doesNotHaveBean(MaintenanceScheduler.class));
    }

    @Test
    void nonsenseDurationsAndStatusesAreRefusedAtStartupWithTheNameOfTheProperty() {
        for (String[] bad : new String[][]{
                {"usage.maintenance.interval=0s", "usage.maintenance.interval"}, {"usage.maintenance.settle=-1m", "usage.maintenance.settle"},
                {"usage.redis.command-timeout=0s", "usage.redis.command-timeout"}, {"usage.concurrency-lease=0s", "usage.concurrency-lease"},
                {"usage.policy-cache-ttl=-1s", "usage.policy-cache-ttl"}, {"usage.consumer.max-past=-1d", "usage.consumer.max-past"},
                {"usage.gateway.refund-from-status=50", "usage.gateway.refund-from-status"}}) {
            runner.withPropertyValues(bad[0]).run(ctx -> assertThat(ctx).as(bad[0]).hasFailed().getFailure().hasStackTraceContaining(bad[1]));
        }
    }

    @Test
    void theStarterWorksWithoutAnyOptionalModuleOnTheClasspath() {
        // that the filter really hides these classes is proved where a property needs one: StarterBrokerTest, missing-jar failure
        runner.withClassLoader(new org.springframework.boot.test.context.FilteredClassLoader(
                        dev.usageplatform.gateway.UsageGatewayFilter.class, dev.usageplatform.consumer.UsageEventConsumer.class,
                        dev.usageplatform.processor.QueueUsageAdapter.class, dev.usageplatform.redis.RedisCounterStore.class,
                        dev.usageplatform.postgres.JdbcLedger.class))
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed().hasSingleBean(UsageEngine.class); // no NoClassDefFoundError from a missing jar
                    assertThat(ctx).doesNotHaveBean(UsageGatewayFilter.class).doesNotHaveBean(UsageEventConsumer.class);
                });
    }
}
