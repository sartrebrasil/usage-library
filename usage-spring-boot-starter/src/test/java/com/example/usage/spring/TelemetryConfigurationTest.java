package com.example.usage.spring;

import com.example.usage.core.MaintenanceScheduler;
import com.example.usage.core.UsageEngine;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.model.UsageEvent;
import com.example.usage.core.spi.UsageTelemetry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryConfigurationTest {
    static final TenantId T = new TenantId("acme");

    final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
            .withUserConfiguration(UsageAutoConfigurationTest.HostStores.class)
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
            .withPropertyValues("usage.counters.type=memory", "usage.maintenance.enabled=true", "usage.maintenance.initial-delay=1h");

    @Test
    void decisionsBecomeTimersTaggedByMetricAndReasonButNeverByTenant() {
        runner.run(ctx -> {
            UsageEngine engine = ctx.getBean(UsageEngine.class);
            for (int i = 0; i < 3; i++) engine.check(T, "API_REQUESTS", 1).toCompletableFuture().join(); // quota is 2

            MeterRegistry meters = ctx.getBean(MeterRegistry.class);
            assertThat(meters.get("usage.check").tag("result", "allowed").timer().count()).isEqualTo(2);
            var denied = meters.get("usage.check").tag("result", "denied").timer();
            assertThat(denied.count()).isEqualTo(1);
            assertThat(denied.getId().getTag("reason")).isEqualTo("quota_exceeded");
            assertThat(denied.getId().getTag("metric")).isEqualTo("API_REQUESTS");
            meters.getMeters().forEach(m -> m.getId().getTags().forEach(t -> assertThat(t.getValue()).isNotEqualTo("acme")));
        });
    }

    @Test
    void ingestPutsTheEventIdOnTheTraceAndTheResultOnTheSpan() {
        List<Observation.Context> stopped = new ArrayList<>();
        ObservationRegistry registry = ObservationRegistry.create();
        registry.observationConfig().observationHandler(new ObservationHandler<>() {
            public boolean supportsContext(Observation.Context c) { return true; }
            public void onStop(Observation.Context c) { stopped.add(c); }
        });
        runner.withBean(ObservationRegistry.class, () -> registry).run(ctx -> {
            UUID id = UUID.randomUUID();
            ctx.getBean(UsageEngine.class).ingest(new UsageEvent(id, T, "API_REQUESTS", 1, java.time.Instant.now())).toCompletableFuture().join();

            var ingest = stopped.stream().filter(c -> c.getName().equals("usage.ingest")).findFirst().orElseThrow();
            assertThat(ingest.getHighCardinalityKeyValue("usage.event.id").getValue()).isEqualTo(id.toString());
            assertThat(ingest.getLowCardinalityKeyValue("result").getValue()).isEqualTo("applied");
        });
    }

    @Test
    void theConsumerCountsEveryOutcomeAndMeasuresHowOldWhatItAppliedIs() {
        runner.withPropertyValues("usage.consumer.enabled=true", "usage.consumer.known-metrics=API_REQUESTS").run(ctx -> {
            var consumer = ctx.getBean(com.example.usage.consumer.UsageEventConsumer.class);
            consumer.handle("not json").toCompletableFuture().join();
            String body = com.example.usage.consumer.UsageEventJson.toJson(
                    new UsageEvent(UUID.randomUUID(), T, "API_REQUESTS", 1, java.time.Instant.now().minusSeconds(30)));
            consumer.handle(body).toCompletableFuture().join();

            MeterRegistry meters = ctx.getBean(MeterRegistry.class);
            assertThat(meters.get("usage.consumer.outcome").tag("disposition", "dead_letter").counter().count()).isEqualTo(1);
            assertThat(meters.get("usage.consumer.outcome").tag("reason", "applied").counter().count()).isEqualTo(1);
            assertThat(meters.get("usage.consumer.lag").timer().max(java.util.concurrent.TimeUnit.SECONDS)).isGreaterThanOrEqualTo(30);
        });
    }

    @Test
    void maintenanceRunsAreCountedAndTheLastSuccessIsExposed() {
        runner.run(ctx -> {
            ctx.getBean(MaintenanceScheduler.class).runOnce().toCompletableFuture().join();

            MeterRegistry meters = ctx.getBean(MeterRegistry.class);
            assertThat(meters.get("usage.maintenance.run").tag("outcome", "completed").timer().count()).isEqualTo(1);
            assertThat(meters.get("usage.maintenance.last.success").gauge().value()).isGreaterThan(0);
            assertThat(meters.find("usage.reconciler.rows").tag("result", "scanned").counter()).isNull(); // zero rows are not counted
        });
    }

    @Test
    void withoutAMeterRegistryOrWhenDisabledThePlatformReportsNothing() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
                .withUserConfiguration(UsageAutoConfigurationTest.HostStores.class)
                .withPropertyValues("usage.counters.type=memory")
                .run(ctx -> assertThat(ctx).hasNotFailed().doesNotHaveBean(UsageTelemetry.class));
        runner.withPropertyValues("usage.telemetry.enabled=false")
                .run(ctx -> assertThat(ctx).hasNotFailed().doesNotHaveBean(UsageTelemetry.class));
    }
}
