package com.example.usage.spring;

import com.example.usage.core.CounterReconciler;
import com.example.usage.core.MaintenanceScheduler;
import com.example.usage.core.spi.UsageTelemetry;
import org.springframework.beans.factory.ObjectProvider;
import com.example.usage.core.PeriodMaintenance;
import com.example.usage.core.spi.CounterStore;
import com.example.usage.core.spi.JobLock;
import com.example.usage.core.spi.PolicyStore;
import com.example.usage.core.spi.SummaryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "usage.maintenance.enabled", havingValue = "true")
class MaintenanceConfiguration {
    private static final Logger log = LoggerFactory.getLogger(UsageAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    CounterReconciler usageReconciler(PolicyStore policies, CounterStore counters, SummaryStore summaries, Clock clock, UsageProperties props) {
        return new CounterReconciler(policies, counters, summaries, clock, props.maintenance().settle());
    }

    @Bean
    @ConditionalOnMissingBean
    PeriodMaintenance usagePeriodMaintenance(SummaryStore summaries, CounterReconciler reconciler, Clock clock, UsageProperties props) {
        return new PeriodMaintenance(summaries, reconciler, clock, props.maintenance().closeTolerance());
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    MaintenanceScheduler usageMaintenanceScheduler(JobLock lock, PeriodMaintenance maintenance, ObjectProvider<UsageTelemetry> telemetry) {
        return new MaintenanceScheduler(lock, maintenance, outcome -> {
            switch (outcome) {
                case MaintenanceScheduler.RunOutcome.Completed c -> log.info("usage maintenance done: {}", c.result());
                case MaintenanceScheduler.RunOutcome.Skipped s -> log.debug("usage maintenance skipped: another instance holds the lock");
                case MaintenanceScheduler.RunOutcome.Failed f -> log.error("usage maintenance failed; will retry on the next tick", f.error());
            }
        }, telemetry.getIfAvailable(() -> UsageTelemetry.NOOP));
    }

    /** Starts the timer after the context is up and stops it first on shutdown. */
    @Bean
    SmartLifecycle usageMaintenanceLifecycle(MaintenanceScheduler scheduler, UsageProperties props) {
        return new SmartLifecycle() {
            private volatile boolean running;

            @Override public void start() {
                scheduler.start(props.maintenance().initialDelay(), props.maintenance().interval());
                running = true;
            }

            @Override public void stop() {
                scheduler.close();
                running = false;
            }

            @Override public boolean isRunning() { return running; }
        };
    }
}
