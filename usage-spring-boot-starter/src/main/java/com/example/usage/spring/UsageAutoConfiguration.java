package com.example.usage.spring;

import com.example.usage.core.UsageEngine;
import com.example.usage.core.spi.CounterStore;
import com.example.usage.core.spi.Ledger;
import com.example.usage.core.spi.PolicyStore;
import com.example.usage.core.spi.UsageSink;
import com.example.usage.core.spi.UsageTelemetry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.util.Assert;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Wires the platform from properties. The engine is always created; every adapter appears only if its jar is on
 * the classpath and its property turns it on, and any bean you define yourself replaces the default one.
 * <p>
 * You must provide: a {@code PolicyStore} and a {@code Ledger} (or a DataSource, which gives the PostgreSQL ones),
 * a counter store (Redis through {@code usage.redis.uri}, or {@code usage.counters.type=memory}), and a
 * {@code UsageSink} wherever events are published (your outbox).
 * shortcut: auto-configuration and starter are one jar; split them if the dependency set must differ.
 */
@AutoConfiguration(afterName = {"org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.micrometer.observation.autoconfigure.ObservationAutoConfiguration"})
@EnableConfigurationProperties(UsageProperties.class)
@Import({
        TelemetryConfiguration.class,
        PostgresConfiguration.class,
        RedisConfiguration.class,
        MemoryCountersConfiguration.class,
        GatewayConfiguration.class,
        ProcessorConfiguration.class,
        ConsumerConfiguration.class,
        AdminConfiguration.class,
        MissingJarFailures.class,
        OutboxSinkConfiguration.class,
        InboxHandlerConfiguration.class,
        MaintenanceConfiguration.class})
public class UsageAutoConfiguration {
    static final String EXECUTOR = "usageExecutor";

    @Bean
    @ConditionalOnMissingBean
    Clock usageClock() {
        return Clock.systemUTC();
    }

    /** Runs the blocking JDBC calls. Virtual threads, so a slow query never starves the event loop. */
    @Bean(name = EXECUTOR, destroyMethod = "close")
    @ConditionalOnMissingBean(name = EXECUTOR)
    ExecutorService usageExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    @ConditionalOnMissingBean
    UsageEngine usageEngine(PolicyStore policies, CounterStore counters, Ledger ledger, ObjectProvider<UsageSink> sink,
                            Clock clock, UsageProperties props, ObjectProvider<UsageTelemetry> telemetry) {
        UsageSink unique = sink.getIfUnique();
        // the gateway publishes every usage event and refund through the sink: without a durable one it would
        // meter nothing durable while counters keep moving, so refuse to start. Consumer-only apps never publish.
        Assert.state(unique != null || !props.gateway().enabled(),
                "usage.gateway.enabled=true needs exactly one UsageSink bean (your durable outbox) to publish usage events");
        UsageSink usageSink = unique != null ? unique : message -> CompletableFuture.failedFuture(
                new IllegalStateException("no UsageSink bean: define one (your outbox) before publishing usage"));
        return new UsageEngine(policies, counters, ledger, usageSink, clock, props.failMode(), props.concurrencyLease(),
                telemetry.getIfAvailable(() -> UsageTelemetry.NOOP));
    }
}
