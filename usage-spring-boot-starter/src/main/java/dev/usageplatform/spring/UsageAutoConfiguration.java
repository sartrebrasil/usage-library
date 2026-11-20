package dev.usageplatform.spring;

import dev.usageplatform.consumer.UsageEventConsumer;
import dev.usageplatform.core.CachedPolicyStore;
import dev.usageplatform.core.CounterReconciler;
import dev.usageplatform.core.CounterStore;
import dev.usageplatform.core.InMemoryCounterStore;
import dev.usageplatform.core.JobLock;
import dev.usageplatform.core.Ledger;
import dev.usageplatform.core.MaintenanceScheduler;
import dev.usageplatform.core.PeriodMaintenance;
import dev.usageplatform.core.PolicyStore;
import dev.usageplatform.core.SummaryStore;
import dev.usageplatform.core.TenantResolver;
import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.UsageSink;
import dev.usageplatform.gateway.MeteredMetric;
import dev.usageplatform.gateway.UsageGatewayFilter;
import com.example.messaging.inbox.InboxResultHandler;
import com.example.outbox.core.OutboxWriter;
import dev.usageplatform.messaging.OutboxUsageSink;
import dev.usageplatform.messaging.UsageInboxHandler;
import dev.usageplatform.postgres.JdbcJobLock;
import dev.usageplatform.postgres.JdbcLedger;
import dev.usageplatform.postgres.JdbcPolicyStore;
import dev.usageplatform.postgres.JdbcSummaryStore;
import dev.usageplatform.postgres.JdbcSubscriptions;
import dev.usageplatform.postgres.UsageSchema;
import dev.usageplatform.processor.QueueUsageAdapter;
import dev.usageplatform.redis.RedisCounterStore;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;
import org.springframework.web.server.ServerWebExchange;

import javax.sql.DataSource;
import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

/**
 * Wires the platform from properties. The engine is always created; every adapter appears only if its jar is on
 * the classpath and its property turns it on, and any bean you define yourself replaces the default one.
 * <p>
 * You must provide: a {@code PolicyStore} and a {@code Ledger} (or a DataSource, which gives the PostgreSQL ones),
 * a counter store (Redis through {@code usage.redis.uri}, or {@code usage.counters.type=memory}), and a
 * {@code UsageSink} wherever events are published (your outbox).
 * shortcut: auto-configuration and starter are one jar; split them if the dependency set must differ.
 */
@AutoConfiguration(afterName = "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration")
@EnableConfigurationProperties(UsageProperties.class)
public class UsageAutoConfiguration {
    private static final Logger log = LoggerFactory.getLogger(UsageAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    Clock usageClock() {
        return Clock.systemUTC();
    }

    /** Runs the blocking JDBC calls. Virtual threads, so a slow query never starves the event loop. */
    @Bean(name = "usageExecutor", destroyMethod = "close")
    @ConditionalOnMissingBean(name = "usageExecutor")
    ExecutorService usageExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    @ConditionalOnMissingBean
    UsageEngine usageEngine(PolicyStore policies, CounterStore counters, Ledger ledger, ObjectProvider<UsageSink> sink,
                            Clock clock, UsageProperties props) {
        UsageSink unique = sink.getIfUnique();
        // the gateway publishes every usage event and refund through the sink: without a durable one it would
        // meter nothing durable while counters keep moving, so refuse to start. Consumer-only apps never publish.
        Assert.state(unique != null || !props.gateway().enabled(),
                "usage.gateway.enabled=true needs exactly one UsageSink bean (your durable outbox) to publish usage events");
        UsageSink usageSink = unique != null ? unique : message -> CompletableFuture.failedFuture(
                new IllegalStateException("no UsageSink bean: define one (your outbox) before publishing usage"));
        return new UsageEngine(policies, counters, ledger, usageSink, clock, props.failMode(), props.concurrencyLease());
    }

    // ---- PostgreSQL ---------------------------------------------------------------------------------------

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(JdbcLedger.class)
    @ConditionalOnBean(DataSource.class)
    static class PostgresConfiguration {

        /** Creating this bean runs the migrations (if asked), and every store below depends on it. */
        @Bean
        UsageSchemaReady usageSchemaReady(DataSource dataSource, UsageProperties props) {
            if (props.postgres().migrate()) UsageSchema.migrate(dataSource, props.postgres().schema());
            return new UsageSchemaReady();
        }

        @Bean
        @ConditionalOnMissingBean(PolicyStore.class)
        CachedPolicyStore usagePolicyStore(DataSource ds, @org.springframework.beans.factory.annotation.Qualifier("usageExecutor") ExecutorService executor,
                                           Clock clock, UsageProperties props, UsageSchemaReady ready) {
            return new CachedPolicyStore(new JdbcPolicyStore(ds, executor, clock, props.postgres().schema()), clock, props.policyCacheTtl());
        }

        @Bean
        @ConditionalOnMissingBean(Ledger.class)
        Ledger usageLedger(DataSource ds, @org.springframework.beans.factory.annotation.Qualifier("usageExecutor") ExecutorService executor,
                           UsageProperties props, UsageSchemaReady ready) {
            return new JdbcLedger(ds, executor, props.postgres().schema());
        }

        @Bean
        @ConditionalOnMissingBean(SummaryStore.class)
        SummaryStore usageSummaryStore(DataSource ds, @org.springframework.beans.factory.annotation.Qualifier("usageExecutor") ExecutorService executor,
                                       UsageProperties props, UsageSchemaReady ready) {
            return new JdbcSummaryStore(ds, executor, props.postgres().schema());
        }

        @Bean
        @ConditionalOnMissingBean(JobLock.class)
        JobLock usageJobLock(DataSource ds, @org.springframework.beans.factory.annotation.Qualifier("usageExecutor") ExecutorService executor, UsageSchemaReady ready) {
            return new JdbcJobLock(ds, executor);
        }

        /** Admin operations (subscribe, change plan). Call {@code CachedPolicyStore.invalidate} after using them. */
        @Bean
        @ConditionalOnMissingBean
        JdbcSubscriptions usageSubscriptions(DataSource ds, UsageProperties props, UsageSchemaReady ready) {
            return new JdbcSubscriptions(ds, props.postgres().schema());
        }
    }

    /** Marker: the schema is in place. */
    static final class UsageSchemaReady {}

    // ---- counters -----------------------------------------------------------------------------------------

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(RedisCounterStore.class)
    @ConditionalOnProperty(name = "usage.counters.type", havingValue = "redis", matchIfMissing = true)
    @ConditionalOnMissingBean(CounterStore.class) // a CounterStore of your own needs neither the Redis client nor its uri
    static class RedisConfiguration {

        @Bean(destroyMethod = "shutdown")
        @ConditionalOnMissingBean(RedisClient.class)
        RedisClient usageRedisClient(UsageProperties props) {
            Assert.hasText(props.redis().uri(), "usage.redis.uri is required when usage.counters.type=redis");
            RedisClient client = RedisClient.create(props.redis().uri());
            // the check is on the request path: a slow or unreachable Redis must fail fast so fail-mode can decide,
            // not buffer commands for Lettuce's default 60 s
            client.setOptions(ClientOptions.builder()
                    .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                    .timeoutOptions(TimeoutOptions.enabled(props.redis().commandTimeout()))
                    .build());
            return client;
        }

        @Bean(destroyMethod = "close")
        StatefulRedisConnection<String, String> usageRedisConnection(RedisClient client) {
            return client.connect();
        }

        @Bean
        @ConditionalOnMissingBean(CounterStore.class)
        CounterStore usageCounterStore(StatefulRedisConnection<String, String> connection) {
            return new RedisCounterStore(connection.async());
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "usage.counters.type", havingValue = "memory")
    static class MemoryCountersConfiguration {

        @Bean
        @ConditionalOnMissingBean(CounterStore.class)
        CounterStore usageCounterStore(Clock clock) {
            log.warn("usage.counters.type=memory: quotas are counted inside this JVM only; never use it with more than one instance");
            return new InMemoryCounterStore(clock);
        }
    }

    // ---- adapters -----------------------------------------------------------------------------------------

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({UsageGatewayFilter.class, GlobalFilter.class})
    @ConditionalOnProperty(name = "usage.gateway.enabled", havingValue = "true")
    static class GatewayConfiguration {

        @Bean
        @ConditionalOnMissingBean
        UsageGatewayFilter usageGatewayFilter(UsageEngine engine, TenantResolver<ServerWebExchange> tenants,
                                              ObjectProvider<InternalTraffic> internal, Clock clock, UsageProperties props) {
            List<MeteredMetric> metrics = props.gateway().metrics().stream().map(m -> {
                Assert.hasText(m.code(), "usage.gateway.metrics[].code is required");
                return switch (m.mode()) {
                    case ACCESS -> m.costHeader() == null || m.costHeader().isBlank()
                            ? MeteredMetric.access(m.code())
                            : MeteredMetric.access(m.code(), costFrom(m.costHeader()));
                    case GUARD -> MeteredMetric.guard(m.code());
                };
            }).toList();
            Assert.notEmpty(metrics, "usage.gateway.metrics must list at least one metric");
            InternalTraffic none = exchange -> false;
            int refundFrom = props.gateway().refundFromStatus();
            return new UsageGatewayFilter(engine, tenants, metrics, internal.getIfAvailable(() -> none), clock, status -> status >= refundFrom);
        }

        private static ToLongFunction<ServerWebExchange> costFrom(String header) {
            return exchange -> {
                // a repeated header is refused: the gateway would bill one value while a backend may read another
                List<String> values = exchange.getRequest().getHeaders().getValuesAsList(header);
                if (values.size() > 1) throw new IllegalArgumentException("repeated cost header " + header);
                return values.isEmpty() ? 1 : Long.parseLong(values.get(0).trim()); // malformed is refused with 400 on purpose
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(QueueUsageAdapter.class)
    @ConditionalOnProperty(name = "usage.processor.metric")
    static class ProcessorConfiguration {

        @Bean
        @ConditionalOnMissingBean
        QueueUsageAdapter queueUsageAdapter(UsageEngine engine, UsageProperties props) {
            UsageProperties.Processor p = props.processor();
            Assert.notEmpty(p.countableStatuses(), "usage.processor.countable-statuses must list the final statuses that are billable");
            return new QueueUsageAdapter(engine, p.metric(), Set.copyOf(p.countableStatuses()), p.deferWhenOverQuota(), p.tenantHeader());
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(UsageEventConsumer.class)
    @ConditionalOnProperty(name = "usage.consumer.enabled", havingValue = "true")
    static class ConsumerConfiguration {

        @Bean
        @ConditionalOnMissingBean
        UsageEventConsumer usageEventConsumer(UsageEngine engine, Clock clock, UsageProperties props) {
            UsageProperties.Consumer c = props.consumer();
            Assert.notEmpty(c.knownMetrics(), "usage.consumer.known-metrics must list the metric codes the consumer accepts");
            Set<String> known = new HashSet<>(c.knownMetrics());
            return new UsageEventConsumer(engine, known::contains, clock, c.maxFuture(), c.maxPast());
        }
    }

    // ---- a property that needs a jar which is not there ----------------------------------------------------

    /**
     * Each adapter appears only when its jar is on the classpath. Turning one on by property without its jar would
     * otherwise be ignored silently, and the application would run without metering. These fail the startup instead.
     */
    @Configuration(proxyBeanMethods = false)
    static class MissingJarFailures {
        @Bean
        @ConditionalOnProperty(name = "usage.gateway.enabled", havingValue = "true")
        @ConditionalOnMissingClass("dev.usageplatform.gateway.UsageGatewayFilter")
        Object usageGatewayJarMissing() {
            throw missing("usage.gateway.enabled=true", "usage-gateway");
        }

        @Bean
        @ConditionalOnProperty(name = "usage.processor.metric")
        @ConditionalOnMissingClass("dev.usageplatform.processor.QueueUsageAdapter")
        Object usageProcessorJarMissing() {
            throw missing("usage.processor.metric", "usage-processor");
        }

        @Bean
        @ConditionalOnProperty(name = "usage.consumer.enabled", havingValue = "true")
        @ConditionalOnMissingClass("dev.usageplatform.consumer.UsageEventConsumer")
        Object usageConsumerJarMissing() {
            throw missing("usage.consumer.enabled=true", "usage-consumer");
        }

        @Bean
        @ConditionalOnProperty(name = "usage.messaging.outbox.destination")
        @ConditionalOnMissingClass("dev.usageplatform.messaging.OutboxUsageSink")
        Object usageOutboxJarMissing() {
            throw missing("usage.messaging.outbox.destination", "usage-messaging");
        }

        @Bean
        @ConditionalOnProperty(name = "usage.messaging.inbox.enabled", havingValue = "true")
        @ConditionalOnMissingClass("dev.usageplatform.messaging.UsageInboxHandler")
        Object usageInboxJarMissing() {
            throw missing("usage.messaging.inbox.enabled=true", "usage-messaging");
        }

        private static IllegalStateException missing(String property, String jar) {
            return new IllegalStateException(property + " needs the " + jar + " jar on the classpath, and it is not there");
        }
    }

    // ---- broker, through messaging-library and outbox-library ----------------------------------------------

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({OutboxUsageSink.class, OutboxWriter.class, PlatformTransactionManager.class})
    @ConditionalOnProperty(name = "usage.messaging.outbox.destination")
    static class OutboxSinkConfiguration {

        @Bean
        @ConditionalOnMissingBean(UsageSink.class)
        UsageSink usageOutboxSink(OutboxWriter writer, ObjectProvider<PlatformTransactionManager> transactions,
                                  @org.springframework.beans.factory.annotation.Qualifier("usageExecutor") ExecutorService executor,
                                  UsageProperties props) {
            UsageProperties.Outbox outbox = props.messaging().outbox();
            boolean gateway = props.gateway().enabled();
            boolean processor = props.processor().metric() != null && !props.processor().metric().isBlank();
            UsageProperties.TransactionMode mode = outbox.transaction();
            if (mode == null) {
                Assert.state(!(gateway && processor), "usage.messaging.outbox.transaction must be set (OWN for the gateway, CALLER for "
                        + "processors): this application is both");
                mode = processor ? UsageProperties.TransactionMode.CALLER : UsageProperties.TransactionMode.OWN;
            }
            Assert.state(!(gateway && mode == UsageProperties.TransactionMode.CALLER),
                    "usage.messaging.outbox.transaction=CALLER cannot be used with usage.gateway.enabled=true: the gateway has no "
                            + "business transaction to join, so no usage would ever be written");
            if (mode == UsageProperties.TransactionMode.CALLER) {
                return OutboxUsageSink.sameTransaction(writer, outbox.destination());
            }
            PlatformTransactionManager manager = transactions.getIfUnique();
            Assert.state(manager != null,
                    "usage.messaging.outbox.transaction=OWN needs exactly one PlatformTransactionManager bean");
            TransactionTemplate template = new TransactionTemplate(manager);
            return OutboxUsageSink.ownTransaction(writer, outbox.destination(), executor,
                    work -> template.executeWithoutResult(status -> work.run()),
                    TransactionSynchronizationManager::isActualTransactionActive);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({UsageInboxHandler.class, InboxResultHandler.class})
    @ConditionalOnProperty(name = "usage.messaging.inbox.enabled", havingValue = "true")
    static class InboxHandlerConfiguration {

        /**
         * The inbox binds a handler to the receiver whose name equals the bean's, or its camelCase form: receiver
         * {@code usage-events} is served by this bean. Not aliased as "usage-events" on purpose: outbox-library names
         * its publisher beans after the destination, and a publisher to "usage-events" would collide with the alias.
         */
        @Bean(name = "usageEvents")
        @ConditionalOnMissingBean(name = "usageEvents")
        InboxResultHandler<String> usageEventsHandler(ObjectProvider<UsageEventConsumer> consumer,
                                                      ObjectProvider<UsageOutcomeObserver> observer, UsageProperties props) {
            UsageEventConsumer usage = consumer.getIfAvailable();
            Assert.state(usage != null, "usage.messaging.inbox.enabled=true needs usage.consumer.enabled=true (and known-metrics)");
            UsageProperties.Inbox inbox = props.messaging().inbox();
            java.util.function.Consumer<UsageEventConsumer.Outcome> watcher = observer.getIfAvailable(() -> outcome -> {
                if ("counter_drift".equals(outcome.reason())) log.warn("usage counted in the ledger but not in the counters; reconciliation will repair it");
            });
            return new UsageInboxHandler<>(usage, inbox.originalNotFoundDelay(), inbox.timeout(), watcher);
        }
    }

    // ---- maintenance --------------------------------------------------------------------------------------

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "usage.maintenance.enabled", havingValue = "true")
    static class MaintenanceConfiguration {

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
        MaintenanceScheduler usageMaintenanceScheduler(JobLock lock, PeriodMaintenance maintenance) {
            return new MaintenanceScheduler(lock, maintenance, outcome -> {
                switch (outcome) {
                    case MaintenanceScheduler.RunOutcome.Completed c -> log.info("usage maintenance done: {}", c.result());
                    case MaintenanceScheduler.RunOutcome.Skipped s -> log.debug("usage maintenance skipped: another instance holds the lock");
                    case MaintenanceScheduler.RunOutcome.Failed f -> log.error("usage maintenance failed; will retry on the next tick", f.error());
                }
            });
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
}
