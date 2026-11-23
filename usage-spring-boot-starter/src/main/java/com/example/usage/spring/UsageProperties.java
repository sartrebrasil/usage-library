package com.example.usage.spring;

import com.example.usage.core.UsageEngine.FailMode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

/** Every setting of the starter, under the {@code usage.} prefix. */
@ConfigurationProperties("usage")
public record UsageProperties(
        /** What to do when the counters or the policy store are down: let requests through (OPEN) or refuse them (CLOSED). */
        @DefaultValue("OPEN") FailMode failMode,
        /** How long a concurrency slot lives if its holder crashes without releasing it. */
        @DefaultValue("15m") Duration concurrencyLease,
        /** How long a tenant's policy is cached before it is read again. Invalidate the cache after admin changes. */
        @DefaultValue("1m") Duration policyCacheTtl,
        @DefaultValue Counters counters,
        @DefaultValue Redis redis,
        @DefaultValue Postgres postgres,
        @DefaultValue Gateway gateway,
        @DefaultValue Processor processor,
        @DefaultValue Consumer consumer,
        @DefaultValue Maintenance maintenance,
        @DefaultValue Messaging messaging,
        @DefaultValue Telemetry telemetry,
        @DefaultValue Admin admin,
        @DefaultValue Admission admission) {
    public UsageProperties {
        requirePositive(concurrencyLease, "usage.concurrency-lease");
        requireNotNegative(policyCacheTtl, "usage.policy-cache-ttl");
    }

    private static void requirePositive(Duration value, String name) {
        if (value == null || value.isZero() || value.isNegative()) throw new IllegalArgumentException(name + " must be greater than zero");
    }

    private static void requireNotNegative(Duration value, String name) {
        if (value == null || value.isNegative()) throw new IllegalArgumentException(name + " must not be negative");
    }

    /**
     * The management API of the usage-admin jar (servlet applications): on by default when the jar is present.
     *
     * @param enabled        false turns the API off without removing the jar
     * @param basePath       prefix of the routes; give the same value to {@code UsageAdminRoutes.apply}
     * @param allowUnsecured the application refuses to start without a SecurityFilterChain; true says something in front of it authenticates the callers
     */
    public record Admin(@DefaultValue("true") boolean enabled, @DefaultValue("/v1/usage") String basePath,
                        @DefaultValue Tenants tenants, @DefaultValue("false") boolean allowUnsecured) {
        public Admin {
            if (basePath == null || !basePath.startsWith("/")) throw new IllegalArgumentException("usage.admin.base-path must start with /");
        }
    }

    /**
     * The {@code UsageAdmission} a servlet service calls to decide with the engine.
     *
     * @param metric the metric whose rate and quota it applies (a request accepted counts one); setting it turns the bean on
     */
    public record Admission(String metric) {}

    /** write=false takes tenant creation and changes out of the API, for a host that owns them (the API then only lists tenants). */
    public record Tenants(@DefaultValue("true") boolean write) {}

    /** enabled=false turns off the metrics and traces even when Micrometer is present. */
    public record Telemetry(@DefaultValue("true") boolean enabled) {}

    public enum CounterType { REDIS, MEMORY }

    /** Wiring to a broker through your messaging-library and outbox-library (needs the usage-messaging jar). */
    public record Messaging(@DefaultValue Outbox outbox, @DefaultValue Inbox inbox) {}

    public enum TransactionMode { OWN, CALLER }

    /**
     * Setting {@code destination} turns the outbox sink on: usage events and compensations are written to the outbox,
     * and its relay publishes them. {@code OWN} writes each in a transaction of its own on a virtual thread (the
     * gateway, which has no business transaction); {@code CALLER} writes in the transaction already open on the
     * calling thread (processors, inside the Persist of the queue adapter).
     * <p>
     * Left unset, it follows the role of the application: CALLER when {@code usage.processor.metric} is set, OWN
     * otherwise. An application that is both gateway and processor must say which. CALLER together with the gateway
     * is refused at startup (the gateway has no transaction to join, so nothing would ever be written), and an OWN sink
     * called inside a transaction refuses at runtime (it would commit usage that the caller's rollback should drop).
     */
    public record Outbox(String destination, TransactionMode transaction, @DefaultValue("TENANT") OutboxOrdering ordering) {}

    /** The ordering the outbox keeps (see {@code OutboxUsageSink.Ordering}); its own enum, so the properties load without the usage-messaging jar. */
    public enum OutboxOrdering { TENANT, EVENT }

    /**
     * {@code enabled} registers the usage consumer as a messaging-inbox handler bean named {@code usageEvents}, so the
     * receiver called {@code usage-events} in {@code messaging.receivers} is served by it. For a receiver with another
     * name, declare your own {@code InboxResultHandler} bean of that name around {@code UsageInboxHandler} instead.
     * Needs {@code usage.consumer.enabled=true}.
     *
     * @param originalNotFoundDelay how long a compensation that arrived before its usage waits in the retry level
     * @param timeout               how long to wait for the consumer before the delivery counts as a transient failure
     */
    public record Inbox(@DefaultValue("false") boolean enabled, @DefaultValue("30s") Duration originalNotFoundDelay,
                        @DefaultValue("10s") Duration timeout) {
        public Inbox {
            requirePositive(originalNotFoundDelay, "usage.messaging.inbox.original-not-found-delay");
            requirePositive(timeout, "usage.messaging.inbox.timeout");
        }
    }

    /** type=memory counts inside one JVM only: for tests and single-instance deployments, never for a cluster. */
    public record Counters(@DefaultValue("redis") CounterType type) {}

    /**
     * @param commandTimeout how long a counter command may take before it fails (and fail-mode decides). Also, while the
     *                       connection is down, commands fail at once instead of queueing.
     */
    public record Redis(String uri, @DefaultValue("500ms") Duration commandTimeout) {
        public Redis {
            requirePositive(commandTimeout, "usage.redis.command-timeout");
        }
    }

    /**
     * @param migrate true runs the platform's own Flyway migrations at startup (history table inside the schema below).
     * @param schema  the schema that holds the platform's tables, created by the migration. Its own schema (default
     *                "usage") keeps the generic table names (tenant, metric, plan...) from colliding with yours and lets
     *                the migration run beside a schema that already has tables. A lowercase identifier; leave it empty
     *                to use the connection's default schema instead (the tables then resolve through the search_path).
     */
    public record Postgres(@DefaultValue("false") boolean migrate, @DefaultValue("usage") String schema) {}

    /**
     * @param refundFromStatus responses with this status or above are refunded; 600 turns refunds off.
     */
    public record Gateway(@DefaultValue("false") boolean enabled, @DefaultValue("500") int refundFromStatus, List<GatewayMetric> metrics) {
        public Gateway {
            if (refundFromStatus < 100) throw new IllegalArgumentException("usage.gateway.refund-from-status must be an HTTP status (100 or more)");
            metrics = metrics == null ? List.of() : List.copyOf(metrics);
        }
    }

    public enum GatewayMode { ACCESS, GUARD }

    /**
     * @param costHeader ACCESS only: request header carrying how much this request consumes (items in a batch).
     *                   Missing means 1; a malformed value is an error, never silently 1, or a client could
     *                   under-report its cost.
     */
    public record GatewayMetric(String code, @DefaultValue("ACCESS") GatewayMode mode, String costHeader) {}

    /** Enabled by setting {@code usage.processor.metric}. */
    public record Processor(String metric, List<String> countableStatuses, @DefaultValue("x-tenant-id") String tenantHeader,
                            @DefaultValue("false") boolean deferWhenOverQuota) {
        public Processor {
            countableStatuses = countableStatuses == null ? List.of() : List.copyOf(countableStatuses);
        }
    }

    /** knownMetrics is required when enabled: an event for any other metric is dead-lettered. */
    public record Consumer(@DefaultValue("false") boolean enabled, List<String> knownMetrics,
                           @DefaultValue("5m") Duration maxFuture, @DefaultValue("90d") Duration maxPast) {
        public Consumer {
            requireNotNegative(maxFuture, "usage.consumer.max-future");
            requireNotNegative(maxPast, "usage.consumer.max-past");
            knownMetrics = knownMetrics == null ? List.of() : List.copyOf(knownMetrics);
        }
    }

    /**
     * @param closeTolerance how long after a period ends late events are still accepted before it is closed
     * @param settle         how long a summary row must be quiet before reconciliation trusts it
     */
    public record Maintenance(@DefaultValue("false") boolean enabled, @DefaultValue("24h") Duration closeTolerance,
                              @DefaultValue("5m") Duration settle, @DefaultValue("1m") Duration initialDelay,
                              @DefaultValue("15m") Duration interval) {
        public Maintenance {
            requireNotNegative(closeTolerance, "usage.maintenance.close-tolerance");
            requireNotNegative(settle, "usage.maintenance.settle");
            requireNotNegative(initialDelay, "usage.maintenance.initial-delay");
            requirePositive(interval, "usage.maintenance.interval");
        }
    }
}
