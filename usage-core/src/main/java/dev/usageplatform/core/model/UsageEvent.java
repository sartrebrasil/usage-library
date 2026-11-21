package dev.usageplatform.core.model;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable fact "tenant consumed quantity of metric at occurredAt". eventId is the idempotency key.
 */
public record UsageEvent(UUID eventId, TenantId tenant, String metricCode, long quantity, Instant occurredAt) implements UsageMessage {
    /**
     * Largest quantity of a single event or check. Counters are doubles in Redis (exact up to 2^53) and
     * BIGINT in PostgreSQL, so one value must stay far below both: a hostile or buggy cost can never
     * overflow or erase a counter.
     */
    public static final long MAX_QUANTITY = 1_000_000_000L;

    public UsageEvent {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(metricCode, "metricCode");
        Objects.requireNonNull(occurredAt, "occurredAt");
        if (quantity <= 0 || quantity > MAX_QUANTITY) throw new IllegalArgumentException("quantity must be in 1.." + MAX_QUANTITY);
    }

    /**
     * Deterministic id from the business item, so a re-sent report is the same event. The tenant is part of
     * it: two tenants whose item ids overlap must never share an event id.
     */
    public static UUID idFor(TenantId tenant, String naturalKey, String metricCode) {
        return UUID.nameUUIDFromBytes((tenant.value() + "|" + naturalKey + "|" + metricCode).getBytes(StandardCharsets.UTF_8));
    }
}
