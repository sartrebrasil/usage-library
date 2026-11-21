package com.example.usage.core.model;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Reverses (part of) a previous usage. History is never edited: this is a new immutable fact.
 * The reversal is charged to the period of the ORIGINAL event, not to occurredAt of the compensation,
 * and an original can be compensated only once.
 */
public record Compensation(UUID eventId, TenantId tenant, String metricCode, long quantity,
                           Instant occurredAt, UUID compensates, String reason) implements UsageMessage {
    public Compensation {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(metricCode, "metricCode");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(compensates, "compensates");
        if (quantity <= 0 || quantity > UsageEvent.MAX_QUANTITY) {
            throw new IllegalArgumentException("quantity must be in 1.." + UsageEvent.MAX_QUANTITY);
        }
        if (reason == null || reason.isBlank()) throw new IllegalArgumentException("reason is required");
    }

    /** The usual way to build one: the id is derived from the original, so a retry is the same event. */
    public static Compensation of(UUID original, TenantId tenant, String metricCode, long quantity,
                                  Instant occurredAt, String reason) {
        return new Compensation(idFor(original), tenant, metricCode, quantity, occurredAt, original, reason);
    }

    public static UUID idFor(UUID original) {
        return UUID.nameUUIDFromBytes(("compensation|" + original).getBytes(StandardCharsets.UTF_8));
    }
}
