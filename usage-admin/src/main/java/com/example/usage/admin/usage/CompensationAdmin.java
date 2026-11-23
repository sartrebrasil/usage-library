package com.example.usage.admin.usage;

import com.example.usage.admin.audit.Audit;
import com.example.usage.admin.support.AdminContext;
import com.example.usage.admin.support.AdminException;
import com.example.usage.admin.support.Check;

import com.example.usage.core.UsageEngine;
import com.example.usage.core.UsageEngine.Compensate;
import com.example.usage.core.model.Compensation;
import com.example.usage.core.model.TenantId;

import java.time.Instant;
import java.util.UUID;

/**
 * Manual reversal of a usage event: a new immutable fact charged to the period of the original, never an edit.
 * It is applied at once through the engine (ledger first, then counters), with no broker in between, so the caller
 * learns the result. A retry of the same original is harmless: the compensation id is derived from it.
 * <p>
 * The counters move through the host's {@code CounterStore}: with Redis configured the quota is given back at once;
 * if the counters could not be moved the answer is {@code COUNTER_DRIFT} and the reconciler repairs result metrics
 * (an access metric's counter only goes up, so it stays high until its period ends).
 */
public final class CompensationAdmin {
    /** {@code quantity} defaults to the whole original. */
    public record Request(UUID originalEventId, Long quantity, String reason) {}

    public record Result(String result, UUID compensationEventId, UUID originalEventId, String tenantId, String metricCode,
                         long quantity, Instant occurredAt) {}

    private final AdminContext ctx;
    private final Audit audit;
    private final UsageEngine engine;
    private final UsageQueries queries;

    public CompensationAdmin(AdminContext ctx, Audit audit, UsageEngine engine, UsageQueries queries) {
        this.ctx = ctx;
        this.audit = audit;
        this.engine = engine;
        this.queries = queries;
    }

    public Result compensate(Request in) {
        if (in == null || in.originalEventId() == null) throw AdminException.invalid("FIELD_REQUIRED", "originalEventId is required");
        String reason = Check.text(in.reason(), "reason", 60); // the ledger column is VARCHAR(60)
        UsageQueries.Event original = queries.event(in.originalEventId());
        if (!"USAGE".equals(original.eventType())) throw AdminException.invalid("NOT_A_USAGE_EVENT", "only a usage event can be compensated");
        long quantity = in.quantity() == null ? original.quantity() : in.quantity();
        if (quantity > original.quantity()) throw AdminException.invalid("QUANTITY_TOO_LARGE", "quantity is larger than the original " + original.quantity());
        Instant now = ctx.clock().instant();
        Compensation c;
        try {
            c = Compensation.of(original.eventId(), new TenantId(original.tenantId()), original.metricCode(), quantity, now, reason);
        } catch (IllegalArgumentException e) {
            throw AdminException.invalid("INVALID_COMPENSATION", e.getMessage());
        }

        Compensate outcome = engine.compensate(c).toCompletableFuture().join();
        switch (outcome) {
            case ALREADY_COMPENSATED -> throw AdminException.conflict("ALREADY_COMPENSATED", "event " + original.eventId() + " was already compensated");
            case INVALID, ORIGINAL_NOT_FOUND -> throw AdminException.invalid("INVALID_COMPENSATION", "the platform refused this compensation: " + outcome);
            case APPLIED, COUNTER_DRIFT, DUPLICATE -> { }
        }
        // a repeat answers with what is stored, not with what this request asked for
        UsageQueries.Event stored = outcome == Compensate.DUPLICATE ? queries.event(c.eventId()) : null;
        Result result = stored == null
                ? new Result(outcome.name(), c.eventId(), original.eventId(), original.tenantId(), original.metricCode(), quantity, now)
                : new Result(outcome.name(), c.eventId(), original.eventId(), original.tenantId(), original.metricCode(), stored.quantity(), stored.occurredAt());
        // The ledger is committed by the engine in its own transaction, so the trail is written right after it. A retry
        // (DUPLICATE) writes it too when it is missing: a failure here must not leave a reversal with no trail for good.
        ctx.inTx(() -> {
            if (!audit.recorded("COMPENSATE", "compensation", c.eventId())) {
                audit.record("COMPENSATE", "compensation", c.eventId(), original.tenantId(), original, result, reason);
            }
            return null;
        });
        return result;
    }
}
