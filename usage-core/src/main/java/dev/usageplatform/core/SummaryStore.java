package dev.usageplatform.core;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * SPI: read and close the per-period totals that billing consumes. One row per tenant, metric, quota
 * window and period, the durable twin of the quota counter with the same window key.
 * <p>
 * Net consumption of a closed period for billing: {@code total + lateTotal - credits}.
 * {@code total} never changes after the period closes; usage that arrives afterwards lands in
 * {@code lateTotal} so nothing is lost and nothing already invoiced is rewritten.
 */
public interface SummaryStore {
    /** plan: the plan in force when the period last received usage while it was open (frozen when it closes). */
    record Summary(TenantId tenant, String metricCode, String windowKey, Period period,
                   long total, long lateTotal, long credits, boolean closed, Plan plan) {}

    CompletionStage<Optional<Summary>> find(TenantId tenant, String metricCode, String windowKey, Instant periodStart);

    /**
     * Every OPEN period whose row was last written before the given instant. Rows touched more recently
     * may still have an event in flight between the ledger and the counters, so reconciliation leaves
     * them alone.
     * shortcut: returns all of them at once; page by tenant range when open rows reach the millions.
     */
    CompletionStage<List<Summary>> openSettledBefore(Instant updatedBefore);

    /**
     * Closes every OPEN period that ended at or before the cutoff. The caller picks the cutoff as
     * {@code now - tolerance}, the grace period for late events. Idempotent.
     *
     * @return how many periods were closed
     */
    CompletionStage<Integer> closeEndedBefore(Instant cutoff);
}
