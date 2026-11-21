package dev.usageplatform.core;

import dev.usageplatform.core.spi.SummaryStore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;

/**
 * The periodic housekeeping of the platform: close the periods whose grace time is over, then repair the
 * counters of the periods still open. Both steps are idempotent, so a repeated or overlapping run is
 * harmless, but run it through {@link MaintenanceScheduler} so only one instance does the work at a time.
 */
public final class PeriodMaintenance {
    public record Result(int closed, CounterReconciler.Report reconciliation) {}

    private final SummaryStore summaries;
    private final CounterReconciler reconciler;
    private final Clock clock;
    private final Duration closeTolerance;

    /**
     * @param closeTolerance how long after a period ends late events are still expected (24 to 72 hours is
     *                       typical): a period is closed, and its total frozen for billing, only after that.
     */
    public PeriodMaintenance(SummaryStore summaries, CounterReconciler reconciler, Clock clock, Duration closeTolerance) {
        this.summaries = summaries;
        this.reconciler = reconciler;
        this.clock = clock;
        this.closeTolerance = closeTolerance;
    }

    public CompletionStage<Result> run() {
        Instant cutoff = clock.instant().minus(closeTolerance);
        return summaries.closeEndedBefore(cutoff)
                .thenCompose(closed -> reconciler.run().thenApply(report -> new Result(closed, report)));
    }
}
