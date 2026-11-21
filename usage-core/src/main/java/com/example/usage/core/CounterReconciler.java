package com.example.usage.core;

import com.example.usage.core.model.Limit;
import com.example.usage.core.model.Limit.Type;
import com.example.usage.core.model.Metric.Mode;
import com.example.usage.core.model.PeriodCalculator;
import com.example.usage.core.model.Policy;
import com.example.usage.core.spi.CounterStore;
import com.example.usage.core.spi.PolicyStore;
import com.example.usage.core.spi.SummaryStore;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import static com.example.usage.core.spi.Stages.done;

/**
 * Repairs the quota counters (a cache) from usage_summary (the truth). Run it from a scheduled job on a
 * single instance; it is idempotent and safe to run twice. It reconciles only OPEN periods whose summary
 * row has not changed for {@code settle}: a younger row may have an event between the ledger commit and
 * the counter update, and repairing it would double count.
 * <ul>
 *   <li>Result metrics (REPORT_ONLY): the counter must equal the summary, so it is repaired up or down.</li>
 *   <li>Access metrics (CONSUME_ON_CHECK): the counter legitimately runs ahead of the summary, because a
 *       check counts before its event arrives, or the event was lost. So it is repaired only upward (a
 *       flushed or restarted Redis would otherwise hand every tenant a fresh quota); counters ahead are
 *       reported as {@code ahead} and left alone.</li>
 * </ul>
 * A row that fails is counted and does not stop the run. Rate and concurrency counters are not reconciled:
 * they live seconds, not periods.
 */
public final class CounterReconciler {
    /** scanned = rows examined; the rest classify those that were not already in agreement. */
    public record Report(int scanned, int repaired, int ahead, int skipped, int failed) {}

    private enum Outcome { OK, REPAIRED, AHEAD, SKIPPED, FAILED }

    private final PolicyStore policies;
    private final CounterStore counters;
    private final SummaryStore summaries;
    private final Clock clock;
    private final Duration settle;

    /** @param settle how long a summary row must be quiet before it is trusted; pick well above the consumer's latency and the app/database clock skew. */
    public CounterReconciler(PolicyStore policies, CounterStore counters, SummaryStore summaries, Clock clock, Duration settle) {
        this.policies = policies;
        this.counters = counters;
        this.summaries = summaries;
        this.clock = clock;
        this.settle = settle;
    }

    public CompletionStage<Report> run() {
        return summaries.openSettledBefore(clock.instant().minus(settle)).thenCompose(rows -> {
            CompletionStage<Report> acc = done(new Report(rows.size(), 0, 0, 0, 0));
            for (SummaryStore.Summary s : rows) {
                acc = acc.thenCompose(report -> reconcile(s).thenApply(outcome -> add(report, outcome)));
            }
            return acc;
        });
    }

    private CompletionStage<Outcome> reconcile(SummaryStore.Summary s) {
        return policies.policyFor(s.tenant())
                .thenCompose(policy -> compare(policy, s))
                .exceptionally(ex -> Outcome.FAILED);
    }

    private CompletionStage<Outcome> compare(Optional<Policy> policy, SummaryStore.Summary s) {
        Optional<Limit> limit = policy.flatMap(p -> p.limitsFor(s.metricCode()).stream()
                .filter(l -> l.type() == Type.QUOTA && PeriodCalculator.windowKey(l.window()).equals(s.windowKey()))
                .findFirst());
        if (limit.isEmpty()) return done(Outcome.SKIPPED); // the limit was removed or changed: nothing owns this counter
        String key = UsageEngine.counterKey(Type.QUOTA, s.tenant(), s.metricCode(), s.windowKey(), s.period().start());
        // The snapshot is only a list of candidates: it was taken before the run walked here, and an event or a
        // compensation may have landed since. Read the row again, and leave it alone if it moved or closed; the next run
        // sees it quiet. Repairing from the old total would be wrong by exactly what happened in between.
        return summaries.find(s.tenant(), s.metricCode(), s.windowKey(), s.period().start()).thenCompose(fresh -> {
            if (fresh.isEmpty() || fresh.get().closed() || fresh.get().total() != s.total()) return done(Outcome.SKIPPED);
            return counters.get(key).thenCompose(actual -> fix(limit.get(), key, fresh.get().total(), actual));
        });
    }

    private CompletionStage<Outcome> fix(Limit limit, String key, long expected, long actual) {
        if (actual == expected) return done(Outcome.OK);
        if (actual > expected && limit.metric().mode() == Mode.CONSUME_ON_CHECK) return done(Outcome.AHEAD);
        // relative delta, so an update that lands meanwhile is kept instead of overwritten
        return counters.add(key, expected - actual, Long.MAX_VALUE, UsageEngine.quotaTtl(limit.window()))
                .thenApply(r -> Outcome.REPAIRED);
    }

    private static Report add(Report r, Outcome o) {
        return switch (o) {
            case OK -> r;
            case REPAIRED -> new Report(r.scanned(), r.repaired() + 1, r.ahead(), r.skipped(), r.failed());
            case AHEAD -> new Report(r.scanned(), r.repaired(), r.ahead() + 1, r.skipped(), r.failed());
            case SKIPPED -> new Report(r.scanned(), r.repaired(), r.ahead(), r.skipped() + 1, r.failed());
            case FAILED -> new Report(r.scanned(), r.repaired(), r.ahead(), r.skipped(), r.failed() + 1);
        };
    }

}
