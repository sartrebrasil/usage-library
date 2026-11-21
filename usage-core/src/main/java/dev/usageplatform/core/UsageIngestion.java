package dev.usageplatform.core;

import dev.usageplatform.core.model.Compensation;
import dev.usageplatform.core.model.Limit.Type;
import dev.usageplatform.core.model.Metric.Mode;
import dev.usageplatform.core.model.PeriodCalculator;
import dev.usageplatform.core.model.Policy;
import dev.usageplatform.core.model.UsageEvent;
import dev.usageplatform.core.spi.CounterStore;
import dev.usageplatform.core.spi.Ledger;
import dev.usageplatform.core.spi.PolicyStore;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static dev.usageplatform.core.spi.Stages.done;

/**
 * The write side of the {@link UsageEngine}: records usage and reversals in the ledger (the source of truth), then
 * moves the counters (a cache). Kept apart from the request path ({@code check}, {@code acquire}), which only reads
 * and reserves counters.
 */
final class UsageIngestion {
    private final PolicyStore policies;
    private final CounterStore counters;
    private final Ledger ledger;

    UsageIngestion(PolicyStore policies, CounterStore counters, Ledger ledger) {
        this.policies = policies;
        this.counters = counters;
        this.ledger = ledger;
    }

    CompletionStage<UsageEngine.Ingest> ingest(UsageEvent e) {
        return policies.policyFor(e.tenant()).thenCompose(opt -> recordAndCount(opt, e));
    }

    CompletionStage<UsageEngine.Compensate> compensate(Compensation c) {
        return ledger.find(c.compensates()).thenCompose(found -> found.isEmpty()
                ? done(UsageEngine.Compensate.ORIGINAL_NOT_FOUND)
                : reverse(c, found.get()));
    }

    private CompletionStage<UsageEngine.Compensate> reverse(Compensation c, UsageEvent original) {
        boolean consistent = original.tenant().equals(c.tenant())
                && original.metricCode().equals(c.metricCode())
                && c.quantity() <= original.quantity();
        if (!consistent) return done(UsageEngine.Compensate.INVALID);
        // the ledger reverses, and hands back, the slots the original was recorded in: the counters are lowered in
        // exactly those, whatever the policy says today
        return ledger.compensate(c, original).thenCompose(out -> switch (out.result()) {
            case DUPLICATE -> done(UsageEngine.Compensate.DUPLICATE);
            case ALREADY_COMPENSATED -> done(UsageEngine.Compensate.ALREADY_COMPENSATED);
            case APPLIED -> lowerCounters(out.slots(), original, c.quantity())
                    .handle((v, ex) -> ex == null ? UsageEngine.Compensate.APPLIED : UsageEngine.Compensate.COUNTER_DRIFT);
        });
    }

    private CompletionStage<Void> lowerCounters(List<Ledger.Slot> slots, UsageEvent original, long quantity) {
        CompletableFuture<?>[] subs = slots.stream()
                .map(s -> counters.add(UsageEngine.counterKey(Type.QUOTA, original.tenant(), original.metricCode(), s.windowKey(), s.period().start()),
                        -quantity, Long.MAX_VALUE, UsageEngine.quotaTtl(Duration.between(s.period().start(), s.period().end())))
                        .toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(subs);
    }

    private CompletionStage<UsageEngine.Ingest> recordAndCount(Optional<Policy> policy, UsageEvent e) {
        return ledger.record(e, slotsFor(policy, e)).thenCompose(isNew -> isNew
                ? moveCounters(policy, e).handle((v, ex) -> ex == null ? UsageEngine.Ingest.APPLIED : UsageEngine.Ingest.COUNTER_DRIFT)
                : done(UsageEngine.Ingest.DUPLICATE));
    }

    private static List<Ledger.Slot> slotsFor(Optional<Policy> policy, UsageEvent e) {
        if (policy.isEmpty()) return List.of();
        Policy p = policy.get();
        return p.limitsFor(e.metricCode()).stream()
                .filter(l -> l.type() == Type.QUOTA)
                .map(l -> new Ledger.Slot(PeriodCalculator.windowKey(l.window()),
                        PeriodCalculator.periodOf(l.window(), p.anchor(), e.occurredAt()), p.plan()))
                .distinct()
                .toList();
    }

    private CompletionStage<Void> moveCounters(Optional<Policy> opt, UsageEvent e) {
        if (opt.isEmpty()) return done(null);
        Policy p = opt.get();
        CompletableFuture<?>[] adds = p.limitsFor(e.metricCode()).stream()
                .filter(l -> l.type() == Type.QUOTA && l.metric().mode() == Mode.REPORT_ONLY)
                .map(l -> counters.add(UsageEngine.key(e.tenant(), l, p.anchor(), e.occurredAt()), e.quantity(),
                        Long.MAX_VALUE, UsageEngine.quotaTtl(l.window())).toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(adds);
    }
}
