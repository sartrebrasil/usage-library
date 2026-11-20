package dev.usageplatform.core;

import dev.usageplatform.core.Limit.Enforcement;
import dev.usageplatform.core.Limit.Type;
import dev.usageplatform.core.Metric.Mode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * The decision engine. Adapters (gateway filter, servlet filter, queue consumer) only translate
 * their world into these four calls:
 * <ul>
 *   <li>{@link #check}: may this tenant consume {@code cost} of this metric now? (quantity, never "1 request")</li>
 *   <li>{@link #acquire}: take a concurrency slot while a long job runs.</li>
 *   <li>{@link #report}: tell the platform something was consumed (after the result is persisted).</li>
 *   <li>{@link #ingest}: central consumer side: record durably, then move counters of REPORT_ONLY metrics.</li>
 * </ul>
 * shortcut: limits of one check are applied one after another with rollback of the ones already applied,
 * not in one atomic script; move to a single Lua call per check if contention shows up.
 */
public final class UsageEngine {
    public enum FailMode { OPEN, CLOSED }

    /** COUNTER_DRIFT: ledger has the event but counters were not moved; reconciliation must repair it. */
    public enum Ingest { DUPLICATE, APPLIED, COUNTER_DRIFT }

    /**
     * ORIGINAL_NOT_FOUND: retry later. ALREADY_COMPENSATED and INVALID: dead-letter.
     * COUNTER_DRIFT: ledger done, counters not lowered; reconciliation repairs it.
     */
    public enum Compensate { APPLIED, DUPLICATE, ORIGINAL_NOT_FOUND, ALREADY_COMPENSATED, INVALID, COUNTER_DRIFT }

    private record Applied(String key, Duration ttl) {}

    private static final Duration QUOTA_GRACE = Duration.ofDays(7);

    private final PolicyStore policies;
    private final CounterStore counters;
    private final Ledger ledger;
    private final UsageSink sink;
    private final Clock clock;
    private final FailMode failMode;
    private final Duration lease;

    /** @param lease how long a concurrency slot lives if its holder crashes without releasing it (each holder has its own). */
    public UsageEngine(PolicyStore policies, CounterStore counters, Ledger ledger, UsageSink sink,
                       Clock clock, FailMode failMode, Duration lease) {
        this.policies = policies;
        this.counters = counters;
        this.ledger = ledger;
        this.sink = sink;
        this.clock = clock;
        this.failMode = failMode;
        this.lease = lease;
    }

    // ---- check -------------------------------------------------------------------------------

    public CompletionStage<Decision> check(TenantId tenant, String metricCode, long cost) {
        return check(tenant, metricCode, cost, clock.instant());
    }

    /**
     * Same as {@link #check(TenantId, String, long)} for the instant {@code at}. A caller that may have to
     * {@link #undoCheck} passes the same instant to both, so a request that straddles a period boundary
     * is undone in the period it was charged to.
     */
    public CompletionStage<Decision> check(TenantId tenant, String metricCode, long cost, Instant at) {
        requirePositive(cost);
        return policies.policyFor(tenant)
                .thenCompose(opt -> opt.isPresent()
                        ? evaluate(opt.get(), tenant, metricCode, cost, at)
                        : done(Decision.deny("no_policy")))
                .exceptionally(ex -> failMode == FailMode.OPEN
                        ? Decision.allowDegraded("store_unavailable")
                        : Decision.deny("store_unavailable"));
    }

    private CompletionStage<Decision> evaluate(Policy p, TenantId t, String metric, long cost, Instant at) {
        List<Limit> limits = p.limitsFor(metric).stream().sorted(Comparator.comparing(Limit::type)).toList();
        List<Applied> applied = new ArrayList<>();
        return step(limits, 0, p, t, cost, at, applied, false)
                .thenApply(d -> d.withPlan(p.plan()))
                // a store failing half way must not leave the earlier limits charged: undo them, then let fail-mode decide
                .exceptionallyCompose(ex -> undoQuietly(applied, cost).thenCompose(v -> CompletableFuture.<Decision>failedFuture(ex)));
    }

    /**
     * Gives back what a successful {@code check} consumed (rate and quota counters of an access metric), for
     * when a later check of the same request is refused: a refused request must not cost quota. Use the same
     * {@code at} as the check. Result metrics and concurrency are read-only in {@code check}, so nothing to undo.
     */
    public CompletionStage<Void> undoCheck(TenantId tenant, String metricCode, long cost, Instant at) {
        requirePositive(cost);
        return policies.policyFor(tenant).thenCompose(opt -> opt.isEmpty() ? done(null) : undo(opt.get(), tenant, metricCode, cost, at));
    }

    private CompletionStage<Void> undo(Policy p, TenantId t, String metric, long cost, Instant at) {
        CompletableFuture<?>[] undone = p.limitsFor(metric).stream()
                .filter(l -> !readOnly(l))
                .map(l -> counters.add(key(t, l, p.anchor(), at), -cost, Long.MAX_VALUE, ttl(l)).toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(undone);
    }

    private CompletionStage<Decision> step(List<Limit> ls, int i, Policy p, TenantId t, long cost,
                                           Instant now, List<Applied> applied, boolean overage) {
        if (i == ls.size()) return done(overage ? Decision.allowOverage() : Decision.allow());
        Limit l = ls.get(i);
        String key = key(t, l, p.anchor(), now);

        if (readOnly(l)) { // only what was already consumed counts: no reservation, overshoot is accepted
            CompletionStage<Long> held = l.type() == Type.CONCURRENCY ? counters.leasesInUse(key) : counters.get(key);
            return held.thenCompose(used -> {
                boolean over = used >= l.value();
                if (over && l.enforcement() == Enforcement.REJECT) return denyAfterRollback(applied, cost, l);
                return step(ls, i + 1, p, t, cost, now, applied, overage || over);
            });
        }
        long cap = l.enforcement() == Enforcement.REJECT ? l.value() : Long.MAX_VALUE;
        return counters.add(key, cost, cap, ttl(l)).thenCompose(r -> {
            if (!r.applied()) return denyAfterRollback(applied, cost, l);
            applied.add(new Applied(key, ttl(l)));
            return step(ls, i + 1, p, t, cost, now, applied, overage || r.value() > l.value());
        });
    }

    /** The refusal stands even if giving the counters back fails: a failed undo must never turn a refusal into an allow. */
    private CompletionStage<Decision> denyAfterRollback(List<Applied> applied, long cost, Limit l) {
        return undoQuietly(applied, cost).thenApply(v -> Decision.deny(reason(l)));
    }

    private CompletionStage<Void> undoQuietly(List<Applied> applied, long cost) {
        CompletableFuture<?>[] undo = applied.stream()
                .map(a -> counters.add(a.key(), -cost, Long.MAX_VALUE, a.ttl()).toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(undo).handle((v, ex) -> (Void) null);
    }

    // ---- concurrency -------------------------------------------------------------------------

    public CompletionStage<Permit> acquire(TenantId tenant, String metricCode, long cost) {
        requirePositive(cost);
        return policies.policyFor(tenant)
                .thenCompose(opt -> acquireWith(opt, tenant, metricCode, cost))
                .exceptionally(ex -> failMode == FailMode.OPEN
                        ? permit(Decision.allowDegraded("store_unavailable"), () -> done(null))
                        : permit(Decision.deny("store_unavailable"), () -> done(null)));
    }

    private CompletionStage<Permit> acquireWith(Optional<Policy> opt, TenantId t, String metric, long cost) {
        if (opt.isEmpty()) return done(permit(Decision.deny("no_policy"), () -> done(null)));
        Policy p = opt.get();
        Optional<Limit> limit = p.limitsFor(metric).stream().filter(l -> l.type() == Type.CONCURRENCY).findFirst();
        if (limit.isEmpty()) return done(permit(Decision.allow().withPlan(p.plan()), () -> done(null)));
        String key = key(t, limit.get(), p.anchor(), clock.instant());
        // one lease per holder: a crashed holder frees only its own slot when its lease runs out, and a release
        // can never remove a slot that belongs to someone else
        String holder = UUID.randomUUID().toString();
        return counters.acquireLease(key, holder, cost, limit.get().value(), lease)
                // a command that timed out on our side may still have run in the store: give the slot back, or it stays
                // taken, under an id nobody knows, for the whole lease
                .exceptionallyCompose(ex -> releaseQuietly(key, holder).thenCompose(v -> CompletableFuture.<CounterStore.Lease>failedFuture(ex)))
                .thenApply(r -> r.granted()
                        ? permit(Decision.allow().withPlan(p.plan()), () -> counters.releaseLease(key, holder))
                        : permit(Decision.deny("concurrency_exceeded").withPlan(p.plan()), () -> done(null)));
    }

    private CompletionStage<Void> releaseQuietly(String key, String holder) {
        try {
            return counters.releaseLease(key, holder).handle((v, ex) -> (Void) null);
        } catch (RuntimeException e) {
            return done(null);
        }
    }

    private static Permit permit(Decision d, Supplier<CompletionStage<Void>> onRelease) {
        AtomicBoolean released = new AtomicBoolean();
        return new Permit() {
            @Override public Decision decision() { return d; }
            @Override public CompletionStage<Void> release() {
                if (!released.compareAndSet(false, true)) return done(null);
                // a failed release must stay retryable, or the slot leaks until the lease runs out
                return onRelease.get().whenComplete((v, ex) -> { if (ex != null) released.set(false); });
            }
        };
    }

    // ---- report / ingest ---------------------------------------------------------------------

    /** Call only after the business result is persisted (through the outbox in production). */
    public CompletionStage<Void> report(UsageEvent event) {
        return sink.publish(event);
    }

    /**
     * Ledger first (source of truth, dedup by eventId), counters second (cache).
     * <ul>
     *   <li>Summaries: the ledger adds the event to the summary of every QUOTA window of the metric,
     *       atomically with recording it. Access and result metrics alike.</li>
     *   <li>Counters: only QUOTA counters of REPORT_ONLY metrics move here; CONSUME_ON_CHECK metrics
     *       were already consumed by {@link #check}.</li>
     * </ul>
     * The policy is read before anything is written, so if it is unavailable the call fails and nothing
     * is recorded (the broker redelivers). A tenant without policy is still recorded, with no summaries.
     * shortcut: periods come from the policy anchor in force now (an event of a previous subscription is cut
     * with the new anchor), and summaries follow the limits in force now; both can be rebuilt from the ledger,
     * which keeps every event.
     */
    public CompletionStage<Ingest> ingest(UsageEvent e) {
        return policies.policyFor(e.tenant()).thenCompose(opt -> recordAndCount(opt, e));
    }

    /** Tell the platform to reverse a previous usage (after the business reversal is persisted, via the outbox). */
    public CompletionStage<Void> reportCompensation(Compensation compensation) {
        return sink.publish(compensation);
    }

    /**
     * Applies a reversal. The original must already be in the ledger; if it is not (events of different
     * reporters have no global order) the answer is ORIGINAL_NOT_FOUND and the caller should retry later,
     * not drop the message. The reversal is charged to the original's period, in the summaries and in the
     * counters (for access metrics too: this is how a refunded request gives its quota back).
     * Counters never go below zero.
     */
    public CompletionStage<Compensate> compensate(Compensation c) {
        return ledger.find(c.compensates()).thenCompose(found -> found.isEmpty()
                ? done(Compensate.ORIGINAL_NOT_FOUND)
                : reverse(c, found.get()));
    }

    private CompletionStage<Compensate> reverse(Compensation c, UsageEvent original) {
        boolean consistent = original.tenant().equals(c.tenant())
                && original.metricCode().equals(c.metricCode())
                && c.quantity() <= original.quantity();
        if (!consistent) return done(Compensate.INVALID);
        // the ledger reverses, and hands back, the slots the original was recorded in: the counters are lowered in
        // exactly those, whatever the policy says today
        return ledger.compensate(c, original).thenCompose(out -> switch (out.result()) {
            case DUPLICATE -> done(Compensate.DUPLICATE);
            case ALREADY_COMPENSATED -> done(Compensate.ALREADY_COMPENSATED);
            case APPLIED -> lowerCounters(out.slots(), original, c.quantity())
                    .handle((v, ex) -> ex == null ? Compensate.APPLIED : Compensate.COUNTER_DRIFT);
        });
    }

    private CompletionStage<Void> lowerCounters(List<Ledger.Slot> slots, UsageEvent original, long quantity) {
        CompletableFuture<?>[] subs = slots.stream()
                .map(s -> counters.add(counterKey(Type.QUOTA, original.tenant(), original.metricCode(), s.windowKey(), s.period().start()),
                        -quantity, Long.MAX_VALUE, Duration.between(s.period().start(), s.period().end()).plus(QUOTA_GRACE))
                        .toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(subs);
    }

    private CompletionStage<Ingest> recordAndCount(Optional<Policy> policy, UsageEvent e) {
        return ledger.record(e, slotsFor(policy, e)).thenCompose(isNew -> isNew
                ? moveCounters(policy, e).handle((v, ex) -> ex == null ? Ingest.APPLIED : Ingest.COUNTER_DRIFT)
                : done(Ingest.DUPLICATE));
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
                .map(l -> counters.add(key(e.tenant(), l, p.anchor(), e.occurredAt()), e.quantity(),
                        Long.MAX_VALUE, ttl(l)).toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(adds);
    }

    // ---- helpers -----------------------------------------------------------------------------

    /** Package-private so tests can inspect counters. Window length is in the key so daily and monthly quotas never collide. */
    static String key(TenantId t, Limit l, Instant anchor, Instant at) {
        if (l.type() == Type.CONCURRENCY) return "concurrency:" + escape(t.value()) + ":" + escape(l.metric().code());
        Instant start = PeriodCalculator.periodOf(l.window(), anchor, at).start();
        return counterKey(l.type(), t, l.metric().code(), PeriodCalculator.windowKey(l.window()), start);
    }

    /** The one definition of a period counter's key, shared with the reconciler. */
    static String counterKey(Type type, TenantId t, String metricCode, String windowKey, Instant periodStart) {
        return type.name().toLowerCase() + ":" + escape(t.value()) + ":" + escape(metricCode) + ":" + windowKey + ":" + periodStart.getEpochSecond();
    }

    /** ':' separates the key parts, so it cannot appear inside one: tenant "a:b" + metric "c" must never equal tenant "a" + metric "b:c". */
    private static String escape(String part) {
        return part.replace("%", "%25").replace(":", "%3A");
    }

    static Duration quotaTtl(Window window) {
        return PeriodCalculator.maxLength(window).plus(QUOTA_GRACE);
    }

    private Duration ttl(Limit l) {
        return switch (l.type()) {
            case RATE -> PeriodCalculator.maxLength(l.window());
            case QUOTA -> quotaTtl(l.window());
            case CONCURRENCY -> lease;
        };
    }

    private static boolean readOnly(Limit l) {
        return l.type() == Type.CONCURRENCY || l.metric().mode() == Mode.REPORT_ONLY;
    }

    private static String reason(Limit l) {
        return switch (l.type()) {
            case RATE -> "rate_limited";
            case QUOTA -> "quota_exceeded";
            case CONCURRENCY -> "concurrency_exceeded";
        };
    }

    /** Largest cost accepted by {@link #check} and {@link #acquire}. */
    public static final long MAX_COST = UsageEvent.MAX_QUANTITY;

    private static void requirePositive(long cost) {
        if (cost <= 0 || cost > MAX_COST) throw new IllegalArgumentException("cost must be in 1.." + MAX_COST);
    }

    private static <T> CompletionStage<T> done(T value) {
        return CompletableFuture.completedFuture(value);
    }
}
