package com.example.usage.core;

import com.example.usage.core.model.Compensation;
import com.example.usage.core.model.Decision;
import com.example.usage.core.model.Limit;
import com.example.usage.core.model.Limit.Enforcement;
import com.example.usage.core.model.Limit.Type;
import com.example.usage.core.model.Metric.Mode;
import com.example.usage.core.model.PeriodCalculator;
import com.example.usage.core.model.Permit;
import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.model.UsageEvent;
import com.example.usage.core.model.Window;
import com.example.usage.core.spi.CounterStore;
import com.example.usage.core.spi.Ledger;
import com.example.usage.core.spi.PolicyStore;
import com.example.usage.core.spi.UsageSink;
import com.example.usage.core.spi.UsageTelemetry;
import com.example.usage.core.spi.UsageTelemetry.Op;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Set;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.example.usage.core.spi.Stages.done;

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

    /** Largest cost accepted by {@link #check} and {@link #acquire}. */
    public static final long MAX_COST = UsageEvent.MAX_QUANTITY;

    private record Applied(String key, Duration ttl) {}

    private static final System.Logger LOG = System.getLogger(UsageEngine.class.getName());
    static final Duration QUOTA_GRACE = Duration.ofDays(7);

    private final PolicyStore policies;
    private final CounterStore counters;
    private final Ledger ledger;
    private final UsageSink sink;
    private final Clock clock;
    private final FailMode failMode;
    private final Duration lease;
    private final UsageIngestion writes;
    private final UsageTelemetry telemetry;
    // a store outage degrades every request: log it once per interval, not once per request
    private final LogThrottle degradedLog = new LogThrottle(Duration.ofSeconds(30), System.nanoTime());

    /** @param lease how long a concurrency slot lives if its holder crashes without releasing it (each holder has its own). */
    public UsageEngine(PolicyStore policies, CounterStore counters, Ledger ledger, UsageSink sink,
                       Clock clock, FailMode failMode, Duration lease) {
        this(policies, counters, ledger, sink, clock, failMode, lease, UsageTelemetry.NOOP);
    }

    /** @param telemetry receives the metrics and spans of every operation; adapters reuse it through {@link #telemetry()}. */
    public UsageEngine(PolicyStore policies, CounterStore counters, Ledger ledger, UsageSink sink,
                       Clock clock, FailMode failMode, Duration lease, UsageTelemetry telemetry) {
        this.telemetry = telemetry;
        this.policies = policies;
        this.counters = counters;
        this.ledger = ledger;
        this.sink = sink;
        this.clock = clock;
        this.failMode = failMode;
        this.lease = lease;
        this.writes = new UsageIngestion(policies, counters, ledger);
    }

    /** The telemetry this engine reports to, for the adapters built around it. */
    public UsageTelemetry telemetry() {
        return telemetry;
    }

    /** Ends {@code op} when {@code stage} completes; tags it from the result. Telemetry never changes the outcome. */
    private <T> CompletionStage<T> observed(Op op, CompletionStage<T> stage, java.util.function.BiConsumer<Op, T> tagger) {
        return stage.whenComplete((value, ex) -> {
            try {
                if (ex == null) tagger.accept(op, value);
                op.end(ex);
            } catch (RuntimeException ignored) {
                // a broken metrics backend must not fail a decision
            }
        });
    }

    private static void tagDecision(Op op, Decision d) {
        op.tag("result", d.allowed() ? "allowed" : "denied")
                .tag("reason", d.reason() == null ? "none" : d.reason())
                .tag("overage", String.valueOf(d.overage()));
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
        return check(tenant, metricCode, cost, at, EnumSet.allOf(Type.class));
    }

    /**
     * Same, but only the limits of the given types take part: the rate limit alone, early in a request, and the quota alone, once the
     * work is known to be accepted. A tenant with no policy is refused whichever types are asked for. Pass the same types, and the same
     * {@code at}, to {@link #undoCheck(TenantId, String, long, Instant, Set)}.
     */
    public CompletionStage<Decision> check(TenantId tenant, String metricCode, long cost, Instant at, Set<Type> only) {
        requirePositive(cost);
        Op op = telemetry.start("check", "metric", metricCode);
        return observed(op, op.inScope(() -> policies.policyFor(tenant)
                .thenCompose(opt -> opt.isPresent()
                        ? evaluate(opt.get(), tenant, metricCode, cost, at, only)
                        : done(Decision.deny(Decision.NO_POLICY)))
                .exceptionally(this::degraded)), UsageEngine::tagDecision);
    }

    /** A store that fails is not silent: fail-mode decides the answer, and the cause is logged, at most once per interval. */
    private Decision degraded(Throwable ex) {
        Throwable cause = ex instanceof CompletionException && ex.getCause() != null ? ex.getCause() : ex;
        // a policy that cannot even be built (an invalid limit) is a configuration bug, not an outage: say so, loudly, with the stack
        boolean invalidPolicy = cause instanceof IllegalArgumentException;
        String reason = invalidPolicy ? Decision.INVALID_POLICY : Decision.STORE_UNAVAILABLE;
        long suppressed = degradedLog.admit(System.nanoTime());
        if (suppressed >= 0) {
            String more = suppressed == 0 ? "" : " (" + suppressed + " similar decisions since the last log line)";
            if (invalidPolicy) LOG.log(System.Logger.Level.ERROR, "usage policy is invalid, fail-mode " + failMode + more, cause);
            else LOG.log(System.Logger.Level.WARNING, "usage store unavailable, fail-mode " + failMode + more + ": " + cause);
        }
        return failMode == FailMode.OPEN ? Decision.allowDegraded(reason) : Decision.deny(reason);
    }

    private CompletionStage<Decision> evaluate(Policy p, TenantId t, String metric, long cost, Instant at, Set<Type> only) {
        // METER measures for the summary and decides nothing
        List<Limit> limits = p.limitsFor(metric).stream().filter(l -> l.type() != Type.METER && only.contains(l.type()))
                .sorted(Comparator.comparing(Limit::type)).toList();
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
        return undoCheck(tenant, metricCode, cost, at, EnumSet.allOf(Type.class));
    }

    /** Gives back only what a {@link #check(TenantId, String, long, Instant, Set) check} of the same types consumed. */
    public CompletionStage<Void> undoCheck(TenantId tenant, String metricCode, long cost, Instant at, Set<Type> only) {
        requirePositive(cost);
        return policies.policyFor(tenant).thenCompose(opt -> opt.isEmpty() ? done(null) : undo(opt.get(), tenant, metricCode, cost, at, only));
    }

    private CompletionStage<Void> undo(Policy p, TenantId t, String metric, long cost, Instant at, Set<Type> only) {
        CompletableFuture<?>[] undone = p.limitsFor(metric).stream()
                .filter(l -> !readOnly(l) && only.contains(l.type()))
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
                recordRatio(l, used);
                if (over && l.enforcement() == Enforcement.REJECT) return denyAfterRollback(applied, cost, l, p, now);
                return step(ls, i + 1, p, t, cost, now, applied, overage || over);
            });
        }
        long cap = l.enforcement() == Enforcement.REJECT ? l.value() : Long.MAX_VALUE;
        return counters.add(key, cost, cap, ttl(l)).thenCompose(r -> {
            recordRatio(l, r.value());
            if (!r.applied()) return denyAfterRollback(applied, cost, l, p, now);
            applied.add(new Applied(key, ttl(l)));
            return step(ls, i + 1, p, t, cost, now, applied, overage || r.value() > l.value());
        });
    }

    /** The refusal stands even if giving the counters back fails: a failed undo must never turn a refusal into an allow. */
    private CompletionStage<Decision> denyAfterRollback(List<Applied> applied, long cost, Limit l, Policy p, Instant at) {
        return undoQuietly(applied, cost).thenApply(v -> Decision.deny(reason(l)).withRetryAfter(retryAfter(l, p, at)));
    }

    /** Until the period the refusing counter belongs to ends (rate and quota); nothing for a concurrency slot, which frees when a job ends. */
    private static Duration retryAfter(Limit l, Policy p, Instant at) {
        if (l.window() == null) return null;
        Duration wait = Duration.between(at, PeriodCalculator.periodOf(l.window(), p.anchor(), at).end());
        return wait.isNegative() ? Duration.ZERO : wait;
    }

    /** How full a quota is, seen at the moment of a check: the metric that tells how close the tenants that are issuing are to their limit. */
    private void recordRatio(Limit l, long used) {
        if (l.type() == Type.QUOTA && l.value() > 0) telemetry.record("quota.ratio", (double) used / l.value(), "metric", l.metric().code());
    }

    private CompletionStage<Void> undoQuietly(List<Applied> applied, long cost) {
        CompletableFuture<?>[] undo = applied.stream()
                .map(a -> counters.add(a.key(), -cost, Long.MAX_VALUE, a.ttl()).toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(undo).handle((v, ex) -> (Void) null);
    }

    /**
     * What a tenant has used so far of the limit of the given type on the metric, in the period that contains {@code at}: the largest total
     * among the limits of that type (a tenant with a daily and a monthly quota reports the fuller one). 0 when there is none or no policy.
     * It reads the counters, so it is as exact as they are.
     */
    public CompletionStage<Long> usageOf(TenantId tenant, String metricCode, Type type, Instant at) {
        return policies.policyFor(tenant).thenCompose(opt -> {
            if (opt.isEmpty()) return done(0L);
            Policy p = opt.get();
            CompletableFuture<Long>[] reads = p.limitsFor(metricCode).stream().filter(l -> l.type() == type)
                    .map(l -> counters.get(key(tenant, l, p.anchor(), at)).toCompletableFuture()).toArray(CompletableFuture[]::new);
            return CompletableFuture.allOf(reads).thenApply(v -> java.util.Arrays.stream(reads).mapToLong(CompletableFuture::join).max().orElse(0L));
        });
    }

    // ---- concurrency -------------------------------------------------------------------------

    public CompletionStage<Permit> acquire(TenantId tenant, String metricCode, long cost) {
        requirePositive(cost);
        Op op = telemetry.start("acquire", "metric", metricCode);
        return observed(op, op.inScope(() -> policies.policyFor(tenant)
                .thenCompose(opt -> acquireWith(opt, tenant, metricCode, cost))
                .exceptionally(ex -> permit(degraded(ex), () -> done(null)))), (o, permit) -> tagDecision(o, permit.decision()));
    }

    private CompletionStage<Permit> acquireWith(Optional<Policy> opt, TenantId t, String metric, long cost) {
        if (opt.isEmpty()) return done(permit(Decision.deny(Decision.NO_POLICY), () -> done(null)));
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
                        : permit(Decision.deny(Decision.CONCURRENCY_EXCEEDED).withPlan(p.plan()), () -> done(null)));
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
        Op op = telemetry.start("ingest", "metric", e.metricCode()).eventId(e.eventId().toString());
        return observed(op, op.inScope(() -> writes.ingest(e)), (o, r) -> o.tag("result", r.name().toLowerCase()));
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
        Op op = telemetry.start("compensate", "metric", c.metricCode()).eventId(c.eventId().toString());
        return observed(op, op.inScope(() -> writes.compensate(c)), (o, r) -> o.tag("result", r.name().toLowerCase()));
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
        return quotaTtl(PeriodCalculator.maxLength(window));
    }

    /** A quota counter outlives its period by {@link #QUOTA_GRACE}, so late events and compensations still find it. */
    static Duration quotaTtl(Duration periodLength) {
        return periodLength.plus(QUOTA_GRACE);
    }

    private Duration ttl(Limit l) {
        return switch (l.type()) {
            case RATE -> PeriodCalculator.maxLength(l.window());
            case QUOTA -> quotaTtl(l.window());
            case CONCURRENCY -> lease;
            case METER -> throw new IllegalStateException("a METER limit has no counter");
        };
    }

    private static boolean readOnly(Limit l) {
        return l.type() == Type.CONCURRENCY || l.type() == Type.METER || l.metric().mode() == Mode.REPORT_ONLY;
    }

    private static String reason(Limit l) {
        return switch (l.type()) {
            case RATE -> Decision.RATE_LIMITED;
            case QUOTA -> Decision.QUOTA_EXCEEDED;
            case CONCURRENCY -> Decision.CONCURRENCY_EXCEEDED;
            case METER -> throw new IllegalStateException("a METER limit never refuses");
        };
    }

    private static void requirePositive(long cost) {
        if (cost <= 0 || cost > MAX_COST) throw new IllegalArgumentException("cost must be in 1.." + MAX_COST);
    }

}
