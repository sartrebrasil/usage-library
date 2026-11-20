package dev.usageplatform.core;

import dev.usageplatform.core.CounterReconciler.Report;
import dev.usageplatform.core.Limit.Enforcement;
import dev.usageplatform.core.Limit.Type;
import dev.usageplatform.core.Metric.Mode;
import dev.usageplatform.core.UsageEngine.FailMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.*;

class CounterReconcilerTest {
    static final Metric ITEMS = new Metric("ITEMS_PROCESSED", Mode.REPORT_ONLY);
    static final Metric API = new Metric("API_REQUESTS", Mode.CONSUME_ON_CHECK);
    static final Instant ANCHOR = Instant.parse("2026-11-01T00:00:00Z");
    static final Instant DAY_START = Instant.parse("2026-11-19T00:00:00Z");
    static final Duration SETTLE = Duration.ofMinutes(5);

    static final class TestClock extends Clock {
        Instant now = Instant.parse("2026-11-19T12:00:00Z");
        void advance(Duration d) { now = now.plus(d); }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    TestClock clock;
    InMemoryCounterStore counters;
    InMemoryLedger ledger;
    Policy policy;

    @BeforeEach
    void setUp() {
        clock = new TestClock();
        counters = new InMemoryCounterStore(clock);
        ledger = new InMemoryLedger(clock);
        policy = new Policy(ANCHOR, List.of(
                new Limit(ITEMS, Type.QUOTA, 100, Duration.ofDays(1), Enforcement.REJECT),
                new Limit(API, Type.QUOTA, 100, Duration.ofDays(1), Enforcement.REJECT)));
    }

    PolicyStore policies() { return t -> CompletableFuture.completedFuture(Optional.of(policy)); }

    UsageEngine engine() {
        return new UsageEngine(policies(), counters, ledger, e -> CompletableFuture.completedFuture(null),
                clock, FailMode.OPEN, Duration.ofMinutes(15));
    }

    Report reconcile(CounterStore store) {
        clock.advance(SETTLE.plusMinutes(1));
        return join(new CounterReconciler(policies(), store, ledger, clock, SETTLE).run());
    }

    Report reconcile() { return reconcile(counters); }

    void ingest(String tenant, Metric m, String item, long qty) {
        join(engine().ingest(new UsageEvent(UsageEvent.idFor(new TenantId(tenant), item, m.code()), new TenantId(tenant), m.code(), qty, clock.instant())));
    }

    static String key(String tenant, Metric m) {
        return UsageEngine.counterKey(Type.QUOTA, new TenantId(tenant), m.code(), "86400", DAY_START);
    }

    long counter(String tenant, Metric m) { return join(counters.get(key(tenant, m))); }

    void shift(String tenant, Metric m, long delta) {
        join(counters.add(key(tenant, m), delta, Long.MAX_VALUE, Duration.ofDays(8)));
    }

    static <X> X join(CompletionStage<X> s) { return s.toCompletableFuture().join(); }

    @Test
    void resultCounterLostByRedisIsRestoredFromTheLedger() {
        ingest("t1", ITEMS, "a", 5);
        shift("t1", ITEMS, -5); // Redis lost it
        assertEquals(new Report(1, 1, 0, 0, 0), reconcile());
        assertEquals(5, counter("t1", ITEMS));
    }

    @Test
    void resultCounterAheadOfTheLedgerIsLoweredToIt() {
        ingest("t1", ITEMS, "a", 5);
        shift("t1", ITEMS, 3); // double counted somewhere
        assertEquals(new Report(1, 1, 0, 0, 0), reconcile());
        assertEquals(5, counter("t1", ITEMS));
    }

    @Test
    void accessCounterBelowTheLedgerIsRaisedButOneAheadIsLeftAlone() {
        ingest("t1", API, "a", 4); // summary says 4, but checks never ran here, so the counter is 0
        assertEquals(new Report(1, 1, 0, 0, 0), reconcile());
        assertEquals(4, counter("t1", API));

        shift("t1", API, 5); // 9: checks counted requests whose events are late or lost
        assertEquals(new Report(1, 0, 1, 0, 0), reconcile());
        assertEquals(9, counter("t1", API));
    }

    @Test
    void secondRunHasNothingLeftToRepair() {
        ingest("t1", ITEMS, "a", 5);
        shift("t1", ITEMS, -5);
        reconcile();
        assertEquals(new Report(1, 0, 0, 0, 0), reconcile());
    }

    @Test
    void rowsStillInFlightAreNotTouched() {
        ingest("t1", ITEMS, "a", 5);
        shift("t1", ITEMS, -5);
        Report r = join(new CounterReconciler(policies(), counters, ledger, clock, SETTLE).run()); // no time passed
        assertEquals(new Report(0, 0, 0, 0, 0), r);
        assertEquals(0, counter("t1", ITEMS));
    }

    @Test
    void closedPeriodsAreIgnored() {
        ingest("t1", ITEMS, "a", 5);
        join(ledger.closeEndedBefore(Instant.parse("2026-11-20T00:00:00Z")));
        assertEquals(new Report(0, 0, 0, 0, 0), reconcile());
    }

    @Test
    void rowWhoseLimitNoLongerExistsIsSkipped() {
        ingest("t1", ITEMS, "a", 5);
        policy = new Policy(ANCHOR, List.of());
        assertEquals(new Report(1, 0, 0, 1, 0), reconcile());
    }

    @Test
    void oneFailingRowDoesNotStopTheRun() {
        ingest("t1", ITEMS, "a", 5);
        ingest("t2", ITEMS, "b", 5);
        shift("t1", ITEMS, -5);
        shift("t2", ITEMS, -5);
        CounterStore flaky = new CounterStore() {
            public CompletionStage<Result> add(String k, long d, long c, Duration t) { return counters.add(k, d, c, t); }
            public CompletionStage<Long> get(String k) {
                return k.contains(":t1:") ? CompletableFuture.failedFuture(new IllegalStateException("redis down")) : counters.get(k);
            }
        };
        Report r = reconcile(flaky);
        assertEquals(2, r.scanned());
        assertEquals(1, r.repaired());
        assertEquals(1, r.failed());
        assertEquals(5, counter("t2", ITEMS));
    }

    @Test
    void aRowThatMovedSinceTheSnapshotIsLeftForTheNextRun() {
        ingest("t1", ITEMS, "a", 5);
        shift("t1", ITEMS, -5); // counter lost its value: a repair is due
        // the snapshot says 5, but by the time the run reads the row again a compensation has taken it to 3
        SummaryStore moved = new SummaryStore() {
            public CompletionStage<Optional<Summary>> find(TenantId t, String m, String w, Instant p) {
                return ledger.find(t, m, w, p).thenApply(o -> o.map(s -> new Summary(s.tenant(), s.metricCode(), s.windowKey(),
                        s.period(), s.total() - 2, s.lateTotal(), s.credits(), s.closed(), s.plan())));
            }
            public CompletionStage<List<Summary>> openSettledBefore(Instant i) { return ledger.openSettledBefore(i); }
            public CompletionStage<Integer> closeEndedBefore(Instant c) { return ledger.closeEndedBefore(c); }
        };
        clock.advance(SETTLE.plusMinutes(1));
        Report r = join(new CounterReconciler(policies(), counters, moved, clock, SETTLE).run());
        assertEquals(new Report(1, 0, 0, 1, 0), r);
        assertEquals(0, counter("t1", ITEMS), "nothing was written from the stale total");
    }
}
