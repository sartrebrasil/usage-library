package dev.usageplatform.core;

import dev.usageplatform.core.Limit.Enforcement;
import dev.usageplatform.core.Limit.Type;
import dev.usageplatform.core.Metric.Mode;
import dev.usageplatform.core.UsageEngine.FailMode;
import dev.usageplatform.core.UsageEngine.Ingest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.*;

class UsageEngineTest {
    static final TenantId T = new TenantId("t1");
    static final Metric API = new Metric("API_REQUESTS", Mode.CONSUME_ON_CHECK);
    static final Metric ITEMS = new Metric("ITEMS_PROCESSED", Mode.REPORT_ONLY);
    static final Instant ANCHOR = Instant.parse("2026-11-01T00:00:00Z");
    static final Duration DAY = Duration.ofDays(1);

    static final class TestClock extends Clock {
        Instant now = Instant.parse("2026-11-19T00:00:00Z");
        void advance(Duration d) { now = now.plus(d); }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    TestClock clock;
    InMemoryCounterStore counters;
    InMemoryLedger ledger;

    @BeforeEach
    void setUp() {
        clock = new TestClock();
        counters = new InMemoryCounterStore(clock);
        ledger = new InMemoryLedger();
    }

    UsageEngine engine(Limit... limits) {
        return engine(counters, FailMode.OPEN, limits);
    }

    UsageEngine engine(CounterStore store, FailMode mode, Limit... limits) {
        PolicyStore policies = t -> done(Optional.of(new Policy(ANCHOR, List.of(limits))));
        return new UsageEngine(policies, store, ledger, e -> done(null), clock, mode, Duration.ofMinutes(15));
    }

    static Limit rate(long v, Duration w) { return new Limit(API, Type.RATE, v, w, Enforcement.REJECT); }
    static Limit quota(Metric m, long v, Duration w, Enforcement e) { return new Limit(m, Type.QUOTA, v, w, e); }
    static Limit conc(long v) { return Limit.concurrency(ITEMS, v); }

    @Test
    void rateLimitRejectsThenRecoversInNextWindow() {
        var e = engine(rate(3, Duration.ofSeconds(1)));
        for (int i = 0; i < 3; i++) assertTrue(join(e.check(T, "API_REQUESTS", 1)).allowed());
        Decision d = join(e.check(T, "API_REQUESTS", 1));
        assertFalse(d.allowed());
        assertEquals("rate_limited", d.reason());
        clock.advance(Duration.ofSeconds(1));
        assertTrue(join(e.check(T, "API_REQUESTS", 1)).allowed());
    }

    @Test
    void monthlyQuotaResetsOnTheAnchorDayNotOnTheFirst() {
        // policy anchor is Nov 1, so the cycle runs Nov 1 -> Dec 1
        var e = engine(new Limit(API, Type.QUOTA, 2, Window.months(1), Enforcement.REJECT));
        assertTrue(join(e.check(T, "API_REQUESTS", 2)).allowed());
        assertFalse(join(e.check(T, "API_REQUESTS", 1)).allowed());
        clock.advance(Duration.ofDays(11)); // Nov 30: still the same cycle
        assertFalse(join(e.check(T, "API_REQUESTS", 1)).allowed());
        clock.advance(Duration.ofDays(1));  // Dec 1: new cycle
        assertTrue(join(e.check(T, "API_REQUESTS", 2)).allowed());
    }

    @Test
    void dailyAndMonthlyQuotasOnTheSameMetricBothApply() {
        var e = engine(quota(API, 5, DAY, Enforcement.REJECT), new Limit(API, Type.QUOTA, 6, Window.months(1), Enforcement.REJECT));
        assertTrue(join(e.check(T, "API_REQUESTS", 5)).allowed());
        clock.advance(DAY);
        assertTrue(join(e.check(T, "API_REQUESTS", 1)).allowed());  // new day, monthly total 6
        assertFalse(join(e.check(T, "API_REQUESTS", 1)).allowed()); // monthly exhausted although today's daily is free
    }

    @Test
    void changingPlanInTheMiddleOfTheCycleKeepsWhatWasAlreadyConsumed() {
        var current = new java.util.concurrent.atomic.AtomicReference<>(
                new Policy(ANCHOR, List.of(quota(API, 5, DAY, Enforcement.REJECT))));
        var e = new UsageEngine(t -> done(Optional.of(current.get())), counters, ledger, x -> done(null),
                clock, FailMode.OPEN, Duration.ofMinutes(15));
        assertTrue(join(e.check(T, "API_REQUESTS", 4)).allowed());
        assertFalse(join(e.check(T, "API_REQUESTS", 2)).allowed());

        current.set(new Policy(ANCHOR, List.of(quota(API, 10, DAY, Enforcement.REJECT)))); // upgrade, same subscription
        assertTrue(join(e.check(T, "API_REQUESTS", 6)).allowed());  // 4 already used + 6 = 10
        assertFalse(join(e.check(T, "API_REQUESTS", 1)).allowed());
    }

    @Test
    void limitsRejectInconsistentWindows() {
        assertThrows(IllegalArgumentException.class, () -> new Limit(API, Type.RATE, 1, Window.months(1), Enforcement.REJECT));
        assertThrows(IllegalArgumentException.class, () -> new Limit(API, Type.QUOTA, 1, (Window) null, Enforcement.REJECT));
    }

    @Test
    void batchIsOneCheckWithCostN() {
        var e = engine(quota(API, 10, DAY, Enforcement.REJECT));
        assertTrue(join(e.check(T, "API_REQUESTS", 6)).allowed());
        assertFalse(join(e.check(T, "API_REQUESTS", 5)).allowed()); // 6 + 5 > 10, nothing consumed
        assertTrue(join(e.check(T, "API_REQUESTS", 4)).allowed());  // exactly 10
    }

    @Test
    void allowOverageFlagsTheExcess() {
        var e = engine(quota(API, 2, DAY, Enforcement.ALLOW_OVERAGE));
        assertFalse(join(e.check(T, "API_REQUESTS", 1)).overage());
        assertFalse(join(e.check(T, "API_REQUESTS", 1)).overage());
        Decision d = join(e.check(T, "API_REQUESTS", 1));
        assertTrue(d.allowed());
        assertTrue(d.overage());
    }

    @Test
    void resultMetricChecksOnlyWhatWasAlreadyConsumed() {
        var e = engine(quota(ITEMS, 10, DAY, Enforcement.REJECT));
        join(e.ingest(event("a", ITEMS, 9)));
        assertTrue(join(e.check(T, "ITEMS_PROCESSED", 500)).allowed()); // 9 < 10: no reservation, overshoot accepted
        join(e.ingest(event("b", ITEMS, 1)));
        Decision d = join(e.check(T, "ITEMS_PROCESSED", 1));
        assertFalse(d.allowed());
        assertEquals("quota_exceeded", d.reason());
    }

    @Test
    void ingestIsIdempotentByEventId() {
        var e = engine(quota(ITEMS, 2, DAY, Enforcement.REJECT));
        UsageEvent ev = event("same", ITEMS, 1);
        assertEquals(Ingest.APPLIED, join(e.ingest(ev)));
        assertEquals(Ingest.DUPLICATE, join(e.ingest(ev)));
        assertTrue(join(e.check(T, "ITEMS_PROCESSED", 1)).allowed()); // used = 1, not 2
    }

    @Test
    void accessMetricHasASingleOwner_ingestDoesNotTouchItsCounter() {
        var e = engine(quota(API, 2, DAY, Enforcement.REJECT));
        join(e.ingest(event("x", API, 5)));
        assertTrue(join(e.check(T, "API_REQUESTS", 1)).allowed()); // would be denied if ingest had counted it
    }

    @Test
    void failedLimitRollsBackTheOnesAlreadyApplied() {
        Limit daily = quota(API, 5, DAY, Enforcement.REJECT);
        Limit thirtyDay = quota(API, 1, Duration.ofDays(30), Enforcement.REJECT);
        var e = engine(daily, thirtyDay);
        assertTrue(join(e.check(T, "API_REQUESTS", 1)).allowed());
        assertFalse(join(e.check(T, "API_REQUESTS", 1)).allowed());
        String dailyKey = UsageEngine.key(T, daily, ANCHOR, clock.instant());
        assertEquals(1L, join(counters.get(dailyKey))); // second attempt was undone
    }

    @Test
    void concurrencyPermitsReleaseOnceAndExpireWithTheLease() {
        var e = engine(conc(2));
        Permit p1 = join(e.acquire(T, "ITEMS_PROCESSED", 1));
        join(e.acquire(T, "ITEMS_PROCESSED", 1));
        assertFalse(join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed());
        assertEquals("concurrency_exceeded", join(e.check(T, "ITEMS_PROCESSED", 1)).reason()); // gateway side only reads

        join(p1.release());
        join(p1.release()); // second release must not free another slot
        assertTrue(join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed());
        assertFalse(join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed());

        clock.advance(Duration.ofMinutes(16)); // holders crashed: the lease frees everything
        assertTrue(join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed());
    }

    @Test
    void storeFailureFollowsFailMode() {
        CounterStore broken = new CounterStore() {
            public CompletionStage<Result> add(String k, long d, long c, Duration t) { return failed(); }
            public CompletionStage<Long> get(String k) { return failed(); }
            private <X> CompletionStage<X> failed() { return CompletableFuture.failedFuture(new IllegalStateException("redis down")); }
        };
        Limit l = rate(1, Duration.ofSeconds(1));
        Decision open = join(engine(broken, FailMode.OPEN, l).check(T, "API_REQUESTS", 1));
        assertTrue(open.allowed());
        assertEquals("store_unavailable", open.reason());
        Decision closed = join(engine(broken, FailMode.CLOSED, l).check(T, "API_REQUESTS", 1));
        assertFalse(closed.allowed());
        assertEquals("store_unavailable", closed.reason());
    }

    @Test
    void counterFailureAfterLedgerIsReportedAsDriftNotLost() {
        CounterStore broken = new CounterStore() {
            public CompletionStage<Result> add(String k, long d, long c, Duration t) {
                return CompletableFuture.failedFuture(new IllegalStateException("redis down"));
            }
            public CompletionStage<Long> get(String k) { return done(0L); }
        };
        var e = engine(broken, FailMode.OPEN, quota(ITEMS, 5, DAY, Enforcement.REJECT));
        UsageEvent ev = event("y", ITEMS, 1);
        assertEquals(Ingest.COUNTER_DRIFT, join(e.ingest(ev)));
        assertEquals(Ingest.DUPLICATE, join(e.ingest(ev))); // ledger kept it: reconcile, don't redeliver
    }

    @Test
    void tenantWithoutPolicyIsDenied() {
        var e = new UsageEngine(t -> done(Optional.empty()), counters, ledger, x -> done(null),
                clock, FailMode.OPEN, Duration.ofMinutes(15));
        assertEquals("no_policy", join(e.check(T, "API_REQUESTS", 1)).reason());
    }

    @Test
    void ingestFeedsTheSummaryOfEveryQuotaWindowForAccessAndResultMetrics() {
        var e = engine(quota(ITEMS, 100, DAY, Enforcement.REJECT),
                new Limit(ITEMS, Type.QUOTA, 1000, Window.months(1), Enforcement.REJECT),
                quota(API, 100, DAY, Enforcement.REJECT));
        UsageEvent items = event("i1", ITEMS, 3);
        join(e.ingest(items));
        join(e.ingest(items)); // redelivery must not add twice
        join(e.ingest(event("a1", API, 2)));

        assertEquals(3, summary("ITEMS_PROCESSED", "86400", "2026-11-19T00:00:00Z").total());
        assertEquals(3, summary("ITEMS_PROCESSED", "m1", "2026-11-01T00:00:00Z").total()); // monthly cycle starts on the anchor
        assertEquals(2, summary("API_REQUESTS", "86400", "2026-11-19T00:00:00Z").total()); // summarised although its counter belongs to check
    }

    @Test
    void closedPeriodKeepsItsTotalAndLateUsageIsKeptSeparately() {
        var e = engine(quota(ITEMS, 100, DAY, Enforcement.REJECT));
        join(e.ingest(event("i1", ITEMS, 3)));
        Instant cutoff = Instant.parse("2026-11-20T00:00:00Z");
        assertEquals(1, join(ledger.closeEndedBefore(cutoff)));
        assertEquals(0, join(ledger.closeEndedBefore(cutoff))); // idempotent

        join(e.ingest(event("late", ITEMS, 2))); // belongs to the closed day
        var s = summary("ITEMS_PROCESSED", "86400", "2026-11-19T00:00:00Z");
        assertTrue(s.closed());
        assertEquals(3, s.total());
        assertEquals(2, s.lateTotal());
    }

    @Test
    void eventOfATenantWithoutPolicyIsStillRecorded() {
        var e = new UsageEngine(t -> done(Optional.empty()), counters, ledger, x -> done(null),
                clock, FailMode.OPEN, Duration.ofMinutes(15));
        assertEquals(Ingest.APPLIED, join(e.ingest(event("n1", ITEMS, 1))));
        assertEquals(Ingest.DUPLICATE, join(e.ingest(event("n1", ITEMS, 1))));
    }

    @Test
    void policyOutageRecordsNothingSoRedeliveryIsNotTreatedAsDuplicate() {
        PolicyStore down = t -> CompletableFuture.failedFuture(new IllegalStateException("db down"));
        var failing = new UsageEngine(down, counters, ledger, x -> done(null), clock, FailMode.OPEN, Duration.ofMinutes(15));
        assertThrows(CompletionException.class, () -> join(failing.ingest(event("p1", ITEMS, 1))));

        var healthy = engine(quota(ITEMS, 100, DAY, Enforcement.REJECT));
        assertEquals(Ingest.APPLIED, join(healthy.ingest(event("p1", ITEMS, 1))));
    }

    @Test
    void aCostOutsideTheAcceptedRangeIsRefusedBeforeTouchingAnyCounter() {
        var e = engine(quota(API, 100, DAY, Enforcement.ALLOW_OVERAGE));
        assertTrue(join(e.check(T, "API_REQUESTS", 5)).allowed());
        for (long bad : new long[]{Long.MAX_VALUE, UsageEngine.MAX_COST + 1, 0, -1}) {
            assertThrows(IllegalArgumentException.class, () -> e.check(T, "API_REQUESTS", bad), "cost " + bad);
            assertThrows(IllegalArgumentException.class, () -> e.acquire(T, "ITEMS_PROCESSED", bad), "cost " + bad);
        }
        assertTrue(join(e.check(T, "API_REQUESTS", UsageEngine.MAX_COST)).overage()); // the largest legal cost is only an overage
        assertThrows(IllegalArgumentException.class, () -> new UsageEvent(java.util.UUID.randomUUID(), T, "M", Long.MAX_VALUE, clock.instant()));
    }

    @Test
    void undoCheckGivesBackExactlyWhatTheCheckConsumedInThatPeriod() {
        var e = engine(quota(API, 5, DAY, Enforcement.REJECT));
        Instant at = clock.instant();
        assertTrue(join(e.check(T, "API_REQUESTS", 5, at)).allowed());
        assertFalse(join(e.check(T, "API_REQUESTS", 1, at)).allowed());

        clock.advance(DAY); // the undo happens after the period boundary, but must hit the period of the check
        join(e.undoCheck(T, "API_REQUESTS", 5, at));
        assertTrue(join(e.check(T, "API_REQUESTS", 5, at)).allowed()); // the quota of the original period is whole again
        assertTrue(join(e.check(T, "API_REQUESTS", 5)).allowed());     // and the current period (a day later) was never touched
    }

    @Test
    void aFailedReleaseCanBeRetried() {
        var failing = new java.util.concurrent.atomic.AtomicBoolean(true);
        CounterStore flaky = new CounterStore() {
            public CompletionStage<Result> add(String k, long d, long c, Duration t) { return counters.add(k, d, c, t); }
            public CompletionStage<Long> get(String k) { return counters.get(k); }
            public CompletionStage<Lease> acquireLease(String k, String h, long w, long c, Duration l) { return counters.acquireLease(k, h, w, c, l); }
            public CompletionStage<Long> leasesInUse(String k) { return counters.leasesInUse(k); }
            public CompletionStage<Void> releaseLease(String k, String h) {
                return failing.get() ? CompletableFuture.failedFuture(new IllegalStateException("redis down")) : counters.releaseLease(k, h);
            }
        };
        var e = engine(flaky, FailMode.OPEN, conc(1));
        Permit p = join(e.acquire(T, "ITEMS_PROCESSED", 1));
        assertThrows(CompletionException.class, () -> join(p.release()));
        assertFalse(join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed()); // still held

        failing.set(false);
        join(p.release()); // the second attempt really frees it
        assertTrue(join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed());
    }

    @Test
    void aLeakedSlotExpiresOnItsOwnEvenWhileTheTenantKeepsWorking() {
        var e = engine(conc(2));
        join(e.acquire(T, "ITEMS_PROCESSED", 1)); // holder A crashes and never releases
        for (int minute = 0; minute < 16; minute++) { // the tenant stays busy the whole time
            clock.advance(Duration.ofMinutes(1));
            join(join(e.acquire(T, "ITEMS_PROCESSED", 1)).release());
        }
        assertTrue(join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed());
        assertTrue(join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed()); // A's slot is gone: both are free
    }

    @Test
    void aBusyTenantIsNeverOverAdmittedWhenOldSlotsExpire() {
        var e = engine(conc(2));
        join(e.acquire(T, "ITEMS_PROCESSED", 1));                                   // A, t0, expires t+15
        Permit b = join(e.acquire(T, "ITEMS_PROCESSED", 1));                        // B, t0, expires t+15
        clock.advance(Duration.ofMinutes(10));
        join(b.release());
        join(e.acquire(T, "ITEMS_PROCESSED", 1));                                   // C, t+10, expires t+25, still running
        clock.advance(Duration.ofMinutes(6));                                       // t+16: A is gone, C is not
        assertTrue(join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed());  // C + this one = 2
        assertFalse(join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed()); // a third would be 3 of 2
    }

    @Test
    void aReleaseCanOnlyFreeItsOwnSlot() {
        var e = engine(conc(2));
        Permit a = join(e.acquire(T, "ITEMS_PROCESSED", 1));
        clock.advance(Duration.ofMinutes(16));                                      // A's lease ran out
        join(e.acquire(T, "ITEMS_PROCESSED", 1));                                   // B takes a slot
        join(e.acquire(T, "ITEMS_PROCESSED", 1));                                   // C takes the other
        join(a.release());                                                          // the late release of A must not free B's or C's
        assertFalse(join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed());
    }

    @Test
    void theGatewaySideCheckCountsOnlyLiveSlots() {
        var e = engine(conc(1));
        join(e.acquire(T, "ITEMS_PROCESSED", 1));
        assertEquals("concurrency_exceeded", join(e.check(T, "ITEMS_PROCESSED", 1)).reason());
        clock.advance(Duration.ofMinutes(16));
        assertTrue(join(e.check(T, "ITEMS_PROCESSED", 1)).allowed());
    }

    @Test
    void eventIdsOfDifferentTenantsNeverCollideForTheSameItem() {
        TenantId other = new TenantId("t2");
        assertNotEquals(UsageEvent.idFor(T, "item-1", "ITEMS_PROCESSED"), UsageEvent.idFor(other, "item-1", "ITEMS_PROCESSED"));
        var e = engine(quota(ITEMS, 100, DAY, Enforcement.REJECT));
        var first = new UsageEvent(UsageEvent.idFor(T, "item-1", "ITEMS_PROCESSED"), T, "ITEMS_PROCESSED", 1, clock.instant());
        var second = new UsageEvent(UsageEvent.idFor(other, "item-1", "ITEMS_PROCESSED"), other, "ITEMS_PROCESSED", 1, clock.instant());
        assertEquals(Ingest.APPLIED, join(e.ingest(first)));
        assertEquals(Ingest.APPLIED, join(e.ingest(second))); // not mistaken for a duplicate of the first tenant's event
    }

    @Test
    void decisionsCarryThePlanTheyWereMadeUnder() {
        Plan pro = new Plan("PRO", 2);
        var e = new UsageEngine(t -> done(Optional.of(new Policy(pro, ANCHOR, List.of(quota(API, 1, DAY, Enforcement.REJECT))))),
                counters, ledger, x -> done(null), clock, FailMode.OPEN, Duration.ofMinutes(15));
        assertEquals(pro, join(e.check(T, "API_REQUESTS", 1)).plan());                 // allowed
        Decision denied = join(e.check(T, "API_REQUESTS", 1));
        assertFalse(denied.allowed());
        assertEquals(pro, denied.plan());                                              // refused: still says which plan refused
        var none = new UsageEngine(t -> done(Optional.empty()), counters, ledger, x -> done(null), clock, FailMode.OPEN, Duration.ofMinutes(15));
        assertNull(join(none.check(T, "API_REQUESTS", 1)).plan());                     // never reached a policy
    }

    @Test
    void planCodeAndVersionAreValidated() {
        assertThrows(IllegalArgumentException.class, () -> new Plan(" ", 1));
        assertThrows(IllegalArgumentException.class, () -> new Plan("P".repeat(61), 1));
        assertThrows(IllegalArgumentException.class, () -> new Plan("PRO", 0));
        assertFalse(Plan.UNSPECIFIED.isSpecified());
        assertTrue(new Plan("PRO", 1).isSpecified());
    }

    @Test
    void theSummaryNamesThePlanOfTheLatestUsageAndFreezesItWhenThePeriodCloses() {
        var current = new java.util.concurrent.atomic.AtomicReference<>(new Plan("PRO", 1));
        var e = new UsageEngine(t -> done(Optional.of(new Policy(current.get(), ANCHOR, List.of(quota(ITEMS, 100, DAY, Enforcement.REJECT))))),
                counters, ledger, x -> done(null), clock, FailMode.OPEN, Duration.ofMinutes(15));
        join(e.ingest(event("a", ITEMS, 1)));
        assertEquals(new Plan("PRO", 1), summary("ITEMS_PROCESSED", "86400", "2026-11-19T00:00:00Z").plan());

        current.set(new Plan("BASIC", 1)); // the tenant moves mid-period
        join(e.ingest(event("b", ITEMS, 1)));
        assertEquals(new Plan("BASIC", 1), summary("ITEMS_PROCESSED", "86400", "2026-11-19T00:00:00Z").plan());

        join(ledger.closeEndedBefore(Instant.parse("2026-11-20T00:00:00Z")));
        current.set(new Plan("ENTERPRISE", 1));
        join(e.ingest(event("late", ITEMS, 1))); // arrives after the close
        var closed = summary("ITEMS_PROCESSED", "86400", "2026-11-19T00:00:00Z");
        assertEquals(new Plan("BASIC", 1), closed.plan()); // what billing reads must not change after the close
        assertEquals(2, closed.total());
        assertEquals(1, closed.lateTotal());
    }

    @Test
    void costMustBePositive() {
        var e = engine(rate(1, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> e.check(T, "API_REQUESTS", 0));
    }

    private SummaryStore.Summary summary(String metric, String windowKey, String periodStart) {
        return join(ledger.find(T, metric, windowKey, Instant.parse(periodStart))).orElseThrow();
    }

    private UsageEvent event(String item, Metric m, long qty) {
        return new UsageEvent(UsageEvent.idFor(T, item, m.code()), T, m.code(), qty, clock.instant());
    }

    static <X> CompletionStage<X> done(X v) { return CompletableFuture.completedFuture(v); }

    static <X> X join(CompletionStage<X> s) { return s.toCompletableFuture().join(); }

    @Test
    void aStoreFailingHalfWayLeavesNoPartialChargeWhateverTheFailMode() {
        Limit daily = quota(API, 5, DAY, Enforcement.REJECT);
        Limit monthly = new Limit(API, Type.QUOTA, 50, Window.months(1), Enforcement.REJECT);
        CounterStore failsOnMonthly = new CounterStore() {
            public CompletionStage<Result> add(String k, long d, long c, Duration t) {
                return k.contains(":m1:") && d > 0 ? CompletableFuture.failedFuture(new IllegalStateException("redis hiccup")) : counters.add(k, d, c, t);
            }
            public CompletionStage<Long> get(String k) { return counters.get(k); }
        };
        for (FailMode mode : FailMode.values()) {
            var e = engine(failsOnMonthly, mode, daily, monthly);
            join(e.check(T, "API_REQUESTS", 3)); // daily is charged, then the monthly counter fails
            assertEquals(0, join(counters.get(UsageEngine.counterKey(Type.QUOTA, T, "API_REQUESTS", "86400", Instant.parse("2026-11-19T00:00:00Z")))),
                    "the daily charge must be undone in " + mode);
        }
    }

    @Test
    void aFailedUndoNeverTurnsARefusalIntoAnAllow() {
        Limit daily = quota(API, 5, DAY, Enforcement.REJECT);
        Limit monthly = new Limit(API, Type.QUOTA, 1, Window.months(1), Enforcement.REJECT);
        CounterStore cannotGiveBack = new CounterStore() {
            public CompletionStage<Result> add(String k, long d, long c, Duration t) {
                return d < 0 ? CompletableFuture.failedFuture(new IllegalStateException("redis hiccup")) : counters.add(k, d, c, t);
            }
            public CompletionStage<Long> get(String k) { return counters.get(k); }
        };
        var e = engine(cannotGiveBack, FailMode.OPEN, daily, monthly); // OPEN is the mode that used to let it through
        Decision refused = join(e.check(T, "API_REQUESTS", 2));        // the monthly quota of 1 refuses a cost of 2
        assertFalse(refused.allowed());
        assertEquals("quota_exceeded", refused.reason());
    }

    @Test
    void manyThreadsChargingOneQuotaNeverExceedIt() throws Exception {
        var e = engine(quota(API, 100, DAY, Enforcement.REJECT));
        var pool = java.util.concurrent.Executors.newFixedThreadPool(16);
        try {
            var calls = new ArrayList<java.util.concurrent.Future<Boolean>>();
            for (int i = 0; i < 400; i++) calls.add(pool.submit(() -> join(e.check(T, "API_REQUESTS", 1)).allowed()));
            long allowed = 0;
            for (var f : calls) if (f.get()) allowed++;
            assertEquals(100, allowed);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aColonInATenantOrMetricCodeCannotMakeTwoKeysCollide() {
        String a = UsageEngine.counterKey(Type.QUOTA, new TenantId("a:b"), "c", "86400", Instant.EPOCH);
        String b = UsageEngine.counterKey(Type.QUOTA, new TenantId("a"), "b:c", "86400", Instant.EPOCH);
        assertNotEquals(a, b);
        assertNotEquals(UsageEngine.counterKey(Type.QUOTA, new TenantId("x%3Ay"), "m", "1", Instant.EPOCH),
                UsageEngine.counterKey(Type.QUOTA, new TenantId("x:y"), "m", "1", Instant.EPOCH)); // the escape itself is unambiguous
    }


    @Test
    void anAcquireThatFailsAfterTheStoreMayHaveRunGivesTheSlotBack() {
        CounterStore timesOut = new CounterStore() {
            public CompletionStage<Result> add(String k, long d, long c, Duration t) { return counters.add(k, d, c, t); }
            public CompletionStage<Long> get(String k) { return counters.get(k); }
            public CompletionStage<Long> leasesInUse(String k) { return counters.leasesInUse(k); }
            public CompletionStage<Void> releaseLease(String k, String h) { return counters.releaseLease(k, h); }
            public CompletionStage<Lease> acquireLease(String k, String h, long w, long c, Duration l) {
                // the command ran in the store, the reply never came back
                return counters.acquireLease(k, h, w, c, l).thenCompose(r -> CompletableFuture.<Lease>failedFuture(new IllegalStateException("timeout")));
            }
        };
        var e = engine(timesOut, FailMode.OPEN, conc(1));
        assertEquals("store_unavailable", join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().reason()); // fail-open: degraded
        assertTrue(join(engine(conc(1)).acquire(T, "ITEMS_PROCESSED", 1)).decision().allowed(), "the orphaned slot must not block the next holder");
    }

    @Test
    void acquireDecisionsCarryThePlan() {
        Plan gold = new Plan("gold", 3);
        PolicyStore policies = t -> done(Optional.of(new Policy(gold, ANCHOR, List.of(conc(1)))));
        var e = new UsageEngine(policies, counters, ledger, x -> done(null), clock, FailMode.OPEN, Duration.ofMinutes(15));
        assertEquals(gold, join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().plan());   // allowed
        assertEquals(gold, join(e.acquire(T, "ITEMS_PROCESSED", 1)).decision().plan());   // concurrency_exceeded
        assertEquals(gold, join(e.acquire(T, "OTHER_METRIC", 1)).decision().plan());      // no limit on it
    }
}
