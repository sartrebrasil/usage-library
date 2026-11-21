package com.example.usage.core;

import com.example.usage.core.UsageEngine.Compensate;
import com.example.usage.core.UsageEngine.FailMode;
import com.example.usage.core.memory.InMemoryCounterStore;
import com.example.usage.core.memory.InMemoryLedger;
import com.example.usage.core.model.Compensation;
import com.example.usage.core.model.Limit;
import com.example.usage.core.model.Limit.Enforcement;
import com.example.usage.core.model.Limit.Type;
import com.example.usage.core.model.Metric;
import com.example.usage.core.model.Metric.Mode;
import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.model.UsageEvent;
import com.example.usage.core.model.Window;
import com.example.usage.core.spi.CounterStore;
import com.example.usage.core.spi.Ledger;
import com.example.usage.core.spi.PolicyStore;
import com.example.usage.core.spi.SummaryStore.Summary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.*;

class CompensationTest {
    static final Metric ITEMS = new Metric("ITEMS_PROCESSED", Mode.REPORT_ONLY);
    static final Metric API = new Metric("API_REQUESTS", Mode.CONSUME_ON_CHECK);
    static final TenantId T = new TenantId("t1");
    static final Instant ANCHOR = Instant.parse("2026-11-01T00:00:00Z");
    static final Instant DAY = Instant.parse("2026-11-19T00:00:00Z");

    static final class TestClock extends Clock {
        Instant now = Instant.parse("2026-11-19T12:00:00Z");
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    TestClock clock;
    InMemoryCounterStore counters;
    InMemoryLedger ledger;
    UsageEngine engine;

    @BeforeEach
    void setUp() {
        clock = new TestClock();
        counters = new InMemoryCounterStore(clock);
        ledger = new InMemoryLedger(clock);
        Policy policy = new Policy(ANCHOR, List.of(
                new Limit(ITEMS, Type.QUOTA, 100, Duration.ofDays(1), Enforcement.REJECT),
                new Limit(API, Type.QUOTA, 100, Duration.ofDays(1), Enforcement.REJECT)));
        engine = new UsageEngine(t -> CompletableFuture.completedFuture(Optional.of(policy)), counters, ledger,
                e -> CompletableFuture.completedFuture(null), clock, FailMode.OPEN, Duration.ofMinutes(15));
    }

    UsageEvent usage(String item, Metric m, long qty) {
        return new UsageEvent(UsageEvent.idFor(T, item, m.code()), T, m.code(), qty, clock.instant());
    }

    Compensation reversal(UsageEvent original, long qty) {
        return Compensation.of(original.eventId(), T, original.metricCode(), qty, clock.instant().plus(Duration.ofDays(2)), "RESULT_REVERTED");
    }

    long counter(Metric m) {
        return join(counters.get(UsageEngine.counterKey(Type.QUOTA, T, m.code(), "86400", DAY)));
    }

    Summary summary(Metric m) {
        return join(ledger.find(T, m.code(), "86400", DAY)).orElseThrow();
    }

    static <X> X join(CompletionStage<X> s) { return s.toCompletableFuture().join(); }

    @Test
    void reversalLowersSummaryAndCounterAndIsIdempotent() {
        UsageEvent original = usage("a", ITEMS, 5);
        join(engine.ingest(original));
        Compensation c = reversal(original, 2);

        assertEquals(Compensate.APPLIED, join(engine.compensate(c)));
        assertEquals(3, summary(ITEMS).total());
        assertEquals(3, counter(ITEMS));

        assertEquals(Compensate.DUPLICATE, join(engine.compensate(c))); // redelivery
        assertEquals(3, summary(ITEMS).total());
        assertEquals(3, counter(ITEMS));
    }

    @Test
    void reversalChargesThePeriodOfTheOriginalNotOfTheCompensation() {
        UsageEvent original = usage("a", ITEMS, 5);
        join(engine.ingest(original));
        join(engine.compensate(reversal(original, 5))); // occurredAt is two days later

        assertEquals(0, summary(ITEMS).total()); // the original day went down
        assertTrue(join(ledger.find(T, "ITEMS_PROCESSED", "86400", DAY.plus(Duration.ofDays(2)))).isEmpty()); // no row appeared for the later day
    }

    @Test
    void reversalArrivingBeforeTheOriginalWaitsForIt() {
        UsageEvent original = usage("a", ITEMS, 5);
        Compensation c = reversal(original, 1);
        assertEquals(Compensate.ORIGINAL_NOT_FOUND, join(engine.compensate(c)));

        join(engine.ingest(original));
        assertEquals(Compensate.APPLIED, join(engine.compensate(c)));
        assertEquals(4, summary(ITEMS).total());
    }

    @Test
    void anOriginalCanBeReversedOnlyOnce() {
        UsageEvent original = usage("a", ITEMS, 5);
        join(engine.ingest(original));
        join(engine.compensate(reversal(original, 2)));

        Compensation other = new Compensation(UUID.randomUUID(), T, "ITEMS_PROCESSED", 1, clock.instant(), original.eventId(), "AGAIN");
        assertEquals(Compensate.ALREADY_COMPENSATED, join(engine.compensate(other)));
        assertEquals(3, summary(ITEMS).total());
        assertEquals(3, counter(ITEMS));
    }

    @Test
    void closedPeriodKeepsItsTotalAndReceivesACredit() {
        UsageEvent original = usage("a", ITEMS, 5);
        join(engine.ingest(original));
        join(ledger.closeEndedBefore(Instant.parse("2026-11-20T00:00:00Z")));

        assertEquals(Compensate.APPLIED, join(engine.compensate(reversal(original, 2))));
        Summary s = summary(ITEMS);
        assertTrue(s.closed());
        assertEquals(5, s.total());
        assertEquals(2, s.credits());
    }

    @Test
    void inconsistentReversalsAreInvalid() {
        UsageEvent original = usage("a", ITEMS, 5);
        join(engine.ingest(original));
        assertEquals(Compensate.INVALID, join(engine.compensate(reversal(original, 6)))); // more than was used
        Compensation otherTenant = Compensation.of(original.eventId(), new TenantId("t2"), "ITEMS_PROCESSED", 1, clock.instant(), "X");
        assertEquals(Compensate.INVALID, join(engine.compensate(otherTenant)));
        Compensation otherMetric = Compensation.of(original.eventId(), T, "API_REQUESTS", 1, clock.instant(), "X");
        assertEquals(Compensate.INVALID, join(engine.compensate(otherMetric)));
        assertEquals(5, summary(ITEMS).total());
    }

    @Test
    void counterNeverGoesBelowZero() {
        UsageEvent original = usage("a", ITEMS, 2);
        join(engine.ingest(original));
        join(counters.add(UsageEngine.counterKey(Type.QUOTA, T, "ITEMS_PROCESSED", "86400", DAY), -2, Long.MAX_VALUE, Duration.ofDays(8))); // Redis lost it
        assertEquals(Compensate.APPLIED, join(engine.compensate(reversal(original, 2))));
        assertEquals(0, counter(ITEMS));
    }

    @Test
    void refundedAccessRequestGivesItsQuotaBack() {
        join(engine.check(T, "API_REQUESTS", 3)); // the gateway consumed 3 at check time
        UsageEvent original = usage("req-1", API, 3);
        join(engine.ingest(original));
        assertEquals(3, counter(API));

        join(engine.compensate(reversal(original, 3))); // e.g. the call ended in a 5xx
        assertEquals(0, counter(API));
        assertEquals(0, summary(API).total());
    }

    @Test
    void counterFailureAfterTheLedgerIsReportedAsDrift() {
        UsageEvent original = usage("a", ITEMS, 2);
        join(engine.ingest(original));
        CounterStore down = new CounterStore() {
            public CompletionStage<Result> add(String k, long d, long c, Duration t) {
                return CompletableFuture.failedFuture(new IllegalStateException("redis down"));
            }
            public CompletionStage<Long> get(String k) { return CompletableFuture.completedFuture(0L); }
        };
        var degraded = new UsageEngine(t -> CompletableFuture.completedFuture(Optional.of(new Policy(ANCHOR,
                List.of(new Limit(ITEMS, Type.QUOTA, 100, Duration.ofDays(1), Enforcement.REJECT))))),
                down, ledger, e -> CompletableFuture.completedFuture(null), clock, FailMode.OPEN, Duration.ofMinutes(15));
        assertEquals(Compensate.COUNTER_DRIFT, join(degraded.compensate(reversal(original, 1))));
        assertEquals(Compensate.DUPLICATE, join(degraded.compensate(reversal(original, 1)))); // the ledger kept it: reconcile, don't redeliver
    }

    @Test
    void compensationIdIsDerivedFromTheOriginal() {
        UUID original = UUID.randomUUID();
        assertEquals(Compensation.idFor(original), Compensation.of(original, T, "M", 1, clock.instant(), "R").eventId());
        assertNotEquals(original, Compensation.idFor(original));
    }

    @Test
    void aReversalUndoesWhatTheOriginalWasRecordedInEvenIfTheLimitsChangedSince() {
        var policy = new java.util.concurrent.atomic.AtomicReference<>(new Policy(ANCHOR,
                List.of(new Limit(ITEMS, Type.QUOTA, 100, Duration.ofDays(1), Enforcement.REJECT))));
        var e = new UsageEngine(t -> CompletableFuture.completedFuture(Optional.of(policy.get())), counters, ledger,
                x -> CompletableFuture.completedFuture(null), clock, FailMode.OPEN, Duration.ofMinutes(15));
        UsageEvent first = usage("first", ITEMS, 5);
        join(e.ingest(first)); // recorded in the daily slot only

        policy.set(new Policy(ANCHOR, List.of( // a monthly quota is added afterwards
                new Limit(ITEMS, Type.QUOTA, 100, Duration.ofDays(1), Enforcement.REJECT),
                new Limit(ITEMS, Type.QUOTA, 1000, Window.months(1), Enforcement.REJECT))));
        join(e.ingest(usage("second", ITEMS, 7))); // recorded in both

        assertEquals(Compensate.APPLIED, join(e.compensate(reversal(first, 5))));

        assertEquals(7, join(ledger.find(T, "ITEMS_PROCESSED", "m1", Instant.parse("2026-11-01T00:00:00Z"))).orElseThrow().total()); // 7, not 2
        assertEquals(7, summary(ITEMS).total());
        assertEquals(7, join(counters.get(UsageEngine.counterKey(Type.QUOTA, T, "ITEMS_PROCESSED", "m1", Instant.parse("2026-11-01T00:00:00Z")))));
    }

    @Test
    void aReversalThatLandsBetweenTheLedgerAndTheCountersIsRepairedByReconciliation() {
        var engineRef = new java.util.concurrent.atomic.AtomicReference<UsageEngine>();
        var original = usage("raced", ITEMS, 4);
        Ledger racing = new Ledger() {
            public CompletionStage<Boolean> record(UsageEvent ev, List<Slot> slots) {
                return ledger.record(ev, slots).thenApply(isNew -> {
                    if (isNew) join(engineRef.get().compensate(reversal(original, 4))); // lands before the counters move
                    return isNew;
                });
            }
            public CompletionStage<Optional<UsageEvent>> find(UUID id) { return ledger.find(id); }
            public CompletionStage<Compensated> compensate(Compensation c, UsageEvent o) { return ledger.compensate(c, o); }
        };
        Policy policy = new Policy(ANCHOR, List.of(new Limit(ITEMS, Type.QUOTA, 100, Duration.ofDays(1), Enforcement.REJECT)));
        PolicyStore policies = t -> CompletableFuture.completedFuture(Optional.of(policy));
        var racy = new UsageEngine(policies, counters, racing, x -> CompletableFuture.completedFuture(null), clock, FailMode.OPEN, Duration.ofMinutes(15));
        engineRef.set(racy);

        join(racy.ingest(original));
        assertEquals(0, summary(ITEMS).total());   // the ledger nets to zero ...
        assertEquals(4, counter(ITEMS));           // ... but the counter absorbed the reversal at zero and then added 4

        clock.now = clock.now.plus(Duration.ofMinutes(10));
        join(new CounterReconciler(policies, counters, ledger, clock, Duration.ofMinutes(5)).run());
        assertEquals(0, counter(ITEMS));           // result metrics are repaired down to the ledger
    }
}
