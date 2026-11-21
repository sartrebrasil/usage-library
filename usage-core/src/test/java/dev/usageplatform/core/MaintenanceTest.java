package dev.usageplatform.core;

import dev.usageplatform.core.MaintenanceScheduler.RunOutcome;
import dev.usageplatform.core.UsageEngine.FailMode;
import dev.usageplatform.core.memory.InMemoryCounterStore;
import dev.usageplatform.core.memory.InMemoryJobLock;
import dev.usageplatform.core.memory.InMemoryLedger;
import dev.usageplatform.core.model.Limit;
import dev.usageplatform.core.model.Limit.Enforcement;
import dev.usageplatform.core.model.Limit.Type;
import dev.usageplatform.core.model.Metric;
import dev.usageplatform.core.model.Metric.Mode;
import dev.usageplatform.core.model.Policy;
import dev.usageplatform.core.model.TenantId;
import dev.usageplatform.core.model.UsageEvent;
import dev.usageplatform.core.spi.PolicyStore;
import dev.usageplatform.core.spi.SummaryStore;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MaintenanceTest {
    static final Metric ITEMS = new Metric("ITEMS_PROCESSED", Mode.REPORT_ONLY);
    static final TenantId T = new TenantId("t1");
    static final Duration TOLERANCE = Duration.ofHours(24);

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
    PeriodMaintenance maintenance;
    InMemoryJobLock lock;

    @BeforeEach
    void setUp() {
        clock = new TestClock();
        counters = new InMemoryCounterStore(clock);
        ledger = new InMemoryLedger(clock);
        lock = new InMemoryJobLock();
        Policy policy = new Policy(Instant.parse("2026-11-01T00:00:00Z"),
                List.of(new Limit(ITEMS, Type.QUOTA, 100, Duration.ofDays(1), Enforcement.REJECT)));
        PolicyStore policies = t -> CompletableFuture.completedFuture(Optional.of(policy));
        engine = new UsageEngine(policies, counters, ledger, e -> CompletableFuture.completedFuture(null), clock, FailMode.OPEN, Duration.ofMinutes(15));
        maintenance = new PeriodMaintenance(ledger, new CounterReconciler(policies, counters, ledger, clock, Duration.ofMinutes(5)), clock, TOLERANCE);
    }

    void ingestToday(String item, long qty) {
        join(engine.ingest(new UsageEvent(UsageEvent.idFor(T, item, "ITEMS_PROCESSED"), T, "ITEMS_PROCESSED", qty, clock.instant())));
    }

    boolean dayClosed() {
        return join(ledger.find(T, "ITEMS_PROCESSED", "86400", Instant.parse("2026-11-19T00:00:00Z"))).orElseThrow().closed();
    }

    static <X> X join(CompletionStage<X> s) { return s.toCompletableFuture().join(); }

    @Test
    void aPeriodIsClosedOnlyAfterTheToleranceForLateEventsHasPassed() {
        ingestToday("a", 5); // the day ends on Nov 20 00:00
        clock.now = Instant.parse("2026-11-20T12:00:00Z"); // ended, but only 12h ago
        assertEquals(0, join(maintenance.run()).closed());
        assertFalse(dayClosed());

        clock.now = Instant.parse("2026-11-21T01:00:00Z"); // more than 24h after the end
        assertEquals(1, join(maintenance.run()).closed());
        assertTrue(dayClosed());
        assertEquals(0, join(maintenance.run()).closed()); // nothing left to close
    }

    @Test
    void eachRunAlsoRepairsTheCountersOfTheOpenPeriods() {
        ingestToday("a", 5);
        join(counters.add(UsageEngine.counterKey(Type.QUOTA, T, "ITEMS_PROCESSED", "86400", Instant.parse("2026-11-19T00:00:00Z")),
                -5, Long.MAX_VALUE, Duration.ofDays(8))); // Redis lost it
        clock.now = clock.now.plus(Duration.ofMinutes(10));
        assertEquals(1, join(maintenance.run()).reconciliation().repaired());
    }

    @Test
    void anInstanceThatDoesNotHoldTheLockSkipsTheRun() {
        var scheduler = new MaintenanceScheduler(lock, maintenance, o -> {});
        CompletableFuture<String> otherInstanceWorking = new CompletableFuture<>();
        var holding = lock.runIfAcquired(MaintenanceScheduler.LOCK_NAME, () -> otherInstanceWorking);

        assertInstanceOf(RunOutcome.Skipped.class, join(scheduler.runOnce()));

        otherInstanceWorking.complete("done");
        join(holding);
        assertInstanceOf(RunOutcome.Completed.class, join(scheduler.runOnce())); // lock is free again
    }

    @Test
    void aFailedRunReportsTheErrorAndReleasesTheLock() {
        var failing = new PeriodMaintenance(new SummaryStore() {
            public CompletionStage<Optional<Summary>> find(TenantId t, String m, String w, Instant s) { return CompletableFuture.completedFuture(Optional.empty()); }
            public CompletionStage<List<Summary>> openSettledBefore(Instant u) { return CompletableFuture.completedFuture(List.of()); }
            public CompletionStage<Integer> closeEndedBefore(Instant c) { return CompletableFuture.failedFuture(new IllegalStateException("db down")); }
        }, null, clock, TOLERANCE);
        var outcome = join(new MaintenanceScheduler(lock, failing, o -> {}).runOnce());
        assertInstanceOf(RunOutcome.Failed.class, outcome);
        assertInstanceOf(RunOutcome.Completed.class, join(new MaintenanceScheduler(lock, maintenance, o -> {}).runOnce()));
    }

    @Test
    void theTimerKeepsRunningEvenWhenAListenerThrows() throws InterruptedException {
        var calls = new AtomicInteger();
        var twice = new CountDownLatch(2);
        try (var scheduler = new MaintenanceScheduler(lock, maintenance, o -> {
            calls.incrementAndGet();
            twice.countDown();
            throw new IllegalStateException("listener bug");
        })) {
            scheduler.start(Duration.ZERO, Duration.ofMillis(20));
            assertTrue(twice.await(3, TimeUnit.SECONDS));
        }
        assertTrue(calls.get() >= 2);
    }

    @Test
    void cannotBeStartedTwice() {
        try (var scheduler = new MaintenanceScheduler(lock, maintenance, o -> {})) {
            scheduler.start(Duration.ofHours(1), Duration.ofHours(1));
            assertThrows(IllegalStateException.class, () -> scheduler.start(Duration.ofHours(1), Duration.ofHours(1)));
        }
    }

    @Test
    void aJobThatReturnsNothingFailsAndStillReleasesTheLock() {
        assertThrows(java.util.concurrent.CompletionException.class, () -> join(lock.<String>runIfAcquired("x", () -> null))); // null stage
        assertThrows(java.util.concurrent.CompletionException.class, () -> join(lock.<String>runIfAcquired("x", () -> CompletableFuture.completedFuture(null)))); // null result
        assertEquals(java.util.Optional.of("ok"), join(lock.runIfAcquired("x", () -> CompletableFuture.completedFuture("ok")))); // not stuck
    }

    @Test
    void closingTheSchedulerWaitsForARunInProgress() throws InterruptedException {
        var started = new CountDownLatch(1);
        var finished = new java.util.concurrent.atomic.AtomicBoolean();
        SummaryStore slow = new SummaryStore() {
            public CompletionStage<java.util.Optional<Summary>> find(TenantId t, String m, String w, Instant s) { return CompletableFuture.completedFuture(java.util.Optional.empty()); }
            public CompletionStage<List<Summary>> openSettledBefore(Instant u) { return CompletableFuture.completedFuture(List.of()); }
            public CompletionStage<Integer> closeEndedBefore(Instant c) {
                started.countDown();
                return CompletableFuture.supplyAsync(() -> {
                    try { Thread.sleep(400); } catch (InterruptedException ignored) { }
                    finished.set(true);
                    return 0;
                });
            }
        };
        PolicyStore none = t -> CompletableFuture.completedFuture(java.util.Optional.empty());
        var job = new PeriodMaintenance(slow, new CounterReconciler(none, counters, slow, clock, Duration.ofMinutes(5)), clock, TOLERANCE);
        var scheduler = new MaintenanceScheduler(lock, job, o -> {});
        scheduler.start(Duration.ZERO, Duration.ofHours(1));
        assertTrue(started.await(3, TimeUnit.SECONDS));

        scheduler.close(); // must not return before the running job did
        assertTrue(finished.get());
    }
}
