package com.example.usage.core.memory;

import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CachedPolicyStoreTest {
    static final TenantId T = new TenantId("t1");
    static final Policy P = new Policy(Instant.EPOCH, List.of());

    static final class TestClock extends Clock {
        Instant now = Instant.parse("2026-11-19T00:00:00Z");
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    final TestClock clock = new TestClock();
    final AtomicInteger loads = new AtomicInteger();

    CachedPolicyStore cache(boolean failing) {
        return new CachedPolicyStore(t -> {
            loads.incrementAndGet();
            return failing ? CompletableFuture.failedFuture(new IllegalStateException("db down"))
                    : CompletableFuture.completedFuture(Optional.of(P));
        }, clock, Duration.ofMinutes(1));
    }

    @Test
    void loadsOncePerTtlAndInvalidateForcesReload() {
        var c = cache(false);
        c.policyFor(T).toCompletableFuture().join();
        c.policyFor(T).toCompletableFuture().join();
        assertEquals(1, loads.get());

        c.invalidate(T);
        c.policyFor(T).toCompletableFuture().join();
        assertEquals(2, loads.get());

        clock.now = clock.now.plus(Duration.ofMinutes(2));
        c.policyFor(T).toCompletableFuture().join();
        assertEquals(3, loads.get());
    }

    @Test
    void failuresAreNotCached() {
        var c = cache(true);
        assertThrows(CompletionException.class, () -> c.policyFor(T).toCompletableFuture().join());
        assertThrows(CompletionException.class, () -> c.policyFor(T).toCompletableFuture().join());
        assertEquals(2, loads.get());
    }

    @Test
    void aStoreThatThrowsInsteadOfFailingItsStageDoesNotLeaveCallersHanging() {
        var calls = new AtomicInteger();
        var c = new CachedPolicyStore(t -> {
            if (calls.incrementAndGet() == 1) throw new IllegalStateException("executor rejected the task");
            return CompletableFuture.completedFuture(Optional.of(P));
        }, clock, Duration.ofMinutes(1));

        var first = c.policyFor(T).toCompletableFuture();
        assertTrue(first.isCompletedExceptionally()); // fails at once, never hangs
        assertEquals(Optional.of(P), c.policyFor(T).toCompletableFuture().join()); // and the failure was not cached
    }

    @Test
    void aFloodOfUnknownTenantsCannotGrowTheCacheWithoutBound() {
        var c = new CachedPolicyStore(t -> CompletableFuture.completedFuture(Optional.empty()), clock, Duration.ofMinutes(1), 50);
        for (int i = 0; i < 1_000; i++) c.policyFor(new TenantId("forged-" + i)).toCompletableFuture().join();
        assertTrue(c.size() <= 50, "size was " + c.size());
        assertEquals(Optional.empty(), c.policyFor(new TenantId("forged-1")).toCompletableFuture().join()); // still works after eviction
    }

    @Test
    void theBoundMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new CachedPolicyStore(t -> null, clock, Duration.ofMinutes(1), 0));
    }

    @Test
    void aFailingDelegateServesTheLastGoodPolicyUntilItGetsTooOld() {
        var down = new java.util.concurrent.atomic.AtomicBoolean();
        var store = new CachedPolicyStore(t -> down.get() ? CompletableFuture.failedFuture(new IllegalStateException("usage-api down"))
                : CompletableFuture.completedFuture(Optional.of(P)), clock, Duration.ofMinutes(1), 100,
                com.example.usage.core.spi.UsageTelemetry.NOOP, Duration.ofMinutes(30));
        assertEquals(Optional.of(P), store.policyFor(T).toCompletableFuture().join());

        down.set(true);
        clock.now = clock.now.plus(Duration.ofMinutes(2)); // the entry expired and the delegate is down
        assertEquals(Optional.of(P), store.policyFor(T).toCompletableFuture().join(), "the last good policy, not the failure");

        clock.now = clock.now.plus(Duration.ofMinutes(40)); // older than the 30 minutes the host accepts
        assertThrows(CompletionException.class, () -> store.policyFor(T).toCompletableFuture().join());

        down.set(false); // and it recovers on its own
        assertEquals(Optional.of(P), store.policyFor(T).toCompletableFuture().join());
    }

    @Test
    void withoutStaleTimeAFailureIsAFailureAsBefore() {
        var down = new java.util.concurrent.atomic.AtomicBoolean();
        var store = new CachedPolicyStore(t -> down.get() ? CompletableFuture.failedFuture(new IllegalStateException("down"))
                : CompletableFuture.completedFuture(Optional.of(P)), clock, Duration.ofMinutes(1));
        store.policyFor(T).toCompletableFuture().join();
        down.set(true);
        clock.now = clock.now.plus(Duration.ofMinutes(2));
        assertThrows(CompletionException.class, () -> store.policyFor(T).toCompletableFuture().join());
    }
}
