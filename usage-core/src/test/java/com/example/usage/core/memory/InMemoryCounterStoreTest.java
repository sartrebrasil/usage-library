package com.example.usage.core.memory;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryCounterStoreTest {
    static final class TestClock extends Clock {
        Instant now = Instant.parse("2026-11-19T12:00:00Z");
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    static final Duration LEASE = Duration.ofMinutes(15);
    final TestClock clock = new TestClock();
    final InMemoryCounterStore store = new InMemoryCounterStore(clock);

    static <X> X join(CompletionStage<X> s) { return s.toCompletableFuture().join(); }

    @Test
    void aHugeDeltaNeverWrapsTheCounterAround() {
        join(store.add("k", 5, 100, LEASE));
        var refused = join(store.add("k", Long.MAX_VALUE, 100, LEASE));
        assertFalse(refused.applied());
        assertEquals(5, join(store.get("k"))); // not erased, not negative

        join(store.add("big", Long.MAX_VALUE, Long.MAX_VALUE, LEASE));
        join(store.add("big", Long.MAX_VALUE, Long.MAX_VALUE, LEASE)); // would wrap to a negative sum
        assertEquals(Long.MAX_VALUE, join(store.get("big")));
    }

    @Test
    void eachHolderHasItsOwnExpiryAndItsOwnWeight() {
        assertTrue(join(store.acquireLease("c", "a", 2, 5, LEASE)).granted());
        clock.now = clock.now.plus(Duration.ofMinutes(10));
        assertTrue(join(store.acquireLease("c", "b", 3, 5, LEASE)).granted());
        assertFalse(join(store.acquireLease("c", "x", 1, 5, LEASE)).granted()); // 2 + 3 = 5 is full
        assertEquals(5, join(store.leasesInUse("c")));

        clock.now = clock.now.plus(Duration.ofMinutes(6)); // a expired, b did not
        assertEquals(3, join(store.leasesInUse("c")));
        assertTrue(join(store.acquireLease("c", "x", 2, 5, LEASE)).granted());
    }

    @Test
    void releasingIsIdempotentAndNeverTouchesOtherHolders() {
        join(store.acquireLease("c", "a", 1, 5, LEASE));
        join(store.acquireLease("c", "b", 1, 5, LEASE));
        join(store.releaseLease("c", "a"));
        join(store.releaseLease("c", "a"));
        join(store.releaseLease("c", "never-existed"));
        assertEquals(1, join(store.leasesInUse("c")));
    }

    @Test
    void acquiringAgainWithTheSameHolderReplacesInsteadOfAdding() {
        join(store.acquireLease("c", "a", 3, 5, LEASE));
        assertTrue(join(store.acquireLease("c", "a", 4, 5, LEASE)).granted());
        assertEquals(4, join(store.leasesInUse("c")));
    }

    @Test
    void aWeightLargerThanTheCapIsRefusedWithoutOverflow() {
        assertFalse(join(store.acquireLease("c", "a", Long.MAX_VALUE, 5, LEASE)).granted());
        assertEquals(0, join(store.leasesInUse("c")));
    }
}
