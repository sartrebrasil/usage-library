package dev.usageplatform.core.spi;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * SPI: fast counters (Redis + Lua in production). A cache of decisions, never the source of truth.
 * Failures must be reported through the returned stage, never thrown.
 */
public interface CounterStore {
    /** applied=false means the cap would be exceeded and nothing changed; value is the current total. */
    record Result(boolean applied, long value) {}

    /**
     * Atomically adds delta (floored at 0, saturating at 2^53). A positive delta is applied only if the new
     * total stays <= cap. Negative deltas always apply (release/rollback).
     * <p>
     * Every call renews the ttl, which is harmless for period counters (the ttl covers the period).
     * Concurrency is not counted here: it uses the per-holder leases below.
     */
    CompletionStage<Result> add(String key, long delta, long cap, Duration ttl);

    /** Current total, 0 if absent or expired. */
    CompletionStage<Long> get(String key);

    // ---- per-holder leases (concurrency limits) ---------------------------------------------------

    /**
     * granted=false means the cap would be exceeded and nothing changed; inUse is the weight currently held by
     * live (unexpired) holders, after the change when granted.
     */
    record Lease(boolean granted, long inUse) {}

    /**
     * Takes {@code weight} slots for {@code holder} if the live holders plus this one stay within {@code cap}.
     * Every holder has its OWN expiry ({@code lease} from now, by the store's clock): a holder that crashes
     * frees exactly its own slots when its lease runs out, whatever the others do, and a release can never
     * remove a slot that belongs to someone else. Re-acquiring with the same holder id replaces its slot.
     * Required if you use CONCURRENCY limits; the counter methods above are not used for them.
     */
    default CompletionStage<Lease> acquireLease(String key, String holder, long weight, long cap, Duration lease) {
        return CompletableFuture.failedFuture(
                new UnsupportedOperationException("this CounterStore has no per-holder leases"));
    }

    /** Frees the holder's slots. Idempotent: releasing twice, or after the lease expired, is a no-op. */
    default CompletionStage<Void> releaseLease(String key, String holder) {
        return CompletableFuture.failedFuture(
                new UnsupportedOperationException("this CounterStore has no per-holder leases"));
    }

    /** Weight held by live (unexpired) holders. */
    default CompletionStage<Long> leasesInUse(String key) {
        return CompletableFuture.failedFuture(
                new UnsupportedOperationException("this CounterStore has no per-holder leases"));
    }
}
