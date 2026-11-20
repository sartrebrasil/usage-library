package dev.usageplatform.core;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps policies off the hot path: one load per tenant per ttl, concurrent callers share the load,
 * failures are never cached. Call {@link #invalidate} when a plan, override or tenant status changes.
 * shortcut: a bounded map with lazy expiry and arbitrary eviction past the bound; swap for Caffeine if you
 * need real LRU.
 */
public final class CachedPolicyStore implements PolicyStore {
    private record Entry(CompletableFuture<Optional<Policy>> value, Instant expires) {}

    private final PolicyStore delegate;
    private final Clock clock;
    private final Duration ttl;
    private final int maxEntries;
    private final ConcurrentHashMap<TenantId, Entry> cache = new ConcurrentHashMap<>();

    public CachedPolicyStore(PolicyStore delegate, Clock clock, Duration ttl) {
        this(delegate, clock, ttl, 100_000);
    }

    /**
     * @param maxEntries upper bound on tenants held. Unknown tenants are cached too (as "no policy"), so without a
     *                   bound a flood of forged tenant ids would grow the heap; over the bound, expired entries go
     *                   first and then arbitrary ones.
     */
    public CachedPolicyStore(PolicyStore delegate, Clock clock, Duration ttl, int maxEntries) {
        if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be >= 1");
        this.delegate = delegate;
        this.clock = clock;
        this.ttl = ttl;
        this.maxEntries = maxEntries;
    }

    @Override
    public CompletionStage<Optional<Policy>> policyFor(TenantId tenant) {
        Instant now = clock.instant();
        CompletableFuture<Optional<Policy>> fresh = new CompletableFuture<>();
        Entry entry = cache.compute(tenant, (k, old) ->
                old != null && old.expires().isAfter(now) ? old : new Entry(fresh, now.plus(ttl)));
        if (entry.value() == fresh) {
            CompletionStage<Optional<Policy>> loading;
            try {
                loading = delegate.policyFor(tenant);
            } catch (RuntimeException e) { // a store that throws instead of failing its stage must not leave callers hanging
                loading = CompletableFuture.failedFuture(e);
            }
            loading.whenComplete((policy, ex) -> {
                if (ex != null) {
                    cache.remove(tenant, entry);
                    fresh.completeExceptionally(ex);
                } else {
                    fresh.complete(policy);
                }
            });
            if (cache.size() > maxEntries) evict(now);
        }
        return entry.value();
    }

    private void evict(Instant now) {
        cache.values().removeIf(e -> !e.expires().isAfter(now));
        for (var it = cache.keySet().iterator(); cache.size() > maxEntries && it.hasNext(); ) {
            it.next();
            it.remove();
        }
    }

    /** Entries currently held, for tests and metrics. */
    int size() {
        return cache.size();
    }

    public void invalidate(TenantId tenant) {
        cache.remove(tenant);
    }
}
