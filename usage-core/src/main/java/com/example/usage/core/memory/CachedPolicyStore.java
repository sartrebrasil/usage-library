package com.example.usage.core.memory;

import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.spi.PolicyStore;
import com.example.usage.core.spi.UsageTelemetry;

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
    /** The last policy the delegate really returned, and when. */
    private record Good(Optional<Policy> policy, Instant at) {}

    private static final class Entry {
        final CompletableFuture<Optional<Policy>> value;
        final Instant expires;
        volatile Good good;

        Entry(CompletableFuture<Optional<Policy>> value, Instant expires, Good good) {
            this.value = value;
            this.expires = expires;
            this.good = good;
        }
    }

    /** While serving a stale policy, the delegate is asked again after this long (or the ttl, if shorter). */
    private static final Duration STALE_RETRY = Duration.ofSeconds(5);

    private final PolicyStore delegate;
    private final Clock clock;
    private final Duration ttl;
    private final int maxEntries;
    private final ConcurrentHashMap<TenantId, Entry> cache = new ConcurrentHashMap<>();
    private final UsageTelemetry telemetry;
    private final Duration staleFor;

    public CachedPolicyStore(PolicyStore delegate, Clock clock, Duration ttl) {
        this(delegate, clock, ttl, 100_000);
    }

    /**
     * @param maxEntries upper bound on tenants held. Unknown tenants are cached too (as "no policy"), so without a
     *                   bound a flood of forged tenant ids would grow the heap; over the bound, expired entries go
     *                   first and then arbitrary ones.
     */
    public CachedPolicyStore(PolicyStore delegate, Clock clock, Duration ttl, int maxEntries) {
        this(delegate, clock, ttl, maxEntries, UsageTelemetry.NOOP);
    }

    /** @param telemetry counts hits and misses ({@code policy.cache}) and exposes the size ({@code policy.cache.size}). */
    public CachedPolicyStore(PolicyStore delegate, Clock clock, Duration ttl, int maxEntries, UsageTelemetry telemetry) {
        this(delegate, clock, ttl, maxEntries, telemetry, Duration.ZERO);
    }

    /**
     * @param staleFor when the delegate fails, a policy it returned at most this long ago is served instead of the failure (and the delegate
     *                 is asked again every few seconds). A host whose policy lives in another service stays up on the last plan it read,
     *                 for as long as it accepts. Zero (the default) never serves a stale policy: a failure is a failure.
     */
    public CachedPolicyStore(PolicyStore delegate, Clock clock, Duration ttl, int maxEntries, UsageTelemetry telemetry, Duration staleFor) {
        if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be >= 1");
        this.delegate = delegate;
        this.clock = clock;
        this.ttl = ttl;
        this.maxEntries = maxEntries;
        this.telemetry = telemetry;
        this.staleFor = staleFor;
        telemetry.gauge("policy.cache.size", cache::size);
    }

    @Override
    public CompletionStage<Optional<Policy>> policyFor(TenantId tenant) {
        Instant now = clock.instant();
        CompletableFuture<Optional<Policy>> fresh = new CompletableFuture<>();
        Entry entry = cache.compute(tenant, (k, old) ->
                old != null && old.expires.isAfter(now) ? old : new Entry(fresh, now.plus(ttl), old == null ? null : old.good));
        telemetry.count("policy.cache", 1, "result", entry.value == fresh ? "miss" : "hit");
        if (entry.value == fresh) {
            CompletionStage<Optional<Policy>> loading;
            try {
                loading = delegate.policyFor(tenant);
            } catch (RuntimeException e) { // a store that throws instead of failing its stage must not leave callers hanging
                loading = CompletableFuture.failedFuture(e);
            }
            loading.whenComplete((policy, ex) -> {
                if (ex == null) {
                    entry.good = new Good(policy, clock.instant());
                    fresh.complete(policy);
                    return;
                }
                Good good = entry.good;
                Instant at = clock.instant();
                if (good != null && !staleFor.isZero() && Duration.between(good.at(), at).compareTo(staleFor) <= 0) {
                    telemetry.count("policy.cache", 1, "result", "stale");
                    // kept for a few seconds: the delegate is not hammered while it is down, and the last good policy still ages out
                    cache.replace(tenant, entry, new Entry(CompletableFuture.completedFuture(good.policy()),
                            at.plus(ttl.compareTo(STALE_RETRY) < 0 ? ttl : STALE_RETRY), good));
                    fresh.complete(good.policy());
                } else {
                    cache.remove(tenant, entry);
                    fresh.completeExceptionally(ex);
                }
            });
            if (cache.size() > maxEntries) evict(now);
        }
        return entry.value;
    }

    private void evict(Instant now) {
        cache.values().removeIf(e -> !e.expires.isAfter(now));
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
