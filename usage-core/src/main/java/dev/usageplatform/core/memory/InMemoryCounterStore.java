package dev.usageplatform.core.memory;

import dev.usageplatform.core.spi.CounterStore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import static dev.usageplatform.core.spi.Stages.done;

/** Single-JVM CounterStore for tests and local use. Not shared across instances. */
public final class InMemoryCounterStore implements CounterStore {
    private record Entry(long value, Instant expires) {}

    private record Held(long weight, Instant expires) {}

    private final Map<String, Entry> map = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Held>> leases = new ConcurrentHashMap<>();
    private final Clock clock;

    public InMemoryCounterStore(Clock clock) { this.clock = clock; }

    @Override
    public CompletionStage<Result> add(String key, long delta, long cap, Duration ttl) {
        Instant now = clock.instant();
        Result[] out = new Result[1];
        map.compute(key, (k, e) -> {
            long cur = live(e, now);
            long sum = cur + delta;
            if (((cur ^ sum) & (delta ^ sum)) < 0) sum = delta > 0 ? Long.MAX_VALUE : 0; // saturate, never wrap around
            if (delta > 0 && sum > cap) {
                out[0] = new Result(false, cur);
                return e;
            }
            long next = Math.max(0, sum);
            out[0] = new Result(true, next);
            return new Entry(next, now.plus(ttl));
        });
        return done(out[0]);
    }

    @Override
    public CompletionStage<Lease> acquireLease(String key, String holder, long weight, long cap, Duration lease) {
        Instant now = clock.instant();
        Lease[] out = new Lease[1];
        leases.compute(key, (k, held) -> {
            Map<String, Held> slots = held == null ? new HashMap<>() : held;
            slots.values().removeIf(h -> !h.expires().isAfter(now));
            slots.remove(holder); // re-acquiring replaces the holder's own slot
            long used = slots.values().stream().mapToLong(Held::weight).sum();
            if (weight > cap || used > cap - weight) {
                out[0] = new Lease(false, used);
            } else {
                slots.put(holder, new Held(weight, now.plus(lease)));
                out[0] = new Lease(true, used + weight);
            }
            return slots;
        });
        return done(out[0]);
    }

    @Override
    public CompletionStage<Void> releaseLease(String key, String holder) {
        leases.computeIfPresent(key, (k, slots) -> {
            slots.remove(holder);
            return slots.isEmpty() ? null : slots;
        });
        return done(null);
    }

    @Override
    public CompletionStage<Long> leasesInUse(String key) {
        Instant now = clock.instant();
        long[] used = new long[1];
        // read under the same per-key lock the writers take
        leases.computeIfPresent(key, (k, slots) -> {
            used[0] = slots.values().stream().filter(h -> h.expires().isAfter(now)).mapToLong(Held::weight).sum();
            return slots;
        });
        return done(used[0]);
    }

    @Override
    public CompletionStage<Long> get(String key) {
        return done(live(map.get(key), clock.instant()));
    }

    private static long live(Entry e, Instant now) {
        return e == null || !e.expires().isAfter(now) ? 0 : e.value();
    }
}
