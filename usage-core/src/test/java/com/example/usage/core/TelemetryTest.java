package com.example.usage.core;

import com.example.usage.core.UsageEngine.FailMode;
import com.example.usage.core.memory.CachedPolicyStore;
import com.example.usage.core.memory.InMemoryCounterStore;
import com.example.usage.core.memory.InMemoryLedger;
import com.example.usage.core.model.Limit;
import com.example.usage.core.model.Limit.Enforcement;
import com.example.usage.core.model.Limit.Type;
import com.example.usage.core.model.Metric;
import com.example.usage.core.model.Metric.Mode;
import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.model.UsageEvent;
import com.example.usage.core.spi.PolicyStore;
import com.example.usage.core.spi.UsageTelemetry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class TelemetryTest {
    static final TenantId T = new TenantId("acme");
    static final Metric API = new Metric("API_REQUESTS", Mode.CONSUME_ON_CHECK);
    static final Instant ANCHOR = Instant.parse("2020-01-01T00:00:00Z");
    final Clock clock = Clock.fixed(Instant.parse("2026-11-19T00:00:00Z"), java.time.ZoneOffset.UTC);

    /** Records every operation with its final tags, plus counts and gauges. */
    static final class Recorder implements UsageTelemetry {
        record Ended(String name, Map<String, String> tags, String eventId, Throwable error) {}
        final List<Ended> ended = new ArrayList<>();
        final Map<String, Long> counts = new LinkedHashMap<>();
        final Map<String, Supplier<Number>> gauges = new LinkedHashMap<>();
        final List<Double> recorded = new ArrayList<>();

        @Override public Op start(String name, String... tags) {
            Map<String, String> t = new LinkedHashMap<>();
            for (int i = 0; i + 1 < tags.length; i += 2) t.put(tags[i], tags[i + 1]);
            String[] id = new String[1];
            return new Op() {
                @Override public Op tag(String k, String v) { t.put(k, v); return this; }
                @Override public Op eventId(String v) { id[0] = v; return this; }
                @Override public void end(Throwable error) { ended.add(new Ended(name, t, id[0], error)); }
            };
        }
        @Override public void count(String name, long amount, String... tags) {
            counts.merge(name + String.join(",", tags), amount, Long::sum);
        }
        @Override public void gauge(String name, Supplier<Number> value, String... tags) { gauges.put(name, value); }
        @Override public void record(String name, double value, String... tags) { if (name.equals("quota.ratio")) recorded.add(value); }
    }

    UsageEngine engine(UsageTelemetry telemetry, Limit... limits) {
        PolicyStore policies = t -> CompletableFuture.completedFuture(Optional.of(new Policy(ANCHOR, List.of(limits))));
        return new UsageEngine(policies, new InMemoryCounterStore(clock), new InMemoryLedger(),
                e -> CompletableFuture.completedFuture(null), clock, FailMode.OPEN, Duration.ofMinutes(15), telemetry);
    }

    @Test
    void aDeniedCheckIsOneOperationTaggedWithTheReasonAndNeverTheTenant() {
        var rec = new Recorder();
        var e = engine(rec, new Limit(API, Type.QUOTA, 1, Duration.ofDays(1), Enforcement.REJECT));
        e.check(T, "API_REQUESTS", 1).toCompletableFuture().join();
        e.check(T, "API_REQUESTS", 1).toCompletableFuture().join();

        assertEquals(2, rec.ended.size());
        assertEquals(Map.of("metric", "API_REQUESTS", "result", "allowed", "reason", "none", "overage", "false"), rec.ended.get(0).tags());
        assertEquals("denied", rec.ended.get(1).tags().get("result"));
        assertEquals("quota_exceeded", rec.ended.get(1).tags().get("reason"));
        rec.ended.forEach(o -> assertFalse(o.tags().containsValue(T.value()), "a tenant id is unbounded: never a tag"));
    }

    @Test
    void aQuotaCheckReportsHowFullTheQuotaIs() {
        var rec = new Recorder();
        var e = engine(rec, new Limit(API, Type.QUOTA, 4, Duration.ofDays(1), Enforcement.REJECT));
        e.check(T, "API_REQUESTS", 1).toCompletableFuture().join();
        e.check(T, "API_REQUESTS", 2).toCompletableFuture().join();
        e.check(T, "API_REQUESTS", 2).toCompletableFuture().join(); // refused: still says how full it is
        assertEquals(List.of(0.25, 0.75, 0.75), rec.recorded);
    }

    @Test
    void ingestCarriesTheEventIdAndTheResultIncludingDuplicates() {
        var rec = new Recorder();
        var e = engine(rec, new Limit(API, Type.QUOTA, 10, Duration.ofDays(1), Enforcement.REJECT));
        var ev = new UsageEvent(UUID.randomUUID(), T, "API_REQUESTS", 1, clock.instant());
        e.ingest(ev).toCompletableFuture().join();
        e.ingest(ev).toCompletableFuture().join();

        assertEquals("applied", rec.ended.get(0).tags().get("result"));
        assertEquals("duplicate", rec.ended.get(1).tags().get("result"));
        assertEquals(ev.eventId().toString(), rec.ended.get(0).eventId());
    }

    @Test
    void aStoreOutageIsVisibleAsAnAllowedDegradedDecision() {
        var rec = new Recorder();
        PolicyStore down = t -> CompletableFuture.failedFuture(new IllegalStateException("db"));
        var e = new UsageEngine(down, new InMemoryCounterStore(clock), new InMemoryLedger(), x -> CompletableFuture.completedFuture(null),
                clock, FailMode.OPEN, Duration.ofMinutes(15), rec);
        e.check(T, "API_REQUESTS", 1).toCompletableFuture().join();

        assertEquals("allowed", rec.ended.get(0).tags().get("result"));
        assertEquals("store_unavailable", rec.ended.get(0).tags().get("reason"));
    }

    @Test
    void aBrokenTelemetryNeverChangesTheDecision() {
        UsageTelemetry broken = new UsageTelemetry() {
            @Override public Op start(String n, String... t) {
                return new Op() {
                    @Override public Op tag(String k, String v) { throw new IllegalStateException("backend down"); }
                    @Override public void end(Throwable error) { throw new IllegalStateException("backend down"); }
                };
            }
        };
        var e = engine(broken, new Limit(API, Type.QUOTA, 1, Duration.ofDays(1), Enforcement.REJECT));
        assertTrue(e.check(T, "API_REQUESTS", 1).toCompletableFuture().join().allowed());
        assertFalse(e.check(T, "API_REQUESTS", 1).toCompletableFuture().join().allowed());
    }

    @Test
    void policyCacheCountsHitsAndMissesAndExposesItsSize() {
        var rec = new Recorder();
        var cache = new CachedPolicyStore(t -> CompletableFuture.completedFuture(Optional.of(new Policy(ANCHOR, List.of()))),
                clock, Duration.ofMinutes(1), 100, rec);
        cache.policyFor(T).toCompletableFuture().join();
        cache.policyFor(T).toCompletableFuture().join();
        cache.policyFor(T).toCompletableFuture().join();

        assertEquals(1L, rec.counts.get("policy.cacheresult,miss"));
        assertEquals(2L, rec.counts.get("policy.cacheresult,hit"));
        assertEquals(1, rec.gauges.get("policy.cache.size").get().intValue());
    }
}
