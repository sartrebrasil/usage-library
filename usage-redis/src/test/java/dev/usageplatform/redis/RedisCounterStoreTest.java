package dev.usageplatform.redis;

import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.UsageEngine.FailMode;
import dev.usageplatform.core.memory.InMemoryLedger;
import dev.usageplatform.core.model.Limit;
import dev.usageplatform.core.model.Limit.Enforcement;
import dev.usageplatform.core.model.Limit.Type;
import dev.usageplatform.core.model.Metric;
import dev.usageplatform.core.model.Metric.Mode;
import dev.usageplatform.core.model.Policy;
import dev.usageplatform.core.model.TenantId;
import dev.usageplatform.core.spi.CounterStore;
import dev.usageplatform.core.spi.CounterStore.Result;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class RedisCounterStoreTest {
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    static RedisClient client;
    static StatefulRedisConnection<String, String> connection;
    static RedisCounterStore store;

    @BeforeAll
    static void connect() {
        client = RedisClient.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        connection = client.connect();
        store = new RedisCounterStore(connection.async());
    }

    @AfterAll
    static void close() {
        connection.close();
        client.shutdown();
    }

    static String key() { return "test:" + UUID.randomUUID(); }

    static <X> X join(CompletionStage<X> s) { return s.toCompletableFuture().join(); }

    @Test
    void addsUnderCapAndRefusesOverCapWithoutChangingTheValue() {
        String k = key();
        assertEquals(new Result(true, 6), join(store.add(k, 6, 10, Duration.ofMinutes(1))));
        assertEquals(new Result(false, 6), join(store.add(k, 5, 10, Duration.ofMinutes(1))));
        assertEquals(new Result(true, 10), join(store.add(k, 4, 10, Duration.ofMinutes(1))));
        assertEquals(10L, join(store.get(k)));
    }

    @Test
    void negativeDeltaAlwaysAppliesAndFloorsAtZero() {
        String k = key();
        join(store.add(k, 3, Long.MAX_VALUE, Duration.ofMinutes(1)));
        assertEquals(new Result(true, 1), join(store.add(k, -2, Long.MAX_VALUE, Duration.ofMinutes(1))));
        assertEquals(new Result(true, 0), join(store.add(k, -5, Long.MAX_VALUE, Duration.ofMinutes(1))));
    }

    @Test
    void missingKeyReadsZeroAndKeysExpire() throws InterruptedException {
        String k = key();
        assertEquals(0L, join(store.get(k)));
        join(store.add(k, 1, 10, Duration.ofMillis(200)));
        Thread.sleep(400);
        assertEquals(0L, join(store.get(k)));
    }

    @Test
    void concurrentAddsNeverExceedTheCap() {
        String k = key();
        List<CompletableFuture<Result>> calls = new ArrayList<>();
        for (int i = 0; i < 200; i++) calls.add(store.add(k, 1, 10, Duration.ofMinutes(1)).toCompletableFuture());
        long applied = calls.stream().map(CompletableFuture::join).filter(Result::applied).count();
        assertEquals(10, applied);
        assertEquals(10L, join(store.get(k)));
    }

    @Test
    void engineEnforcesQuotaThroughRedis() {
        Metric api = new Metric("API_REQUESTS", Mode.CONSUME_ON_CHECK);
        Limit quota = new Limit(api, Type.QUOTA, 2, Duration.ofDays(1), Enforcement.REJECT);
        UsageEngine engine = new UsageEngine(
                t -> CompletableFuture.completedFuture(Optional.of(new Policy(Instant.parse("2026-11-01T00:00:00Z"), List.of(quota)))),
                store, new InMemoryLedger(), e -> CompletableFuture.completedFuture(null),
                Clock.fixed(Instant.parse("2026-11-19T12:00:00Z"), java.time.ZoneOffset.UTC), FailMode.CLOSED, Duration.ofMinutes(15)); // fixed: a daily quota must not reset at midnight mid-test
        TenantId tenant = new TenantId("redis-" + UUID.randomUUID());

        assertTrue(join(engine.check(tenant, "API_REQUESTS", 1)).allowed());
        assertTrue(join(engine.check(tenant, "API_REQUESTS", 1)).allowed());
        assertEquals("quota_exceeded", join(engine.check(tenant, "API_REQUESTS", 1)).reason());
    }

    @Test
    void aHugeDeltaIsClampedAndTheCounterStaysReadable() {
        String k = key();
        Result first = join(store.add(k, Long.MAX_VALUE / 2, Long.MAX_VALUE, Duration.ofMinutes(1)));
        assertTrue(first.applied());
        assertEquals(9007199254740992L, first.value()); // clamped at 2^53, exact in a double
        assertEquals(9007199254740992L, join(store.get(k))); // plain integer text, never exponent notation
        join(store.add(k, Long.MAX_VALUE / 2, Long.MAX_VALUE, Duration.ofMinutes(1)));
        assertEquals(9007199254740992L, join(store.get(k)));
        assertFalse(join(store.add(key(), Long.MAX_VALUE, 100, Duration.ofMinutes(1))).applied()); // a quota is never skipped
    }

    @Test
    void eachHolderExpiresOnItsOwnAndOnlyItsOwnSlotIsFreed() throws InterruptedException {
        String k = key();
        // sleeps only ever run long, so the "still held" check has the wide margin (800 ms) and the "gone" check the narrow one
        Duration lease = Duration.ofMillis(3000);
        assertTrue(join(store.acquireLease(k, "a", 1, 2, lease)).granted());
        Thread.sleep(1000);
        assertTrue(join(store.acquireLease(k, "b", 1, 2, lease)).granted()); // b expires 1 s after a
        assertFalse(join(store.acquireLease(k, "c", 1, 2, lease)).granted());

        Thread.sleep(2200); // a is gone (3.2 s of 3 s), b is still held (2.2 s of 3 s)
        assertEquals(1L, join(store.leasesInUse(k)));
        assertTrue(join(store.acquireLease(k, "c", 1, 2, lease)).granted());
        assertFalse(join(store.acquireLease(k, "d", 1, 2, lease)).granted());
    }

    @Test
    void releaseIsIdempotentAndNeverFreesAnotherHoldersSlot() {
        String k = key();
        join(store.acquireLease(k, "a", 1, 3, Duration.ofMinutes(1)));
        join(store.acquireLease(k, "b", 2, 3, Duration.ofMinutes(1)));
        join(store.releaseLease(k, "a"));
        join(store.releaseLease(k, "a"));
        join(store.releaseLease(k, "ghost"));
        assertEquals(2L, join(store.leasesInUse(k))); // b's weight untouched
        assertTrue(join(store.acquireLease(k, "b", 3, 3, Duration.ofMinutes(1))).granted()); // same holder replaces itself
        assertEquals(3L, join(store.leasesInUse(k)));
    }

    @Test
    void concurrentAcquiresNeverExceedTheCap() {
        String k = key();
        List<CompletableFuture<CounterStore.Lease>> calls = new ArrayList<>();
        for (int i = 0; i < 200; i++) calls.add(store.acquireLease(k, "h" + i, 1, 10, Duration.ofMinutes(1)).toCompletableFuture());
        assertEquals(10, calls.stream().map(CompletableFuture::join).filter(CounterStore.Lease::granted).count());
        assertEquals(10L, join(store.leasesInUse(k)));
    }

    @Test
    void aWeightLargerThanTheCapIsRefused() {
        assertFalse(join(store.acquireLease(key(), "a", Long.MAX_VALUE, 5, Duration.ofMinutes(1))).granted());
    }

    // ---- the engine's multi-step behaviour against real Redis (the same cases the in-memory tests cover) ------

    static final Metric API = new Metric("API_REQUESTS", Mode.CONSUME_ON_CHECK);
    static final Metric ITEMS = new Metric("ITEMS_PROCESSED", Mode.REPORT_ONLY);
    static final Instant NOON = Instant.parse("2026-11-19T12:00:00Z");

    static UsageEngine engineOver(Limit... limits) {
        return new UsageEngine(
                t -> CompletableFuture.completedFuture(Optional.of(new Policy(Instant.parse("2026-11-01T00:00:00Z"), List.of(limits)))),
                store, new InMemoryLedger(), e -> CompletableFuture.completedFuture(null),
                Clock.fixed(NOON, java.time.ZoneOffset.UTC), FailMode.CLOSED, Duration.ofMinutes(15));
    }

    @Test
    void aLimitThatRefusesRollsBackTheOnesAlreadyAppliedInRedis() {
        Limit daily = new Limit(API, Type.QUOTA, 5, Duration.ofDays(1), Enforcement.REJECT);
        Limit thirtyDay = new Limit(API, Type.QUOTA, 1, Duration.ofDays(30), Enforcement.REJECT);
        var engine = engineOver(daily, thirtyDay);
        TenantId tenant = new TenantId("rollback-" + UUID.randomUUID());
        assertTrue(join(engine.check(tenant, "API_REQUESTS", 1)).allowed());
        assertFalse(join(engine.check(tenant, "API_REQUESTS", 1)).allowed()); // the 30-day quota refuses the second
        // the daily counter must read 1, not 2: an engine with only the daily limit (same counter) still fits 4 more of 5
        assertTrue(join(engineOver(daily).check(tenant, "API_REQUESTS", 4)).allowed(), "the refused attempt leaked a daily slot");
    }

    @Test
    void undoCheckGivesBackTheQuotaInRedis() {
        var engine = engineOver(new Limit(API, Type.QUOTA, 3, Duration.ofDays(1), Enforcement.REJECT));
        TenantId tenant = new TenantId("undo-" + UUID.randomUUID());
        assertTrue(join(engine.check(tenant, "API_REQUESTS", 3, NOON)).allowed());
        assertFalse(join(engine.check(tenant, "API_REQUESTS", 1, NOON)).allowed());
        join(engine.undoCheck(tenant, "API_REQUESTS", 3, NOON));
        assertTrue(join(engine.check(tenant, "API_REQUESTS", 3, NOON)).allowed());
    }

    @Test
    void concurrencyPermitsAreTakenAndReleasedInRedis() {
        var engine = engineOver(Limit.concurrency(ITEMS, 1));
        TenantId tenant = new TenantId("lease-" + UUID.randomUUID());
        var first = join(engine.acquire(tenant, "ITEMS_PROCESSED", 1));
        assertTrue(first.decision().allowed());
        assertEquals("concurrency_exceeded", join(engine.acquire(tenant, "ITEMS_PROCESSED", 1)).decision().reason());
        join(first.release());
        join(first.release()); // releasing twice is harmless
        assertTrue(join(engine.acquire(tenant, "ITEMS_PROCESSED", 1)).decision().allowed());
    }
}
