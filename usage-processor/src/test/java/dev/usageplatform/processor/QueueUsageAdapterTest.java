package dev.usageplatform.processor;

import dev.usageplatform.core.InMemoryCounterStore;
import dev.usageplatform.core.InMemoryLedger;
import dev.usageplatform.core.Limit;
import dev.usageplatform.core.Limit.Enforcement;
import dev.usageplatform.core.Limit.Type;
import dev.usageplatform.core.Metric;
import dev.usageplatform.core.Metric.Mode;
import dev.usageplatform.core.Policy;
import dev.usageplatform.core.TenantId;
import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.UsageEngine.FailMode;
import dev.usageplatform.core.UsageEvent;
import dev.usageplatform.processor.QueueUsageAdapter.Done;
import dev.usageplatform.processor.QueueUsageAdapter.Result;
import dev.usageplatform.processor.QueueUsageAdapter.Verdict;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class QueueUsageAdapterTest {
    static final Metric ITEMS = new Metric("ITEMS_PROCESSED", Mode.REPORT_ONLY);
    static final TenantId T = new TenantId("acme");
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-19T00:00:00Z"), ZoneOffset.UTC);
    static final Instant FINISHED = Instant.parse("2026-11-19T00:00:05Z");
    static final Map<String, String> HEADERS = Map.of("X-Tenant-Id", "acme");

    final List<Optional<UsageEvent>> persisted = new ArrayList<>();
    UsageEngine engine;
    InMemoryLedger ledger;

    @BeforeEach
    void setUp() {
        persisted.clear();
        ledger = new InMemoryLedger();
        engine = engineWith(Limit.concurrency(ITEMS, 1));
    }

    UsageEngine engineWith(Limit... limits) {
        return new UsageEngine(t -> CompletableFuture.completedFuture(Optional.of(new Policy(Instant.parse("2026-11-01T00:00:00Z"), List.of(limits)))),
                new InMemoryCounterStore(CLOCK), ledger, e -> CompletableFuture.completedFuture(null),
                CLOCK, FailMode.OPEN, Duration.ofMinutes(15));
    }

    QueueUsageAdapter adapter(boolean deferWhenOver) {
        return new QueueUsageAdapter(engine, "ITEMS_PROCESSED", Set.of("COMPLETED", "APPROVED"), deferWhenOver, "x-tenant-id");
    }

    static CompletionStage<Done<String>> finished(String status) {
        return CompletableFuture.completedFuture(new Done<>("payload", status, FINISHED));
    }

    CompletionStage<Void> save(String result, Optional<UsageEvent> usage) {
        persisted.add(usage);
        return CompletableFuture.completedFuture(null);
    }

    Result handle(QueueUsageAdapter a, Map<String, String> headers, String item, String status) {
        return a.<String>handle(headers, item, () -> finished(status), this::save).toCompletableFuture().join();
    }

    @Test
    void countableStatusPersistsResultAndUsageTogetherWithDeterministicId() {
        Result r = handle(adapter(false), HEADERS, "doc-1", "COMPLETED");

        assertEquals(Verdict.COMPLETED, r.verdict());
        UsageEvent e = persisted.get(0).orElseThrow();
        assertEquals(UsageEvent.idFor(T, "doc-1", "ITEMS_PROCESSED"), e.eventId());
        assertEquals(T, e.tenant());
        assertEquals(FINISHED, e.occurredAt());
    }

    @Test
    void redeliveredItemProducesTheSameEvent() {
        handle(adapter(false), HEADERS, "doc-1", "COMPLETED");
        handle(adapter(false), HEADERS, "doc-1", "COMPLETED");
        assertEquals(persisted.get(0), persisted.get(1));
        // and the ledger really does count it once: the second delivery is a duplicate
        assertEquals(UsageEngine.Ingest.APPLIED, engine.ingest(persisted.get(0).orElseThrow()).toCompletableFuture().join());
        assertEquals(UsageEngine.Ingest.DUPLICATE, engine.ingest(persisted.get(1).orElseThrow()).toCompletableFuture().join());
    }

    @Test
    void nonCountableStatusPersistsResultWithoutUsage() {
        handle(adapter(false), HEADERS, "doc-2", "REJECTED");
        assertTrue(persisted.get(0).isEmpty());
    }

    @Test
    void messageWithoutTenantIsNotProcessed() {
        var calls = new AtomicInteger();
        Result r = adapter(false).<String>handle(Map.of(), "doc-3",
                () -> { calls.incrementAndGet(); return finished("COMPLETED"); }, this::save).toCompletableFuture().join();
        assertEquals(Verdict.NO_TENANT, r.verdict());
        assertEquals(0, calls.get());
    }

    @Test
    void headerNameIsCaseInsensitive() {
        assertEquals(Verdict.COMPLETED, handle(adapter(false), Map.of("X-TENANT-ID", "acme"), "doc-4", "COMPLETED").verdict());
    }

    @Test
    void sameHeaderUnderTwoCasingsWithDifferentTenantsIsNotProcessed() {
        var calls = new AtomicInteger();
        Result r = adapter(false).<String>handle(Map.of("X-Tenant-Id", "acme", "x-tenant-id", "other"), "doc-6",
                () -> { calls.incrementAndGet(); return finished("COMPLETED"); }, this::save).toCompletableFuture().join();
        assertEquals(Verdict.NO_TENANT, r.verdict());
        assertEquals(0, calls.get());
        // the same value under two casings is not ambiguous
        assertEquals(Verdict.COMPLETED, handle(adapter(false), Map.of("X-Tenant-Id", "acme", "x-tenant-id", "acme"), "doc-7", "COMPLETED").verdict());
    }

    @Test
    void fullConcurrencyDefersWithoutRunningTheWork() {
        engine.acquire(T, "ITEMS_PROCESSED", 1).toCompletableFuture().join(); // someone else holds the only slot
        var calls = new AtomicInteger();
        Result r = adapter(false).<String>handle(HEADERS, "doc-5",
                () -> { calls.incrementAndGet(); return finished("COMPLETED"); }, this::save).toCompletableFuture().join();
        assertEquals(Verdict.DEFERRED, r.verdict());
        assertEquals("concurrency_exceeded", r.detail());
        assertEquals(0, calls.get());
    }

    @Test
    void failedWorkReleasesThePermitAndPropagates() {
        var boom = new IllegalStateException("boom");
        var failing = adapter(false).<String>handle(HEADERS, "doc-6", () -> { throw boom; }, this::save);
        var ex = assertThrows(CompletionException.class, () -> failing.toCompletableFuture().join());
        assertSame(boom, ex.getCause());
        assertTrue(persisted.isEmpty());
        assertEquals(Verdict.COMPLETED, handle(adapter(false), HEADERS, "doc-6", "COMPLETED").verdict()); // slot was freed
    }

    @Test
    void failedPersistenceReleasesThePermitAndPropagates() {
        var failing = adapter(false).<String>handle(HEADERS, "doc-7", () -> finished("COMPLETED"),
                (r, u) -> CompletableFuture.failedFuture(new IllegalStateException("db down")));
        assertThrows(CompletionException.class, () -> failing.toCompletableFuture().join());
        assertEquals(Verdict.COMPLETED, handle(adapter(false), HEADERS, "doc-7", "COMPLETED").verdict());
    }

    @Test
    void guardDefersWhenTenantIsAlreadyOverQuotaOnlyIfEnabled() {
        engine = engineWith(new Limit(ITEMS, Type.QUOTA, 1, Duration.ofDays(1), Enforcement.REJECT));
        engine.ingest(new UsageEvent(UsageEvent.idFor(T, "old", "ITEMS_PROCESSED"), T, "ITEMS_PROCESSED", 1, CLOCK.instant()))
                .toCompletableFuture().join();

        assertEquals(Verdict.DEFERRED, handle(adapter(true), HEADERS, "doc-8", "COMPLETED").verdict());
        assertEquals(Verdict.COMPLETED, handle(adapter(false), HEADERS, "doc-8", "COMPLETED").verdict());
    }

    @Test
    void sameItemIdOfTwoTenantsGivesDifferentUsageEvents() {
        handle(adapter(false), Map.of("X-Tenant-Id", "acme"), "doc-1", "COMPLETED");
        handle(adapter(false), Map.of("X-Tenant-Id", "globex"), "doc-1", "COMPLETED");
        assertNotEquals(persisted.get(0).orElseThrow().eventId(), persisted.get(1).orElseThrow().eventId());
        assertEquals("globex", persisted.get(1).orElseThrow().tenant().value());
    }
}
