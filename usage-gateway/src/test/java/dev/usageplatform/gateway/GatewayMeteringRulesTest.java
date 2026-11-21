package dev.usageplatform.gateway;

import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.UsageEngine.FailMode;
import dev.usageplatform.core.memory.InMemoryCounterStore;
import dev.usageplatform.core.memory.InMemoryLedger;
import dev.usageplatform.core.model.Compensation;
import dev.usageplatform.core.model.Limit;
import dev.usageplatform.core.model.Limit.Enforcement;
import dev.usageplatform.core.model.Limit.Type;
import dev.usageplatform.core.model.Metric;
import dev.usageplatform.core.model.Metric.Mode;
import dev.usageplatform.core.model.Plan;
import dev.usageplatform.core.model.Policy;
import dev.usageplatform.core.model.TenantId;
import dev.usageplatform.core.model.UsageEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Regression tests for what a refused, malformed or abandoned request is allowed to cost. */
class GatewayMeteringRulesTest {
    static final Metric API = new Metric("API_REQUESTS", Mode.CONSUME_ON_CHECK);
    static final Metric OTHER = new Metric("OTHER_REQUESTS", Mode.CONSUME_ON_CHECK);
    static final Metric ITEMS = new Metric("ITEMS_PROCESSED", Mode.REPORT_ONLY);
    static final TenantId ACME = new TenantId("acme");
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-19T00:00:00Z"), ZoneOffset.UTC);
    static final Duration DAY = Duration.ofDays(1);

    final List<UsageEvent> events = new ArrayList<>();
    final List<Compensation> compensations = new ArrayList<>();
    UsageEngine engine;

    @BeforeEach
    void setUp() {
        Policy policy = new Policy(Instant.parse("2026-11-01T00:00:00Z"), List.of(
                new Limit(API, Type.QUOTA, 5, DAY, Enforcement.REJECT),
                new Limit(OTHER, Type.QUOTA, 1, DAY, Enforcement.REJECT),
                new Limit(ITEMS, Type.QUOTA, 1, DAY, Enforcement.REJECT)));
        engine = new UsageEngine(t -> CompletableFuture.completedFuture(Optional.of(policy)), new InMemoryCounterStore(CLOCK),
                new InMemoryLedger(CLOCK), message -> {
                    switch (message) {
                        case UsageEvent e -> events.add(e);
                        case Compensation c -> compensations.add(c);
                    }
                    return CompletableFuture.completedFuture(null);
                }, CLOCK, FailMode.CLOSED, Duration.ofMinutes(15));
    }

    UsageGatewayFilter filter(MeteredMetric... metrics) {
        return new UsageGatewayFilter(engine, ex -> CompletableFuture.completedFuture(Optional.of(ACME)), List.of(metrics), ex -> false, CLOCK);
    }

    static MockServerWebExchange request(String... headers) {
        var b = MockServerHttpRequest.get("/orders");
        for (int i = 0; i < headers.length; i += 2) b.header(headers[i], headers[i + 1]);
        return MockServerWebExchange.from(b.build());
    }

    boolean allowed(String metric, long cost) {
        return engine.check(ACME, metric, cost).toCompletableFuture().join().allowed();
    }

    UsageGatewayFilter filterUnder(Plan plan, MeteredMetric... metrics) {
        Policy policy = new Policy(plan, Instant.parse("2026-11-01T00:00:00Z"), List.of(new Limit(API, Type.QUOTA, 5, DAY, Enforcement.REJECT)));
        var planned = new UsageEngine(t -> CompletableFuture.completedFuture(Optional.of(policy)), new InMemoryCounterStore(CLOCK),
                new InMemoryLedger(CLOCK), message -> CompletableFuture.completedFuture(null), CLOCK, FailMode.CLOSED, Duration.ofMinutes(15));
        return new UsageGatewayFilter(planned, ex -> CompletableFuture.completedFuture(Optional.of(ACME)), List.of(metrics), ex -> false, CLOCK);
    }

    @Test
    void thePlanIsForwardedAndAnyPlanHeaderFromTheClientIsReplaced() {
        var seen = new java.util.concurrent.atomic.AtomicReference<org.springframework.web.server.ServerWebExchange>();
        var forged = request("X-Plan-Code", "FREE", "x-plan-version", "99", "X-Plan-Anything", "1");
        filterUnder(new Plan("PRO", 3), MeteredMetric.access("API_REQUESTS")).filter(forged, c -> {
            seen.set(c);
            return Mono.empty();
        }).block();
        var headers = seen.get().getRequest().getHeaders();
        assertEquals(List.of("PRO"), headers.get("X-Plan-Code"));
        assertEquals(List.of("3"), headers.get("X-Plan-Version"));
        assertNull(headers.getFirst("X-Plan-Anything"));
    }

    @Test
    void withoutARealPlanNoPlanHeaderIsSentAndForgedOnesAreStillDropped() {
        var seen = new java.util.concurrent.atomic.AtomicReference<org.springframework.web.server.ServerWebExchange>();
        filterUnder(Plan.UNSPECIFIED, MeteredMetric.access("API_REQUESTS")).filter(request("X-Plan-Code", "FREE"), c -> {
            seen.set(c);
            return Mono.empty();
        }).block();
        assertNull(seen.get().getRequest().getHeaders().getFirst("X-Plan-Code"));
    }

    @Test
    void aRefusedGuardDoesNotCostTheAccessQuota() {
        engine.ingest(new UsageEvent(UsageEvent.idFor(ACME, "x", "ITEMS_PROCESSED"), ACME, "ITEMS_PROCESSED", 1, CLOCK.instant()))
                .toCompletableFuture().join(); // the result quota is already used up

        var ex = request();
        // configured with the access metric first: the guard must still be asked first
        filter(MeteredMetric.access("OTHER_REQUESTS"), MeteredMetric.guard("ITEMS_PROCESSED")).filter(ex, c -> Mono.empty()).block();

        assertEquals(HttpStatus.TOO_MANY_REQUESTS, ex.getResponse().getStatusCode());
        assertTrue(allowed("OTHER_REQUESTS", 1));  // untouched: the one request it has is still there
        assertFalse(allowed("OTHER_REQUESTS", 1)); // and only now is it gone
    }

    @Test
    void whenALaterAccessMetricRefusesTheEarlierOnesAreGivenBack() {
        assertTrue(allowed("OTHER_REQUESTS", 1)); // OTHER is now exhausted
        var ex = request();
        var reached = new AtomicInteger();

        filter(MeteredMetric.access("API_REQUESTS", e -> 5), MeteredMetric.access("OTHER_REQUESTS"))
                .filter(ex, c -> { reached.incrementAndGet(); return Mono.empty(); }).block();

        assertEquals(HttpStatus.TOO_MANY_REQUESTS, ex.getResponse().getStatusCode());
        assertEquals(0, reached.get());
        assertTrue(events.isEmpty());
        assertTrue(allowed("API_REQUESTS", 5)); // the 5 consumed before OTHER refused are back
    }

    @Test
    void unusableCostsAreRefusedWith400AndCostNothing() {
        for (String bad : new String[]{"abc", "0", "-3", String.valueOf(Long.MAX_VALUE), String.valueOf(UsageEngine.MAX_COST + 1), "1.5", ""}) {
            var ex = request("X-Item-Count", bad);
            var reached = new AtomicInteger();
            filter(MeteredMetric.access("API_REQUESTS", e -> Long.parseLong(e.getRequest().getHeaders().getFirst("X-Item-Count"))))
                    .filter(ex, c -> { reached.incrementAndGet(); return Mono.empty(); }).block();
            assertEquals(HttpStatus.BAD_REQUEST, ex.getResponse().getStatusCode(), "cost '" + bad + "'");
            assertEquals(0, reached.get());
        }
        assertTrue(events.isEmpty());
        assertTrue(allowed("API_REQUESTS", 5)); // every refused request left the quota whole
    }

    @Test
    void aRequestAbandonedByTheClientIsStillBilledExactlyOnceAndNotRefunded() {
        var neverEnds = filter(MeteredMetric.access("API_REQUESTS")).filter(request(), c -> Mono.never());
        var subscription = neverEnds.subscribe();
        assertTrue(events.isEmpty()); // nothing is reported while the request is still running

        subscription.dispose(); // the client hangs up

        assertEquals(1, events.size());
        assertEquals("API_REQUESTS", events.get(0).metricCode());
        assertTrue(compensations.isEmpty());
        assertFalse(allowed("API_REQUESTS", 5)); // and its quota stays consumed
    }

    @Test
    void aRequestThatFinishesIsReportedOnceEvenIfTheClientCancelsAfterwards() {
        var subscription = filter(MeteredMetric.access("API_REQUESTS")).filter(request(), c -> Mono.empty()).subscribe();
        subscription.dispose();
        assertEquals(1, events.size());
    }

    // ---- every reason the engine can give maps to the documented status ---------------------------------

    private String refusedWith(HttpStatus status, UsageEngine custom, MeteredMetric... metrics) {
        var ex = request();
        new UsageGatewayFilter(custom, e -> CompletableFuture.completedFuture(Optional.of(ACME)), List.of(metrics), e -> false, CLOCK)
                .filter(ex, c -> Mono.empty()).block();
        assertEquals(status, ex.getResponse().getStatusCode());
        return ex.getResponse().getBodyAsString().block();
    }

    private UsageEngine engineFor(Policy policy, FailMode mode, dev.usageplatform.core.spi.CounterStore counters) {
        return new UsageEngine(t -> CompletableFuture.completedFuture(Optional.ofNullable(policy)), counters, new InMemoryLedger(CLOCK),
                message -> CompletableFuture.completedFuture(null), CLOCK, mode, Duration.ofMinutes(15));
    }

    @Test
    void aTenantWithoutAPolicyGets403() {
        assertEquals("{\"error\":\"no_policy\"}", refusedWith(HttpStatus.FORBIDDEN,
                engineFor(null, FailMode.CLOSED, new InMemoryCounterStore(CLOCK)), MeteredMetric.access("API_REQUESTS")));
    }

    @Test
    void aStoreOutageUnderFailClosedGets503() {
        var down = new dev.usageplatform.core.spi.CounterStore() {
            public java.util.concurrent.CompletionStage<Result> add(String k, long d, long c, Duration t) { return CompletableFuture.failedFuture(new IllegalStateException("redis down")); }
            public java.util.concurrent.CompletionStage<Long> get(String k) { return CompletableFuture.failedFuture(new IllegalStateException("redis down")); }
        };
        Policy p = new Policy(Instant.parse("2026-11-01T00:00:00Z"), List.of(new Limit(API, Type.QUOTA, 5, DAY, Enforcement.REJECT)));
        assertEquals("{\"error\":\"store_unavailable\"}", refusedWith(HttpStatus.SERVICE_UNAVAILABLE,
                engineFor(p, FailMode.CLOSED, down), MeteredMetric.access("API_REQUESTS")));
    }

    @Test
    void theRateLimitAnswers429() {
        Policy p = new Policy(Instant.parse("2026-11-01T00:00:00Z"), List.of(new Limit(API, Type.RATE, 1, Duration.ofSeconds(1), Enforcement.REJECT)));
        var rateLimited = engineFor(p, FailMode.CLOSED, new InMemoryCounterStore(CLOCK));
        var filter = new UsageGatewayFilter(rateLimited, e -> CompletableFuture.completedFuture(Optional.of(ACME)),
                List.of(MeteredMetric.access("API_REQUESTS")), e -> false, CLOCK);
        filter.filter(request(), c -> Mono.empty()).block();
        var second = request();
        filter.filter(second, c -> Mono.empty()).block();
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, second.getResponse().getStatusCode());
        assertEquals("{\"error\":\"rate_limited\"}", second.getResponse().getBodyAsString().block());
    }

    @Test
    void aFullConcurrencyLimitAnswers429() {
        Policy p = new Policy(Instant.parse("2026-11-01T00:00:00Z"), List.of(Limit.concurrency(ITEMS, 1)));
        var busy = engineFor(p, FailMode.CLOSED, new InMemoryCounterStore(CLOCK));
        busy.acquire(ACME, "ITEMS_PROCESSED", 1).toCompletableFuture().join(); // a processor holds the only slot
        assertEquals("{\"error\":\"concurrency_exceeded\"}", refusedWith(HttpStatus.TOO_MANY_REQUESTS, busy, MeteredMetric.guard("ITEMS_PROCESSED")));
    }
}
