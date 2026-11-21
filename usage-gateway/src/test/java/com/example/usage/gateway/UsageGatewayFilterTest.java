package com.example.usage.gateway;

import com.example.usage.core.UsageEngine;
import com.example.usage.core.UsageEngine.FailMode;
import com.example.usage.core.memory.InMemoryCounterStore;
import com.example.usage.core.memory.InMemoryLedger;
import com.example.usage.core.model.Compensation;
import com.example.usage.core.model.Limit;
import com.example.usage.core.model.Limit.Enforcement;
import com.example.usage.core.model.Limit.Type;
import com.example.usage.core.model.Metric;
import com.example.usage.core.model.Metric.Mode;
import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.model.UsageEvent;
import com.example.usage.core.spi.TenantResolver;
import com.example.usage.core.spi.UsageSink;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntPredicate;

import static org.junit.jupiter.api.Assertions.*;

class UsageGatewayFilterTest {
    static final Metric API = new Metric("API_REQUESTS", Mode.CONSUME_ON_CHECK);
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-19T00:00:00Z"), ZoneOffset.UTC);
    static final Instant ANCHOR = Instant.parse("2026-11-01T00:00:00Z");

    final List<UsageEvent> events = new ArrayList<>();
    final List<Compensation> compensations = new ArrayList<>();
    final AtomicReference<ServerWebExchange> forwarded = new AtomicReference<>();
    final GatewayFilterChain chain = ex -> { forwarded.set(ex); return Mono.empty(); };
    UsageSink sink;

    @BeforeEach
    void setUp() {
        events.clear();
        compensations.clear();
        forwarded.set(null);
        sink = message -> {
            switch (message) {
                case UsageEvent e -> events.add(e);
                case Compensation c -> compensations.add(c);
            }
            return CompletableFuture.completedFuture(null);
        };
    }

    UsageGatewayFilter filter(MeteredMetric metric, Enforcement enforcement, long quota) {
        return filter(metric, enforcement, quota, status -> status >= 500);
    }

    UsageGatewayFilter filter(MeteredMetric metric, Enforcement enforcement, long quota, IntPredicate refundStatus) {
        UsageEngine engine = new UsageEngine(
                t -> CompletableFuture.completedFuture(Optional.of(new Policy(ANCHOR,
                        List.of(new Limit(API, Type.QUOTA, quota, Duration.ofDays(1), enforcement))))),
                new InMemoryCounterStore(CLOCK), new InMemoryLedger(), sink, CLOCK, FailMode.OPEN, Duration.ofMinutes(15));
        TenantResolver<ServerWebExchange> tenants = ex -> CompletableFuture.completedFuture(
                Optional.ofNullable(ex.getRequest().getHeaders().getFirst("X-Client")).map(TenantId::new));
        return new UsageGatewayFilter(engine, tenants, List.of(metric), ex -> ex.getRequest().getHeaders().getFirst("X-Internal") != null, CLOCK, refundStatus);
    }

    static MockServerWebExchange request(String... headers) {
        MockServerHttpRequest.BaseBuilder<?> b = MockServerHttpRequest.get("/orders");
        for (int i = 0; i < headers.length; i += 2) b.header(headers[i], headers[i + 1]);
        return MockServerWebExchange.from(b.build());
    }

    @Test
    void allowedRequestIsForwardedWithTrustedTenantHeaderAndReported() {
        var f = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.REJECT, 10);
        f.filter(request("X-Client", "acme", "X-Tenant-Id", "spoofed", "X-Tenant-Plan", "gold"), chain).block();

        var headers = forwarded.get().getRequest().getHeaders();
        assertEquals(List.of("acme"), headers.get("X-Tenant-Id"));
        assertNull(headers.getFirst("X-Tenant-Plan"));
        assertEquals(1, events.size());
        assertEquals(1, events.get(0).quantity());
        assertEquals("acme", events.get(0).tenant().value());
    }

    @Test
    void overQuotaIsRefusedWith429AndNothingIsForwardedOrReported() {
        var f = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.REJECT, 1);
        f.filter(request("X-Client", "acme"), chain).block();
        forwarded.set(null);
        events.clear();

        var ex = request("X-Client", "acme");
        f.filter(ex, chain).block();

        assertEquals(HttpStatus.TOO_MANY_REQUESTS, ex.getResponse().getStatusCode());
        assertEquals("{\"error\":\"quota_exceeded\"}", ex.getResponse().getBodyAsString().block());
        assertNull(forwarded.get());
        assertTrue(events.isEmpty());
    }

    @Test
    void batchCostComesFromTheRequestAndIsCheckedOnce() {
        var batch = MeteredMetric.access("API_REQUESTS",
                ex -> Long.parseLong(ex.getRequest().getHeaders().getFirst("X-Item-Count")));
        var f = filter(batch, Enforcement.REJECT, 5);

        var tooBig = request("X-Client", "acme", "X-Item-Count", "6");
        f.filter(tooBig, chain).block();
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, tooBig.getResponse().getStatusCode());

        f.filter(request("X-Client", "acme", "X-Item-Count", "5"), chain).block();
        assertEquals(5, events.get(0).quantity());
    }

    @Test
    void overageIsForwardedDownstreamAsHeader() {
        var f = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.ALLOW_OVERAGE, 1);
        f.filter(request("X-Client", "acme"), chain).block();
        f.filter(request("X-Client", "acme", "X-Overage", "false"), chain).block();
        assertEquals(List.of("true"), forwarded.get().getRequest().getHeaders().get("X-Overage"));
    }

    @Test
    void clientNotLinkedToATenantGets403() {
        var f = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.REJECT, 10);
        var ex = request();
        f.filter(ex, chain).block();
        assertEquals(HttpStatus.FORBIDDEN, ex.getResponse().getStatusCode());
        assertNull(forwarded.get());
    }

    @Test
    void internalTrafficIsNotMetered() {
        var f = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.REJECT, 0);
        f.filter(request("X-Internal", "1"), chain).block();
        assertNotNull(forwarded.get());
        assertTrue(events.isEmpty());
    }

    @Test
    void lostUsageEventNeverFailsTheResponse() {
        sink = e -> CompletableFuture.failedFuture(new IllegalStateException("broker down"));
        var f = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.REJECT, 10);
        var ex = request("X-Client", "acme");
        assertDoesNotThrow(() -> f.filter(ex, chain).block());
        assertNotNull(forwarded.get());
    }

    static GatewayFilterChain respondingWith(HttpStatus status, AtomicReference<ServerWebExchange> seen) {
        return ex -> { seen.set(ex); ex.getResponse().setStatusCode(status); return Mono.empty(); };
    }

    @Test
    void serverErrorGivesTheQuotaBackThroughACompensationOfTheSameUsage() {
        var f = filter(MeteredMetric.access("API_REQUESTS", ex -> 3), Enforcement.REJECT, 10);
        f.filter(request("X-Client", "acme"), respondingWith(HttpStatus.BAD_GATEWAY, forwarded)).block();

        assertEquals(1, events.size());
        assertEquals(1, compensations.size());
        Compensation c = compensations.get(0);
        assertEquals(events.get(0).eventId(), c.compensates());
        assertEquals(Compensation.idFor(events.get(0).eventId()), c.eventId());
        assertEquals(3, c.quantity());
        assertEquals("API_REQUESTS", c.metricCode());
        assertEquals("acme", c.tenant().value());
    }

    @Test
    void successAndClientErrorsAreBilledAsUsual() {
        var f = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.REJECT, 10);
        f.filter(request("X-Client", "acme"), respondingWith(HttpStatus.OK, forwarded)).block();
        f.filter(request("X-Client", "acme"), respondingWith(HttpStatus.NOT_FOUND, forwarded)).block();
        assertEquals(2, events.size());
        assertTrue(compensations.isEmpty());
    }

    @Test
    void upstreamExceptionIsRefundedAndStillPropagates() {
        var f = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.REJECT, 10);
        var boom = new IllegalStateException("connection reset");
        var failing = f.filter(request("X-Client", "acme"), ex -> Mono.error(boom));
        var thrown = assertThrows(IllegalStateException.class, failing::block);
        assertSame(boom, thrown);
        assertEquals(1, events.size());
        assertEquals(1, compensations.size());
    }

    @Test
    void anUpstreamExceptionIsRefundedOnlyIfTheConfiguredPredicateRefundsA500() {
        var boom = new IllegalStateException("connection reset");
        var disabled = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.REJECT, 10, status -> status >= 600); // "refunds off"
        assertThrows(IllegalStateException.class, () -> disabled.filter(request("X-Client", "acme"), ex -> Mono.error(boom)).block());
        assertEquals(1, events.size());          // still billed
        assertTrue(compensations.isEmpty());     // and not refunded: 600 really turns refunds off, for exceptions too

        var enabled = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.REJECT, 10, status -> status >= 500);
        assertThrows(IllegalStateException.class, () -> enabled.filter(request("X-Client", "acme"), ex -> Mono.error(boom)).block());
        assertEquals(1, compensations.size());
    }

    @Test
    void whichStatusesAreRefundedIsConfigurable() {
        var f = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.REJECT, 10, status -> status == 503);
        f.filter(request("X-Client", "acme"), respondingWith(HttpStatus.BAD_GATEWAY, forwarded)).block();
        assertTrue(compensations.isEmpty());
        f.filter(request("X-Client", "acme"), respondingWith(HttpStatus.SERVICE_UNAVAILABLE, forwarded)).block();
        assertEquals(1, compensations.size());
    }

    @Test
    void noRefundIsSentForAUsageEventThePlatformNeverReceived() {
        sink = message -> message instanceof UsageEvent
                ? CompletableFuture.failedFuture(new IllegalStateException("broker down"))
                : CompletableFuture.completedFuture(null);
        var f = filter(MeteredMetric.access("API_REQUESTS"), Enforcement.REJECT, 10);
        assertDoesNotThrow(() -> f.filter(request("X-Client", "acme"), respondingWith(HttpStatus.BAD_GATEWAY, forwarded)).block());
        assertTrue(compensations.isEmpty());
    }

    @Test
    void guardOnlyMetricsAreNeverRefunded() {
        var f = filter(MeteredMetric.guard("API_REQUESTS"), Enforcement.REJECT, 10);
        f.filter(request("X-Client", "acme"), respondingWith(HttpStatus.BAD_GATEWAY, forwarded)).block();
        assertTrue(events.isEmpty());
        assertTrue(compensations.isEmpty());
    }

    @Test
    void aDegradedAllowIsNotUndoneWhenALaterMetricRefuses() {
        Metric a = new Metric("METRIC_A", Mode.CONSUME_ON_CHECK);
        Metric b = new Metric("METRIC_B", Mode.CONSUME_ON_CHECK);
        InMemoryCounterStore real = new InMemoryCounterStore(CLOCK);
        List<Long> negativeAdds = new ArrayList<>();
        com.example.usage.core.spi.CounterStore spy = new com.example.usage.core.spi.CounterStore() {
            public CompletionStage<Result> add(String k, long d, long c, Duration t) {
                if (d < 0) negativeAdds.add(d);
                // the store is down for A only: its check degrades to an allow that charged nothing
                return k.contains("METRIC_A") ? CompletableFuture.failedFuture(new IllegalStateException("down")) : real.add(k, d, c, t);
            }
            public CompletionStage<Long> get(String k) { return real.get(k); }
        };
        UsageEngine engine = new UsageEngine(
                t -> CompletableFuture.completedFuture(Optional.of(new Policy(ANCHOR, List.of(
                        new Limit(a, Type.QUOTA, 10, Duration.ofDays(1), Enforcement.REJECT),
                        new Limit(b, Type.QUOTA, 0, Duration.ofDays(1), Enforcement.REJECT))))),
                spy, new InMemoryLedger(), sink, CLOCK, FailMode.OPEN, Duration.ofMinutes(15));
        TenantResolver<ServerWebExchange> tenants = ex -> CompletableFuture.completedFuture(Optional.of(new TenantId("acme")));
        var f = new UsageGatewayFilter(engine, tenants, List.of(MeteredMetric.access("METRIC_A"), MeteredMetric.access("METRIC_B")),
                ex -> false, CLOCK, status -> status >= 500);

        MockServerWebExchange ex = request();
        f.filter(ex, chain).block();

        assertEquals(HttpStatus.TOO_MANY_REQUESTS, ex.getResponse().getStatusCode());
        assertTrue(negativeAdds.isEmpty(), "nothing was charged for A, so nothing may be given back: " + negativeAdds);
    }
}
