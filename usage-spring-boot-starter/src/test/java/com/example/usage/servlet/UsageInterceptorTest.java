package com.example.usage.servlet;

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
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class UsageInterceptorTest {
    static final Metric API = new Metric("API_REQUESTS", Mode.CONSUME_ON_CHECK);
    static final Metric ITEMS = new Metric("ITEMS", Mode.CONSUME_ON_CHECK);
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-19T00:00:00Z"), ZoneOffset.UTC);
    static final Instant ANCHOR = Instant.parse("2026-11-01T00:00:00Z");

    final List<UsageEvent> events = new ArrayList<>();
    final List<Compensation> compensations = new ArrayList<>();
    UsageSink sink;

    @BeforeEach
    void setUp() {
        sink = message -> {
            switch (message) {
                case UsageEvent e -> events.add(e);
                case Compensation c -> compensations.add(c);
            }
            return CompletableFuture.completedFuture(null);
        };
    }

    UsageInterceptor interceptor(long apiQuota, long itemsQuota, RouteMetric... metrics) {
        UsageEngine engine = new UsageEngine(
                t -> CompletableFuture.completedFuture(Optional.of(new Policy(ANCHOR, List.of(
                        new Limit(API, Type.QUOTA, apiQuota, Duration.ofDays(1), Enforcement.REJECT),
                        new Limit(ITEMS, Type.QUOTA, itemsQuota, Duration.ofDays(1), Enforcement.REJECT))))),
                new InMemoryCounterStore(CLOCK), new InMemoryLedger(), sink, CLOCK, FailMode.OPEN, Duration.ofMinutes(15));
        TenantResolver<HttpServletRequest> tenants = r -> CompletableFuture.completedFuture(
                Optional.ofNullable(r.getHeader("X-Client")).map(TenantId::new)); // stands in for the JWT claim
        return new UsageInterceptor(engine, tenants, List.of(metrics), CLOCK);
    }

    static MockHttpServletRequest request(String path, String... headers) {
        MockHttpServletRequest r = new MockHttpServletRequest("POST", path);
        for (int i = 0; i < headers.length; i += 2) r.addHeader(headers[i], headers[i + 1]);
        return r;
    }

    /** One whole request: the interceptor, then (when it let the request in) a response with this status. */
    boolean call(UsageInterceptor in, MockHttpServletRequest req, MockHttpServletResponse res, int status) throws Exception {
        boolean proceed = in.preHandle(req, res, new Object());
        if (proceed) {
            res.setStatus(status);
            in.afterCompletion(req, res, new Object(), null);
        }
        return proceed;
    }

    @Test
    void aRequestOnAMeteredRouteIsReportedAndCarriesTheTenant() throws Exception {
        var in = interceptor(10, 10, RouteMetric.access("/orders/**", "API_REQUESTS"));
        var req = request("/orders/42", "X-Client", "acme");
        assertTrue(call(in, req, new MockHttpServletResponse(), 200));
        assertEquals("acme", ((TenantId) req.getAttribute(UsageInterceptor.TENANT_ATTRIBUTE)).value());
        assertEquals(1, events.size());
        assertEquals("API_REQUESTS", events.get(0).metricCode());
    }

    @Test
    void aRouteWithoutMetricsIsNotMeteredAndNeedsNoTenant() throws Exception {
        var in = interceptor(10, 10, RouteMetric.access("/orders/**", "API_REQUESTS"));
        assertTrue(call(in, request("/health"), new MockHttpServletResponse(), 200));
        assertTrue(events.isEmpty());
    }

    @Test
    void overQuotaIsRefusedWith429AndRetryAfterAndNothingIsReported() throws Exception {
        var in = interceptor(1, 10, RouteMetric.access("/orders/**", "API_REQUESTS"));
        call(in, request("/orders/1", "X-Client", "acme"), new MockHttpServletResponse(), 200);
        events.clear();

        var res = new MockHttpServletResponse();
        assertFalse(call(in, request("/orders/2", "X-Client", "acme"), res, 200));
        assertEquals(429, res.getStatus());
        assertEquals("{\"error\":\"quota_exceeded\"}", res.getContentAsString());
        assertTrue(Long.parseLong(res.getHeader("Retry-After")) >= 1);
        assertTrue(events.isEmpty());
    }

    @Test
    void theCostComesFromTheHeaderOfTheRoute() throws Exception {
        var in = interceptor(10, 5, RouteMetric.access("/batches", "ITEMS", r -> Long.parseLong(r.getHeader("X-Item-Count"))));
        var tooBig = new MockHttpServletResponse();
        assertFalse(call(in, request("/batches", "X-Client", "acme", "X-Item-Count", "6"), tooBig, 200));
        assertEquals(429, tooBig.getStatus());

        assertTrue(call(in, request("/batches", "X-Client", "acme", "X-Item-Count", "5"), new MockHttpServletResponse(), 200));
        assertEquals(5, events.get(0).quantity());
    }

    @Test
    void theContainersErrorForwardIsTheSameRequestAndIsNotMeteredAgain() throws Exception {
        var in = interceptor(10, 10, RouteMetric.access("/**", "API_REQUESTS"));
        var req = request("/orders/1", "X-Client", "acme");
        in.preHandle(req, new MockHttpServletResponse(), new Object());
        var error = request("/error", "X-Client", "acme");
        error.setDispatcherType(jakarta.servlet.DispatcherType.ERROR);
        assertTrue(in.preHandle(error, new MockHttpServletResponse(), new Object()));
        in.afterCompletion(error, new MockHttpServletResponse(), new Object(), null); // nothing to report: it was not metered
        in.afterCompletion(req, new MockHttpServletResponse(), new Object(), null);
        assertEquals(1, events.size());
    }

    @Test
    void twoRoutesThatMatchTheSamePathChargeTheSameMetricOnce() throws Exception {
        var in = interceptor(10, 10, RouteMetric.access("/**", "API_REQUESTS"), RouteMetric.access("/orders/**", "API_REQUESTS"));
        call(in, request("/orders/1", "X-Client", "acme"), new MockHttpServletResponse(), 200);
        assertEquals(1, events.size());
    }

    @Test
    void aMalformedCostIsRefusedWith400() throws Exception {
        var in = interceptor(10, 10, RouteMetric.access("/batches", "ITEMS", r -> Long.parseLong(r.getHeader("X-Item-Count"))));
        var res = new MockHttpServletResponse();
        assertFalse(call(in, request("/batches", "X-Client", "acme", "X-Item-Count", "many"), res, 200));
        assertEquals(400, res.getStatus());
        assertTrue(events.isEmpty());
    }

    @Test
    void aTenantThatIsNotLinkedIsRefusedWith403() throws Exception {
        var in = interceptor(10, 10, RouteMetric.access("/orders/**", "API_REQUESTS"));
        var res = new MockHttpServletResponse();
        assertFalse(call(in, request("/orders/1"), res, 200));
        assertEquals(403, res.getStatus());
    }

    @Test
    void aServerErrorIsReportedAndThenRefunded() throws Exception {
        var in = interceptor(10, 10, RouteMetric.access("/orders/**", "API_REQUESTS"));
        call(in, request("/orders/1", "X-Client", "acme"), new MockHttpServletResponse(), 503);
        assertEquals(1, events.size());
        assertEquals(1, compensations.size());
    }

    @Test
    void anExceptionIsRefundedToo() throws Exception {
        var in = interceptor(10, 10, RouteMetric.access("/orders/**", "API_REQUESTS"));
        var req = request("/orders/1", "X-Client", "acme");
        var res = new MockHttpServletResponse();
        in.preHandle(req, res, new Object());
        in.afterCompletion(req, res, new Object(), new IllegalStateException("boom"));
        assertEquals(1, compensations.size());
    }

    @Test
    void whenASecondMetricRefusesTheFirstOneIsGivenBack() throws Exception {
        var in = interceptor(1, 0 + 1, RouteMetric.access("/batches", "API_REQUESTS"),
                RouteMetric.access("/batches", "ITEMS", r -> 2)); // ITEMS quota is 1, the request costs 2
        var res = new MockHttpServletResponse();
        assertFalse(call(in, request("/batches", "X-Client", "acme"), res, 200));
        assertEquals(429, res.getStatus());
        // the API_REQUESTS unit was given back: the next request on a route that only counts it still fits in its quota of 1
        var other = interceptor(1, 1, RouteMetric.access("/batches", "API_REQUESTS"));
        assertTrue(call(other, request("/batches", "X-Client", "acme"), new MockHttpServletResponse(), 200));
    }
}
