package com.example.usage.servlet;

import com.example.usage.core.UsageEngine;
import com.example.usage.core.model.Compensation;
import com.example.usage.core.model.Decision;
import com.example.usage.core.model.Plan;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.model.UsageEvent;
import com.example.usage.core.spi.TenantResolver;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.util.UrlPathHelper;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.IntPredicate;

/**
 * The blocking counterpart of the gateway filter, for a Spring MVC service. {@code preHandle} checks the metrics of the routes the
 * request path matches and refuses with 429 / 403 / 503 (and {@code Retry-After}); {@code afterCompletion} reports the usage, and
 * gives it back when the request ended in a refunded status or with an exception. A request that matches no route is not metered.
 * The tenant comes from the host's {@link TenantResolver} (typically the JWT already validated by Spring Security).
 * It differs from the gateway in one way: request headers cannot be rewritten here, so tenant, plan and overage are request attributes.
 */
public final class UsageInterceptor implements HandlerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(UsageInterceptor.class);
    public static final String TENANT_ATTRIBUTE = UsageInterceptor.class.getName() + ".tenant";
    public static final String PLAN_ATTRIBUTE = UsageInterceptor.class.getName() + ".plan";
    public static final String OVERAGE_ATTRIBUTE = UsageInterceptor.class.getName() + ".overage";
    private static final String METERED_ATTRIBUTE = UsageInterceptor.class.getName() + ".metered";

    private record Metered(TenantId tenant, List<RouteMetric> metrics, long[] costs, Instant at) {}

    private final UsageEngine engine;
    private final TenantResolver<HttpServletRequest> tenants;
    private final List<RouteMetric> metrics;
    private final List<PathPattern> patterns;
    private final Clock clock;
    private final IntPredicate refundStatus;

    public UsageInterceptor(UsageEngine engine, TenantResolver<HttpServletRequest> tenants, List<RouteMetric> metrics, Clock clock) {
        this(engine, tenants, metrics, clock, status -> status >= 500);
    }

    /** @param refundStatus which response statuses are not billed (default: every 5xx). */
    public UsageInterceptor(UsageEngine engine, TenantResolver<HttpServletRequest> tenants, List<RouteMetric> metrics, Clock clock,
                            IntPredicate refundStatus) {
        if (metrics.isEmpty()) throw new IllegalArgumentException("at least one metric is required");
        this.engine = engine;
        this.tenants = tenants;
        // guards first: refuse on a result metric before any access counter is touched
        this.metrics = metrics.stream().sorted(Comparator.comparing(RouteMetric::emitsUsage)).toList();
        this.patterns = this.metrics.stream().map(m -> PathPatternParser.defaultInstance.parse(m.pattern())).toList();
        this.clock = clock;
        this.refundStatus = refundStatus;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws IOException {
        // only the request itself: the async re-dispatch and the container's ERROR forward are the same client request, already metered
        if (request.getDispatcherType() != DispatcherType.REQUEST) return true;
        String path = UrlPathHelper.defaultInstance.getPathWithinApplication(request);
        var container = org.springframework.http.server.PathContainer.parsePath(path);
        // one charge per metric code, even when two routes (/** and /orders/**) both match: the first one wins
        Map<String, RouteMetric> byCode = new LinkedHashMap<>();
        for (int i = 0; i < metrics.size(); i++) if (patterns.get(i).matches(container)) byCode.putIfAbsent(metrics.get(i).code(), metrics.get(i));
        if (byCode.isEmpty()) return true;
        List<RouteMetric> matched = List.copyOf(byCode.values());

        TenantId tenant = tenants.resolve(request).toCompletableFuture().join().orElse(null);
        if (tenant == null) return reject(response, HttpStatus.FORBIDDEN, "client_not_linked", null);

        Instant at = clock.instant();
        long[] costs = new long[matched.size()];
        try {
            for (int i = 0; i < costs.length; i++) {
                costs[i] = matched.get(i).cost().applyAsLong(request);
                if (costs[i] < 1 || costs[i] > UsageEngine.MAX_COST) throw new IllegalArgumentException("cost out of range");
            }
        } catch (IllegalArgumentException e) {
            return reject(response, HttpStatus.BAD_REQUEST, "invalid_cost", null);
        }

        List<Decision> decisions = new ArrayList<>();
        for (int i = 0; i < costs.length; i++) {
            Decision d;
            try {
                d = engine.check(tenant, matched.get(i).code(), costs[i], at).toCompletableFuture().join();
            } catch (RuntimeException e) { // the store failed mid-way: what the earlier metrics took must go back before the error goes up
                giveBack(tenant, matched, costs, at, decisions, i);
                throw e;
            }
            decisions.add(d);
            if (!d.allowed()) {
                giveBack(tenant, matched, costs, at, decisions, i);
                return reject(response, statusFor(d.reason()), d.reason(), d.retryAfter());
            }
        }
        Plan plan = decisions.stream().map(Decision::plan).filter(Objects::nonNull).findFirst().orElse(null);
        request.setAttribute(TENANT_ATTRIBUTE, tenant);
        if (plan != null && plan.isSpecified()) request.setAttribute(PLAN_ATTRIBUTE, plan);
        if (decisions.stream().anyMatch(Decision::overage)) request.setAttribute(OVERAGE_ATTRIBUTE, true);
        request.setAttribute(METERED_ATTRIBUTE, new Metered(tenant, matched, costs, at));
        return true;
    }

    /** Exactly one report per request, whichever way it ended. Best effort: the response never fails because of the usage pipeline. */
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        if (!(request.getAttribute(METERED_ATTRIBUTE) instanceof Metered m)) return;
        request.removeAttribute(METERED_ATTRIBUTE);
        // an exception ends as a 500: refunded exactly when the configured predicate refunds a 500
        boolean refund = refundStatus.test(ex != null ? 500 : response.getStatus());
        for (int i = 0; i < m.metrics().size(); i++) {
            RouteMetric metric = m.metrics().get(i);
            if (!metric.emitsUsage()) continue;
            UsageEvent usage = new UsageEvent(UUID.randomUUID(), m.tenant(), metric.code(), m.costs()[i], m.at());
            String step = "usage";
            try {
                engine.report(usage).toCompletableFuture().join();
                step = "refund";
                if (refund) engine.reportCompensation(Compensation.of(usage.eventId(), m.tenant(), metric.code(), m.costs()[i], clock.instant(), "UPSTREAM_ERROR"))
                        .toCompletableFuture().join();
            } catch (RuntimeException e) {
                engine.telemetry().count("servlet.event.lost", 1, "kind", step, "metric", metric.code());
                log.error("usage event ({}) lost for tenant={} metric={}", step, m.tenant().value(), metric.code(), e);
            }
        }
    }

    /** Returns what the access metrics before the refused one consumed. A degraded allow charged nothing, so it is not undone. */
    private void giveBack(TenantId tenant, List<RouteMetric> matched, long[] costs, Instant at, List<Decision> decisions, int refusedIndex) {
        for (int i = 0; i < refusedIndex; i++) {
            if (!matched.get(i).emitsUsage() || decisions.get(i).reason() != null) continue;
            try {
                engine.undoCheck(tenant, matched.get(i).code(), costs[i], at).toCompletableFuture().join();
            } catch (RuntimeException e) {
                engine.telemetry().count("servlet.refund.failed", 1, "metric", matched.get(i).code());
                log.error("could not give back quota for tenant={} metric={}", tenant.value(), matched.get(i).code(), e);
            }
        }
    }

    private static HttpStatus statusFor(String reason) {
        return switch (reason) {
            case Decision.RATE_LIMITED, Decision.QUOTA_EXCEEDED, Decision.CONCURRENCY_EXCEEDED -> HttpStatus.TOO_MANY_REQUESTS;
            case Decision.NO_POLICY -> HttpStatus.FORBIDDEN;
            default -> HttpStatus.SERVICE_UNAVAILABLE;
        };
    }

    /** {@code retryAfter}, when the refusal has one, goes out as Retry-After in whole seconds, rounded up (at least 1). */
    private static boolean reject(HttpServletResponse response, HttpStatus status, String code, Duration retryAfter) throws IOException {
        response.setStatus(status.value());
        if (retryAfter != null) response.setHeader(HttpHeaders.RETRY_AFTER, Long.toString(Math.max(1, (retryAfter.toMillis() + 999) / 1000)));
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getOutputStream().write(("{\"error\":\"" + code + "\"}").getBytes(StandardCharsets.UTF_8));
        return false;
    }
}
