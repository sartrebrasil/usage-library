package com.example.usage.gateway;

import org.springframework.web.server.ServerWebExchange;

import java.util.function.ToLongFunction;

/**
 * A metric the gateway checks on each request. {@code cost} is evaluated once per request, before any check;
 * derive it from headers or the route, never from the body. Throw IllegalArgumentException (a malformed value
 * is one already) to refuse the request with a 400; a cost outside 1..UsageEngine.MAX_COST is refused too.
 * {@code emitsUsage} is true for access metrics the gateway owns, false for result metrics it only guards.
 */
public record MeteredMetric(String code, ToLongFunction<ServerWebExchange> cost, boolean emitsUsage) {
    /** Counted by the gateway: 1 per request. */
    public static MeteredMetric access(String code) {
        return new MeteredMetric(code, ex -> 1, true);
    }

    /** Counted by the gateway with a quantity (e.g. items in a batch, from a header). */
    public static MeteredMetric access(String code, ToLongFunction<ServerWebExchange> cost) {
        return new MeteredMetric(code, cost, true);
    }

    /** Result metric owned by processors: the gateway only refuses when the tenant is already over. */
    public static MeteredMetric guard(String code) {
        return new MeteredMetric(code, ex -> 1, false);
    }
}
