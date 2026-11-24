package com.example.usage.servlet;

import jakarta.servlet.http.HttpServletRequest;

import java.util.function.ToLongFunction;

/**
 * A metric checked on the requests whose path matches {@code pattern} (Spring path pattern, e.g. {@code /orders/**}).
 * {@code cost} is evaluated once per request, before any check; derive it from headers or the route, never from the body.
 * Throw IllegalArgumentException (a malformed number is one already) to refuse the request with a 400.
 * {@code emitsUsage} is true for access metrics this service owns, false for result metrics it only guards.
 */
public record RouteMetric(String pattern, String code, ToLongFunction<HttpServletRequest> cost, boolean emitsUsage) {
    /** Counted by the interceptor: 1 per request. */
    public static RouteMetric access(String pattern, String code) {
        return new RouteMetric(pattern, code, r -> 1, true);
    }

    /** Counted by the interceptor with a quantity (e.g. items in a batch, from a header). */
    public static RouteMetric access(String pattern, String code, ToLongFunction<HttpServletRequest> cost) {
        return new RouteMetric(pattern, code, cost, true);
    }

    /** Result metric owned by processors: the interceptor only refuses when the tenant is already over. */
    public static RouteMetric guard(String pattern, String code) {
        return new RouteMetric(pattern, code, r -> 1, false);
    }
}
