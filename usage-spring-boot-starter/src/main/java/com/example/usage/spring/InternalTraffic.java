package com.example.usage.spring;

import org.springframework.web.server.ServerWebExchange;

import java.util.function.Predicate;

/**
 * Define one bean of this type to tell the gateway filter which requests are service-to-service calls that
 * must not be metered (how you recognise them depends on your identity provider). Without it, everything is metered.
 */
@FunctionalInterface
public interface InternalTraffic extends Predicate<ServerWebExchange> {}
