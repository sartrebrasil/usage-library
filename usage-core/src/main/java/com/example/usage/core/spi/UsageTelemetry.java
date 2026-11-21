package com.example.usage.core.spi;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * SPI: where the platform reports what it does (metrics and traces). The core has no metrics library: the starter
 * implements this over Micrometer. Every method defaults to doing nothing, so {@link #NOOP} costs nothing.
 * <p>
 * Tags are {@code key, value} pairs and must be low-cardinality: never a tenant id. The only high-cardinality value
 * is the event id, and it goes through {@link Op#eventId}, which a metrics backend ignores and a tracer keeps.
 */
public interface UsageTelemetry {
    UsageTelemetry NOOP = new UsageTelemetry() {};

    /** One timed, traced operation. {@link #end} must be called exactly once. */
    interface Op {
        Op NOOP = new Op() {};

        default Op tag(String key, String value) { return this; }

        default Op eventId(String id) { return this; }

        /** Runs {@code body} with this operation as the parent of anything started inside it (on this thread). */
        default <T> T inScope(Supplier<T> body) { return body.get(); }

        /** @param error null when the operation finished normally */
        default void end(Throwable error) {}
    }

    default Op start(String name, String... tags) { return Op.NOOP; }

    default void count(String name, long amount, String... tags) {}

    default void time(String name, Duration duration, String... tags) {}

    default void gauge(String name, Supplier<Number> value, String... tags) {}
}
