package com.example.usage.spring;

import com.example.usage.core.spi.UsageTelemetry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * {@link UsageTelemetry} over Micrometer: counters, timers and gauges go to the {@link MeterRegistry}, and each
 * {@link Op} is an {@link Observation}, so one operation yields a timer (through the registry's meter handler) and a
 * span (through the tracing bridge, when one is present). Names get the {@code usage.} prefix.
 */
final class MicrometerUsageTelemetry implements UsageTelemetry {
    private final MeterRegistry meters;
    private final ObservationRegistry observations;

    MicrometerUsageTelemetry(MeterRegistry meters, ObservationRegistry observations) {
        this.meters = meters;
        this.observations = observations;
    }

    @Override
    public Op start(String name, String... tags) {
        Observation o = Observation.createNotStarted("usage." + name, observations);
        for (int i = 0; i + 1 < tags.length; i += 2) o.lowCardinalityKeyValue(tags[i], tags[i + 1]);
        return new ObservedOp(o.start());
    }

    @Override
    public void count(String name, long amount, String... tags) {
        if (amount > 0) Counter.builder("usage." + name).tags(tags).register(meters).increment(amount);
    }

    @Override
    public void time(String name, Duration duration, String... tags) {
        Timer.builder("usage." + name).tags(tags).register(meters).record(duration);
    }

    @Override
    public void gauge(String name, Supplier<Number> value, String... tags) {
        Gauge.builder("usage." + name, value).tags(Tags.of(tags)).strongReference(true).register(meters);
    }

    private record ObservedOp(Observation observation) implements Op {
        @Override public Op tag(String key, String value) {
            observation.lowCardinalityKeyValue(key, value);
            return this;
        }

        @Override public Op eventId(String id) {
            observation.highCardinalityKeyValue("usage.event.id", id);
            return this;
        }

        @Override public <T> T inScope(Supplier<T> body) {
            try (Observation.Scope ignored = observation.openScope()) {
                return body.get();
            }
        }

        @Override public void end(Throwable error) {
            if (error != null) observation.error(error);
            observation.stop();
        }
    }
}
