package com.example.usage.core.model;

import java.time.Duration;
import java.util.Objects;

/**
 * A rule on a metric. Enum order of {@link Type} is the evaluation order (cheap/short first).
 * shortcut: no THROTTLE. Periods are cut from the policy anchor, which the store must take from the tenant's
 * subscription (not from the plan assignment), so a plan change swaps limits without moving the cycle.
 */
public record Limit(Metric metric, Type type, long value, Window window, Enforcement enforcement) {
    /**
     * METER only measures: it makes the ingest keep a per-period summary (what billing reads) for the metric and never takes part
     * in a decision. It has a window and no value, so a billed metric needs no made-up quota to be summarized.
     */
    public enum Type { RATE, QUOTA, CONCURRENCY, METER }
    public enum Enforcement { REJECT, ALLOW_OVERAGE }

    public Limit {
        Objects.requireNonNull(metric, "metric");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(enforcement, "enforcement");
        if (value < 0) throw new IllegalArgumentException("value < 0");
        if (type != Type.CONCURRENCY && window == null)
            throw new IllegalArgumentException(type + " needs a window");
        if (type == Type.RATE && !(window instanceof Window.Fixed))
            throw new IllegalArgumentException("RATE needs a fixed window");
        if (type == Type.RATE && metric.mode() != Metric.Mode.CONSUME_ON_CHECK)
            throw new IllegalArgumentException("RATE applies to access metrics only");
        if (type == Type.METER && value != 0)
            throw new IllegalArgumentException("METER only measures: its value must be 0");
        if (type != Type.QUOTA && enforcement != Enforcement.REJECT)
            throw new IllegalArgumentException("only QUOTA may allow overage");
    }

    /** Shorthand for a fixed-length window (null for CONCURRENCY, which has none). */
    public Limit(Metric metric, Type type, long value, Duration window, Enforcement enforcement) {
        this(metric, type, value, window == null ? null : Window.fixed(window), enforcement);
    }

    public static Limit concurrency(Metric metric, long value) {
        return new Limit(metric, Type.CONCURRENCY, value, (Window) null, Enforcement.REJECT);
    }
}
