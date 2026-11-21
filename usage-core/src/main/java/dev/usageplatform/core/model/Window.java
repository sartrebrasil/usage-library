package dev.usageplatform.core.model;

import java.time.Duration;
import java.util.Objects;

/** How a limit's period is cut. Pure data: {@link PeriodCalculator} is the only place that turns it into dates. */
public sealed interface Window {
    /** Back-to-back periods of exactly this length, aligned to the policy anchor. */
    record Fixed(Duration length) implements Window {
        public Fixed {
            Objects.requireNonNull(length, "length");
            if (length.isZero() || length.isNegative()) throw new IllegalArgumentException("window must be positive");
        }
    }

    /** Calendar periods of {@code count} months, starting on the anchor's day and time (UTC). */
    record Months(int count) implements Window {
        public Months {
            if (count <= 0) throw new IllegalArgumentException("months must be positive");
        }
    }

    static Window fixed(Duration length) { return new Fixed(length); }

    static Window months(int count) { return new Months(count); }
}
