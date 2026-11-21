package dev.usageplatform.core.model;

import java.time.Instant;

/** Half-open interval [start, end). */
public record Period(Instant start, Instant end) {
    public boolean contains(Instant t) {
        return !t.isBefore(start) && t.isBefore(end);
    }
}
