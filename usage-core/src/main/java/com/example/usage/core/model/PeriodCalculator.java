package com.example.usage.core.model;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/**
 * The one place that decides which period an instant belongs to. Gateway, consumer, reconciliation and
 * billing must all call this, never re-derive it, or counters and invoices drift apart.
 * <p>
 * Everything is UTC. Monthly periods start on the anchor's day and time; every boundary is computed from
 * the anchor (not from the previous boundary), so an anchor on the 31st gives Jan 31, Feb 28 (29 in leap
 * years), Mar 31, Apr 30... and returns to the 31st whenever the month has one.
 */
public final class PeriodCalculator {
    private PeriodCalculator() {}

    /** The period of {@code window} that contains {@code at}. Works for instants before the anchor too. */
    public static Period periodOf(Window window, Instant anchor, Instant at) {
        return switch (window) {
            case Window.Fixed f -> fixed(f.length(), anchor, at);
            case Window.Months m -> months(m.count(), anchor, at);
        };
    }

    /** Stable id of a window's width, used in counter keys and summary rows ("86400", "m1"). */
    public static String windowKey(Window window) {
        return switch (window) {
            case Window.Fixed f -> Long.toString(f.length().toSeconds());
            case Window.Months m -> "m" + m.count();
        };
    }

    /** Upper bound of a period's length, for choosing counter TTLs. */
    public static Duration maxLength(Window window) {
        return switch (window) {
            case Window.Fixed f -> f.length();
            case Window.Months m -> Duration.ofDays(31L * m.count());
        };
    }

    private static Period fixed(Duration length, Instant anchor, Instant at) {
        long n = Math.floorDiv(Duration.between(anchor, at).toMillis(), length.toMillis());
        Instant start = anchor.plus(length.multipliedBy(n));
        return new Period(start, start.plus(length));
    }

    private static Period months(int count, Instant anchor, Instant at) {
        ZonedDateTime a = anchor.atZone(ZoneOffset.UTC);
        // the estimate can be off by one around clamped month ends, so settle it against the real boundaries
        long k = ChronoUnit.MONTHS.between(a, at.atZone(ZoneOffset.UTC)) / count;
        while (boundary(a, k, count).isAfter(at)) k--;
        while (!boundary(a, k + 1, count).isAfter(at)) k++;
        return new Period(boundary(a, k, count), boundary(a, k + 1, count));
    }

    private static Instant boundary(ZonedDateTime anchor, long k, int count) {
        return anchor.plusMonths(k * count).toInstant();
    }
}
