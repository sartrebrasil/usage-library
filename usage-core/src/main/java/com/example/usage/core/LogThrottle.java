package com.example.usage.core;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Lets one event per interval through and counts the rest, so a store outage logs once per interval instead of once per request. */
final class LogThrottle {
    private final long intervalNanos;
    private final AtomicLong nextAt;
    private final LongAdder suppressed = new LongAdder();

    /** The first event is admitted. */
    LogThrottle(Duration interval, long nowNanos) {
        this.intervalNanos = interval.toNanos();
        this.nextAt = new AtomicLong(nowNanos);
    }

    /** Returns -1 if this event is to be suppressed, otherwise how many were suppressed since the last admitted one. */
    long admit(long nowNanos) {
        long next = nextAt.get();
        if (nowNanos - next >= 0 && nextAt.compareAndSet(next, nowNanos + intervalNanos)) return suppressed.sumThenReset();
        suppressed.increment();
        return -1;
    }
}
