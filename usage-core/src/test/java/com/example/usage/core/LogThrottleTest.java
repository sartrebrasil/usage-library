package com.example.usage.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogThrottleTest {
    private static final long SECOND = Duration.ofSeconds(1).toNanos();

    @Test
    void admitsOnePerIntervalAndReportsHowManyWereSuppressed() {
        var throttle = new LogThrottle(Duration.ofSeconds(10), 5 * SECOND);
        assertEquals(0, throttle.admit(5 * SECOND), "the first event is admitted");
        assertEquals(-1, throttle.admit(6 * SECOND));
        assertEquals(-1, throttle.admit(14 * SECOND));
        assertEquals(2, throttle.admit(15 * SECOND), "the next interval reports the two it swallowed");
        assertEquals(-1, throttle.admit(15 * SECOND));
        assertEquals(1, throttle.admit(25 * SECOND));
    }

    @Test
    void worksWhenTheNanoClockIsNegative() {
        var throttle = new LogThrottle(Duration.ofSeconds(1), -10 * SECOND);
        assertEquals(0, throttle.admit(-10 * SECOND));
        assertEquals(-1, throttle.admit(-10 * SECOND + 1));
        assertEquals(1, throttle.admit(-9 * SECOND));
    }
}
