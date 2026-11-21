package com.example.usage.core.model;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class PeriodCalculatorTest {
    static Instant t(String s) { return Instant.parse(s); }

    static void assertPeriod(String start, String end, Period p) {
        assertEquals(new Period(t(start), t(end)), p);
    }

    @Test
    void fixedPeriodsAreAlignedToTheAnchorAndHalfOpen() {
        Window day = Window.fixed(Duration.ofDays(1));
        Instant anchor = t("2026-11-01T06:00:00Z");
        assertPeriod("2026-11-19T06:00:00Z", "2026-11-20T06:00:00Z", PeriodCalculator.periodOf(day, anchor, t("2026-11-19T12:00:00Z")));
        assertPeriod("2026-11-18T06:00:00Z", "2026-11-19T06:00:00Z", PeriodCalculator.periodOf(day, anchor, t("2026-11-19T05:59:59Z")));
        assertPeriod("2026-11-19T06:00:00Z", "2026-11-20T06:00:00Z", PeriodCalculator.periodOf(day, anchor, t("2026-11-19T06:00:00Z")));
    }

    @Test
    void fixedPeriodsAlsoWorkBeforeTheAnchor() {
        assertPeriod("2026-10-31T00:00:00Z", "2026-11-01T00:00:00Z",
                PeriodCalculator.periodOf(Window.fixed(Duration.ofDays(1)), t("2026-11-01T00:00:00Z"), t("2026-10-31T10:00:00Z")));
    }

    @Test
    void monthlyPeriodStartsOnTheAnchorDayAndTime() {
        Instant anchor = t("2026-11-19T14:30:00Z");
        Window month = Window.months(1);
        assertPeriod("2026-11-19T14:30:00Z", "2026-12-19T14:30:00Z", PeriodCalculator.periodOf(month, anchor, t("2026-12-19T14:29:59Z")));
        assertPeriod("2026-12-19T14:30:00Z", "2027-01-19T14:30:00Z", PeriodCalculator.periodOf(month, anchor, t("2026-12-19T14:30:00Z")));
    }

    @Test
    void anchorOnThe31stClampsToShortMonthsAndComesBack() {
        Instant anchor = t("2026-01-31T00:00:00Z");
        Window month = Window.months(1);
        assertPeriod("2026-01-31T00:00:00Z", "2026-02-28T00:00:00Z", PeriodCalculator.periodOf(month, anchor, t("2026-02-15T00:00:00Z")));
        assertPeriod("2026-02-28T00:00:00Z", "2026-03-31T00:00:00Z", PeriodCalculator.periodOf(month, anchor, t("2026-02-28T00:00:00Z")));
        assertPeriod("2026-02-28T00:00:00Z", "2026-03-31T00:00:00Z", PeriodCalculator.periodOf(month, anchor, t("2026-03-30T23:59:59Z")));
        assertPeriod("2026-03-31T00:00:00Z", "2026-04-30T00:00:00Z", PeriodCalculator.periodOf(month, anchor, t("2026-03-31T00:00:00Z")));
        assertPeriod("2026-04-30T00:00:00Z", "2026-05-31T00:00:00Z", PeriodCalculator.periodOf(month, anchor, t("2026-05-30T00:00:00Z")));
    }

    @Test
    void leapYearFebruaryKeepsTheLastDay() {
        Instant anchor = t("2028-01-30T00:00:00Z");
        assertPeriod("2028-02-29T00:00:00Z", "2028-03-30T00:00:00Z",
                PeriodCalculator.periodOf(Window.months(1), anchor, t("2028-02-29T10:00:00Z")));
    }

    @Test
    void multiMonthPeriodsAndInstantsBeforeTheAnchor() {
        assertPeriod("2026-04-15T00:00:00Z", "2026-07-15T00:00:00Z",
                PeriodCalculator.periodOf(Window.months(3), t("2026-01-15T00:00:00Z"), t("2026-05-01T00:00:00Z")));
        assertPeriod("2025-12-20T00:00:00Z", "2026-01-20T00:00:00Z",
                PeriodCalculator.periodOf(Window.months(1), t("2026-01-20T00:00:00Z"), t("2025-12-25T00:00:00Z")));
    }

    @Test
    void everyDayBelongsToExactlyOnePeriodAndPeriodsAreContiguous() {
        for (String anchor : new String[]{"2026-01-31T00:00:00Z", "2026-01-29T23:59:59Z", "2026-03-15T08:00:00Z"}) {
            for (Window w : new Window[]{Window.months(1), Window.months(2), Window.fixed(Duration.ofDays(7))}) {
                Instant a = t(anchor);
                for (Instant at = t("2025-06-01T00:00:00Z"); at.isBefore(t("2028-06-01T00:00:00Z")); at = at.plus(Duration.ofHours(7))) {
                    Period p = PeriodCalculator.periodOf(w, a, at);
                    assertTrue(p.contains(at), anchor + " " + w + " " + at);
                    assertEquals(p.start(), PeriodCalculator.periodOf(w, a, p.start()).start());
                    assertEquals(p.end(), PeriodCalculator.periodOf(w, a, p.end()).start());
                    assertFalse(PeriodCalculator.periodOf(w, a, p.start().minusSeconds(1)).contains(p.start()));
                }
            }
        }
    }

    @Test
    void ttlUpperBoundCoversTheLongestPeriod() {
        assertEquals(Duration.ofDays(1), PeriodCalculator.maxLength(Window.fixed(Duration.ofDays(1))));
        assertEquals(Duration.ofDays(31), PeriodCalculator.maxLength(Window.months(1)));
    }
}
