package com.example.usage.core.model;

import java.time.Duration;

/**
 * plan is the plan in force when the decision was made (null if the decision never reached a policy).
 * retryAfter, on a refusal by a rate or quota limit, is how long until its period ends and the counter starts again (null otherwise).
 */
public record Decision(boolean allowed, boolean overage, String reason, Plan plan, Duration retryAfter) {
    public Decision(boolean allowed, boolean overage, String reason, Plan plan) {
        this(allowed, overage, reason, plan, null);
    }

    /** The reasons a decision carries; stable codes, safe to use as metric tags. */
    public static final String NO_POLICY = "no_policy";
    public static final String STORE_UNAVAILABLE = "store_unavailable";
    /** The policy could not be built (an invalid limit): a configuration error, answered by fail-mode like an outage. */
    public static final String INVALID_POLICY = "invalid_policy";
    public static final String RATE_LIMITED = "rate_limited";
    public static final String QUOTA_EXCEEDED = "quota_exceeded";
    public static final String CONCURRENCY_EXCEEDED = "concurrency_exceeded";

    public static Decision allow() { return new Decision(true, false, null, null); }
    public static Decision allowOverage() { return new Decision(true, true, null, null); }
    public static Decision allowDegraded(String reason) { return new Decision(true, false, reason, null); }
    public static Decision deny(String reason) { return new Decision(false, false, reason, null); }

    public Decision withPlan(Plan plan) {
        return new Decision(allowed, overage, reason, plan, retryAfter);
    }

    public Decision withRetryAfter(Duration retryAfter) {
        return new Decision(allowed, overage, reason, plan, retryAfter);
    }
}
