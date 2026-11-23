package com.example.usage.spring;

import com.example.usage.core.model.Decision;

import java.time.Duration;

/**
 * The engine refused: a service translates it into its own error (a 429 with {@code Retry-After}, a 403...).
 * {@link #kind()} says what to answer, {@link #retryAfterSeconds()} when to come back.
 */
public final class UsageDenied extends RuntimeException {
    public enum Kind {
        /** The per-window rate limit was reached: try again within the window. */
        RATE_LIMITED,
        /** The quota of the period is used up: it renews when the period ends. */
        QUOTA_EXCEEDED,
        /** The tenant has no policy in force (no subscription, or suspended). */
        SUBSCRIPTION_REQUIRED,
        /** The policy or the counters could not be read and the fail mode is CLOSED. */
        UNAVAILABLE
    }

    private final Kind kind;
    private final Decision decision;

    UsageDenied(Decision decision) {
        super("usage refused: " + decision.reason());
        this.decision = decision;
        this.kind = switch (decision.reason()) {
            case Decision.RATE_LIMITED -> Kind.RATE_LIMITED;
            case Decision.QUOTA_EXCEEDED -> Kind.QUOTA_EXCEEDED;
            case Decision.NO_POLICY -> Kind.SUBSCRIPTION_REQUIRED;
            default -> Kind.UNAVAILABLE;
        };
    }

    public Kind kind() { return kind; }

    public Decision decision() { return decision; }

    /** Whole seconds, rounded up, at least 1 when the refusal has a period to wait for; 0 when it has none. */
    public long retryAfterSeconds() {
        Duration wait = decision.retryAfter();
        return wait == null ? 0 : Math.max(1, (wait.toMillis() + 999) / 1000);
    }
}
