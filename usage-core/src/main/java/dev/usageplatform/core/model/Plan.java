package dev.usageplatform.core.model;

import java.util.Objects;

/**
 * Which plan a policy came from. Plans are versioned: (code, version) names exactly the limits and prices in
 * force, so a total can be tied to the plan it was measured under even after the plan changes.
 */
public record Plan(String code, int version) {
    /** For stores that have no notion of plans (tests, simple hosts). Never confuse it with a real plan. */
    public static final Plan UNSPECIFIED = new Plan("UNSPECIFIED", 1);

    public Plan {
        Objects.requireNonNull(code, "code");
        if (code.isBlank() || code.length() > 60) throw new IllegalArgumentException("plan code must be 1..60 characters");
        if (version < 1) throw new IllegalArgumentException("plan version must be >= 1");
    }

    public boolean isSpecified() {
        return !equals(UNSPECIFIED);
    }
}
