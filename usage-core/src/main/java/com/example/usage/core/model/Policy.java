package com.example.usage.core.model;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * What a tenant may do: the plan it is on, the limits of that plan (overrides already applied) and the cycle
 * anchor of its subscription. The anchor must not change when only the plan changes, or counters and summaries lose continuity.
 */
public record Policy(Plan plan, Instant anchor, List<Limit> limits) {
    public Policy {
        Objects.requireNonNull(plan, "plan");
    }

    /** For stores with no notion of plans: the plan is {@link Plan#UNSPECIFIED}. */
    public Policy(Instant anchor, List<Limit> limits) {
        this(Plan.UNSPECIFIED, anchor, limits);
    }

    public List<Limit> limitsFor(String metricCode) {
        return limits.stream().filter(l -> l.metric().code().equals(metricCode)).toList();
    }
}
