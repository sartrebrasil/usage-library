package dev.usageplatform.core;

/** plan is the plan in force when the decision was made (null if the decision never reached a policy). */
public record Decision(boolean allowed, boolean overage, String reason, Plan plan) {
    public static Decision allow() { return new Decision(true, false, null, null); }
    public static Decision allowOverage() { return new Decision(true, true, null, null); }
    public static Decision allowDegraded(String reason) { return new Decision(true, false, reason, null); }
    public static Decision deny(String reason) { return new Decision(false, false, reason, null); }

    public Decision withPlan(Plan plan) {
        return new Decision(allowed, overage, reason, plan);
    }
}
