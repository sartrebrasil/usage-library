package dev.usageplatform.core;

import java.util.UUID;

/**
 * Everything a reporter publishes: a usage or its reversal. A sink handles both through one method, so an
 * implementation cannot forget one of them (a forgotten reversal would silently never refund anything).
 */
public sealed interface UsageMessage permits UsageEvent, Compensation {
    UUID eventId();

    TenantId tenant();

    String metricCode();

    long quantity();
}
