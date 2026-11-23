package com.example.usage.admin.security;

/** The authorities the API checks, in the engine's {@code <domain>:<action>} style. Platform roles: never hand them to a tenant's client. */
public final class UsageAdminRoles {
    private UsageAdminRoles() {}

    /** Every listing and search, including usage, summaries and the audit trail. */
    public static final String READ = "usage:read";
    /** Changes to tenants, metrics, plans, limits, subscriptions and overrides. */
    public static final String WRITE = "usage:write";
    /** Manual compensation: it changes what will be billed, so it has a role of its own. */
    public static final String COMPENSATE = "usage:compensate";
}
