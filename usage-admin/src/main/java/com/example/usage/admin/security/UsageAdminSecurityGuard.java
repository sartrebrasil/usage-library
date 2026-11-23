package com.example.usage.admin.security;

import org.springframework.beans.factory.ListableBeanFactory;

/**
 * Startup check: the API is not served without a security chain in front of it. The API changes what tenants may
 * consume and what is billed, and the module does not authenticate anyone itself: an application that forgot
 * {@link UsageAdminRoutes} would expose it to everybody.
 */
public final class UsageAdminSecurityGuard {
    private static final System.Logger LOG = System.getLogger(UsageAdminSecurityGuard.class.getName());
    private static final String CHAIN = "org.springframework.security.web.SecurityFilterChain";

    private UsageAdminSecurityGuard() {}

    public static UsageAdminSecurityGuard verify(ListableBeanFactory beans, boolean allowUnsecured) {
        if (allowUnsecured) {
            LOG.log(System.Logger.Level.WARNING, "usage.admin.allow-unsecured=true: the usage management API relies on protection outside this application");
            return new UsageAdminSecurityGuard();
        }
        boolean secured;
        try {
            secured = beans.getBeanNamesForType(Class.forName(CHAIN), true, false).length > 0;
        } catch (ClassNotFoundException e) {
            secured = false; // no Spring Security at all
        }
        if (!secured) {
            throw new IllegalStateException("usage-admin serves endpoints that change tenants, plans and billing, but the application has no "
                    + "SecurityFilterChain. Add Spring Security and call UsageAdminRoutes.apply(...) in your chain, or set "
                    + "usage.admin.allow-unsecured=true if something else in front of this application authenticates the callers.");
        }
        return new UsageAdminSecurityGuard();
    }
}
