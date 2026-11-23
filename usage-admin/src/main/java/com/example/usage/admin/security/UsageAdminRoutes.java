package com.example.usage.admin.security;

import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AuthorizeHttpRequestsConfigurer;

/**
 * The authorization rules of this API, for the host to add to its own {@code SecurityFilterChain}: the host owns the
 * chain (token validation, error format, deny-by-default), this module only says which authority each route needs.
 * <pre>{@code
 * ApiSecurity.chain(http, routes -> {
 *     routes.requestMatchers("/v1/tenants/**").hasAuthority("tenant:admin");
 *     UsageAdminRoutes.apply(routes, "/v1/usage");   // before anything that would deny the rest
 * });
 * }</pre>
 */
public final class UsageAdminRoutes {
    private UsageAdminRoutes() {}

    /** Uses the default base path {@code /v1/usage}. */
    public static AuthorizeHttpRequestsConfigurer<HttpSecurity>.AuthorizationManagerRequestMatcherRegistry apply(AuthorizeHttpRequestsConfigurer<HttpSecurity>.AuthorizationManagerRequestMatcherRegistry routes) {
        return apply(routes, "/v1/usage");
    }

    /** @param basePath the same value as {@code usage.admin.base-path} */
    public static AuthorizeHttpRequestsConfigurer<HttpSecurity>.AuthorizationManagerRequestMatcherRegistry apply(AuthorizeHttpRequestsConfigurer<HttpSecurity>.AuthorizationManagerRequestMatcherRegistry routes, String basePath) {
        return routes
                .requestMatchers(HttpMethod.POST, basePath + "/compensations").hasAuthority(UsageAdminRoles.COMPENSATE)
                .requestMatchers(HttpMethod.GET, basePath + "/**").hasAuthority(UsageAdminRoles.READ)
                .requestMatchers(basePath + "/**").hasAuthority(UsageAdminRoles.WRITE);
    }
}
