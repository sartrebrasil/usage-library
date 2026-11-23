package com.example.usage.admin;

import com.example.usage.admin.audit.Audit;
import com.example.usage.admin.catalog.MetricAdmin;
import com.example.usage.admin.catalog.PlanAdmin;
import com.example.usage.admin.security.UsageAdminRoutes;
import com.example.usage.admin.security.UsageAdminSecurityGuard;
import com.example.usage.admin.support.AdminContext;
import com.example.usage.admin.tenant.OverrideAdmin;
import com.example.usage.admin.tenant.SubscriptionAdmin;
import com.example.usage.admin.tenant.TenantAdmin;
import com.example.usage.admin.usage.CompensationAdmin;
import com.example.usage.admin.usage.UsageQueries;
import com.example.usage.admin.web.CatalogController;
import com.example.usage.admin.web.CompensationController;
import com.example.usage.admin.web.TenantsController;
import com.example.usage.admin.web.UsageController;

import com.example.usage.core.UsageEngine;
import com.example.usage.core.memory.CachedPolicyStore;
import com.example.usage.core.model.TenantId;
import com.example.usage.postgres.Tables;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import javax.sql.DataSource;
import java.time.Clock;
import java.util.function.Supplier;

/**
 * The management API, loaded by having this jar on the classpath of a servlet application with a {@code DataSource}.
 * It works on the platform's tables in the schema {@code usage.postgres.schema} (default {@code usage}), the same one
 * the starter uses. {@code usage.admin.enabled=false} turns it off; {@code usage.admin.base-path} moves it (default
 * {@code /v1/usage}: pass the same value to {@link UsageAdminRoutes}).
 * <p>
 * The host decides who may call it: add {@link UsageAdminRoutes} to its security chain. After a change that affects a
 * tenant the cached policy of this process is dropped; other instances see it when {@code usage.policy-cache-ttl} expires.
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "com.example.usage.spring.UsageAutoConfiguration"})
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(name = "usage.admin.enabled", havingValue = "true", matchIfMissing = true)
@Import({TenantsController.class, CatalogController.class, UsageController.class})
public class UsageAdminAutoConfiguration {

    /**
     * The management API changes what tenants may consume and what is billed: refuse to start without a security chain
     * unless the application says it secures the API some other way ({@code usage.admin.allow-unsecured=true}).
     */
    @Bean
    UsageAdminSecurityGuard usageAdminSecurityGuard(Environment env, org.springframework.beans.factory.ListableBeanFactory beans) {
        return UsageAdminSecurityGuard.verify(beans, env.getProperty("usage.admin.allow-unsecured", Boolean.class, false));
    }

    @Bean
    AdminContext usageAdminContext(ObjectProvider<DataSource> dataSources, ObjectProvider<CachedPolicyStore> cache,
                                   ObjectProvider<Clock> clock, Environment env) {
        DataSource dataSource = dataSources.getIfUnique();
        if (dataSource == null) throw new IllegalStateException("usage-admin needs exactly one DataSource (or a @Primary one): it is the one holding the usage schema");
        // always its own transaction manager over this DataSource: a host's other managers (Kafka, JMS) would not cover the SQL
        PlatformTransactionManager tm = new DataSourceTransactionManager(dataSource);
        String schema = env.getProperty("usage.postgres.schema", "usage");
        return new AdminContext(JdbcClient.create(dataSource), Tables.of(schema), new TransactionTemplate(tm),
                tenant -> cache.ifAvailable(c -> c.invalidate(tenant)), clock.getIfAvailable(Clock::systemUTC));
    }

    @Bean
    @ConditionalOnMissingBean(name = "usageAdminActor")
    Supplier<String> usageAdminActor() {
        return ActorFromSecurity.supplier();
    }

    @Bean
    Audit usageAudit(AdminContext ctx, ObjectProvider<JsonMapper> json, Supplier<String> usageAdminActor) {
        return new Audit(ctx, json.getIfUnique(() -> JsonMapper.builder().build()), usageAdminActor);
    }

    @Bean
    TenantAdmin usageTenantAdmin(AdminContext ctx, Audit audit) { return new TenantAdmin(ctx, audit); }

    @Bean
    MetricAdmin usageMetricAdmin(AdminContext ctx, Audit audit) { return new MetricAdmin(ctx, audit); }

    @Bean
    PlanAdmin usagePlanAdmin(AdminContext ctx, Audit audit) { return new PlanAdmin(ctx, audit); }

    @Bean
    SubscriptionAdmin usageSubscriptionAdmin(AdminContext ctx, Audit audit, TenantAdmin tenants) { return new SubscriptionAdmin(ctx, audit, tenants); }

    @Bean
    OverrideAdmin usageOverrideAdmin(AdminContext ctx, Audit audit, TenantAdmin tenants) { return new OverrideAdmin(ctx, audit, tenants); }

    @Bean
    UsageQueries usageQueries(AdminContext ctx) { return new UsageQueries(ctx); }

    /** Manual compensation needs the platform's engine; without one the endpoint is simply not there. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(UsageEngine.class)
    @Import(CompensationController.class)
    static class Compensations {
        @Bean
        CompensationAdmin usageCompensationAdmin(AdminContext ctx, Audit audit, UsageEngine engine, UsageQueries queries) {
            return new CompensationAdmin(ctx, audit, engine, queries);
        }
    }

    /** The name in the token, when Spring Security is there; otherwise "unknown". Define a bean named usageAdminActor to take it from elsewhere. */
    static final class ActorFromSecurity {
        static Supplier<String> supplier() {
            try {
                Class.forName("org.springframework.security.core.context.SecurityContextHolder");
            } catch (ClassNotFoundException e) {
                return () -> "unknown";
            }
            return () -> {
                var auth = org.springframework.security.core.context.SecurityContextHolder.getContext().getAuthentication();
                return auth == null || auth.getName() == null ? "unknown" : auth.getName();
            };
        }
    }
}
