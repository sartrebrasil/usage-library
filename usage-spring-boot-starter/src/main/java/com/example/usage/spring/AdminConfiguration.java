package com.example.usage.spring;

import com.example.usage.admin.audit.Audit;
import com.example.usage.admin.catalog.MetricAdmin;
import com.example.usage.admin.catalog.PlanAdmin;
import com.example.usage.admin.security.UsageAdminSecurityGuard;
import com.example.usage.admin.support.AdminContext;
import com.example.usage.admin.tenant.OverrideAdmin;
import com.example.usage.admin.tenant.SubscriptionAdmin;
import com.example.usage.admin.tenant.TenantAdmin;
import com.example.usage.admin.usage.CompensationAdmin;
import com.example.usage.admin.usage.UsageQueries;
import com.example.usage.admin.web.CatalogController;
import com.example.usage.admin.web.CompensationController;
import com.example.usage.admin.web.TenantWritesController;
import com.example.usage.admin.web.TenantsController;
import com.example.usage.admin.web.UsageController;
import com.example.usage.core.UsageEngine;
import com.example.usage.core.memory.CachedPolicyStore;
import com.example.usage.postgres.Tables;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

import javax.sql.DataSource;
import java.time.Clock;
import java.util.function.Supplier;

/**
 * The management API of the usage-admin jar, in a servlet application with a {@code DataSource}: tenants, metrics, plans,
 * subscriptions, overrides, usage search, audit trail and manual compensation. It works on the platform's tables in the schema
 * {@code usage.postgres.schema}, and uses the starter's own {@code UsageEngine}, policy cache and ledger. Settings are {@code usage.admin.*}.
 * <p>
 * The host decides who may call it: add {@code UsageAdminRoutes} to its security chain (the application refuses to start without one).
 * After a change that affects a tenant the cached policy of this process is dropped; other instances see it when
 * {@code usage.policy-cache-ttl} expires.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(TenantAdmin.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnBean(DataSource.class)
@ConditionalOnProperty(name = "usage.admin.enabled", havingValue = "true", matchIfMissing = true)
@Import({TenantsController.class, CatalogController.class, UsageController.class, CompensationController.class})
class AdminConfiguration {

    /**
     * The API changes what tenants may consume and what is billed: refuse to start without a security chain unless the
     * application says it secures the API some other way ({@code usage.admin.allow-unsecured=true}).
     */
    @Bean
    UsageAdminSecurityGuard usageAdminSecurityGuard(UsageProperties props, ListableBeanFactory beans) {
        return UsageAdminSecurityGuard.verify(beans, props.admin().allowUnsecured());
    }

    @Bean
    AdminContext usageAdminContext(ObjectProvider<DataSource> dataSources, ObjectProvider<CachedPolicyStore> cache, Clock clock, UsageProperties props) {
        DataSource dataSource = dataSources.getIfUnique();
        if (dataSource == null) throw new IllegalStateException("usage-admin needs exactly one DataSource (or a @Primary one): it is the one holding the usage schema");
        // always its own transaction manager over this DataSource: a host's other managers (Kafka, JMS) would not cover the SQL
        return new AdminContext(JdbcClient.create(dataSource), Tables.of(props.postgres().schema()),
                new TransactionTemplate(new DataSourceTransactionManager(dataSource)),
                tenant -> cache.ifAvailable(c -> c.invalidate(tenant)), clock);
    }

    /** The name in the token, when Spring Security is there; otherwise "unknown". Define a bean named usageAdminActor to take it from elsewhere. */
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

    /** Applies the reversal through the starter's engine, which the starter always creates. */
    @Bean
    CompensationAdmin usageCompensationAdmin(AdminContext ctx, Audit audit, UsageEngine engine, UsageQueries queries) {
        return new CompensationAdmin(ctx, audit, engine, queries);
    }

    /** Tenant creation and changes, unless the host owns them: {@code usage.admin.tenants.write=false}. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "usage.admin.tenants.write", havingValue = "true", matchIfMissing = true)
    @Import(TenantWritesController.class)
    static class TenantWrites {
    }

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
