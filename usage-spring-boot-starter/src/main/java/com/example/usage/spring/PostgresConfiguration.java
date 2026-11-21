package com.example.usage.spring;

import com.example.usage.core.memory.CachedPolicyStore;
import com.example.usage.core.spi.UsageTelemetry;
import org.springframework.beans.factory.ObjectProvider;
import com.example.usage.core.spi.JobLock;
import com.example.usage.core.spi.Ledger;
import com.example.usage.core.spi.PolicyStore;
import com.example.usage.core.spi.SummaryStore;
import com.example.usage.postgres.JdbcJobLock;
import com.example.usage.postgres.JdbcLedger;
import com.example.usage.postgres.JdbcPolicyStore;
import com.example.usage.postgres.JdbcSubscriptions;
import com.example.usage.postgres.JdbcSummaryStore;
import com.example.usage.postgres.UsageSchema;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import javax.sql.DataSource;

import static com.example.usage.spring.UsageAutoConfiguration.EXECUTOR;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(JdbcLedger.class)
@ConditionalOnBean(DataSource.class)
class PostgresConfiguration {

    /** Creating this bean runs the migrations (if asked), and every store below depends on it. */
    @Bean
    UsageSchemaReady usageSchemaReady(DataSource dataSource, UsageProperties props) {
        if (props.postgres().migrate()) UsageSchema.migrate(dataSource, props.postgres().schema());
        return new UsageSchemaReady();
    }

    @Bean
    @ConditionalOnMissingBean(PolicyStore.class)
    CachedPolicyStore usagePolicyStore(DataSource ds, @Qualifier(EXECUTOR) ExecutorService executor,
                                       Clock clock, UsageProperties props, UsageSchemaReady ready,
                                       ObjectProvider<UsageTelemetry> telemetry) {
        return new CachedPolicyStore(new JdbcPolicyStore(ds, executor, clock, props.postgres().schema()), clock, props.policyCacheTtl(),
                100_000, telemetry.getIfAvailable(() -> UsageTelemetry.NOOP));
    }

    @Bean
    @ConditionalOnMissingBean(Ledger.class)
    Ledger usageLedger(DataSource ds, @Qualifier(EXECUTOR) ExecutorService executor,
                       UsageProperties props, UsageSchemaReady ready) {
        return new JdbcLedger(ds, executor, props.postgres().schema());
    }

    @Bean
    @ConditionalOnMissingBean(SummaryStore.class)
    SummaryStore usageSummaryStore(DataSource ds, @Qualifier(EXECUTOR) ExecutorService executor,
                                   UsageProperties props, UsageSchemaReady ready) {
        return new JdbcSummaryStore(ds, executor, props.postgres().schema());
    }

    @Bean
    @ConditionalOnMissingBean(JobLock.class)
    JobLock usageJobLock(DataSource ds, @Qualifier(EXECUTOR) ExecutorService executor, UsageSchemaReady ready) {
        return new JdbcJobLock(ds, executor);
    }

    /** Admin operations (subscribe, change plan). Call {@code CachedPolicyStore.invalidate} after using them. */
    @Bean
    @ConditionalOnMissingBean
    JdbcSubscriptions usageSubscriptions(DataSource ds, UsageProperties props, UsageSchemaReady ready) {
        return new JdbcSubscriptions(ds, props.postgres().schema());
    }

    /** Marker: the schema is in place. */
    static final class UsageSchemaReady {}
}
