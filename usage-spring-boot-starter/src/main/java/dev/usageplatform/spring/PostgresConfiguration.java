package dev.usageplatform.spring;

import dev.usageplatform.core.memory.CachedPolicyStore;
import dev.usageplatform.core.spi.JobLock;
import dev.usageplatform.core.spi.Ledger;
import dev.usageplatform.core.spi.PolicyStore;
import dev.usageplatform.core.spi.SummaryStore;
import dev.usageplatform.postgres.JdbcJobLock;
import dev.usageplatform.postgres.JdbcLedger;
import dev.usageplatform.postgres.JdbcPolicyStore;
import dev.usageplatform.postgres.JdbcSubscriptions;
import dev.usageplatform.postgres.JdbcSummaryStore;
import dev.usageplatform.postgres.UsageSchema;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import javax.sql.DataSource;

import static dev.usageplatform.spring.UsageAutoConfiguration.EXECUTOR;

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
                                       Clock clock, UsageProperties props, UsageSchemaReady ready) {
        return new CachedPolicyStore(new JdbcPolicyStore(ds, executor, clock, props.postgres().schema()), clock, props.policyCacheTtl());
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
