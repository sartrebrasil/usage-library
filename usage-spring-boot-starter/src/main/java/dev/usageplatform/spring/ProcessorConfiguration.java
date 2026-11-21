package dev.usageplatform.spring;

import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.processor.QueueUsageAdapter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import java.util.Set;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(QueueUsageAdapter.class)
@ConditionalOnProperty(name = "usage.processor.metric")
class ProcessorConfiguration {

    @Bean
    @ConditionalOnMissingBean
    QueueUsageAdapter queueUsageAdapter(UsageEngine engine, UsageProperties props) {
        UsageProperties.Processor p = props.processor();
        Assert.notEmpty(p.countableStatuses(), "usage.processor.countable-statuses must list the final statuses that are billable");
        return new QueueUsageAdapter(engine, p.metric(), Set.copyOf(p.countableStatuses()), p.deferWhenOverQuota(), p.tenantHeader());
    }
}
