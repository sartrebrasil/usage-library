package com.example.usage.spring;

import com.example.usage.core.UsageEngine;
import com.example.usage.processor.QueueUsageAdapter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.example.usage.spring.UsageAutoConfiguration.EXECUTOR;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "usage.processor.metric")
class ProcessorConfiguration {

    @Bean
    @ConditionalOnMissingBean
    QueueUsageAdapter queueUsageAdapter(UsageEngine engine, UsageProperties props, @Qualifier(EXECUTOR) ExecutorService executor) {
        UsageProperties.Processor p = props.processor();
        Assert.notEmpty(p.countableStatuses(), "usage.processor.countable-statuses must list the final statuses that are billable");
        return new QueueUsageAdapter(engine, p.metric(), Set.copyOf(p.countableStatuses()), p.deferWhenOverQuota(), p.tenantHeader(), executor);
    }
}
