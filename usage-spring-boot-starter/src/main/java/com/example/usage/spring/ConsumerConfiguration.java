package com.example.usage.spring;

import com.example.usage.consumer.UsageEventConsumer;
import com.example.usage.core.UsageEngine;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import java.time.Clock;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(UsageEventConsumer.class)
@ConditionalOnProperty(name = "usage.consumer.enabled", havingValue = "true")
class ConsumerConfiguration {

    @Bean
    @ConditionalOnMissingBean
    UsageEventConsumer usageEventConsumer(UsageEngine engine, Clock clock, UsageProperties props) {
        UsageProperties.Consumer c = props.consumer();
        Assert.notEmpty(c.knownMetrics(), "usage.consumer.known-metrics must list the metric codes the consumer accepts");
        Set<String> known = new HashSet<>(c.knownMetrics());
        return new UsageEventConsumer(engine, known::contains, clock, c.maxFuture(), c.maxPast());
    }
}
