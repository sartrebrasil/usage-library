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
@ConditionalOnClass(tools.jackson.databind.json.JsonMapper.class) // jackson is optional in usage-core
@ConditionalOnProperty(name = "usage.consumer.enabled", havingValue = "true")
class ConsumerConfiguration {

    @Bean
    @ConditionalOnMissingBean
    UsageEventConsumer usageEventConsumer(UsageEngine engine, Clock clock, UsageProperties props) {
        UsageProperties.Consumer c = props.consumer();
        Assert.state(!c.knownMetrics().isEmpty() || !c.knownMetricPrefixes().isEmpty(),
                "usage.consumer.known-metrics (or known-metric-prefixes) must list the metric codes the consumer accepts");
        Set<String> known = new HashSet<>(c.knownMetrics());
        var prefixes = c.knownMetricPrefixes();
        return new UsageEventConsumer(engine, metric -> known.contains(metric) || prefixes.stream().anyMatch(metric::startsWith), clock, c.maxFuture(), c.maxPast());
    }
}
