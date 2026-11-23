package com.example.usage.spring;

import com.example.usage.consumer.UsageEventConsumer;
import com.example.usage.gateway.UsageGatewayFilter;
import com.example.usage.messaging.OutboxUsageSink;
import com.example.usage.messaging.UsageInboxHandler;
import com.example.usage.processor.QueueUsageAdapter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Each adapter appears only when its jar is on the classpath. Turning one on by property without its jar would
 * otherwise be ignored silently, and the application would run without metering. These fail the startup instead.
 */
@Configuration(proxyBeanMethods = false)
class MissingJarFailures {
    @Bean
    @ConditionalOnProperty(name = "usage.gateway.enabled", havingValue = "true")
    @ConditionalOnMissingClass("com.example.usage.gateway.UsageGatewayFilter")
    Object usageGatewayJarMissing() {
        throw missing("usage.gateway.enabled=true", "usage-gateway");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.processor.metric")
    @ConditionalOnMissingClass("com.example.usage.processor.QueueUsageAdapter")
    Object usageProcessorJarMissing() {
        throw missing("usage.processor.metric", "usage-processor");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.consumer.enabled", havingValue = "true")
    @ConditionalOnMissingClass("com.example.usage.consumer.UsageEventConsumer")
    Object usageConsumerJarMissing() {
        throw missing("usage.consumer.enabled=true", "usage-consumer");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.messaging.outbox.destination")
    @ConditionalOnMissingClass("com.example.usage.messaging.OutboxUsageSink")
    Object usageOutboxJarMissing() {
        throw missing("usage.messaging.outbox.destination", "usage-messaging");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.messaging.inbox.enabled", havingValue = "true")
    @ConditionalOnMissingClass("com.example.usage.messaging.UsageInboxHandler")
    Object usageInboxJarMissing() {
        throw missing("usage.messaging.inbox.enabled=true", "usage-messaging");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.admin.enabled", havingValue = "true") // only when asked for: with no property the API is simply absent without its jar
    @ConditionalOnMissingClass("com.example.usage.admin.tenant.TenantAdmin")
    Object usageAdminJarMissing() {
        throw missing("usage.admin.enabled=true", "usage-admin");
    }

    private static IllegalStateException missing(String property, String jar) {
        return new IllegalStateException(property + " needs the " + jar + " jar on the classpath, and it is not there");
    }
}
