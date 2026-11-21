package dev.usageplatform.spring;

import dev.usageplatform.consumer.UsageEventConsumer;
import dev.usageplatform.gateway.UsageGatewayFilter;
import dev.usageplatform.messaging.OutboxUsageSink;
import dev.usageplatform.messaging.UsageInboxHandler;
import dev.usageplatform.processor.QueueUsageAdapter;
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
    @ConditionalOnMissingClass("dev.usageplatform.gateway.UsageGatewayFilter")
    Object usageGatewayJarMissing() {
        throw missing("usage.gateway.enabled=true", "usage-gateway");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.processor.metric")
    @ConditionalOnMissingClass("dev.usageplatform.processor.QueueUsageAdapter")
    Object usageProcessorJarMissing() {
        throw missing("usage.processor.metric", "usage-processor");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.consumer.enabled", havingValue = "true")
    @ConditionalOnMissingClass("dev.usageplatform.consumer.UsageEventConsumer")
    Object usageConsumerJarMissing() {
        throw missing("usage.consumer.enabled=true", "usage-consumer");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.messaging.outbox.destination")
    @ConditionalOnMissingClass("dev.usageplatform.messaging.OutboxUsageSink")
    Object usageOutboxJarMissing() {
        throw missing("usage.messaging.outbox.destination", "usage-messaging");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.messaging.inbox.enabled", havingValue = "true")
    @ConditionalOnMissingClass("dev.usageplatform.messaging.UsageInboxHandler")
    Object usageInboxJarMissing() {
        throw missing("usage.messaging.inbox.enabled=true", "usage-messaging");
    }

    private static IllegalStateException missing(String property, String jar) {
        return new IllegalStateException(property + " needs the " + jar + " jar on the classpath, and it is not there");
    }
}
