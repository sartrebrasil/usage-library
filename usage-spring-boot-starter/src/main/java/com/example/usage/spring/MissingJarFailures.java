package com.example.usage.spring;

import com.example.usage.messaging.OutboxUsageSink;
import com.example.usage.messaging.UsageInboxHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnNotWebApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
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
    @ConditionalOnMissingClass("org.springframework.cloud.gateway.filter.GlobalFilter")
    Object usageGatewayJarMissing() {
        throw missing("usage.gateway.enabled=true", "spring-cloud-starter-gateway-server-webflux");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.consumer.enabled", havingValue = "true")
    @ConditionalOnMissingClass("tools.jackson.databind.json.JsonMapper")
    Object usageConsumerJarMissing() {
        throw missing("usage.consumer.enabled=true", "jackson-databind");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.messaging.outbox.destination")
    @ConditionalOnMissingClass("com.example.outbox.core.OutboxWriter")
    Object usageOutboxJarMissing() {
        throw missing("usage.messaging.outbox.destination", "outbox-core");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.messaging.inbox.enabled", havingValue = "true")
    @ConditionalOnMissingClass("com.example.messaging.inbox.InboxResultHandler")
    Object usageInboxJarMissing() {
        throw missing("usage.messaging.inbox.enabled=true", "messaging-inbox");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.admin.enabled", havingValue = "true") // only when asked for: with no property the API is simply absent without its jar
    @ConditionalOnMissingClass("com.example.usage.admin.tenant.TenantAdmin")
    Object usageAdminJarMissing() {
        throw missing("usage.admin.enabled=true", "usage-admin");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.postgres.migrate", havingValue = "true")
    @ConditionalOnMissingClass("org.flywaydb.core.Flyway")
    Object usageFlywayJarMissing() {
        throw missing("usage.postgres.migrate=true", "flyway-core and flyway-database-postgresql");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.counters.type", havingValue = "redis") // only when asked for: with no property the default falls through to the other stores
    @ConditionalOnMissingClass("io.lettuce.core.RedisClient")
    Object usageRedisJarMissing() {
        throw missing("usage.counters.type=redis", "lettuce-core");
    }

    // ---- an adapter turned on in the wrong kind of application: nothing would be metered, so the startup fails instead
    @Bean
    @ConditionalOnProperty(name = "usage.gateway.enabled", havingValue = "true")
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    Object usageGatewayInAServletApplication() {
        throw wrongApplication("usage.gateway.enabled=true", "reactive (Spring Cloud Gateway)", "servlet");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.gateway.enabled", havingValue = "true")
    @ConditionalOnNotWebApplication
    Object usageGatewayInANonWebApplication() {
        throw wrongApplication("usage.gateway.enabled=true", "reactive (Spring Cloud Gateway)", "non-web");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.servlet.enabled", havingValue = "true")
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
    Object usageServletInAReactiveApplication() {
        throw wrongApplication("usage.servlet.enabled=true", "servlet (Spring MVC)", "reactive");
    }

    @Bean
    @ConditionalOnProperty(name = "usage.servlet.enabled", havingValue = "true")
    @ConditionalOnNotWebApplication
    Object usageServletInANonWebApplication() {
        throw wrongApplication("usage.servlet.enabled=true", "servlet (Spring MVC)", "non-web");
    }

    private static IllegalStateException wrongApplication(String property, String needs, String is) {
        return new IllegalStateException(property + " needs a " + needs + " application, and this one is " + is);
    }

    private static IllegalStateException missing(String property, String jar) {
        return new IllegalStateException(property + " needs the " + jar + " jar on the classpath, and it is not there");
    }
}
