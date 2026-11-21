package com.example.usage.demo;

import com.example.usage.consumer.UsageEventConsumer;
import com.example.usage.consumer.UsageEventJson;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.spi.TenantResolver;
import com.example.usage.core.spi.UsageSink;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.server.ServerWebExchange;

import java.util.Map;
import java.util.Optional;

import static com.example.usage.core.spi.Stages.done;

/**
 * The two things a host application must supply to the gateway starter: who the caller is, and where usage goes.
 */
@Configuration(proxyBeanMethods = false)
class DemoConfiguration {

    /** API key -> tenant. A real gateway would validate a JWT or look the key up; the platform only needs the tenant id. */
    private static final Map<String, String> TENANT_BY_API_KEY = Map.of("key-acme", "acme", "key-globex", "globex", "key-initech", "initech");

    @Bean
    TenantResolver<ServerWebExchange> tenantResolver() {
        return exchange -> done(Optional
                .ofNullable(exchange.getRequest().getHeaders().getFirst("X-Api-Key"))
                .map(TENANT_BY_API_KEY::get)
                .map(TenantId::new));
    }

    /**
     * Where the gateway sends each usage event and each refund. This demo has no broker, so it hands them to the central
     * consumer in the same process, which writes the ledger and the per-period summary. In production publish through
     * the outbox instead (usage.messaging.outbox.destination) and consume with the messaging-library.
     * The consumer is looked up on first use: it needs the engine, and the engine needs this sink.
     */
    @Bean
    UsageSink usageSink(ObjectProvider<UsageEventConsumer> consumer) {
        return message -> consumer.getObject().handle(UsageEventJson.toJson(message)).thenApply(outcome -> null);
    }
}
