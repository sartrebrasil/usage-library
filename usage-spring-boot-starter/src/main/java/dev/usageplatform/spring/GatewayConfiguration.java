package dev.usageplatform.spring;

import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.spi.TenantResolver;
import dev.usageplatform.gateway.MeteredMetric;
import dev.usageplatform.gateway.UsageGatewayFilter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.web.server.ServerWebExchange;

import java.time.Clock;
import java.util.List;
import java.util.function.ToLongFunction;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({UsageGatewayFilter.class, GlobalFilter.class})
@ConditionalOnProperty(name = "usage.gateway.enabled", havingValue = "true")
class GatewayConfiguration {

    @Bean
    @ConditionalOnMissingBean
    UsageGatewayFilter usageGatewayFilter(UsageEngine engine, TenantResolver<ServerWebExchange> tenants,
                                          ObjectProvider<InternalTraffic> internal, Clock clock, UsageProperties props) {
        List<MeteredMetric> metrics = props.gateway().metrics().stream().map(GatewayConfiguration::meteredMetric).toList();
        Assert.notEmpty(metrics, "usage.gateway.metrics must list at least one metric");
        InternalTraffic none = exchange -> false;
        int refundFrom = props.gateway().refundFromStatus();
        return new UsageGatewayFilter(engine, tenants, metrics, internal.getIfAvailable(() -> none), clock, status -> status >= refundFrom);
    }

    private static MeteredMetric meteredMetric(UsageProperties.GatewayMetric m) {
        Assert.hasText(m.code(), "usage.gateway.metrics[].code is required");
        return switch (m.mode()) {
            case ACCESS -> m.costHeader() == null || m.costHeader().isBlank()
                    ? MeteredMetric.access(m.code())
                    : MeteredMetric.access(m.code(), costFrom(m.costHeader()));
            case GUARD -> MeteredMetric.guard(m.code());
        };
    }

    private static ToLongFunction<ServerWebExchange> costFrom(String header) {
        return exchange -> {
            // a repeated header is refused: the gateway would bill one value while a backend may read another
            List<String> values = exchange.getRequest().getHeaders().getValuesAsList(header);
            if (values.size() > 1) throw new IllegalArgumentException("repeated cost header " + header);
            return values.isEmpty() ? 1 : Long.parseLong(values.get(0).trim()); // malformed is refused with 400 on purpose
        };
    }
}
