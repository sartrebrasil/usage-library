package com.example.usage.spring;

import com.example.usage.core.UsageEngine;
import com.example.usage.core.spi.TenantResolver;
import com.example.usage.servlet.RouteMetric;
import com.example.usage.servlet.UsageInterceptor;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.time.Clock;
import java.util.Enumeration;
import java.util.List;
import java.util.function.ToLongFunction;

@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass({HandlerInterceptor.class, HttpServletRequest.class})
@ConditionalOnProperty(name = "usage.servlet.enabled", havingValue = "true")
class ServletConfiguration {

    @Bean
    @ConditionalOnMissingBean
    UsageInterceptor usageInterceptor(UsageEngine engine, TenantResolver<HttpServletRequest> tenants, Clock clock, UsageProperties props) {
        List<RouteMetric> metrics = props.servlet().routes().stream().flatMap(r -> {
            Assert.hasText(r.path(), "usage.servlet.routes[].path is required");
            return r.metrics().stream().map(m -> routeMetric(r.path(), m));
        }).toList();
        Assert.notEmpty(metrics, "usage.servlet.routes must list at least one route with a metric");
        int refundFrom = props.servlet().refundFromStatus();
        return new UsageInterceptor(engine, tenants, metrics, clock, status -> status >= refundFrom);
    }

    @Bean
    WebMvcConfigurer usageInterceptorRegistration(UsageInterceptor interceptor) {
        return new WebMvcConfigurer() {
            @Override public void addInterceptors(InterceptorRegistry registry) {
                registry.addInterceptor(interceptor);
            }
        };
    }

    private static RouteMetric routeMetric(String path, UsageProperties.GatewayMetric m) {
        Assert.hasText(m.code(), "usage.servlet.routes[].metrics[].code is required");
        return switch (m.mode()) {
            case ACCESS -> m.costHeader() == null || m.costHeader().isBlank()
                    ? RouteMetric.access(path, m.code())
                    : RouteMetric.access(path, m.code(), costFrom(m.costHeader()));
            case GUARD -> RouteMetric.guard(path, m.code());
        };
    }

    private static ToLongFunction<HttpServletRequest> costFrom(String header) {
        return request -> {
            // a repeated header is refused: this service would bill one value while a backend may read another
            Enumeration<String> values = request.getHeaders(header);
            if (values == null || !values.hasMoreElements()) return 1;
            String first = values.nextElement();
            if (values.hasMoreElements()) throw new IllegalArgumentException("repeated cost header " + header);
            return Long.parseLong(first.trim()); // malformed is refused with 400 on purpose
        };
    }
}
