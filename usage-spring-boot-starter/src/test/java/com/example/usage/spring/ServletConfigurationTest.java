package com.example.usage.spring;

import com.example.usage.core.model.TenantId;
import com.example.usage.core.spi.TenantResolver;
import com.example.usage.core.spi.UsageSink;
import com.example.usage.servlet.UsageInterceptor;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class ServletConfigurationTest {
    static final String[] ON = {"usage.counters.type=memory", "usage.servlet.enabled=true",
            "usage.servlet.routes[0].path=/orders/**", "usage.servlet.routes[0].metrics[0].code=API_REQUESTS",
            "usage.servlet.routes[1].path=/batches", "usage.servlet.routes[1].metrics[0].code=ITEMS",
            "usage.servlet.routes[1].metrics[0].cost-header=X-Item-Count"};

    final WebApplicationContextRunner web = new WebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
            .withUserConfiguration(UsageAutoConfigurationTest.HostStores.class)
            .withBean(UsageSink.class, () -> message -> CompletableFuture.completedFuture(null))
            .withBean("tenants", TenantResolver.class, () -> (TenantResolver<HttpServletRequest>) r -> CompletableFuture.completedFuture(Optional.of(new TenantId("acme"))));

    @Test
    void theInterceptorIsRegisteredInAServletApplicationWhenEnabled() {
        web.withPropertyValues(ON).run(ctx -> assertThat(ctx).hasNotFailed().hasSingleBean(UsageInterceptor.class).hasSingleBean(WebMvcConfigurer.class));
        web.withPropertyValues("usage.counters.type=memory").run(ctx -> assertThat(ctx).doesNotHaveBean(UsageInterceptor.class)); // off unless asked
    }

    @Test
    void turningItOnInAnApplicationOfAnotherKindFailsTheStartup() {
        new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(UsageAutoConfiguration.class))
                .withUserConfiguration(UsageAutoConfigurationTest.HostStores.class)
                .withBean(UsageSink.class, () -> message -> CompletableFuture.completedFuture(null))
                .withPropertyValues(ON).run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage.servlet.enabled=true needs a servlet"));
    }

    @Test
    void theGatewayFilterInAServletApplicationFailsTheStartup() {
        web.withPropertyValues("usage.counters.type=memory", "usage.gateway.enabled=true", "usage.gateway.metrics[0].code=API_REQUESTS")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage.gateway.enabled=true needs a reactive"));
    }

    @Test
    void enabledWithoutRoutesFailsTheStartup() {
        web.withPropertyValues("usage.counters.type=memory", "usage.servlet.enabled=true")
                .run(ctx -> assertThat(ctx).hasFailed().getFailure().hasStackTraceContaining("usage.servlet.routes"));
    }
}
