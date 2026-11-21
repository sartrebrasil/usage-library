package com.example.usage.spring;

import com.example.usage.core.spi.UsageTelemetry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Metrics and traces over Micrometer, when it is on the classpath and a {@code MeterRegistry} exists. Spans appear
 * when the application has a tracing bridge (micrometer-tracing) registered on its {@code ObservationRegistry}.
 * Without a registry the platform reports to {@link UsageTelemetry#NOOP}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({MeterRegistry.class, ObservationRegistry.class})
@ConditionalOnProperty(name = "usage.telemetry.enabled", havingValue = "true", matchIfMissing = true)
class TelemetryConfiguration {

    @Bean
    @ConditionalOnMissingBean(UsageTelemetry.class)
    @ConditionalOnBean(MeterRegistry.class)
    UsageTelemetry usageTelemetry(MeterRegistry meters, ObjectProvider<ObservationRegistry> observations) {
        // the application's registry (Boot wires the meter handler and the tracer into it); without one, ours: timers only
        ObservationRegistry registry = observations.getIfAvailable(() -> {
            ObservationRegistry own = ObservationRegistry.create();
            own.observationConfig().observationHandler(new DefaultMeterObservationHandler(meters));
            return own;
        });
        return new MicrometerUsageTelemetry(meters, registry);
    }
}
