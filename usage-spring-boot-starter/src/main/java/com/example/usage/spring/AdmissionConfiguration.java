package com.example.usage.spring;

import com.example.usage.core.UsageEngine;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Clock;

/** {@link UsageAdmission} for a servlet service that decides with the engine; on when {@code usage.admission.metric} is set. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(TransactionSynchronizationManager.class)
@ConditionalOnProperty(name = "usage.admission.metric")
class AdmissionConfiguration {
    @Bean
    @ConditionalOnMissingBean
    UsageAdmission usageAdmission(UsageEngine engine, UsageProperties props, Clock clock) {
        return new UsageAdmission(engine, props.admission().metric(), clock);
    }
}
