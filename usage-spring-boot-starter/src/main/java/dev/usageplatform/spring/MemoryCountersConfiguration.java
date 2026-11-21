package dev.usageplatform.spring;

import dev.usageplatform.core.memory.InMemoryCounterStore;
import dev.usageplatform.core.spi.CounterStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "usage.counters.type", havingValue = "memory")
class MemoryCountersConfiguration {
    private static final Logger log = LoggerFactory.getLogger(UsageAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(CounterStore.class)
    CounterStore usageCounterStore(Clock clock) {
        log.warn("usage.counters.type=memory: quotas are counted inside this JVM only; never use it with more than one instance");
        return new InMemoryCounterStore(clock);
    }
}
