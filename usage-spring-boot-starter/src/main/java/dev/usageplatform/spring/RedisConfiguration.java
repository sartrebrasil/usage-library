package dev.usageplatform.spring;

import dev.usageplatform.core.CounterStore;
import dev.usageplatform.redis.RedisCounterStore;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(RedisCounterStore.class)
@ConditionalOnProperty(name = "usage.counters.type", havingValue = "redis", matchIfMissing = true)
@ConditionalOnMissingBean(CounterStore.class) // a CounterStore of your own needs neither the Redis client nor its uri
class RedisConfiguration {

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(RedisClient.class)
    RedisClient usageRedisClient(UsageProperties props) {
        Assert.hasText(props.redis().uri(), "usage.redis.uri is required when usage.counters.type=redis");
        RedisClient client = RedisClient.create(props.redis().uri());
        // the check is on the request path: a slow or unreachable Redis must fail fast so fail-mode can decide,
        // not buffer commands for Lettuce's default 60 s
        client.setOptions(ClientOptions.builder()
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .timeoutOptions(TimeoutOptions.enabled(props.redis().commandTimeout()))
                .build());
        return client;
    }

    @Bean(destroyMethod = "close")
    StatefulRedisConnection<String, String> usageRedisConnection(RedisClient client) {
        return client.connect();
    }

    @Bean
    @ConditionalOnMissingBean(CounterStore.class)
    CounterStore usageCounterStore(StatefulRedisConnection<String, String> connection) {
        return new RedisCounterStore(connection.async());
    }
}
