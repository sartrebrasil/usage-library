package dev.usageplatform.spring;

import com.example.messaging.inbox.InboxResultHandler;
import dev.usageplatform.consumer.UsageEventConsumer;
import dev.usageplatform.messaging.UsageInboxHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import java.util.function.Consumer;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({UsageInboxHandler.class, InboxResultHandler.class})
@ConditionalOnProperty(name = "usage.messaging.inbox.enabled", havingValue = "true")
class InboxHandlerConfiguration {
    private static final Logger log = LoggerFactory.getLogger(UsageAutoConfiguration.class);

    /**
     * The inbox binds a handler to the receiver whose name equals the bean's, or its camelCase form: receiver
     * {@code usage-events} is served by this bean. Not aliased as "usage-events" on purpose: outbox-library names
     * its publisher beans after the destination, and a publisher to "usage-events" would collide with the alias.
     */
    @Bean(name = "usageEvents")
    @ConditionalOnMissingBean(name = "usageEvents")
    InboxResultHandler<String> usageEventsHandler(ObjectProvider<UsageEventConsumer> consumer,
                                                  ObjectProvider<UsageOutcomeObserver> observer, UsageProperties props) {
        UsageEventConsumer usage = consumer.getIfAvailable();
        Assert.state(usage != null, "usage.messaging.inbox.enabled=true needs usage.consumer.enabled=true (and known-metrics)");
        UsageProperties.Inbox inbox = props.messaging().inbox();
        Consumer<UsageEventConsumer.Outcome> watcher = observer.getIfAvailable(() -> outcome -> {
            if (UsageEventConsumer.COUNTER_DRIFT_REASON.equals(outcome.reason())) log.warn("usage counted in the ledger but not in the counters; reconciliation will repair it");
        });
        return new UsageInboxHandler<>(usage, inbox.originalNotFoundDelay(), inbox.timeout(), watcher);
    }
}
