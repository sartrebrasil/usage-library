package com.example.usage.spring;

import com.example.outbox.core.OutboxWriter;
import com.example.usage.core.spi.UsageSink;
import com.example.usage.messaging.OutboxUsageSink;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.util.concurrent.ExecutorService;

import static com.example.usage.spring.UsageAutoConfiguration.EXECUTOR;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({OutboxUsageSink.class, OutboxWriter.class, PlatformTransactionManager.class})
@ConditionalOnProperty(name = "usage.messaging.outbox.destination")
class OutboxSinkConfiguration {

    @Bean
    @ConditionalOnMissingBean(UsageSink.class)
    UsageSink usageOutboxSink(OutboxWriter writer, ObjectProvider<PlatformTransactionManager> transactions,
                              @Qualifier(EXECUTOR) ExecutorService executor,
                              UsageProperties props) {
        UsageProperties.Outbox outbox = props.messaging().outbox();
        boolean gateway = props.gateway().enabled();
        boolean processor = props.processor().metric() != null && !props.processor().metric().isBlank();
        UsageProperties.TransactionMode mode = outbox.transaction();
        if (mode == null) {
            Assert.state(!(gateway && processor), "usage.messaging.outbox.transaction must be set (OWN for the gateway, CALLER for "
                    + "processors): this application is both");
            mode = processor ? UsageProperties.TransactionMode.CALLER : UsageProperties.TransactionMode.OWN;
        }
        Assert.state(!(gateway && mode == UsageProperties.TransactionMode.CALLER),
                "usage.messaging.outbox.transaction=CALLER cannot be used with usage.gateway.enabled=true: the gateway has no "
                        + "business transaction to join, so no usage would ever be written");
        if (mode == UsageProperties.TransactionMode.CALLER) {
            return OutboxUsageSink.sameTransaction(writer, outbox.destination()).orderedBy(ordering(outbox));
        }
        PlatformTransactionManager manager = transactions.getIfUnique();
        Assert.state(manager != null,
                "usage.messaging.outbox.transaction=OWN needs exactly one PlatformTransactionManager bean");
        TransactionTemplate template = new TransactionTemplate(manager);
        return OutboxUsageSink.ownTransaction(writer, outbox.destination(), executor,
                work -> template.executeWithoutResult(status -> work.run()),
                TransactionSynchronizationManager::isActualTransactionActive).orderedBy(ordering(outbox));
    }

    private static OutboxUsageSink.Ordering ordering(UsageProperties.Outbox outbox) {
        return OutboxUsageSink.Ordering.valueOf(outbox.ordering().name());
    }
}
