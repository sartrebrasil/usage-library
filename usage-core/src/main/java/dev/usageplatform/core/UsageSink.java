package dev.usageplatform.core;

import java.util.concurrent.CompletionStage;

/**
 * SPI: transport of usage events and compensations from reporters to the central consumer (outbox + broker
 * in production). It must be durable: the gateway publishes after the response and does not retry, so a sink
 * that loses messages under-bills. Serialize with {@code UsageEventJson.toJson(message)} from usage-consumer.
 */
@FunctionalInterface
public interface UsageSink {
    CompletionStage<Void> publish(UsageMessage message);
}
