package dev.usageplatform.spring;

import dev.usageplatform.consumer.UsageEventConsumer;

import java.util.function.Consumer;

/**
 * Define one bean of this type to see every answer of the usage consumer (for metrics). Without it, only
 * {@code counter_drift}, an ACK that reconciliation must repair, is logged, as a warning.
 */
@FunctionalInterface
public interface UsageOutcomeObserver extends Consumer<UsageEventConsumer.Outcome> {}
