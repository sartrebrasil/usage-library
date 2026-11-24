package com.example.usage.messaging;

import com.example.messaging.inbox.HandlerResult;
import com.example.messaging.inbox.InboxResultHandler;
import com.example.messaging.inbox.TypedMessage;
import com.example.usage.consumer.UsageEventConsumer;
import com.example.usage.consumer.UsageEventConsumer.Outcome;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * The central usage consumer as a messaging-inbox handler, so the broker (Kafka, SQS, JMS, RabbitMQ, GCP, Azure...)
 * is whatever messaging-library is configured for: this class knows none of them.
 * <p>
 * How each answer of {@link UsageEventConsumer} becomes an inbox result:
 * <ul>
 *   <li>ACK (applied, duplicate, counter_drift): the handler returns normally. {@code counter_drift} is passed to
 *       the observer, because it is an ACK that reconciliation must repair.</li>
 *   <li>RETRY original_not_found: a compensation that got ahead of its usage waits in the delayed level for
 *       {@code originalNotFoundDelay} instead of the computed backoff.</li>
 *   <li>RETRY anything else (ledger or policy store down): the handler throws, so the inbox's own retry levels,
 *       backoff and dead-lettering apply.</li>
 *   <li>DEAD_LETTER (malformed, unknown metric, rejected by the database...): {@code HandlerResult.reject(reason)},
 *       straight to the failed destination with no retry.</li>
 * </ul>
 * The message payload is read as raw text, whatever payload type the handler is registered with, and parsed by the
 * platform's own contract ({@code UsageEventJson}). Partition or order the topic by tenant id.
 * <p>
 * The consumer is asynchronous and the inbox worker is a thread of its own, so this blocks on it, with a timeout.
 *
 * @param <T> the payload type the inbox deserializes to; it is ignored
 */
public final class UsageInboxHandler<T> implements InboxResultHandler<T> {
    /** Thrown to make the inbox retry with its own backoff. */
    public static final class RetryableUsageException extends RuntimeException {
        public RetryableUsageException(String reason, Throwable cause) {
            super(reason, cause);
        }
    }

    private final UsageEventConsumer consumer;
    private final Duration originalNotFoundDelay;
    private final Duration timeout;
    private final Consumer<Outcome> observer;

    /**
     * @param originalNotFoundDelay how long a compensation waits for its usage event (for example 30 s)
     * @param timeout               how long to wait for the consumer before treating the delivery as a transient failure
     * @param observer              sees every outcome, for metrics (counter_drift above all)
     */
    public UsageInboxHandler(UsageEventConsumer consumer, Duration originalNotFoundDelay, Duration timeout, Consumer<Outcome> observer) {
        this.consumer = consumer;
        this.originalNotFoundDelay = originalNotFoundDelay;
        this.timeout = timeout;
        this.observer = observer;
    }

    @Override
    public HandlerResult handle(TypedMessage<T> message) throws Exception {
        Outcome outcome;
        try {
            outcome = consumer.handle(message.message().payloadAsString()).toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException | ExecutionException e) {
            throw new RetryableUsageException("usage consumer did not answer", e);
        }
        try {
            observer.accept(outcome);
        } catch (RuntimeException ignored) { // a metrics hook must never turn a counted message into a redelivery
        }
        return switch (outcome.disposition()) {
            case ACK -> HandlerResult.ack();
            case RETRY -> {
                if (UsageEventConsumer.ORIGINAL_NOT_FOUND_REASON.equals(outcome.reason())) yield HandlerResult.retryAfter(originalNotFoundDelay);
                throw new RetryableUsageException(outcome.reason(), null);
            }
            case DEAD_LETTER -> HandlerResult.reject(outcome.reason());
        };
    }
}
