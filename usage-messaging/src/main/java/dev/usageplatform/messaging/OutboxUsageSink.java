package dev.usageplatform.messaging;

import com.example.outbox.core.OutboxMessage;
import com.example.outbox.core.OutboxWriter;
import dev.usageplatform.consumer.UsageEventJson;
import dev.usageplatform.core.model.Compensation;
import dev.usageplatform.core.model.UsageEvent;
import dev.usageplatform.core.model.UsageMessage;
import dev.usageplatform.core.spi.UsageSink;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * {@link UsageSink} on top of outbox-library: a usage event or a compensation is written to the outbox, and the
 * relay publishes it to whatever broker outbox-library is configured for.
 * <ul>
 *   <li>{@code messageId} is a fresh random id for each outbox row, and the event id travels in the
 *       {@value #EVENT_ID_HEADER} header. The row id must not be the event id: event ids are deterministic (the
 *       same item always yields the same one) and {@code outbox_message.id} is a primary key, so re-publishing an item
 *       that was already published, for example after a redelivery, would fail the insert and abort the host's whole
 *       transaction. The ledger is what makes a repeat harmless: it deduplicates by the event id inside the payload.</li>
 *   <li>{@code aggregateId} is the tenant id, which gives the per-tenant ordering the consumer wants.</li>
 *   <li>{@code eventType} is {@value #USAGE_TYPE} or {@value #COMPENSATION_TYPE}; the payload is the platform's JSON.</li>
 * </ul>
 * <b>Which transaction.</b> {@code OutboxWriter.enqueue} writes in the caller's active transaction. Use {@link #sameTransaction}
 * in a processor, inside the {@code Persist} of {@code QueueUsageAdapter}: the usage then commits, or rolls back, with
 * the result. Use {@link #ownTransaction} where there is no business transaction, such as the gateway after it
 * responded: each message gets a transaction of its own, run on the executor you give so a reactive thread never blocks.
 */
public final class OutboxUsageSink implements UsageSink {
    public static final String USAGE_TYPE = "usage.event";
    public static final String COMPENSATION_TYPE = "usage.compensation";
    /** Header with the platform's event id, for tracing a usage event from the outbox to the ledger. */
    public static final String EVENT_ID_HEADER = "usage-event-id";
    private static final String AGGREGATE_TYPE = "tenant";

    private final OutboxWriter writer;
    private final String destination;
    private final Executor executor;
    private final Consumer<Runnable> transaction;
    private final BooleanSupplier callerInTransaction;

    private OutboxUsageSink(OutboxWriter writer, String destination, Executor executor, Consumer<Runnable> transaction,
                            BooleanSupplier callerInTransaction) {
        this.writer = writer;
        this.destination = destination;
        this.executor = executor;
        this.transaction = transaction;
        this.callerInTransaction = callerInTransaction;
    }

    /** Writes in the transaction already open on the calling thread. */
    public static OutboxUsageSink sameTransaction(OutboxWriter writer, String destination) {
        return new OutboxUsageSink(writer, destination, Runnable::run, Runnable::run, () -> false);
    }

    /**
     * Writes each message in a new transaction on {@code executor}.
     *
     * @param transaction runs the given work inside a new transaction, for example
     *                    {@code work -> transactionTemplate.executeWithoutResult(status -> work.run())}
     */
    public static OutboxUsageSink ownTransaction(OutboxWriter writer, String destination, Executor executor, Consumer<Runnable> transaction) {
        return ownTransaction(writer, destination, executor, transaction, () -> false);
    }

    /**
     * Same, but refuses to publish (a failed stage, nothing written) when {@code callerInTransaction} says the calling
     * thread already has a business transaction open. A sink that writes in a transaction of its own would commit the
     * usage even if the caller's transaction later rolls back, which bills for work that was not saved; the
     * caller should have used {@link #sameTransaction}. In Spring pass
     * {@code TransactionSynchronizationManager::isActualTransactionActive}.
     */
    public static OutboxUsageSink ownTransaction(OutboxWriter writer, String destination, Executor executor,
                                                 Consumer<Runnable> transaction, BooleanSupplier callerInTransaction) {
        return new OutboxUsageSink(writer, destination, executor, transaction, callerInTransaction);
    }

    @Override
    public CompletionStage<Void> publish(UsageMessage message) {
        if (callerInTransaction.getAsBoolean()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "this usage sink writes in a transaction of its own but was called inside one: use the same-transaction sink (transaction=CALLER)"));
        }
        OutboxMessage outbox = toOutbox(message);
        try {
            return CompletableFuture.runAsync(() -> transaction.accept(() -> writer.enqueue(outbox)), executor);
        } catch (RuntimeException rejected) { // the executor refused the work: a failed stage, not a throw
            return CompletableFuture.failedFuture(rejected);
        }
    }

    private OutboxMessage toOutbox(UsageMessage message) {
        return switch (message) {
            case UsageEvent e -> new OutboxMessage(UUID.randomUUID(), AGGREGATE_TYPE, e.tenant().value(), null, USAGE_TYPE,
                    UsageEventJson.toJson(e), Map.of(EVENT_ID_HEADER, e.eventId().toString()), destination, e.occurredAt());
            case Compensation c -> new OutboxMessage(UUID.randomUUID(), AGGREGATE_TYPE, c.tenant().value(), null, COMPENSATION_TYPE,
                    UsageEventJson.toJson(c), Map.of(EVENT_ID_HEADER, c.eventId().toString()), destination, c.occurredAt());
        };
    }
}
