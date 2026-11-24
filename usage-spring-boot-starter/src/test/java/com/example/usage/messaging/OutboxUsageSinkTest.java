package com.example.usage.messaging;

import com.example.outbox.core.OutboxMessage;
import com.example.outbox.core.OutboxWriter;
import com.example.usage.consumer.UsageEventJson;
import com.example.usage.core.model.Compensation;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.model.UsageEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class OutboxUsageSinkTest {
    static final TenantId T = new TenantId("acme");
    static final UsageEvent USAGE = new UsageEvent(UUID.randomUUID(), T, "API_REQUESTS", 3, Instant.parse("2026-11-19T11:59:00Z"));

    static final class CapturingWriter implements OutboxWriter {
        final List<OutboxMessage> written = new ArrayList<>();
        final AtomicBoolean insideTransaction = new AtomicBoolean();
        final List<Boolean> wasInsideTransaction = new ArrayList<>();
        final List<String> threads = new ArrayList<>();
        RuntimeException failure;

        @Override public void enqueue(OutboxMessage message) {
            if (failure != null) throw failure;
            written.add(message);
            wasInsideTransaction.add(insideTransaction.get());
            threads.add(Thread.currentThread().getName());
        }

        @Override public void enqueue(String aggregateType, String aggregateId, Long aggregateVersion, String eventType, Object payload, String destination) {
            throw new UnsupportedOperationException("the sink writes ready JSON");
        }
    }

    @Test
    void orderedByEventEachMessageIsItsOwnAggregateSoNothingWaitsForAnother() {
        var writer = new CapturingWriter();
        var sink = OutboxUsageSink.sameTransaction(writer, "usage-events").orderedBy(OutboxUsageSink.Ordering.EVENT);
        var other = new UsageEvent(UUID.randomUUID(), T, "API_REQUESTS", 1, Instant.parse("2026-11-19T12:00:00Z"));
        sink.publish(USAGE).toCompletableFuture().join();
        sink.publish(other).toCompletableFuture().join();

        assertEquals("usage-event", writer.written.get(0).aggregateType());
        assertEquals(USAGE.eventId().toString(), writer.written.get(0).aggregateId());
        assertNotEquals(writer.written.get(0).aggregateId(), writer.written.get(1).aggregateId()); // same tenant, no shared lane
        assertEquals(USAGE, ((UsageEventJson.Parsed.Usage) UsageEventJson.parse(writer.written.get(0).payload())).event());
    }

    @Test
    void aUsageEventBecomesAnOutboxMessageKeyedByItsIdAndOrderedByTenant() {
        var writer = new CapturingWriter();
        OutboxUsageSink.sameTransaction(writer, "usage-events").publish(USAGE).toCompletableFuture().join();

        OutboxMessage m = writer.written.get(0);
        assertEquals(USAGE.eventId().toString(), m.headers().get(OutboxUsageSink.EVENT_ID_HEADER)); // traceable to the ledger
        assertNotEquals(USAGE.eventId(), m.messageId());     // the row id is random: see the test below
        assertEquals("tenant", m.aggregateType());
        assertEquals("acme", m.aggregateId());              // per-tenant ordering
        assertEquals("usage.event", m.eventType());
        assertEquals("usage-events", m.destination());
        assertEquals(USAGE.occurredAt(), m.occurredAt());
        assertEquals(USAGE, ((UsageEventJson.Parsed.Usage) UsageEventJson.parse(m.payload())).event()); // the consumer reads exactly this
    }

    @Test
    void aCompensationUsesItsOwnTypeAndPayload() {
        var writer = new CapturingWriter();
        Compensation c = Compensation.of(USAGE.eventId(), T, "API_REQUESTS", 3, Instant.parse("2026-11-19T12:00:00Z"), "UPSTREAM_ERROR");
        OutboxUsageSink.sameTransaction(writer, "usage-events").publish(c).toCompletableFuture().join();

        OutboxMessage m = writer.written.get(0);
        assertEquals(c.eventId().toString(), m.headers().get(OutboxUsageSink.EVENT_ID_HEADER));
        assertEquals("usage.compensation", m.eventType());
        assertEquals(c, ((UsageEventJson.Parsed.Reversal) UsageEventJson.parse(m.payload())).compensation());
    }

    @Test
    void publishingTheSameEventTwiceGivesTwoDistinctRowIds() {
        // event ids are deterministic and outbox_message.id is a primary key: a repeat must never reuse the row id
        var writer = new CapturingWriter();
        var sink = OutboxUsageSink.sameTransaction(writer, "usage-events");
        sink.publish(USAGE).toCompletableFuture().join();
        sink.publish(USAGE).toCompletableFuture().join();
        assertNotEquals(writer.written.get(0).messageId(), writer.written.get(1).messageId());
        assertEquals(writer.written.get(0).payload(), writer.written.get(1).payload()); // same event: the ledger deduplicates it
    }

    @Test
    void aWriterFailureIsAFailedStageNotAThrow() {
        var writer = new CapturingWriter();
        writer.failure = new IllegalStateException("outbox table is gone");
        var stage = OutboxUsageSink.sameTransaction(writer, "usage-events").publish(USAGE);
        assertThrows(CompletionException.class, () -> stage.toCompletableFuture().join());
    }

    @Test
    void ownTransactionWritesInsideANewTransactionOnTheGivenExecutor() {
        var writer = new CapturingWriter();
        var executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "usage-outbox"));
        try {
            var sink = OutboxUsageSink.ownTransaction(writer, "usage-events", executor, work -> {
                writer.insideTransaction.set(true);
                try {
                    work.run();
                } finally {
                    writer.insideTransaction.set(false);
                }
            });
            sink.publish(USAGE).toCompletableFuture().join();

            assertEquals(List.of(true), writer.wasInsideTransaction); // the enqueue happened inside the transaction
            assertEquals(List.of("usage-outbox"), writer.threads);    // and not on the caller's thread
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void anOwnTransactionSinkCalledInsideATransactionRefusesInsteadOfBillingWorkThatMayRollBack() {
        var writer = new CapturingWriter();
        var inside = new AtomicBoolean(true);
        var sink = OutboxUsageSink.ownTransaction(writer, "usage-events", Runnable::run, Runnable::run, inside::get);
        assertThrows(CompletionException.class, () -> sink.publish(USAGE).toCompletableFuture().join());
        assertTrue(writer.written.isEmpty());

        inside.set(false); // outside any transaction it works as usual
        sink.publish(USAGE).toCompletableFuture().join();
        assertEquals(1, writer.written.size());
    }

    @Test
    void aRefusedTaskIsAFailedStage() {
        var executor = Executors.newSingleThreadExecutor();
        executor.shutdown(); // rejects everything from now on
        var sink = OutboxUsageSink.ownTransaction(new CapturingWriter(), "usage-events", executor, Runnable::run);
        assertThrows(CompletionException.class, () -> sink.publish(USAGE).toCompletableFuture().join());
    }
}
