package com.example.usage.consumer;

import com.example.usage.consumer.UsageEventConsumer.Disposition;
import com.example.usage.consumer.UsageEventConsumer.Outcome;
import com.example.usage.core.UsageEngine;
import com.example.usage.core.UsageEngine.FailMode;
import com.example.usage.core.memory.InMemoryCounterStore;
import com.example.usage.core.memory.InMemoryLedger;
import com.example.usage.core.model.Compensation;
import com.example.usage.core.model.Limit;
import com.example.usage.core.model.Limit.Enforcement;
import com.example.usage.core.model.Limit.Type;
import com.example.usage.core.model.Metric;
import com.example.usage.core.model.Metric.Mode;
import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.model.UsageEvent;
import com.example.usage.core.spi.CounterStore;
import com.example.usage.core.spi.Ledger;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.*;

class UsageEventConsumerTest {
    static final Metric ITEMS = new Metric("ITEMS_PROCESSED", Mode.REPORT_ONLY);
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-19T12:00:00Z"), ZoneOffset.UTC);
    static final TenantId T = new TenantId("acme");
    static final String ID = "b3f1c2a4-8d5e-4c1a-9a77-2f0e6d1b9c33";

    final InMemoryCounterStore counters = new InMemoryCounterStore(CLOCK);

    UsageEngine engine(Ledger ledger, CounterStore store) {
        Limit quota = new Limit(ITEMS, Type.QUOTA, 5, Duration.ofDays(1), Enforcement.REJECT);
        return new UsageEngine(
                t -> CompletableFuture.completedFuture(Optional.of(new Policy(Instant.parse("2026-11-01T00:00:00Z"), List.of(quota)))),
                store, ledger, e -> CompletableFuture.completedFuture(null), CLOCK, FailMode.CLOSED, Duration.ofMinutes(15));
    }

    UsageEventConsumer consumer(Ledger ledger, CounterStore store) {
        return new UsageEventConsumer(engine(ledger, store), Set.of("ITEMS_PROCESSED")::contains, CLOCK,
                Duration.ofMinutes(5), Duration.ofDays(90));
    }

    UsageEventConsumer consumer() { return consumer(new InMemoryLedger(), counters); }

    static String json(String tenant, String metric, String quantity, String occurredAt, String id) {
        return """
                {"eventId":"%s","eventType":"USAGE","schemaVersion":2,"tenantId":%s,"metricCode":"%s",
                 "quantity":%s,"occurredAt":"%s","source":{"service":"item-processor"},"attributes":{"x":"y"}}
                """.formatted(id, tenant, metric, quantity, occurredAt);
    }

    static String valid(String quantity) {
        return json("\"acme\"", "ITEMS_PROCESSED", quantity, "2026-11-19T11:59:00Z", ID);
    }

    static Outcome join(CompletionStage<Outcome> s) { return s.toCompletableFuture().join(); }

    static void assertDead(String reason, Outcome o) {
        assertEquals(Disposition.DEAD_LETTER, o.disposition());
        assertEquals(reason, o.reason());
    }

    @Test
    void validEventIsCountedAndAcked() {
        var c = consumer();
        assertEquals(new Outcome(Disposition.ACK, "applied"), join(c.handle(valid("5"))));
        // quota 5 is now consumed: a check on the same engine would be refused
        assertEquals(5L, counters.get("quota:acme:ITEMS_PROCESSED:86400:" + Instant.parse("2026-11-19T00:00:00Z").getEpochSecond())
                .toCompletableFuture().join());
    }

    @Test
    void redeliveredEventIsAckedAndCountedOnce() {
        var c = consumer();
        join(c.handle(valid("2")));
        assertEquals(new Outcome(Disposition.ACK, "duplicate"), join(c.handle(valid("2"))));
        assertEquals(2L, counters.get("quota:acme:ITEMS_PROCESSED:86400:" + Instant.parse("2026-11-19T00:00:00Z").getEpochSecond())
                .toCompletableFuture().join());
    }

    @Test
    void malformedMessagesAreDeadLettered() {
        var c = consumer();
        assertDead("invalid_json", join(c.handle("not json")));
        assertDead("invalid_json", join(c.handle(null)));
        assertDead("invalid_json", join(c.handle("[1,2]")));
        assertDead("missing_tenant_id", join(c.handle(json("null", "ITEMS_PROCESSED", "1", "2026-11-19T11:59:00Z", ID))));
        assertDead("missing_tenant_id", join(c.handle(json("\"  \"", "ITEMS_PROCESSED", "1", "2026-11-19T11:59:00Z", ID))));
        assertDead("invalid_quantity", join(c.handle(valid("0"))));
        assertDead("invalid_quantity", join(c.handle(valid("-3"))));
        assertDead("invalid_quantity", join(c.handle(valid("1.5"))));
        assertDead("invalid_event_id", join(c.handle(json("\"acme\"", "ITEMS_PROCESSED", "1", "2026-11-19T11:59:00Z", "nope"))));
        assertDead("invalid_occurred_at", join(c.handle(json("\"acme\"", "ITEMS_PROCESSED", "1", "yesterday", ID))));
    }

    @Test
    void unknownMetricEventTypeAndFutureSchemaAreDeadLettered() {
        var c = consumer();
        assertDead("unknown_metric", join(c.handle(json("\"acme\"", "MYSTERY", "1", "2026-11-19T11:59:00Z", ID))));
        assertDead("unsupported_event_type", join(c.handle(valid("1").replace("\"USAGE\"", "\"REFUND\""))));
        assertDead("invalid_compensates", join(c.handle(valid("1").replace("\"USAGE\"", "\"COMPENSATION\""))));
        assertDead("unsupported_schema_version", join(c.handle(valid("1").replace("\"schemaVersion\":2", "\"schemaVersion\":3"))));
    }

    @Test
    void eventsOutsideTheTimeWindowAreDeadLettered() {
        var c = consumer();
        assertDead("occurred_at_out_of_range", join(c.handle(json("\"acme\"", "ITEMS_PROCESSED", "1", "2026-11-19T12:30:00Z", ID))));
        assertDead("occurred_at_out_of_range", join(c.handle(json("\"acme\"", "ITEMS_PROCESSED", "1", "2026-06-01T00:00:00Z", ID))));
    }

    @Test
    void version1MessageWithoutTypeOrVersionIsAcceptedAndUnknownFieldsIgnored() {
        String v1 = """
                {"eventId":"%s","tenantId":"acme","metricCode":"ITEMS_PROCESSED","occurredAt":"2026-11-19T11:59:00Z","futureField":42}
                """.formatted(ID);
        assertEquals("applied", join(consumer().handle(v1)).reason()); // quantity defaults to 1
    }

    @Test
    void ledgerFailureAsksForRedeliveryAndCountsNothing() {
        Ledger down = (e, slots) -> CompletableFuture.failedFuture(new IllegalStateException("db down"));
        var c = consumer(down, counters);
        assertEquals(new Outcome(Disposition.RETRY, "ledger_unavailable"), join(c.handle(valid("1"))));
        assertEquals(0L, counters.get("quota:acme:ITEMS_PROCESSED:86400:" + Instant.parse("2026-11-19T00:00:00Z").getEpochSecond())
                .toCompletableFuture().join());
    }

    @Test
    void counterFailureIsAckedButFlaggedForReconciliation() {
        CounterStore down = new CounterStore() {
            public CompletionStage<Result> add(String k, long d, long c, Duration t) {
                return CompletableFuture.failedFuture(new IllegalStateException("redis down"));
            }
            public CompletionStage<Long> get(String k) { return CompletableFuture.completedFuture(0L); }
        };
        assertEquals(new Outcome(Disposition.ACK, "counter_drift"), join(consumer(new InMemoryLedger(), down).handle(valid("1"))));
    }

    @Test
    void producerAndConsumerShareTheSameContract() {
        UsageEvent e = new UsageEvent(UUID.fromString(ID), T, "ITEMS_PROCESSED", 3, Instant.parse("2026-11-19T11:59:00Z"));
        assertEquals(e, UsageEventJson.fromJson(UsageEventJson.toJson(e)));
        assertEquals("applied", join(consumer().handle(UsageEventJson.toJson(e))).reason());
    }

    static String reversal(String compensatesId, String quantity) {
        UsageEvent shape = new UsageEvent(UUID.randomUUID(), T, "ITEMS_PROCESSED", 1, Instant.parse("2026-11-19T11:59:00Z"));
        return UsageEventJson.toJson(Compensation.of(UUID.fromString(compensatesId), T, "ITEMS_PROCESSED",
                Long.parseLong(quantity), Instant.parse("2026-11-19T11:59:30Z"), "RESULT_REVERTED"));
    }

    @Test
    void compensationIsAppliedHeldUntilItsOriginalArrivesAndOnlyOnce() {
        var c = consumer();
        String reversal = reversal(ID, "2");
        assertEquals(new Outcome(Disposition.RETRY, "original_not_found"), join(c.handle(reversal))); // original not here yet

        join(c.handle(valid("5")));
        assertEquals(new Outcome(Disposition.ACK, "compensated"), join(c.handle(reversal)));
        assertEquals(3L, counters.get("quota:acme:ITEMS_PROCESSED:86400:" + Instant.parse("2026-11-19T00:00:00Z").getEpochSecond())
                .toCompletableFuture().join());
        assertEquals(new Outcome(Disposition.ACK, "duplicate"), join(c.handle(reversal)));

        String another = UsageEventJson.toJson(new Compensation(UUID.randomUUID(), T, "ITEMS_PROCESSED", 1,
                Instant.parse("2026-11-19T11:59:40Z"), UUID.fromString(ID), "AGAIN"));
        assertDead("already_compensated", join(c.handle(another)));
    }

    @Test
    void reversalOfMoreThanWasUsedIsDeadLettered() {
        var c = consumer();
        join(c.handle(valid("2")));
        assertDead("invalid_compensation", join(c.handle(reversal(ID, "3"))));
    }

    @Test
    void malformedReversalsAreDeadLettered() {
        var c = consumer();
        String noReason = reversal(ID, "1").replace("\"reason\":\"RESULT_REVERTED\"", "\"x\":1");
        assertDead("missing_reason", join(c.handle(noReason)));
        assertDead("invalid_compensates", join(c.handle(reversal(ID, "1").replaceAll("\"compensates\":\"[^\"]*\"", "\"compensates\":\"nope\""))));
    }

    @Test
    void reversalRoundTripsThroughTheContract() {
        Compensation c = Compensation.of(UUID.fromString(ID), T, "ITEMS_PROCESSED", 2, Instant.parse("2026-11-19T11:59:30Z"), "RESULT_REVERTED");
        assertEquals(new UsageEventJson.Parsed.Reversal(c), UsageEventJson.parse(UsageEventJson.toJson(c)));
    }

    @Test
    void aDatabaseThatRejectsTheDataDeadLettersItButTransientErrorsAreRetried() {
        for (String rejected : new String[]{"22003", "23514", "22001"}) { // data exception, integrity violation
            Ledger refuses = (e, slots) -> CompletableFuture.failedFuture(
                    new IllegalStateException("ledger write failed", new SQLException("value rejected", rejected)));
            assertDead("rejected_by_database", join(consumer(refuses, counters).handle(valid("1"))));
        }
        for (String transientState : new String[]{"40P01", "40001", "08006", "57P01"}) { // deadlock, serialization, connection
            Ledger flaky = (e, slots) -> CompletableFuture.failedFuture(
                    new IllegalStateException("ledger write failed", new SQLException("try again", transientState)));
            assertEquals(new Outcome(Disposition.RETRY, "ledger_unavailable"), join(consumer(flaky, counters).handle(valid("1"))), transientState);
        }
    }

    @Test
    void valuesThatWouldBreakTheDatabaseAreRefusedBeforeReachingIt() {
        var c = consumer();
        assertDead("invalid_quantity", join(c.handle(valid("5000000000"))));
        assertDead("invalid_quantity", join(c.handle(valid(String.valueOf(UsageEvent.MAX_QUANTITY + 1)))));
        assertDead("invalid_quantity", join(c.handle(valid("99999999999999999999999"))));
        assertDead("invalid_tenant_id", join(c.handle(json("\"" + "t".repeat(129) + "\"", "ITEMS_PROCESSED", "1", "2026-11-19T11:59:00Z", ID))));
        assertDead("invalid_metric_code", join(c.handle(json("\"acme\"", "M".repeat(61), "1", "2026-11-19T11:59:00Z", ID))));
        String longReason = UsageEventJson.toJson(new Compensation(UUID.randomUUID(), T, "ITEMS_PROCESSED", 1,
                Instant.parse("2026-11-19T11:59:30Z"), UUID.fromString(ID), "R".repeat(61)));
        assertDead("invalid_reason", join(c.handle(longReason)));
        assertDead("unsupported_schema_version", join(c.handle(valid("1").replace("\"schemaVersion\":2", "\"schemaVersion\":99999999999"))));
        assertEquals("applied", join(c.handle(valid(String.valueOf(UsageEvent.MAX_QUANTITY)))).reason()); // the largest legal value still works
    }

    @Test
    void oneSerializerCoversBothKindsOfMessage() {
        UsageEvent usage = new UsageEvent(UUID.fromString(ID), T, "ITEMS_PROCESSED", 1, Instant.parse("2026-11-19T11:59:00Z"));
        Compensation reversal = Compensation.of(usage.eventId(), T, "ITEMS_PROCESSED", 1, Instant.parse("2026-11-19T11:59:30Z"), "X");
        assertEquals(new UsageEventJson.Parsed.Usage(usage), UsageEventJson.parse(UsageEventJson.toJson((com.example.usage.core.model.UsageMessage) usage)));
        assertEquals(new UsageEventJson.Parsed.Reversal(reversal), UsageEventJson.parse(UsageEventJson.toJson((com.example.usage.core.model.UsageMessage) reversal)));
    }
}
