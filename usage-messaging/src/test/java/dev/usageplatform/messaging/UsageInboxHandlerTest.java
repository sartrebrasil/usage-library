package dev.usageplatform.messaging;

import com.example.messaging.inbox.Delivery;
import com.example.messaging.inbox.HandlerResult;
import com.example.messaging.inbox.InboxMessage;
import com.example.messaging.inbox.TypedMessage;
import dev.usageplatform.consumer.UsageEventConsumer;
import dev.usageplatform.consumer.UsageEventConsumer.Outcome;
import dev.usageplatform.consumer.UsageEventJson;
import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.UsageEngine.FailMode;
import dev.usageplatform.core.memory.InMemoryCounterStore;
import dev.usageplatform.core.memory.InMemoryLedger;
import dev.usageplatform.core.model.Compensation;
import dev.usageplatform.core.model.Limit;
import dev.usageplatform.core.model.Limit.Enforcement;
import dev.usageplatform.core.model.Limit.Type;
import dev.usageplatform.core.model.Metric;
import dev.usageplatform.core.model.Metric.Mode;
import dev.usageplatform.core.model.Policy;
import dev.usageplatform.core.model.TenantId;
import dev.usageplatform.core.model.UsageEvent;
import dev.usageplatform.core.spi.CounterStore;
import dev.usageplatform.core.spi.Ledger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.*;

class UsageInboxHandlerTest {
    static final Metric ITEMS = new Metric("ITEMS_PROCESSED", Mode.REPORT_ONLY);
    static final TenantId T = new TenantId("acme");
    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-19T12:00:00Z"), ZoneOffset.UTC);
    static final Duration WAIT = Duration.ofSeconds(30);

    final List<Outcome> seen = new ArrayList<>();
    java.util.function.Consumer<Outcome> observer;
    Ledger ledger;
    CounterStore counters;

    @BeforeEach
    void setUp() {
        ledger = new InMemoryLedger(CLOCK);
        counters = new InMemoryCounterStore(CLOCK);
        seen.clear();
        observer = seen::add;
    }

    UsageInboxHandler<Object> handler(Duration timeout) {
        Policy policy = new Policy(Instant.parse("2026-11-01T00:00:00Z"),
                List.of(new Limit(ITEMS, Type.QUOTA, 100, Duration.ofDays(1), Enforcement.REJECT)));
        var engine = new UsageEngine(t -> CompletableFuture.completedFuture(Optional.of(policy)), counters, ledger,
                m -> CompletableFuture.completedFuture(null), CLOCK, FailMode.CLOSED, Duration.ofMinutes(15));
        var consumer = new UsageEventConsumer(engine, Set.of("ITEMS_PROCESSED")::contains, CLOCK, Duration.ofMinutes(5), Duration.ofDays(90));
        return new UsageInboxHandler<>(consumer, WAIT, timeout, o -> observer.accept(o));
    }

    UsageInboxHandler<Object> handler() { return handler(Duration.ofSeconds(5)); }

    static TypedMessage<Object> delivery(String body) {
        var message = new InboxMessage(UUID.randomUUID().toString(), "usage.event", null, null, "acme", "tenant", null, null,
                Map.of(), body.getBytes(java.nio.charset.StandardCharsets.UTF_8), "usage-events", Delivery.first());
        return new TypedMessage<>(message, null);
    }

    static UsageEvent usage(String item, long qty) {
        return new UsageEvent(UsageEvent.idFor(T, item, "ITEMS_PROCESSED"), T, "ITEMS_PROCESSED", qty, Instant.parse("2026-11-19T11:59:00Z"));
    }

    static String json(UsageEvent e) { return UsageEventJson.toJson(e); }

    @Test
    void aCountedEventAndItsRedeliveryAreBothAcknowledged() throws Exception {
        UsageEvent e = usage("a", 3);
        assertInstanceOf(HandlerResult.Ack.class, handler().handle(delivery(json(e))));
        assertInstanceOf(HandlerResult.Ack.class, handler().handle(delivery(json(e))));
        assertEquals(List.of("applied", "duplicate"), seen.stream().map(Outcome::reason).toList());
    }

    @Test
    void aCompensationAheadOfItsUsageWaitsInTheDelayedLevelThenIsApplied() throws Exception {
        UsageEvent original = usage("a", 3);
        String reversal = UsageEventJson.toJson(Compensation.of(original.eventId(), T, "ITEMS_PROCESSED", 1, Instant.parse("2026-11-19T11:59:30Z"), "X"));

        var wait = assertInstanceOf(HandlerResult.RetryAfter.class, handler().handle(delivery(reversal)));
        assertEquals(WAIT, wait.delay()); // the delay we chose, not the computed backoff

        handler().handle(delivery(json(original)));
        assertInstanceOf(HandlerResult.Ack.class, handler().handle(delivery(reversal)));
        assertEquals("compensated", seen.get(seen.size() - 1).reason());
    }

    @Test
    void anOutageIsRetriedByTheInboxThroughAnException() {
        ledger = (e, slots) -> CompletableFuture.failedFuture(new IllegalStateException("db down"));
        var thrown = assertThrows(UsageInboxHandler.RetryableUsageException.class, () -> handler().handle(delivery(json(usage("a", 1)))));
        assertEquals("ledger_unavailable", thrown.getMessage());
    }

    @Test
    void messagesThatCanNeverBeCountedAreRejectedWithTheirReason() throws Exception {
        var invalid = assertInstanceOf(HandlerResult.Reject.class, handler().handle(delivery("not json")));
        assertEquals("invalid_json", invalid.reason());
        var unknown = assertInstanceOf(HandlerResult.Reject.class, handler().handle(delivery(
                json(new UsageEvent(UUID.randomUUID(), T, "MYSTERY", 1, Instant.parse("2026-11-19T11:59:00Z"))))));
        assertEquals("unknown_metric", unknown.reason());
    }

    @Test
    void counterDriftIsAcknowledgedButReachesTheObserver() throws Exception {
        counters = new CounterStore() {
            public CompletionStage<Result> add(String k, long d, long c, Duration t) { return CompletableFuture.failedFuture(new IllegalStateException("redis down")); }
            public CompletionStage<Long> get(String k) { return CompletableFuture.completedFuture(0L); }
        };
        assertInstanceOf(HandlerResult.Ack.class, handler().handle(delivery(json(usage("a", 1)))));
        assertEquals("counter_drift", seen.get(0).reason()); // an ACK that reconciliation must repair: the metric sees it
    }

    @Test
    void aConsumerThatNeverAnswersBecomesAnInboxRetry() {
        ledger = (e, slots) -> new CompletableFuture<>(); // never completes
        assertThrows(UsageInboxHandler.RetryableUsageException.class,
                () -> handler(Duration.ofMillis(150)).handle(delivery(json(usage("a", 1)))));
    }

    @Test
    void anObserverThatThrowsDoesNotTurnACountedMessageIntoARedelivery() throws Exception {
        observer = o -> { throw new IllegalStateException("metrics backend down"); };
        assertInstanceOf(HandlerResult.Ack.class, handler().handle(delivery(json(usage("a", 1)))));
    }
}
