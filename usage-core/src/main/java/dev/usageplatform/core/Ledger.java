package dev.usageplatform.core;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * SPI: the durable, deduplicating record of usage (PostgreSQL in production: insert ... on conflict
 * do nothing). This is the source of truth; the counters are derived from it.
 */
public interface Ledger {
    /** One usage_summary row an event contributes to: a quota window and the period containing the event. */
    record Slot(String windowKey, Period period, Plan plan) {}

    enum CompensationResult { APPLIED, DUPLICATE, ALREADY_COMPENSATED }

    /** slots: the slots the original event was recorded in (only when APPLIED), so counters can be lowered in exactly those. */
    record Compensated(CompensationResult result, List<Slot> slots) {}

    /**
     * Records the event and, only if it is new, adds its quantity to every slot, all in one atomic step:
     * a crash can never leave an event recorded but missing from the summaries, or the reverse.
     *
     * @return true if the event is new, false if its eventId was already recorded (slots untouched).
     */
    CompletionStage<Boolean> record(UsageEvent event, List<Slot> slots);

    /** A recorded USAGE event by id (compensations are not returned: they cannot be compensated). */
    default CompletionStage<Optional<UsageEvent>> find(UUID eventId) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("this ledger does not support compensation"));
    }

    /**
     * Atomically records the compensation, marks the original as compensated and reverses the slots the
     * ORIGINAL was recorded in (not the ones today's policy would compute, which may differ): an OPEN period's
     * total goes down (never below zero), a CLOSED period is left untouched and its credits go up.
     * <ul>
     *   <li>DUPLICATE: this compensation's eventId was already recorded (redelivery). Nothing changes.</li>
     *   <li>ALREADY_COMPENSATED: the original was reversed by a different compensation. Nothing changes.</li>
     * </ul>
     */
    default CompletionStage<Compensated> compensate(Compensation compensation, UsageEvent original) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("this ledger does not support compensation"));
    }
}
