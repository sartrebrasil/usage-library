package dev.usageplatform.consumer;

import dev.usageplatform.consumer.UsageEventJson.InvalidEventException;
import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.UsageEngine.Compensate;
import dev.usageplatform.core.UsageEngine.Ingest;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import static dev.usageplatform.core.Stages.done;

/**
 * The central consumer, broker-agnostic: the host passes each message body and acts on the outcome
 * (commit / redeliver later / publish to the dead-letter topic). It never throws and never loses an event
 * silently: every message ends in exactly one {@link Disposition}.
 * <p>
 * A COMPENSATION whose original has not arrived yet is answered RETRY ("original_not_found"), so it is held and
 * redelivered instead of dropped; give the retry a limit that ends in the dead-letter topic.
 * <p>
 * Partition the topic by tenantId so one consumer owns a tenant's counters at a time; correctness does
 * not depend on it, because the ledger deduplicates.
 */
public final class UsageEventConsumer {
    public enum Disposition {
        /** Counted, or already counted. Commit the message. */
        ACK,
        /** Infrastructure failure before the event was recorded. Redeliver later; it is safe to repeat. */
        RETRY,
        /** The message can never be counted as sent. Publish to the dead-letter topic and alert. */
        DEAD_LETTER
    }

    /** The reason of an ACK whose ledger write succeeded but whose counters did not move: reconciliation repairs it. */
    public static final String COUNTER_DRIFT_REASON = "counter_drift";

    /** reason is a short stable code the host should use as a metric tag; {@link #COUNTER_DRIFT_REASON} is an ACK that needs reconciliation. */
    public record Outcome(Disposition disposition, String reason) {}

    private final UsageEngine engine;
    private final Predicate<String> knownMetric;
    private final Clock clock;
    private final Duration maxFuture;
    private final Duration maxPast;

    /**
     * @param maxFuture how far ahead of now an occurredAt may be (clock skew), beyond it the event is dead-lettered
     * @param maxPast   how old an event may be, beyond it the event is dead-lettered instead of landing in a closed period
     */
    public UsageEventConsumer(UsageEngine engine, Predicate<String> knownMetric, Clock clock,
                              Duration maxFuture, Duration maxPast) {
        this.engine = engine;
        this.knownMetric = knownMetric;
        this.clock = clock;
        this.maxFuture = maxFuture;
        this.maxPast = maxPast;
    }

    public CompletionStage<Outcome> handle(String messageBody) {
        UsageEventJson.Parsed parsed;
        try {
            parsed = UsageEventJson.parse(messageBody);
        } catch (InvalidEventException e) {
            return done(dead(e.reason()));
        }
        String metric = parsed instanceof UsageEventJson.Parsed.Usage u ? u.event().metricCode()
                : ((UsageEventJson.Parsed.Reversal) parsed).compensation().metricCode();
        Instant at = parsed instanceof UsageEventJson.Parsed.Usage u ? u.event().occurredAt()
                : ((UsageEventJson.Parsed.Reversal) parsed).compensation().occurredAt();
        if (!knownMetric.test(metric)) return done(dead("unknown_metric"));
        Instant now = clock.instant();
        if (at.isAfter(now.plus(maxFuture)) || at.isBefore(now.minus(maxPast))) {
            return done(dead("occurred_at_out_of_range"));
        }
        CompletionStage<Outcome> outcome = parsed instanceof UsageEventJson.Parsed.Usage u
                ? engine.ingest(u.event()).thenApply(UsageEventConsumer::outcomeFor)
                : engine.compensate(((UsageEventJson.Parsed.Reversal) parsed).compensation()).thenApply(UsageEventConsumer::outcomeFor);
        return outcome.exceptionally(UsageEventConsumer::failure);
    }

    /**
     * A database that refused the data itself (SQLSTATE class 22 data exception, 23 integrity violation) will
     * refuse it again on every redelivery, and would dead-letter the whole partition behind it: dead-letter
     * this one message. Everything else (outage, deadlock 40P01, serialization failure 40001) is transient.
     */
    private static Outcome failure(Throwable error) {
        int depth = 0;
        for (Throwable t = error; t != null && depth < 10; t = t.getCause(), depth++) {
            if (t instanceof SQLException sql && sql.getSQLState() != null
                    && (sql.getSQLState().startsWith("22") || sql.getSQLState().startsWith("23"))) {
                return dead("rejected_by_database");
            }
        }
        return new Outcome(Disposition.RETRY, "ledger_unavailable");
    }

    private static Outcome outcomeFor(Ingest result) {
        return switch (result) {
            case APPLIED -> new Outcome(Disposition.ACK, "applied");
            case DUPLICATE -> new Outcome(Disposition.ACK, "duplicate");
            case COUNTER_DRIFT -> new Outcome(Disposition.ACK, COUNTER_DRIFT_REASON);
        };
    }

    private static Outcome outcomeFor(Compensate result) {
        return switch (result) {
            case APPLIED -> new Outcome(Disposition.ACK, "compensated");
            case DUPLICATE -> new Outcome(Disposition.ACK, "duplicate");
            case COUNTER_DRIFT -> new Outcome(Disposition.ACK, COUNTER_DRIFT_REASON);
            // the usage it reverses has not arrived yet: hold the message and redeliver (the broker's retry limit decides when to give up)
            case ORIGINAL_NOT_FOUND -> new Outcome(Disposition.RETRY, "original_not_found");
            case ALREADY_COMPENSATED -> dead("already_compensated");
            case INVALID -> dead("invalid_compensation");
        };
    }

    private static Outcome dead(String reason) {
        return new Outcome(Disposition.DEAD_LETTER, reason);
    }

}
