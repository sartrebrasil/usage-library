package dev.usageplatform.core;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Single-JVM Ledger and SummaryStore for tests: dedup by eventId, summaries per slot, compensation. */
public final class InMemoryLedger implements Ledger, SummaryStore {
    private static final class Row {
        final TenantId tenant;
        final String metricCode;
        final String windowKey;
        final Period period;
        long total, late, credits;
        boolean closed;
        Instant updated;
        Plan plan = Plan.UNSPECIFIED;

        Row(TenantId tenant, String metricCode, String windowKey, Period period) {
            this.tenant = tenant;
            this.metricCode = metricCode;
            this.windowKey = windowKey;
            this.period = period;
        }

        Summary toSummary() {
            return new Summary(tenant, metricCode, windowKey, period, total, late, credits, closed, plan);
        }
    }

    private final Set<UUID> seen = new HashSet<>();
    private final Map<UUID, UsageEvent> usage = new HashMap<>();
    private final Map<UUID, List<Slot>> slotsOfEvent = new HashMap<>();
    private final Map<UUID, UUID> compensatedBy = new HashMap<>();
    private final Map<String, Row> rows = new HashMap<>();
    private final Clock clock;

    public InMemoryLedger() { this(Clock.systemUTC()); }

    /** The clock stamps row updates, which reconciliation uses to skip rows still in flight. */
    public InMemoryLedger(Clock clock) { this.clock = clock; }

    @Override
    public synchronized CompletionStage<Boolean> record(UsageEvent e, List<Slot> slots) {
        if (!seen.add(e.eventId())) return CompletableFuture.completedFuture(false);
        usage.put(e.eventId(), e);
        slotsOfEvent.put(e.eventId(), List.copyOf(slots));
        for (Slot s : slots) {
            Row r = rows.computeIfAbsent(key(e.tenant(), e.metricCode(), s.windowKey(), s.period().start()),
                    k -> new Row(e.tenant(), e.metricCode(), s.windowKey(), s.period()));
            if (r.closed) {
                r.late += e.quantity();
            } else {
                r.total += e.quantity();
                r.plan = s.plan(); // a closed period keeps the plan it was closed under
            }
            r.updated = clock.instant();
        }
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public synchronized CompletionStage<Optional<UsageEvent>> find(UUID eventId) {
        return CompletableFuture.completedFuture(Optional.ofNullable(usage.get(eventId)));
    }

    @Override
    public synchronized CompletionStage<Compensated> compensate(Compensation c, UsageEvent original) {
        if (seen.contains(c.eventId())) return CompletableFuture.completedFuture(new Compensated(CompensationResult.DUPLICATE, List.of()));
        if (compensatedBy.containsKey(original.eventId())) {
            return CompletableFuture.completedFuture(new Compensated(CompensationResult.ALREADY_COMPENSATED, List.of()));
        }
        seen.add(c.eventId());
        compensatedBy.put(original.eventId(), c.eventId());
        List<Slot> slots = slotsOfEvent.getOrDefault(original.eventId(), List.of());
        for (Slot s : slots) {
            Row r = rows.get(key(original.tenant(), original.metricCode(), s.windowKey(), s.period().start()));
            if (r == null) continue; // shortcut: no row to reverse (limits changed since); the ledger still has both events
            if (r.closed) r.credits += c.quantity();
            else r.total = Math.max(0, r.total - c.quantity());
            r.updated = clock.instant();
        }
        return CompletableFuture.completedFuture(new Compensated(CompensationResult.APPLIED, slots));
    }

    @Override
    public synchronized CompletionStage<Optional<Summary>> find(TenantId tenant, String metricCode, String windowKey, Instant periodStart) {
        return CompletableFuture.completedFuture(Optional.ofNullable(rows.get(key(tenant, metricCode, windowKey, periodStart)))
                .map(Row::toSummary));
    }

    @Override
    public synchronized CompletionStage<List<Summary>> openSettledBefore(Instant updatedBefore) {
        List<Summary> out = new ArrayList<>();
        for (Row r : rows.values()) {
            if (!r.closed && r.updated.isBefore(updatedBefore)) out.add(r.toSummary());
        }
        return CompletableFuture.completedFuture(out);
    }

    @Override
    public synchronized CompletionStage<Integer> closeEndedBefore(Instant cutoff) {
        int n = 0;
        for (Row r : rows.values()) {
            if (!r.closed && !r.period.end().isAfter(cutoff)) {
                r.closed = true;
                n++;
            }
        }
        return CompletableFuture.completedFuture(n);
    }

    private static String key(TenantId t, String metric, String windowKey, Instant start) {
        return t.value() + "|" + metric + "|" + windowKey + "|" + start.toEpochMilli();
    }
}
