package dev.usageplatform.postgres;

import dev.usageplatform.core.model.Compensation;
import dev.usageplatform.core.model.Period;
import dev.usageplatform.core.model.Plan;
import dev.usageplatform.core.model.TenantId;
import dev.usageplatform.core.model.UsageEvent;
import dev.usageplatform.core.spi.Ledger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import javax.sql.DataSource;

import static dev.usageplatform.postgres.Jdbc.inTransaction;
import static dev.usageplatform.postgres.Jdbc.utc;
import static dev.usageplatform.postgres.Jdbc.query;

/**
 * Ledger on PostgreSQL. One transaction per event: the insert is the dedup (ON CONFLICT DO NOTHING) and,
 * only when the event is new, the same transaction records the slots the event contributed to and adds it to
 * those summaries. Safe under redelivery and concurrent consumers (the summary upsert is a single atomic
 * statement per row). JDBC is blocking, so it runs on the executor you give it (a virtual-thread executor on
 * JDK 21 is the natural choice).
 * <p>
 * Summary rows are always written in (window_key, period_start) order, in every transaction that touches
 * several of them, so two consumers can never wait on each other's rows (no deadlock).
 * <p>
 * A compensation reverses exactly the slots its original was recorded in ({@code usage_event_slot}), not the
 * ones today's policy would compute: changing limits after the fact cannot make a reversal subtract what the
 * original never added. A new event for a CLOSED period does not touch its total: it goes to late_total.
 * shortcut: an event for a period that never had a row and was already closed upstream creates an OPEN
 * row, which the next close run closes; if billing already invoiced that period it must read late rows too.
 */
public final class JdbcLedger implements Ledger {
    private static final Comparator<Slot> LOCK_ORDER = Comparator
            .comparing(Slot::windowKey)
            .thenComparing(s -> s.period().start());

    private static final String INSERT_EVENT = """
            INSERT INTO {usage_event} (event_id, occurred_at, tenant_id, metric_code, quantity)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (event_id, occurred_at) DO NOTHING
            """;

    // the dedup by id alone: usage_event's key includes occurred_at (it is partitioned by it), so a producer that
    // re-stamps the time would slip past it. Inserted first, in the event's own transaction.
    private static final String CLAIM_ID = """
            INSERT INTO {usage_event_id} (event_id) VALUES (?) ON CONFLICT (event_id) DO NOTHING
            """;

    private static final String INSERT_SLOT = """
            INSERT INTO {usage_event_slot} (event_id, window_key, period_start, period_end, plan_code, plan_version)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (event_id, window_key, period_start) DO NOTHING
            """;

    private static final String SELECT_SLOTS = """
            SELECT window_key, period_start, period_end, plan_code, plan_version
              FROM {usage_event_slot} WHERE event_id = ? ORDER BY window_key, period_start
            """;

    private static final String UPSERT_SUMMARY = """
            INSERT INTO {usage_summary} (tenant_id, metric_code, window_key, period_start, period_end, total, plan_code, plan_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (tenant_id, metric_code, window_key, period_start) DO UPDATE SET
                total      = usage_summary.total
                             + CASE WHEN usage_summary.status = 'OPEN' THEN EXCLUDED.total ELSE 0 END,
                late_total = usage_summary.late_total
                             + CASE WHEN usage_summary.status = 'CLOSED' THEN EXCLUDED.total ELSE 0 END,
                plan_code    = CASE WHEN usage_summary.status = 'OPEN' THEN EXCLUDED.plan_code ELSE usage_summary.plan_code END,
                plan_version = CASE WHEN usage_summary.status = 'OPEN' THEN EXCLUDED.plan_version ELSE usage_summary.plan_version END,
                updated_at = now()
            """;

    private static final String FIND = """
            SELECT event_id, occurred_at, tenant_id, metric_code, quantity
              FROM {usage_event} WHERE event_id = ? AND event_type = 'USAGE'
            """;

    private static final String INSERT_COMPENSATION_EVENT = """
            INSERT INTO {usage_event} (event_id, occurred_at, tenant_id, metric_code, quantity, event_type, compensates_id, reason)
            VALUES (?, ?, ?, ?, ?, 'COMPENSATION', ?, ?)
            ON CONFLICT (event_id, occurred_at) DO NOTHING
            """;

    private static final String MARK_COMPENSATED = """
            INSERT INTO {usage_compensation} (compensates_id, compensation_event_id) VALUES (?, ?)
            ON CONFLICT (compensates_id) DO NOTHING
            """;

    // an OPEN period's total goes down (never below zero); a CLOSED period is untouched and gets a credit
    private static final String REVERSE_SUMMARY = """
            UPDATE {usage_summary} SET
                total      = CASE WHEN status = 'OPEN' THEN GREATEST(total - ?, 0) ELSE total END,
                credits    = credits + CASE WHEN status = 'CLOSED' THEN ? ELSE 0 END,
                updated_at = now()
             WHERE tenant_id = ? AND metric_code = ? AND window_key = ? AND period_start = ?
            """;

    private final DataSource dataSource;
    private final Executor executor;
    private final Tables tables;

    public JdbcLedger(DataSource dataSource, Executor executor) {
        this(dataSource, executor, null);
    }

    /** @param schema the schema holding the platform's tables, or null to find them through the search_path. */
    public JdbcLedger(DataSource dataSource, Executor executor, String schema) {
        this.dataSource = dataSource;
        this.executor = executor;
        this.tables = Tables.of(schema);
    }

    @Override
    public CompletionStage<Boolean> record(UsageEvent e, List<Slot> slots) {
        List<Slot> ordered = slots.stream().sorted(LOCK_ORDER).toList();
        return CompletableFuture.supplyAsync(() -> inTransaction(dataSource, "ledger write failed", c -> {
            boolean isNew = insertEvent(c, e);
            if (isNew && !ordered.isEmpty()) {
                insertSlots(c, e, ordered);
                addToSummaries(c, e, ordered);
            }
            return isNew;
        }), executor);
    }

    @Override
    public CompletionStage<Optional<UsageEvent>> find(UUID eventId) {
        return CompletableFuture.supplyAsync(() -> {
            return query(dataSource, tables.sql(FIND), "ledger read failed", ps -> {
                ps.setObject(1, eventId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return Optional.<UsageEvent>empty();
                    return Optional.of(new UsageEvent(eventId, new TenantId(rs.getString("tenant_id")), rs.getString("metric_code"),
                            rs.getLong("quantity"), rs.getObject("occurred_at", OffsetDateTime.class).toInstant()));
                }
            });
        }, executor);
    }

    @Override
    public CompletionStage<Compensated> compensate(Compensation comp, UsageEvent original) {
        // DUPLICATE and ALREADY_COMPENSATED must leave no trace, not even the event row: only APPLIED commits
        return CompletableFuture.supplyAsync(() -> inTransaction(dataSource, "compensation failed",
                c -> applyCompensation(c, comp, original), result -> result.result() == CompensationResult.APPLIED), executor);
    }

    private Compensated applyCompensation(Connection c, Compensation comp, UsageEvent original) throws SQLException {
        if (!claimId(c, comp.eventId())) return new Compensated(CompensationResult.DUPLICATE, List.of());
        try (PreparedStatement ps = c.prepareStatement(tables.sql(INSERT_COMPENSATION_EVENT))) {
            ps.setObject(1, comp.eventId());
            ps.setObject(2, utc(comp.occurredAt()));
            ps.setString(3, comp.tenant().value());
            ps.setString(4, comp.metricCode());
            ps.setLong(5, comp.quantity());
            ps.setObject(6, comp.compensates());
            ps.setString(7, comp.reason());
            if (ps.executeUpdate() == 0) return new Compensated(CompensationResult.DUPLICATE, List.of());
        }
        try (PreparedStatement ps = c.prepareStatement(tables.sql(MARK_COMPENSATED))) {
            ps.setObject(1, comp.compensates());
            ps.setObject(2, comp.eventId());
            if (ps.executeUpdate() == 0) return new Compensated(CompensationResult.ALREADY_COMPENSATED, List.of());
        }
        List<Slot> slots = slotsOf(c, original.eventId()); // already in lock order
        try (PreparedStatement ps = c.prepareStatement(tables.sql(REVERSE_SUMMARY))) {
            for (Slot s : slots) {
                ps.setLong(1, comp.quantity());
                ps.setLong(2, comp.quantity());
                ps.setString(3, original.tenant().value());
                ps.setString(4, original.metricCode());
                ps.setString(5, s.windowKey());
                ps.setObject(6, utc(s.period().start()));
                ps.addBatch();
            }
            ps.executeBatch();
        }
        return new Compensated(CompensationResult.APPLIED, slots);
    }

    private List<Slot> slotsOf(Connection c, UUID eventId) throws SQLException {
        List<Slot> slots = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(tables.sql(SELECT_SLOTS))) {
            ps.setObject(1, eventId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    slots.add(new Slot(rs.getString("window_key"),
                            new Period(rs.getObject("period_start", OffsetDateTime.class).toInstant(),
                                    rs.getObject("period_end", OffsetDateTime.class).toInstant()),
                            new Plan(rs.getString("plan_code"), rs.getInt("plan_version"))));
                }
            }
        }
        return slots;
    }

    /** True if this id was never seen; a second claim of the same id (any timestamp) is a duplicate. */
    private boolean claimId(Connection c, UUID eventId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(tables.sql(CLAIM_ID))) {
            ps.setObject(1, eventId);
            return ps.executeUpdate() == 1;
        }
    }

    private boolean insertEvent(Connection c, UsageEvent e) throws SQLException {
        if (!claimId(c, e.eventId())) return false;
        try (PreparedStatement ps = c.prepareStatement(tables.sql(INSERT_EVENT))) {
            ps.setObject(1, e.eventId());
            ps.setObject(2, utc(e.occurredAt()));
            ps.setString(3, e.tenant().value());
            ps.setString(4, e.metricCode());
            ps.setLong(5, e.quantity());
            return ps.executeUpdate() == 1;
        }
    }

    private void insertSlots(Connection c, UsageEvent e, List<Slot> slots) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(tables.sql(INSERT_SLOT))) {
            for (Slot s : slots) {
                ps.setObject(1, e.eventId());
                ps.setString(2, s.windowKey());
                ps.setObject(3, utc(s.period().start()));
                ps.setObject(4, utc(s.period().end()));
                ps.setString(5, s.plan().code());
                ps.setInt(6, s.plan().version());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void addToSummaries(Connection c, UsageEvent e, List<Slot> slots) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(tables.sql(UPSERT_SUMMARY))) {
            for (Slot s : slots) {
                ps.setString(1, e.tenant().value());
                ps.setString(2, e.metricCode());
                ps.setString(3, s.windowKey());
                ps.setObject(4, utc(s.period().start()));
                ps.setObject(5, utc(s.period().end()));
                ps.setLong(6, e.quantity());
                ps.setString(7, s.plan().code());
                ps.setInt(8, s.plan().version());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }
}
