package com.example.usage.admin.usage;

import com.example.usage.admin.support.AdminContext;
import com.example.problem.ApiException;
import com.example.usage.admin.support.Page;
import com.example.usage.admin.support.Where;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;

/** Read-only search of what was consumed: the ledger and the per-period summaries billing reads. Nothing here writes. */
public final class UsageQueries {
    public record Summary(String tenantId, String metricCode, String windowKey, Instant periodStart, Instant periodEnd,
                          long total, long lateTotal, long credits, long net, String planCode, int planVersion, String status, Instant updatedAt) {}

    public record Event(UUID eventId, Instant occurredAt, String tenantId, String metricCode, long quantity, String eventType,
                        UUID compensatesId, UUID compensatedBy, String reason, Instant recordedAt) {}

    private static final String EVENT = """
            SELECT e.event_id, e.occurred_at, e.tenant_id, e.metric_code, e.quantity, e.event_type, e.compensates_id,
                   c.compensation_event_id AS compensated_by, e.reason, e.recorded_at
              FROM {usage_event} e LEFT JOIN {usage_compensation} c ON c.compensates_id = e.event_id
            """;

    private final AdminContext ctx;

    public UsageQueries(AdminContext ctx) {
        this.ctx = ctx;
    }

    /** {@code net} is the consumption billing charges: {@code total + lateTotal - credits}. */
    public Page<Summary> summaries(String tenantId, String metric, String windowKey, String status, Instant from, Instant to, Page.Request page) {
        Where w = new Where().eq("tenant_id", tenantId).eq("metric_code", metric).eq("window_key", windowKey).eq("status", status)
                .since("period_start", from).before("period_start", to);
        return Page.of(ctx.jdbc().sql(ctx.sql("""
                        SELECT tenant_id, metric_code, window_key, period_start, period_end, total, late_total, credits,
                               plan_code, plan_version, status, updated_at FROM {usage_summary}""") + w.sql()
                        + " ORDER BY period_start DESC, tenant_id, metric_code, window_key LIMIT :limit OFFSET :offset")
                .params(w.params()).param("limit", page.size() + 1).param("offset", page.offset())
                .query(UsageQueries::summary).list(), page);
    }

    /** The ledger is large: a search needs a tenant or a start time, never the whole table. */
    public Page<Event> events(String tenantId, String metric, String type, Instant from, Instant to, Page.Request page) {
        boolean noTenant = tenantId == null || tenantId.isBlank();
        if (noTenant && from == null) throw ApiException.badRequest("FILTER_REQUIRED", "give tenantId or from: the ledger is too large to list unfiltered");
        Where w = new Where().eq("e.tenant_id", tenantId).eq("e.metric_code", metric).eq("e.event_type", type)
                .since("e.occurred_at", from).before("e.occurred_at", to);
        return Page.of(ctx.jdbc().sql(ctx.sql(EVENT) + w.sql() + " ORDER BY e.occurred_at DESC, e.event_id LIMIT :limit OFFSET :offset")
                .params(w.params()).param("limit", page.size() + 1).param("offset", page.offset())
                .query(UsageQueries::event).list(), page);
    }

    public Event event(UUID id) {
        return ctx.jdbc().sql(ctx.sql(EVENT + " WHERE e.event_id = :id")).param("id", id).query(UsageQueries::event).optional()
                .orElseThrow(() -> ApiException.notFound("EVENT_NOT_FOUND", "event " + id + " does not exist"));
    }

    private static Summary summary(ResultSet rs, int row) throws SQLException {
        long total = rs.getLong("total"), late = rs.getLong("late_total"), credits = rs.getLong("credits");
        return new Summary(rs.getString("tenant_id"), rs.getString("metric_code"), rs.getString("window_key"),
                AdminContext.instant(rs, "period_start"), AdminContext.instant(rs, "period_end"), total, late, credits, total + late - credits,
                rs.getString("plan_code"), rs.getInt("plan_version"), rs.getString("status"), AdminContext.instant(rs, "updated_at"));
    }

    private static Event event(ResultSet rs, int row) throws SQLException {
        return new Event(rs.getObject("event_id", UUID.class), AdminContext.instant(rs, "occurred_at"), rs.getString("tenant_id"),
                rs.getString("metric_code"), rs.getLong("quantity"), rs.getString("event_type"), rs.getObject("compensates_id", UUID.class),
                rs.getObject("compensated_by", UUID.class), rs.getString("reason"), AdminContext.instant(rs, "recorded_at"));
    }
}
