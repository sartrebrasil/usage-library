package com.example.usage.admin.catalog;

import com.example.usage.admin.audit.Audit;
import com.example.usage.admin.support.AdminContext;
import com.example.problem.ApiException;
import com.example.usage.admin.support.Check;
import com.example.usage.admin.support.Page;
import com.example.usage.admin.support.Where;

import com.example.usage.core.model.Limit;
import com.example.usage.core.model.Limit.Enforcement;
import com.example.usage.core.model.Limit.Type;
import com.example.usage.core.model.Metric;
import com.example.usage.core.model.Metric.Mode;
import com.example.usage.core.model.Window;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Versioned plans and their limits. A plan is a version: once a tenant has been on it, its limits are frozen (changing
 * them would change what every subscriber may consume, with no trace), so a change means cloning it into the next version.
 */
public final class PlanAdmin {
    public record PlanView(UUID id, String code, String name, int version, String status, Instant createdAt) {}

    public record PlanDetail(PlanView plan, List<LimitView> limits) {}

    public record LimitView(UUID id, UUID planId, String metric, String type, long value, Long windowSeconds, Integer windowMonths, String enforcement) {}

    /** Creates version 1 of a new code, or the next version of an existing one (empty, without limits). */
    public record CreatePlan(String code, String name) {}

    /** Either field may be null: only what is given changes. */
    public record UpdatePlan(String name, String status) {}

    public record CloneRequest(String name) {}

    /** Exactly one of {@code windowSeconds} / {@code windowMonths} for RATE and QUOTA; none for CONCURRENCY. */
    public record LimitRequest(String metric, String type, Long value, Long windowSeconds, Integer windowMonths, String enforcement) {}

    private static final Set<String> STATUSES = Set.of("ACTIVE", "DEPRECATED");
    private static final String PLAN = "SELECT id, code, name, version, status, created_at FROM {plan}";
    private static final String LIMIT = """
            SELECT pl.id, pl.plan_id, m.code AS metric, pl.limit_type, pl.limit_value, pl.window_seconds, pl.window_months, pl.enforcement
              FROM {plan_limit} pl JOIN {metric} m ON m.id = pl.metric_id
            """;

    private final AdminContext ctx;
    private final Audit audit;

    public PlanAdmin(AdminContext ctx, Audit audit) {
        this.ctx = ctx;
        this.audit = audit;
    }

    public PlanView create(CreatePlan in) {
        if (in == null) throw ApiException.badRequest("FIELD_REQUIRED", "body is required");
        String code = Check.text(in.code(), "code", 60);
        String name = Check.text(in.name(), "name", 200);
        return ctx.inTx(() -> {
            UUID id = insertPlan(code, name);
            PlanView created = plan(id);
            audit.record("CREATE", "plan", id, null, null, created, null);
            return created;
        });
    }

    public PlanView update(UUID id, UpdatePlan in) {
        if (in == null || (in.name() == null && in.status() == null)) throw ApiException.badRequest("NOTHING_TO_UPDATE", "give name or status");
        return ctx.inTx(() -> {
            PlanView before = plan(id);
            ctx.jdbc().sql(ctx.sql("UPDATE {plan} SET name = :name, status = :status WHERE id = :id"))
                    .param("name", in.name() == null ? before.name() : Check.text(in.name(), "name", 200))
                    .param("status", in.status() == null ? before.status() : Check.oneOf(in.status(), STATUSES, "status"))
                    .param("id", id).update();
            PlanView after = plan(id);
            audit.record("UPDATE", "plan", id, null, before, after, null);
            return after;
        });
    }

    /** The next version of the same code, with a copy of the limits: the way to change a plan that already has subscribers. */
    public PlanDetail cloneToNextVersion(UUID id, CloneRequest in) {
        return ctx.inTx(() -> {
            PlanView source = plan(id);
            UUID copy = insertPlan(source.code(), in == null || in.name() == null ? source.name() : Check.text(in.name(), "name", 200));
            ctx.jdbc().sql(ctx.sql("""
                            INSERT INTO {plan_limit} (id, plan_id, metric_id, limit_type, limit_value, window_seconds, window_months, enforcement)
                            SELECT gen_random_uuid(), :copy, metric_id, limit_type, limit_value, window_seconds, window_months, enforcement
                              FROM {plan_limit} WHERE plan_id = :source
                            """)).param("copy", copy).param("source", id).update();
            PlanDetail created = detail(copy);
            audit.record("CLONE", "plan", copy, null, source, created, "cloned from " + source.code() + " v" + source.version());
            return created;
        });
    }

    public PlanDetail detail(UUID id) {
        return new PlanDetail(plan(id), limits(id));
    }

    public Page<PlanView> list(String code, String status, Page.Request page) {
        Where w = new Where().eq("code", code).eq("status", status);
        return Page.of(ctx.jdbc().sql(ctx.sql(PLAN) + w.sql() + " ORDER BY code, version DESC LIMIT :limit OFFSET :offset")
                .params(w.params()).param("limit", page.size() + 1).param("offset", page.offset())
                .query(PlanAdmin::planView).list(), page);
    }

    // ---- limits ------------------------------------------------------------------------------

    public List<LimitView> limits(UUID planId) {
        plan(planId);
        return ctx.jdbc().sql(ctx.sql(LIMIT + " WHERE pl.plan_id = :plan ORDER BY m.code, pl.limit_type, pl.window_seconds, pl.window_months"))
                .param("plan", planId).query(PlanAdmin::limitView).list();
    }

    public LimitView addLimit(UUID planId, LimitRequest in) {
        return ctx.inTx(() -> {
            requireEditable(planId);
            Resolved r = resolve(in);
            UUID id = UUID.randomUUID();
            try {
                ctx.jdbc().sql(ctx.sql("""
                                INSERT INTO {plan_limit} (id, plan_id, metric_id, limit_type, limit_value, window_seconds, window_months, enforcement)
                                VALUES (:id, :plan, :metric, :type, :value, :seconds, :months, :enforcement)
                                """))
                        .param("id", id).param("plan", planId).param("metric", r.metricId()).param("type", r.limit().type().name())
                        .param("value", r.limit().value()).param("seconds", r.seconds(), java.sql.Types.BIGINT)
                        .param("months", r.months(), java.sql.Types.INTEGER).param("enforcement", r.limit().enforcement().name()).update();
            } catch (org.springframework.dao.DuplicateKeyException e) {
                throw ApiException.conflict("LIMIT_EXISTS", "the plan already has a limit of this type and window on " + in.metric());
            }
            LimitView created = limit(id);
            audit.record("CREATE", "plan_limit", id, null, null, created, null);
            return created;
        });
    }

    public LimitView updateLimit(UUID planId, UUID limitId, LimitRequest in) {
        return ctx.inTx(() -> {
            requireEditable(planId);
            LimitView before = limitOf(planId, limitId);
            Resolved r = resolve(in);
            ctx.jdbc().sql(ctx.sql("""
                            UPDATE {plan_limit} SET metric_id = :metric, limit_type = :type, limit_value = :value,
                                   window_seconds = :seconds, window_months = :months, enforcement = :enforcement
                             WHERE id = :id
                            """))
                    .param("metric", r.metricId()).param("type", r.limit().type().name()).param("value", r.limit().value())
                    .param("seconds", r.seconds(), java.sql.Types.BIGINT).param("months", r.months(), java.sql.Types.INTEGER)
                    .param("enforcement", r.limit().enforcement().name()).param("id", limitId).update();
            LimitView after = limit(limitId);
            audit.record("UPDATE", "plan_limit", limitId, null, before, after, null);
            return after;
        });
    }

    public void deleteLimit(UUID planId, UUID limitId) {
        ctx.inTx(() -> {
            requireEditable(planId);
            LimitView before = limitOf(planId, limitId);
            ctx.jdbc().sql(ctx.sql("DELETE FROM {plan_limit} WHERE id = :id")).param("id", limitId).update();
            audit.record("DELETE", "plan_limit", limitId, null, before, null, null);
            return null;
        });
    }

    // ---- internals ---------------------------------------------------------------------------

    private record Resolved(UUID metricId, Limit limit, Long seconds, Integer months) {}

    /** Builds the core {@link Limit} from the request, so the platform's own rules (RATE only on access metrics, only QUOTA may overage...) are the ones enforced. */
    private Resolved resolve(LimitRequest in) {
        if (in == null) throw ApiException.badRequest("FIELD_REQUIRED", "body is required");
        String metricCode = Check.text(in.metric(), "metric", 60);
        Type type = enumOf(Type.class, in.type(), "type");
        Enforcement enforcement = in.enforcement() == null ? Enforcement.REJECT : enumOf(Enforcement.class, in.enforcement(), "enforcement");
        if (in.value() == null) throw ApiException.badRequest("FIELD_REQUIRED", "value is required");
        if (in.windowSeconds() != null && in.windowMonths() != null) {
            throw ApiException.badRequest("INVALID_WINDOW", "give windowSeconds or windowMonths, not both");
        }
        var row = ctx.jdbc().sql(ctx.sql("SELECT id, count_trigger FROM {metric} WHERE code = :code")).param("code", metricCode)
                .query((rs, i) -> new Object[] {rs.getObject("id", UUID.class), rs.getString("count_trigger")}).optional()
                .orElseThrow(() -> ApiException.notFound("METRIC_NOT_FOUND", "metric " + metricCode + " does not exist"));
        Metric metric = new Metric(metricCode, "ON_FINAL_STATUS".equals(row[1]) ? Mode.REPORT_ONLY : Mode.CONSUME_ON_CHECK);
        Limit limit;
        try {
            Window window = in.windowSeconds() != null ? Window.fixed(Duration.ofSeconds(in.windowSeconds()))
                    : in.windowMonths() != null ? Window.months(in.windowMonths()) : null;
            limit = new Limit(metric, type, in.value(), window, enforcement); // refuses what the platform would refuse
        } catch (IllegalArgumentException e) {
            throw ApiException.badRequest("INVALID_LIMIT", e.getMessage());
        }
        return new Resolved((UUID) row[0], limit, in.windowSeconds(), in.windowMonths());
    }

    private static <E extends Enum<E>> E enumOf(Class<E> type, String value, String field) {
        try {
            return Enum.valueOf(type, value == null ? "" : value);
        } catch (IllegalArgumentException e) {
            throw ApiException.badRequest("INVALID_" + field.toUpperCase(), field + " must be one of " + List.of(type.getEnumConstants()));
        }
    }

    private void requireEditable(UUID planId) {
        // locks the plan row: a subscription taking this plan at the same time waits (it takes a shared lock), so it cannot slip in after the check
        ctx.jdbc().sql(ctx.sql("SELECT id FROM {plan} WHERE id = :id FOR UPDATE")).param("id", planId).query(UUID.class).optional()
                .orElseThrow(() -> ApiException.notFound("PLAN_NOT_FOUND", "plan " + planId + " does not exist"));
        boolean used = ctx.jdbc().sql(ctx.sql("SELECT EXISTS (SELECT 1 FROM {tenant_plan} WHERE plan_id = :id)")).param("id", planId)
                .query(Boolean.class).single();
        if (used) throw ApiException.conflict("PLAN_IN_USE", "a tenant is or was on this plan: clone it to the next version to change its limits");
    }

    private UUID insertPlan(String code, String name) {
        UUID id = UUID.randomUUID();
        try {
            ctx.jdbc().sql(ctx.sql("""
                            INSERT INTO {plan} (id, code, name, version)
                            SELECT :id, :code, :name, COALESCE(MAX(version), 0) + 1 FROM {plan} WHERE code = :code
                            """)).param("id", id).param("code", code).param("name", name).update();
        } catch (org.springframework.dao.DuplicateKeyException e) { // two versions of the same code created at the same moment
            throw ApiException.conflict("PLAN_VERSION_CONFLICT", "another version of " + code + " was created at the same time: try again");
        }
        return id;
    }

    private PlanView plan(UUID id) {
        return ctx.jdbc().sql(ctx.sql(PLAN + " WHERE id = :id")).param("id", id).query(PlanAdmin::planView).optional()
                .orElseThrow(() -> ApiException.notFound("PLAN_NOT_FOUND", "plan " + id + " does not exist"));
    }

    private LimitView limit(UUID id) {
        return ctx.jdbc().sql(ctx.sql(LIMIT + " WHERE pl.id = :id")).param("id", id).query(PlanAdmin::limitView).single();
    }

    private LimitView limitOf(UUID planId, UUID limitId) {
        return ctx.jdbc().sql(ctx.sql(LIMIT + " WHERE pl.id = :id AND pl.plan_id = :plan")).param("id", limitId).param("plan", planId)
                .query(PlanAdmin::limitView).optional()
                .orElseThrow(() -> ApiException.notFound("LIMIT_NOT_FOUND", "limit " + limitId + " is not in plan " + planId));
    }

    private static PlanView planView(ResultSet rs, int row) throws SQLException {
        return new PlanView(rs.getObject("id", UUID.class), rs.getString("code"), rs.getString("name"), rs.getInt("version"),
                rs.getString("status"), AdminContext.instant(rs, "created_at"));
    }

    private static LimitView limitView(ResultSet rs, int row) throws SQLException {
        long seconds = rs.getLong("window_seconds");
        Long windowSeconds = rs.wasNull() ? null : seconds;
        int months = rs.getInt("window_months");
        Integer windowMonths = rs.wasNull() ? null : months;
        return new LimitView(rs.getObject("id", UUID.class), rs.getObject("plan_id", UUID.class), rs.getString("metric"),
                rs.getString("limit_type"), rs.getLong("limit_value"), windowSeconds, windowMonths, rs.getString("enforcement"));
    }
}
