package com.example.usage.admin;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

/**
 * A tenant's exception to one limit of its current plan ("this client gets 5000 instead of 1000"). It hangs on the plan
 * assignment, so it ends when the tenant changes plan.
 */
public final class OverrideAdmin {
    public record View(UUID id, UUID limitId, String metric, String type, Long windowSeconds, Integer windowMonths,
                       long planValue, long value, String reason) {}

    public record Create(UUID limitId, Long value, String reason) {}

    public record Update(Long value, String reason) {}

    private static final String SELECT = """
            SELECT o.id, o.plan_limit_id, m.code AS metric, pl.limit_type, pl.window_seconds, pl.window_months,
                   pl.limit_value AS plan_value, o.limit_value AS value, o.reason
              FROM {tenant_limit_override} o
              JOIN {plan_limit} pl ON pl.id = o.plan_limit_id
              JOIN {metric} m ON m.id = pl.metric_id
              JOIN {tenant_plan} tp ON tp.id = o.tenant_plan_id
            """;

    private final AdminContext ctx;
    private final Audit audit;
    private final TenantAdmin tenants;

    OverrideAdmin(AdminContext ctx, Audit audit, TenantAdmin tenants) {
        this.ctx = ctx;
        this.audit = audit;
        this.tenants = tenants;
    }

    public List<View> list(String tenantId) {
        tenants.get(tenantId);
        return ctx.jdbc().sql(ctx.sql(SELECT + " WHERE tp.tenant_id = :tenant AND tp.valid_to IS NULL ORDER BY m.code, pl.limit_type"))
                .param("tenant", tenantId).query(OverrideAdmin::view).list();
    }

    public View create(String tenantId, Create in) {
        if (in == null || in.limitId() == null) throw AdminException.invalid("FIELD_REQUIRED", "limitId is required");
        long value = value(in.value());
        String reason = Check.text(in.reason(), "reason", 300);
        return ctx.changingTenant(tenantId, () -> {
            tenants.get(tenantId);
            UUID tenantPlan = ctx.jdbc().sql(ctx.sql("""
                            SELECT tp.id FROM {tenant_plan} tp JOIN {plan_limit} pl ON pl.plan_id = tp.plan_id
                             WHERE tp.tenant_id = :tenant AND tp.valid_to IS NULL AND pl.id = :limit
                            """)).param("tenant", tenantId).param("limit", in.limitId()).query(UUID.class).optional()
                    .orElseThrow(() -> AdminException.notFound("LIMIT_NOT_IN_PLAN", "limit " + in.limitId() + " is not a limit of the plan tenant " + tenantId + " is on"));
            UUID id = UUID.randomUUID();
            try {
                ctx.jdbc().sql(ctx.sql("INSERT INTO {tenant_limit_override} (id, tenant_plan_id, plan_limit_id, limit_value, reason) VALUES (:id, :tp, :limit, :value, :reason)"))
                        .param("id", id).param("tp", tenantPlan).param("limit", in.limitId()).param("value", value).param("reason", reason).update();
            } catch (org.springframework.dao.DuplicateKeyException e) {
                throw AdminException.conflict("OVERRIDE_EXISTS", "this limit already has an override: update it");
            }
            View created = get(tenantId, id);
            audit.record("CREATE", "override", id, tenantId, null, created, reason);
            return created;
        });
    }

    public View update(String tenantId, UUID id, Update in) {
        if (in == null || (in.value() == null && in.reason() == null)) throw AdminException.invalid("NOTHING_TO_UPDATE", "give value or reason");
        return ctx.changingTenant(tenantId, () -> {
            View before = get(tenantId, id);
            String reason = in.reason() == null ? before.reason() : Check.text(in.reason(), "reason", 300);
            ctx.jdbc().sql(ctx.sql("UPDATE {tenant_limit_override} SET limit_value = :value, reason = :reason WHERE id = :id"))
                    .param("value", in.value() == null ? before.value() : value(in.value())).param("reason", reason).param("id", id).update();
            View after = get(tenantId, id);
            audit.record("UPDATE", "override", id, tenantId, before, after, reason);
            return after;
        });
    }

    public void delete(String tenantId, UUID id) {
        ctx.changingTenant(tenantId, () -> {
            View before = get(tenantId, id);
            ctx.jdbc().sql(ctx.sql("DELETE FROM {tenant_limit_override} WHERE id = :id")).param("id", id).update();
            audit.record("DELETE", "override", id, tenantId, before, null, before.reason());
            return null;
        });
    }

    private View get(String tenantId, UUID id) {
        return ctx.jdbc().sql(ctx.sql(SELECT + " WHERE o.id = :id AND tp.tenant_id = :tenant AND tp.valid_to IS NULL")).param("id", id).param("tenant", tenantId)
                .query(OverrideAdmin::view).optional()
                .orElseThrow(() -> AdminException.notFound("OVERRIDE_NOT_FOUND", "override " + id + " not found for tenant " + tenantId));
    }

    private static long value(Long v) {
        if (v == null || v < 0) throw AdminException.invalid("INVALID_VALUE", "value must be >= 0");
        return v;
    }

    private static View view(ResultSet rs, int row) throws SQLException {
        long seconds = rs.getLong("window_seconds");
        Long windowSeconds = rs.wasNull() ? null : seconds;
        int months = rs.getInt("window_months");
        Integer windowMonths = rs.wasNull() ? null : months;
        return new View(rs.getObject("id", UUID.class), rs.getObject("plan_limit_id", UUID.class), rs.getString("metric"), rs.getString("limit_type"),
                windowSeconds, windowMonths, rs.getLong("plan_value"), rs.getLong("value"), rs.getString("reason"));
    }
}
