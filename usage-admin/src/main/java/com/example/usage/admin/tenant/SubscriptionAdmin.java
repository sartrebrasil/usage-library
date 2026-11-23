package com.example.usage.admin.tenant;

import com.example.usage.admin.audit.Audit;
import com.example.usage.admin.support.AdminContext;
import com.example.usage.admin.support.AdminException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * A tenant's subscription (the billing cycle) and the plan it is on. Changing the plan swaps the limits and leaves the
 * subscription, so counters and summaries of the running period carry over. Every change drops the tenant's cached policy.
 */
public final class SubscriptionAdmin {
    public record Subscription(UUID id, String tenantId, Instant cycleAnchor, Instant startedAt, Instant endedAt) {}

    public record PlanAssignment(UUID id, String planCode, int planVersion, String planName, Instant validFrom, Instant validTo) {}

    /** {@code startsAt} (default now) is also the cycle anchor: periods start on its day and time, in UTC. */
    public record Subscribe(String planCode, Integer planVersion, Instant startsAt) {}

    /** {@code at} defaults to now. */
    public record ChangePlan(String planCode, Integer planVersion, Instant at) {}

    public record End(Instant at) {}

    private static final String SUBSCRIPTION = "SELECT id, tenant_id, cycle_anchor, started_at, ended_at FROM {tenant_subscription}";

    private final AdminContext ctx;
    private final Audit audit;
    private final TenantAdmin tenants;

    public SubscriptionAdmin(AdminContext ctx, Audit audit, TenantAdmin tenants) {
        this.ctx = ctx;
        this.audit = audit;
        this.tenants = tenants;
    }

    public Subscription subscribe(String tenantId, Subscribe in) {
        if (in == null) throw AdminException.invalid("FIELD_REQUIRED", "body is required");
        Instant at = in.startsAt() == null ? ctx.clock().instant() : in.startsAt();
        return ctx.changingTenant(tenantId, () -> {
            tenants.get(tenantId);
            UUID plan = activePlan(in.planCode(), in.planVersion());
            UUID id = UUID.randomUUID();
            try {
                ctx.jdbc().sql(ctx.sql("INSERT INTO {tenant_subscription} (id, tenant_id, cycle_anchor, started_at) VALUES (:id, :tenant, :at, :at)"))
                        .param("id", id).param("tenant", tenantId).param("at", AdminContext.ts(at)).update();
            } catch (org.springframework.dao.DuplicateKeyException e) {
                throw AdminException.conflict("ALREADY_SUBSCRIBED", "tenant " + tenantId + " already has an open subscription");
            }
            openPlan(tenantId, plan, at);
            Subscription created = subscription(id);
            audit.record("SUBSCRIBE", "subscription", id, tenantId, null, created, in.planCode() + " v" + in.planVersion());
            return created;
        });
    }

    public PlanAssignment changePlan(String tenantId, ChangePlan in) {
        if (in == null) throw AdminException.invalid("FIELD_REQUIRED", "body is required");
        Instant at = notInTheFuture(in.at());
        return ctx.changingTenant(tenantId, () -> {
            tenants.get(tenantId);
            UUID plan = activePlan(in.planCode(), in.planVersion());
            PlanAssignment before = current(tenantId);
            int closed = ctx.jdbc().sql(ctx.sql("UPDATE {tenant_plan} SET valid_to = :at WHERE tenant_id = :tenant AND valid_to IS NULL AND valid_from < :at"))
                    .param("at", AdminContext.ts(at)).param("tenant", tenantId).update();
            if (closed == 0) throw AdminException.conflict("NO_PLAN_IN_FORCE", "tenant " + tenantId + " has no plan that started before " + at);
            openPlan(tenantId, plan, at);
            PlanAssignment after = current(tenantId);
            audit.record("CHANGE_PLAN", "tenant_plan", after.id(), tenantId, before, after, null);
            return after;
        });
    }

    public Subscription end(String tenantId, End in) {
        Instant at = notInTheFuture(in == null ? null : in.at());
        return ctx.changingTenant(tenantId, () -> {
            tenants.get(tenantId);
            Subscription before = open(tenantId);
            if (!at.isAfter(before.startedAt())) throw AdminException.invalid("INVALID_END", "the end must be after the start " + before.startedAt());
            // the plan in force must end too, so it must have started before the end: otherwise it would stay open for good and block the next subscription
            Instant planFrom = current(tenantId).validFrom();
            if (!at.isAfter(planFrom)) throw AdminException.invalid("INVALID_END", "the end must be after the current plan started (" + planFrom + ")");
            ctx.jdbc().sql(ctx.sql("UPDATE {tenant_subscription} SET ended_at = :at WHERE id = :id")).param("at", AdminContext.ts(at)).param("id", before.id()).update();
            ctx.jdbc().sql(ctx.sql("UPDATE {tenant_plan} SET valid_to = :at WHERE tenant_id = :tenant AND valid_to IS NULL"))
                    .param("at", AdminContext.ts(at)).param("tenant", tenantId).update();
            Subscription after = subscription(before.id());
            audit.record("END_SUBSCRIPTION", "subscription", before.id(), tenantId, before, after, null);
            return after;
        });
    }

    public List<Subscription> subscriptions(String tenantId) {
        tenants.get(tenantId);
        return ctx.jdbc().sql(ctx.sql(SUBSCRIPTION + " WHERE tenant_id = :tenant ORDER BY started_at DESC")).param("tenant", tenantId)
                .query(SubscriptionAdmin::subscription).list();
    }

    /** Every plan the tenant has been on, newest first. */
    public List<PlanAssignment> planHistory(String tenantId) {
        tenants.get(tenantId);
        return ctx.jdbc().sql(ctx.sql("""
                        SELECT tp.id, p.code, p.version, p.name, tp.valid_from, tp.valid_to
                          FROM {tenant_plan} tp JOIN {plan} p ON p.id = tp.plan_id
                         WHERE tp.tenant_id = :tenant ORDER BY tp.valid_from DESC
                        """)).param("tenant", tenantId).query(SubscriptionAdmin::assignment).list();
    }

    // ---- internals ---------------------------------------------------------------------------

    private UUID activePlan(String code, Integer version) {
        if (code == null || code.isBlank() || version == null) throw AdminException.invalid("FIELD_REQUIRED", "planCode and planVersion are required");
        var row = ctx.jdbc().sql(ctx.sql("SELECT id, status FROM {plan} WHERE code = :code AND version = :version FOR SHARE")).param("code", code).param("version", version)
                .query((rs, i) -> new Object[] {rs.getObject("id", UUID.class), rs.getString("status")}).optional()
                .orElseThrow(() -> AdminException.notFound("PLAN_NOT_FOUND", "plan " + code + " v" + version + " does not exist"));
        if (!"ACTIVE".equals(row[1])) throw AdminException.conflict("PLAN_DEPRECATED", "plan " + code + " v" + version + " is deprecated: pick a newer version");
        return (UUID) row[0];
    }

    private void openPlan(String tenantId, UUID plan, Instant from) {
        try {
            ctx.jdbc().sql(ctx.sql("INSERT INTO {tenant_plan} (id, tenant_id, plan_id, valid_from) VALUES (:id, :tenant, :plan, :from)"))
                    .param("id", UUID.randomUUID()).param("tenant", tenantId).param("plan", plan).param("from", AdminContext.ts(from)).update();
        } catch (org.springframework.dao.DuplicateKeyException e) {
            throw AdminException.conflict("PLAN_ALREADY_OPEN", "tenant " + tenantId + " still has a plan in force: change it instead of subscribing");
        }
    }

    /**
     * The moment a change takes effect: now by default, never later. A plan that starts in the future would become "the
     * open one" while the old plan is still the one the engine reads, and overrides would attach to the wrong plan.
     */
    private Instant notInTheFuture(Instant at) {
        Instant now = ctx.clock().instant();
        if (at == null) return now;
        if (at.isAfter(now.plusSeconds(5))) throw AdminException.invalid("FUTURE_NOT_SUPPORTED", "the change must take effect now or in the past, not at " + at);
        return at;
    }

    private Subscription open(String tenantId) {
        return ctx.jdbc().sql(ctx.sql(SUBSCRIPTION + " WHERE tenant_id = :tenant AND ended_at IS NULL")).param("tenant", tenantId)
                .query(SubscriptionAdmin::subscription).optional()
                .orElseThrow(() -> AdminException.notFound("NO_OPEN_SUBSCRIPTION", "tenant " + tenantId + " has no open subscription"));
    }

    private PlanAssignment current(String tenantId) {
        return ctx.jdbc().sql(ctx.sql("""
                        SELECT tp.id, p.code, p.version, p.name, tp.valid_from, tp.valid_to
                          FROM {tenant_plan} tp JOIN {plan} p ON p.id = tp.plan_id
                         WHERE tp.tenant_id = :tenant AND tp.valid_to IS NULL
                        """)).param("tenant", tenantId).query(SubscriptionAdmin::assignment).optional()
                .orElseThrow(() -> AdminException.conflict("NO_PLAN_IN_FORCE", "tenant " + tenantId + " has no plan in force"));
    }

    private Subscription subscription(UUID id) {
        return ctx.jdbc().sql(ctx.sql(SUBSCRIPTION + " WHERE id = :id")).param("id", id).query(SubscriptionAdmin::subscription).single();
    }

    private static Subscription subscription(ResultSet rs, int row) throws SQLException {
        return new Subscription(rs.getObject("id", UUID.class), rs.getString("tenant_id"), AdminContext.instant(rs, "cycle_anchor"),
                AdminContext.instant(rs, "started_at"), AdminContext.instant(rs, "ended_at"));
    }

    private static PlanAssignment assignment(ResultSet rs, int row) throws SQLException {
        return new PlanAssignment(rs.getObject("id", UUID.class), rs.getString("code"), rs.getInt("version"), rs.getString("name"),
                AdminContext.instant(rs, "valid_from"), AdminContext.instant(rs, "valid_to"));
    }
}
