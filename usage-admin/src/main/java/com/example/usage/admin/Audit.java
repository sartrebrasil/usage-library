package com.example.usage.admin;

import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

/** The append-only trail of every change made through the API. */
public final class Audit {
    public record Entry(UUID id, Instant occurredAt, String actor, String action, String entityType, String entityId,
                        String tenantId, JsonNode before, JsonNode after, String reason) {}

    private final AdminContext ctx;
    private final JsonMapper json;
    private final Supplier<String> actor;

    Audit(AdminContext ctx, JsonMapper json, Supplier<String> actor) {
        this.ctx = ctx;
        this.json = json;
        this.actor = actor;
    }

    /** Call inside the transaction of the change, so either both are recorded or neither. */
    void record(String action, String entityType, Object entityId, String tenantId, Object before, Object after, String reason) {
        ctx.jdbc().sql(ctx.sql("""
                        INSERT INTO {usage_audit} (id, actor, action, entity_type, entity_id, tenant_id, before_state, after_state, reason)
                        VALUES (:id, :actor, :action, :type, :entity, :tenant, CAST(:before AS jsonb), CAST(:after AS jsonb), :reason)
                        """))
                .param("id", UUID.randomUUID()).param("actor", actor.get()).param("action", action)
                .param("type", entityType).param("entity", String.valueOf(entityId))
                .param("tenant", tenantId, java.sql.Types.VARCHAR)
                .param("before", before == null ? null : json.writeValueAsString(before), java.sql.Types.VARCHAR)
                .param("after", after == null ? null : json.writeValueAsString(after), java.sql.Types.VARCHAR)
                .param("reason", reason, java.sql.Types.VARCHAR)
                .update();
    }

    /** Whether a change to this entity was already recorded with this action: lets a retry complete a trail that was cut short. */
    boolean recorded(String action, String entityType, Object entityId) {
        return ctx.jdbc().sql(ctx.sql("SELECT EXISTS (SELECT 1 FROM {usage_audit} WHERE action = :action AND entity_type = :type AND entity_id = :entity)"))
                .param("action", action).param("type", entityType).param("entity", String.valueOf(entityId)).query(Boolean.class).single();
    }

    public Page<Entry> search(String entityType, String entityId, String tenantId, String actorName, Instant from, Instant to, Page.Request page) {
        Where w = new Where();
        w.eq("entity_type", entityType).eq("entity_id", entityId).eq("tenant_id", tenantId).eq("actor", actorName)
                .since("occurred_at", from).before("occurred_at", to);
        return Page.of(ctx.jdbc().sql(ctx.sql("""
                        SELECT id, occurred_at, actor, action, entity_type, entity_id, tenant_id,
                               before_state::text AS before_state, after_state::text AS after_state, reason
                          FROM {usage_audit}""") + w.sql() + " ORDER BY occurred_at DESC, id LIMIT :limit OFFSET :offset")
                .params(w.params()).param("limit", page.size() + 1).param("offset", page.offset())
                .query(this::entry).list(), page);
    }

    private Entry entry(ResultSet rs, int row) throws SQLException {
        return new Entry(rs.getObject("id", UUID.class), AdminContext.instant(rs, "occurred_at"), rs.getString("actor"),
                rs.getString("action"), rs.getString("entity_type"), rs.getString("entity_id"), rs.getString("tenant_id"),
                parse(rs.getString("before_state")), parse(rs.getString("after_state")), rs.getString("reason"));
    }

    private JsonNode parse(String text) {
        return text == null ? null : json.readTree(text);
    }
}
