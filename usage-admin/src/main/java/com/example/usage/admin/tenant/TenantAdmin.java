package com.example.usage.admin.tenant;

import com.example.usage.admin.audit.Audit;
import com.example.usage.admin.support.AdminContext;
import com.example.problem.ApiException;
import com.example.usage.admin.support.Check;
import com.example.usage.admin.support.Page;
import com.example.usage.admin.support.Where;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Set;
import java.util.regex.Pattern;

/** Tenants of the platform. Never deleted (history points at them): a tenant is suspended instead. */
public final class TenantAdmin {
    public record View(String id, String name, String status, Instant createdAt) {}

    public record Create(String id, String name) {}

    /** Either field may be null: only what is given changes. */
    public record Update(String name, String status) {}

    private static final Pattern ID = Pattern.compile("[a-z0-9][a-z0-9-]{1,62}");
    private static final Set<String> STATUSES = Set.of("ACTIVE", "SUSPENDED");
    private static final String SELECT = "SELECT id, name, status, created_at FROM {tenant}";

    private final AdminContext ctx;
    private final Audit audit;

    public TenantAdmin(AdminContext ctx, Audit audit) {
        this.ctx = ctx;
        this.audit = audit;
    }

    public View create(Create in) {
        if (in == null || in.id() == null || !ID.matcher(in.id()).matches()) {
            throw ApiException.badRequest("INVALID_TENANT_ID", "id must match " + ID.pattern());
        }
        String name = Check.text(in.name(), "name", 200);
        return ctx.changingTenant(in.id(), () -> {
            try {
                ctx.jdbc().sql(ctx.sql("INSERT INTO {tenant} (id, name) VALUES (:id, :name)")).param("id", in.id()).param("name", name).update();
            } catch (org.springframework.dao.DuplicateKeyException e) {
                throw ApiException.conflict("TENANT_EXISTS", "tenant " + in.id() + " already exists");
            }
            View created = get(in.id());
            audit.record("CREATE", "tenant", created.id(), created.id(), null, created, null);
            return created;
        });
    }

    public View update(String id, Update in) {
        if (in == null || (in.name() == null && in.status() == null)) throw ApiException.badRequest("NOTHING_TO_UPDATE", "give name or status");
        String name = in.name() == null ? null : Check.text(in.name(), "name", 200);
        String status = in.status() == null ? null : Check.oneOf(in.status(), STATUSES, "status");
        return ctx.changingTenant(id, () -> {
            View before = get(id);
            ctx.jdbc().sql(ctx.sql("UPDATE {tenant} SET name = :name, status = :status WHERE id = :id"))
                    .param("name", name == null ? before.name() : name).param("status", status == null ? before.status() : status)
                    .param("id", id).update();
            View after = get(id);
            audit.record("UPDATE", "tenant", id, id, before, after, null);
            return after;
        });
    }

    public View get(String id) {
        return ctx.jdbc().sql(ctx.sql(SELECT + " WHERE id = :id")).param("id", id).query(TenantAdmin::view).optional()
                .orElseThrow(() -> ApiException.notFound("TENANT_NOT_FOUND", "tenant " + id + " does not exist"));
    }

    public Page<View> list(String q, String status, Page.Request page) {
        Where w = new Where().prefix("id", q).eq("status", status);
        return Page.of(ctx.jdbc().sql(ctx.sql(SELECT) + w.sql() + " ORDER BY id LIMIT :limit OFFSET :offset")
                .params(w.params()).param("limit", page.size() + 1).param("offset", page.offset())
                .query(TenantAdmin::view).list(), page);
    }

    private static View view(ResultSet rs, int row) throws SQLException {
        return new View(rs.getString("id"), rs.getString("name"), rs.getString("status"), AdminContext.instant(rs, "created_at"));
    }
}
