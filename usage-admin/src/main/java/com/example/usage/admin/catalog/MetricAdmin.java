package com.example.usage.admin.catalog;

import com.example.usage.admin.audit.Audit;
import com.example.usage.admin.support.AdminContext;
import com.example.problem.ApiException;
import com.example.usage.admin.support.Check;
import com.example.usage.admin.support.Page;
import com.example.usage.admin.support.Where;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * What is measured. The {@code code} never changes, and neither does {@code countTrigger} once the metric has
 * recorded usage: it decides who moves the counter (the check or the ingest), so flipping it would count usage twice or never.
 */
public final class MetricAdmin {
    public record View(UUID id, String code, String description, String unit, String countTrigger) {}

    public record Create(String code, String description, String unit, String countTrigger) {}

    /** Any field may be null: only what is given changes. */
    public record Update(String description, String unit, String countTrigger) {}

    private static final Pattern CODE = Pattern.compile("[A-Z][A-Z0-9_]{1,59}");
    private static final Set<String> TRIGGERS = Set.of("ON_REQUEST", "ON_FINAL_STATUS");
    private static final String SELECT = "SELECT id, code, description, unit, count_trigger FROM {metric}";

    private final AdminContext ctx;
    private final Audit audit;

    public MetricAdmin(AdminContext ctx, Audit audit) {
        this.ctx = ctx;
        this.audit = audit;
    }

    public View create(Create in) {
        if (in == null || in.code() == null || !CODE.matcher(in.code()).matches()) {
            throw ApiException.badRequest("INVALID_METRIC_CODE", "code must match " + CODE.pattern());
        }
        String unit = Check.text(in.unit(), "unit", 30);
        String description = Check.optionalText(in.description(), "description", 200);
        String trigger = in.countTrigger() == null ? "ON_REQUEST" : Check.oneOf(in.countTrigger(), TRIGGERS, "countTrigger");
        return ctx.inTx(() -> {
            UUID id = UUID.randomUUID();
            try {
                ctx.jdbc().sql(ctx.sql("INSERT INTO {metric} (id, code, description, unit, count_trigger) VALUES (:id, :code, :description, :unit, :trigger)"))
                        .param("id", id).param("code", in.code()).param("description", description, java.sql.Types.VARCHAR)
                        .param("unit", unit).param("trigger", trigger).update();
            } catch (org.springframework.dao.DuplicateKeyException e) {
                throw ApiException.conflict("METRIC_EXISTS", "metric " + in.code() + " already exists");
            }
            View created = get(id);
            audit.record("CREATE", "metric", id, null, null, created, null);
            return created;
        });
    }

    public View update(UUID id, Update in) {
        if (in == null || (in.description() == null && in.unit() == null && in.countTrigger() == null)) {
            throw ApiException.badRequest("NOTHING_TO_UPDATE", "give description, unit or countTrigger");
        }
        return ctx.inTx(() -> {
            View before = get(id);
            String trigger = in.countTrigger() == null ? before.countTrigger() : Check.oneOf(in.countTrigger(), TRIGGERS, "countTrigger");
            if (!trigger.equals(before.countTrigger()) && hasUsage(before.code())) {
                throw ApiException.conflict("METRIC_HAS_USAGE", "countTrigger cannot change: usage was already recorded for " + before.code());
            }
            ctx.jdbc().sql(ctx.sql("UPDATE {metric} SET description = :description, unit = :unit, count_trigger = :trigger WHERE id = :id"))
                    .param("description", in.description() == null ? before.description() : Check.text(in.description(), "description", 200), java.sql.Types.VARCHAR)
                    .param("unit", in.unit() == null ? before.unit() : Check.text(in.unit(), "unit", 30))
                    .param("trigger", trigger).param("id", id).update();
            View after = get(id);
            audit.record("UPDATE", "metric", id, null, before, after, null);
            return after;
        });
    }

    public View get(UUID id) {
        return ctx.jdbc().sql(ctx.sql(SELECT + " WHERE id = :id")).param("id", id).query(MetricAdmin::view).optional()
                .orElseThrow(() -> ApiException.notFound("METRIC_NOT_FOUND", "metric " + id + " does not exist"));
    }

    public Page<View> list(String q, Page.Request page) {
        Where w = new Where().prefix("code", q);
        return Page.of(ctx.jdbc().sql(ctx.sql(SELECT) + w.sql() + " ORDER BY code LIMIT :limit OFFSET :offset")
                .params(w.params()).param("limit", page.size() + 1).param("offset", page.offset())
                .query(MetricAdmin::view).list(), page);
    }

    /** EXISTS stops at the first row; it only scans the whole ledger when the metric truly has no usage. */
    private boolean hasUsage(String code) {
        return ctx.jdbc().sql(ctx.sql("SELECT EXISTS (SELECT 1 FROM {usage_event} WHERE metric_code = :code)")).param("code", code)
                .query(Boolean.class).single();
    }

    private static View view(ResultSet rs, int row) throws SQLException {
        return new View(rs.getObject("id", UUID.class), rs.getString("code"), rs.getString("description"), rs.getString("unit"), rs.getString("count_trigger"));
    }
}
