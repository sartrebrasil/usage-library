package dev.usageplatform.postgres;

import dev.usageplatform.core.Limit;
import dev.usageplatform.core.Limit.Enforcement;
import dev.usageplatform.core.Limit.Type;
import dev.usageplatform.core.Metric;
import dev.usageplatform.core.Metric.Mode;
import dev.usageplatform.core.Plan;
import dev.usageplatform.core.Policy;
import dev.usageplatform.core.PolicyStore;
import dev.usageplatform.core.TenantId;
import dev.usageplatform.core.Window;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import static dev.usageplatform.postgres.Jdbc.utc;
import static dev.usageplatform.postgres.Jdbc.query;

/**
 * Reads the tenant's current plan, applies overrides and builds the core {@link Policy}. The policy's anchor is
 * the cycle anchor of the tenant's subscription, so a plan change never moves the periods.
 * Empty when the tenant is unknown, suspended, or has no subscription and plan in force (the engine then
 * answers no_policy).
 * Wrap it in a CachedPolicyStore: this runs a query per call.
 * shortcut: invalid configuration (e.g. a RATE limit on a result metric) fails the load, so the engine
 * applies its fail mode; add validation to the admin side before the dashboard exists.
 */
public final class JdbcPolicyStore implements PolicyStore {
    private static final String QUERY = """
            SELECT ts.cycle_anchor, p.code AS plan_code, p.version AS plan_version,
                   m.code, m.count_trigger,
                   pl.limit_type, pl.window_seconds, pl.window_months, pl.enforcement,
                   COALESCE(o.limit_value, pl.limit_value) AS limit_value
              FROM {tenant} t
              JOIN {tenant_subscription} ts ON ts.tenant_id = t.id
                                         AND ts.started_at <= ? AND (ts.ended_at IS NULL OR ts.ended_at > ?)
              JOIN {tenant_plan} tp ON tp.tenant_id = t.id
                                 AND tp.valid_from <= ? AND (tp.valid_to IS NULL OR tp.valid_to > ?)
              JOIN {plan} p ON p.id = tp.plan_id
              LEFT JOIN {plan_limit} pl ON pl.plan_id = tp.plan_id
              LEFT JOIN {metric} m ON m.id = pl.metric_id
              LEFT JOIN {tenant_limit_override} o ON o.tenant_plan_id = tp.id AND o.plan_limit_id = pl.id
             WHERE t.id = ? AND t.status = 'ACTIVE'
            """;

    private final DataSource dataSource;
    private final Executor executor;
    private final Clock clock;
    private final Tables tables;

    public JdbcPolicyStore(DataSource dataSource, Executor executor, Clock clock) {
        this(dataSource, executor, clock, null);
    }

    /** @param schema the schema holding the platform's tables, or null to find them through the search_path. */
    public JdbcPolicyStore(DataSource dataSource, Executor executor, Clock clock, String schema) {
        this.dataSource = dataSource;
        this.executor = executor;
        this.clock = clock;
        this.tables = Tables.of(schema);
    }

    @Override
    public CompletionStage<Optional<Policy>> policyFor(TenantId tenant) {
        return CompletableFuture.supplyAsync(() -> load(tenant), executor);
    }

    private Optional<Policy> load(TenantId tenant) {
        OffsetDateTime now = utc(clock.instant());
        return query(dataSource, tables.sql(QUERY), "policy load failed", ps -> {
            ps.setObject(1, now);
            ps.setObject(2, now);
            ps.setObject(3, now);
            ps.setObject(4, now);
            ps.setString(5, tenant.value());
            try (ResultSet rs = ps.executeQuery()) {
                Instant anchor = null;
                Plan plan = null;
                List<Limit> limits = new ArrayList<>();
                while (rs.next()) {
                    anchor = rs.getObject("cycle_anchor", OffsetDateTime.class).toInstant();
                    plan = new Plan(rs.getString("plan_code"), rs.getInt("plan_version"));
                    String code = rs.getString("code");
                    if (code != null) limits.add(toLimit(rs, code));
                }
                return anchor == null ? Optional.empty() : Optional.of(new Policy(plan, anchor, List.copyOf(limits)));
            }
        });
    }

    private static Limit toLimit(ResultSet rs, String code) throws SQLException {
        Mode mode = "ON_FINAL_STATUS".equals(rs.getString("count_trigger")) ? Mode.REPORT_ONLY : Mode.CONSUME_ON_CHECK;
        long seconds = rs.getLong("window_seconds");
        Window window = rs.wasNull() ? null : Window.fixed(Duration.ofSeconds(seconds));
        int months = rs.getInt("window_months");
        if (!rs.wasNull()) window = Window.months(months);
        return new Limit(new Metric(code, mode), Type.valueOf(rs.getString("limit_type")),
                rs.getLong("limit_value"), window, Enforcement.valueOf(rs.getString("enforcement")));
    }
}
