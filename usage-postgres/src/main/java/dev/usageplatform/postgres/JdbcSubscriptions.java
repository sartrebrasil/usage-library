package dev.usageplatform.postgres;

import dev.usageplatform.core.model.TenantId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import javax.sql.DataSource;

import static dev.usageplatform.postgres.Jdbc.inTransaction;
import static dev.usageplatform.postgres.Jdbc.utc;

/**
 * Admin operations on a tenant's subscription and plan. Blocking and transactional on purpose: these are rare
 * management calls, not the request path.
 * <p>
 * Call {@code CachedPolicyStore.invalidate(tenant)} (and publish the invalidation to the other instances)
 * after either operation, or the old policy stays in force until its cache entry expires.
 * shortcut: no cancel/renew yet; to end a subscription set ended_at and plan valid_to by hand.
 */
public final class JdbcSubscriptions {
    private static final String OPEN_SUBSCRIPTION = """
            INSERT INTO {tenant_subscription} (id, tenant_id, cycle_anchor, started_at)
            VALUES (gen_random_uuid(), ?, ?, ?)
            """;
    private static final String CLOSE_PLAN = """
            UPDATE {tenant_plan} SET valid_to = ? WHERE tenant_id = ? AND valid_to IS NULL AND valid_from < ?
            """;
    private static final String OPEN_PLAN = """
            INSERT INTO {tenant_plan} (id, tenant_id, plan_id, valid_from)
            SELECT gen_random_uuid(), ?, p.id, ?
              FROM {plan} p
              JOIN {tenant_subscription} ts ON ts.tenant_id = ? AND ts.ended_at IS NULL
             WHERE p.code = ? AND p.version = ?
            """;

    private final DataSource dataSource;
    private final Tables tables;

    public JdbcSubscriptions(DataSource dataSource) {
        this(dataSource, null);
    }

    /** @param schema the schema holding the platform's tables, or null to find them through the search_path. */
    public JdbcSubscriptions(DataSource dataSource, String schema) {
        this.dataSource = dataSource;
        this.tables = Tables.of(schema);
    }

    /** Starts a subscription whose billing cycle is anchored at {@code startsAt}, on the given plan. */
    public void subscribe(TenantId tenant, String planCode, int planVersion, Instant startsAt) {
        inTransaction(dataSource, "subscription change failed", c -> {
            try (PreparedStatement ps = c.prepareStatement(tables.sql(OPEN_SUBSCRIPTION))) {
                ps.setString(1, tenant.value());
                ps.setObject(2, utc(startsAt));
                ps.setObject(3, utc(startsAt));
                ps.executeUpdate(); // a second open subscription violates the unique index
            }
            openPlan(c, tenant, planCode, planVersion, startsAt);
            return null;
        });
    }

    /**
     * Moves the tenant to another plan from {@code at} on. The subscription, and therefore the cycle and every
     * counter and summary of the running period, is untouched. Overrides of the previous plan end with it.
     * Fails, changing nothing, if there is no plan in force that started before {@code at} or the new plan is unknown.
     */
    public void changePlan(TenantId tenant, String planCode, int planVersion, Instant at) {
        inTransaction(dataSource, "subscription change failed", c -> {
            try (PreparedStatement ps = c.prepareStatement(tables.sql(CLOSE_PLAN))) {
                ps.setObject(1, utc(at));
                ps.setString(2, tenant.value());
                ps.setObject(3, utc(at));
                if (ps.executeUpdate() == 0) throw new IllegalStateException("no plan in force that started before " + at);
            }
            openPlan(c, tenant, planCode, planVersion, at);
            return null;
        });
    }

    private void openPlan(Connection c, TenantId tenant, String planCode, int planVersion, Instant from) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(tables.sql(OPEN_PLAN))) {
            ps.setString(1, tenant.value());
            ps.setObject(2, utc(from));
            ps.setString(3, tenant.value());
            ps.setString(4, planCode);
            ps.setInt(5, planVersion);
            if (ps.executeUpdate() == 0) throw new IllegalArgumentException("unknown plan " + planCode + " v" + planVersion + " or no open subscription");
        }
    }
}
