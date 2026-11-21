package dev.usageplatform.postgres;

import dev.usageplatform.core.Period;
import dev.usageplatform.core.Plan;
import dev.usageplatform.core.SummaryStore;
import dev.usageplatform.core.TenantId;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import static dev.usageplatform.postgres.Jdbc.utc;

/**
 * Reads and closes usage_summary rows. Run {@link #closeEndedBefore} from a scheduled job on a single
 * instance at a time (a database advisory lock or the host's scheduler lock); it is idempotent, so a
 * double run is harmless. Closing skips rows another transaction is writing at that moment instead of waiting
 * for them (waiting could deadlock with a consumer); the next run closes them.
 */
public final class JdbcSummaryStore implements SummaryStore {
    private static final String FIND = """
            SELECT period_start, period_end, total, late_total, credits, status, plan_code, plan_version
              FROM {usage_summary}
             WHERE tenant_id = ? AND metric_code = ? AND window_key = ? AND period_start = ?
            """;
    private static final String OPEN_SETTLED = """
            SELECT tenant_id, metric_code, window_key, period_start, period_end, total, late_total, credits, status,
                   plan_code, plan_version
              FROM {usage_summary}
             WHERE status = 'OPEN' AND updated_at < ?
            """;
    private static final String CLOSE = """
            UPDATE {usage_summary} SET status = 'CLOSED', updated_at = now()
             WHERE (tenant_id, metric_code, window_key, period_start) IN (
                   SELECT tenant_id, metric_code, window_key, period_start FROM {usage_summary}
                    WHERE status = 'OPEN' AND period_end <= ?
                      FOR UPDATE SKIP LOCKED)
            """;

    private final DataSource dataSource;
    private final Executor executor;
    private final Tables tables;

    public JdbcSummaryStore(DataSource dataSource, Executor executor) {
        this(dataSource, executor, null);
    }

    /** @param schema the schema holding the platform's tables, or null to find them through the search_path. */
    public JdbcSummaryStore(DataSource dataSource, Executor executor, String schema) {
        this.dataSource = dataSource;
        this.executor = executor;
        this.tables = Tables.of(schema);
    }

    @Override
    public CompletionStage<Optional<Summary>> find(TenantId tenant, String metricCode, String windowKey, Instant periodStart) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(tables.sql(FIND))) {
                ps.setString(1, tenant.value());
                ps.setString(2, metricCode);
                ps.setString(3, windowKey);
                ps.setObject(4, utc(periodStart));
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return Optional.<Summary>empty();
                    Period period = new Period(rs.getObject("period_start", OffsetDateTime.class).toInstant(),
                            rs.getObject("period_end", OffsetDateTime.class).toInstant());
                    return Optional.of(new Summary(tenant, metricCode, windowKey, period, rs.getLong("total"),
                            rs.getLong("late_total"), rs.getLong("credits"), "CLOSED".equals(rs.getString("status")),
                            new Plan(rs.getString("plan_code"), rs.getInt("plan_version"))));
                }
            } catch (SQLException ex) {
                throw new IllegalStateException("summary read failed", ex);
            }
        }, executor);
    }

    @Override
    public CompletionStage<List<Summary>> openSettledBefore(Instant updatedBefore) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(tables.sql(OPEN_SETTLED))) {
                ps.setObject(1, utc(updatedBefore));
                try (ResultSet rs = ps.executeQuery()) {
                    List<Summary> out = new ArrayList<>();
                    while (rs.next()) {
                        Period period = new Period(rs.getObject("period_start", OffsetDateTime.class).toInstant(),
                                rs.getObject("period_end", OffsetDateTime.class).toInstant());
                        out.add(new Summary(new TenantId(rs.getString("tenant_id")), rs.getString("metric_code"),
                                rs.getString("window_key"), period, rs.getLong("total"), rs.getLong("late_total"),
                                rs.getLong("credits"), "CLOSED".equals(rs.getString("status")),
                                new Plan(rs.getString("plan_code"), rs.getInt("plan_version"))));
                    }
                    return out;
                }
            } catch (SQLException ex) {
                throw new IllegalStateException("summary scan failed", ex);
            }
        }, executor);
    }

    @Override
    public CompletionStage<Integer> closeEndedBefore(Instant cutoff) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(tables.sql(CLOSE))) {
                ps.setObject(1, utc(cutoff));
                return ps.executeUpdate();
            } catch (SQLException ex) {
                throw new IllegalStateException("summary close failed", ex);
            }
        }, executor);
    }
}
