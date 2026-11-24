package com.example.usage.postgres;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;

import static com.example.usage.postgres.Jdbc.utc;

/**
 * Deletes the bookkeeping that only matters while an event can still arrive or be reversed: the dedup ids
 * ({@code usage_event_id}) and the slots a compensation reverses ({@code usage_event_slot}). Without it both grow
 * for ever. It does not touch {@code usage_event} (drop its partitions instead), {@code usage_summary} (billing reads
 * it) or {@code usage_compensation} (tiny, and it is what keeps an original from being reversed twice).
 * <p>
 * Pick {@code cutoff} older than everything that can still reach the platform: the consumer's maximum event age
 * (90 days by default), the close tolerance, and the longest delay you allow between a usage and its compensation.
 * Past the cutoff a redelivery would be counted again, and a compensation would find no slots to reverse.
 * Blocking; run it from a scheduled job, in batches so no single statement holds locks for long.
 */
public final class JdbcRetention {
    private static final int BATCH = 10_000;
    private static final String PURGE_IDS = """
            DELETE FROM {usage_event_id} WHERE ctid IN (
                SELECT ctid FROM {usage_event_id} WHERE recorded_at < ? LIMIT %d)
            """.formatted(BATCH);
    private static final String PURGE_SLOTS = """
            DELETE FROM {usage_event_slot} WHERE ctid IN (
                SELECT ctid FROM {usage_event_slot} WHERE period_end < ? LIMIT %d)
            """.formatted(BATCH);

    public record Purged(long ids, long slots) {}

    private final DataSource dataSource;
    private final Tables tables;

    public JdbcRetention(DataSource dataSource, String schema) {
        this.dataSource = dataSource;
        this.tables = Tables.of(schema);
    }

    public Purged purgeOlderThan(Instant cutoff) {
        return new Purged(purge(PURGE_IDS, cutoff), purge(PURGE_SLOTS, cutoff));
    }

    private long purge(String sql, Instant cutoff) {
        long total = 0;
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(tables.sql(sql))) {
            c.setAutoCommit(true); // each batch commits on its own
            ps.setObject(1, utc(cutoff));
            int n;
            do {
                n = ps.executeUpdate();
                total += n;
            } while (n == BATCH);
        } catch (SQLException ex) {
            throw new IllegalStateException("retention purge failed", ex);
        }
        return total;
    }
}
