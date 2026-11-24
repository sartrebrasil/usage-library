package com.example.usage.postgres;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.function.Predicate;

/** The boilerplate every store repeats: UTC timestamps and one transaction around a unit of work. Blocking: run it on the executor. */
final class Jdbc {
    private Jdbc() {}

    interface Work<T> {
        T run(Connection c) throws SQLException;
    }

    interface Query<T> {
        T run(PreparedStatement ps) throws SQLException;
    }

    static OffsetDateTime utc(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    /** Runs one statement on its own connection (a read, or a single autocommitted update); an SQLException becomes IllegalStateException(failure). */
    static <T> T query(DataSource dataSource, String sql, String failure, Query<T> work) {
        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            return work.run(ps);
        } catch (SQLException ex) {
            throw new IllegalStateException(failure, ex);
        }
    }

    /** Commits when the work returns. A failure rolls back and is rethrown (an SQLException as IllegalStateException(failure)). */
    static <T> T inTransaction(DataSource dataSource, String failure, Work<T> work) {
        return inTransaction(dataSource, failure, work, result -> true);
    }

    /** As above, but rolls back, without failing, when {@code commitIf} refuses the result: the work then leaves no trace. */
    static <T> T inTransaction(DataSource dataSource, String failure, Work<T> work, Predicate<T> commitIf) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try {
                T result = work.run(c);
                if (commitIf.test(result)) c.commit();
                else c.rollback();
                return result;
            } catch (SQLException | RuntimeException ex) {
                rollbackQuietly(c, ex);
                throw ex;
            }
        } catch (SQLException ex) {
            throw new IllegalStateException(failure, ex);
        }
    }

    /** A failed rollback must not hide the error that caused it. */
    private static void rollbackQuietly(Connection c, Exception cause) {
        try {
            c.rollback();
        } catch (SQLException rollbackError) {
            cause.addSuppressed(rollbackError);
        }
    }
}
