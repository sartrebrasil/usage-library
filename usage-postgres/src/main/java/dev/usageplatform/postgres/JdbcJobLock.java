package dev.usageplatform.postgres;

import dev.usageplatform.core.JobLock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * JobLock on a PostgreSQL transaction-level advisory lock: one connection stays in an open transaction for
 * as long as the job runs and the lock vanishes with it, on commit, rollback, or when the process or the
 * connection dies, so a crashed instance can never leave the job locked. The lock is transaction-scoped
 * (not session-scoped) so a pooled connection can never go back to the pool still holding it.
 * <p>
 * The job itself uses other connections; this one only holds the lock, so size the pool for one extra. The
 * lock lives in an open transaction, so an {@code idle_in_transaction_session_timeout} shorter than the job
 * would end it early and let a second instance start: keep that timeout off, or longer than any job.
 * shortcut: lock keys are a 32-bit hash of the name inside one fixed namespace; two names that collide would
 * exclude each other, harmless for the few jobs there are.
 */
public final class JdbcJobLock implements JobLock {
    private static final int NAMESPACE = 0x75736167; // "usag"

    private final DataSource dataSource;
    private final Executor executor;

    public JdbcJobLock(DataSource dataSource, Executor executor) {
        this.dataSource = dataSource;
        this.executor = executor;
    }

    @Override
    public <T> CompletionStage<Optional<T>> runIfAcquired(String name, Supplier<CompletionStage<T>> job) {
        return CompletableFuture.supplyAsync(() -> tryLock(name), executor).thenCompose(held -> {
            if (held == null) return CompletableFuture.completedFuture(Optional.<T>empty());
            CompletableFuture<Optional<T>> out = new CompletableFuture<>();
            CompletionStage<T> running;
            try {
                running = job.get();
                if (running == null) running = CompletableFuture.failedFuture(new IllegalStateException("the job returned a null stage"));
            } catch (RuntimeException e) {
                running = CompletableFuture.failedFuture(e);
            }
            running.whenComplete((result, ex) -> {
                // rollback and close block: never on the thread that completed the job (it may be an event loop)
                Runnable finish = () -> {
                    release(held);
                    if (ex != null) out.completeExceptionally(ex);
                    else if (result == null) out.completeExceptionally(new IllegalStateException("the job returned null"));
                    else out.complete(Optional.of(result));
                };
                try {
                    executor.execute(finish);
                } catch (RuntimeException rejected) { // executor shut down: release anyway, the lock must not stay held
                    finish.run();
                }
            });
            return out;
        });
    }

    /** @return the connection holding the lock, or null when someone else holds it. */
    private Connection tryLock(String name) {
        Connection c = null;
        try {
            c = dataSource.getConnection();
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("SELECT pg_try_advisory_xact_lock(?, ?)")) {
                ps.setInt(1, NAMESPACE);
                ps.setInt(2, name.hashCode());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    if (rs.getBoolean(1)) return c;
                }
            }
            release(c);
            return null;
        } catch (SQLException ex) {
            release(c);
            throw new IllegalStateException("job lock failed", ex);
        }
    }

    private static void release(Connection c) {
        if (c == null) return;
        try {
            c.rollback(); // ends the transaction, which frees the advisory lock
        } catch (SQLException ignored) {
            // closing the connection below frees it anyway
        } finally {
            try {
                c.close();
            } catch (SQLException ignored) {
                // nothing left to do
            }
        }
    }
}
