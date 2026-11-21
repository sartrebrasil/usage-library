package dev.usageplatform.core;

import dev.usageplatform.core.spi.JobLock;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Runs {@link PeriodMaintenance} on a timer, guarded by a {@link JobLock}. Every instance can run one of
 * these: the lock lets only one of them do the work on each tick, and the others report SKIPPED.
 * <p>
 * Runs never overlap within an instance, a failed run does not stop the schedule, and a listener that throws
 * does not either. Use {@link #runOnce()} instead of {@link #start} when the host has its own scheduler
 * (Spring's @Scheduled, Quartz, a Kubernetes CronJob).
 */
public final class MaintenanceScheduler implements AutoCloseable {
    public sealed interface RunOutcome {
        /** Another instance held the lock. */
        record Skipped() implements RunOutcome {}

        record Completed(PeriodMaintenance.Result result) implements RunOutcome {}

        /** Nothing is retried: the next tick runs again, and the work is idempotent. */
        record Failed(Throwable error) implements RunOutcome {}
    }

    static final String LOCK_NAME = "usage-period-maintenance";

    private final JobLock lock;
    private final PeriodMaintenance job;
    private final Consumer<RunOutcome> listener;
    private ScheduledExecutorService executor;

    /** @param listener receives every outcome; the host turns it into logs, metrics and alerts. */
    public MaintenanceScheduler(JobLock lock, PeriodMaintenance job, Consumer<RunOutcome> listener) {
        this.lock = lock;
        this.job = job;
        this.listener = listener;
    }

    public CompletionStage<RunOutcome> runOnce() {
        return lock.runIfAcquired(LOCK_NAME, job::run).handle((result, error) -> {
            if (error != null) return new RunOutcome.Failed(error);
            return result.<RunOutcome>map(RunOutcome.Completed::new).orElseGet(RunOutcome.Skipped::new);
        });
    }

    /** Starts the timer; fixed delay, so a slow run postpones the next one instead of overlapping it. */
    public synchronized void start(Duration initialDelay, Duration delay) {
        if (executor != null) throw new IllegalStateException("already started");
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "usage-maintenance");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::tick, initialDelay.toMillis(), delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void tick() {
        try { // an exception escaping here would cancel every later run
            listener.accept(runOnce().toCompletableFuture().join());
        } catch (Throwable ignored) {
            // the listener's own failure must not stop the schedule
        }
    }

    /** Stops the timer and waits (up to 10 s) for a run in progress, so the stores it uses are not closed under it. */
    @Override
    public synchronized void close() {
        if (executor == null) return;
        executor.shutdown(); // no new ticks; the one running is allowed to finish
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) executor.shutdownNow();
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        executor = null;
    }
}
