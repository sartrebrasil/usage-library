package com.example.usage.core;

import com.example.usage.core.spi.JobLock;
import com.example.usage.core.spi.UsageTelemetry;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
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
    private final UsageTelemetry telemetry;
    private final AtomicLong lastSuccess = new AtomicLong();
    private ScheduledExecutorService executor;

    /** @param listener receives every outcome; the host turns it into logs, metrics and alerts. */
    public MaintenanceScheduler(JobLock lock, PeriodMaintenance job, Consumer<RunOutcome> listener) {
        this(lock, job, listener, UsageTelemetry.NOOP);
    }

    /** @param telemetry gets one {@code maintenance.run} per run (outcome tag), the rows reconciled and the last success time. */
    public MaintenanceScheduler(JobLock lock, PeriodMaintenance job, Consumer<RunOutcome> listener, UsageTelemetry telemetry) {
        this.telemetry = telemetry;
        // epoch seconds of the last run that completed (0 = never): alert when it is older than a few intervals
        telemetry.gauge("maintenance.last.success", lastSuccess::get);
        this.lock = lock;
        this.job = job;
        this.listener = listener;
    }

    public CompletionStage<RunOutcome> runOnce() {
        UsageTelemetry.Op op = telemetry.start("maintenance.run");
        return op.inScope(() -> lock.runIfAcquired(LOCK_NAME, job::run)).handle((result, error) -> {
            RunOutcome outcome = error != null ? new RunOutcome.Failed(error)
                    : result.<RunOutcome>map(RunOutcome.Completed::new).orElseGet(RunOutcome.Skipped::new);
            record(op, outcome);
            return outcome;
        });
    }

    private void record(UsageTelemetry.Op op, RunOutcome outcome) {
        try {
            switch (outcome) {
                case RunOutcome.Completed c -> {
                    lastSuccess.set(java.time.Instant.now().getEpochSecond());
                    var r = c.result().reconciliation();
                    telemetry.count("maintenance.periods.closed", c.result().closed());
                    telemetry.count("reconciler.rows", r.scanned(), "result", "scanned");
                    telemetry.count("reconciler.rows", r.repaired(), "result", "repaired");
                    telemetry.count("reconciler.rows", r.ahead(), "result", "ahead");
                    telemetry.count("reconciler.rows", r.skipped(), "result", "skipped");
                    telemetry.count("reconciler.rows", r.failed(), "result", "failed");
                    op.tag("outcome", "completed").end(null);
                }
                case RunOutcome.Skipped s -> op.tag("outcome", "skipped").end(null);
                case RunOutcome.Failed f -> op.tag("outcome", "failed").end(f.error());
            }
        } catch (RuntimeException ignored) {
            // a broken metrics backend must not turn a finished run into a failed one
        }
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
