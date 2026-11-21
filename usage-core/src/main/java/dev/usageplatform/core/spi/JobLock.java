package dev.usageplatform.core.spi;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** SPI: makes a periodic job run on one instance at a time (a database advisory lock in production). */
public interface JobLock {
    /**
     * Runs the job only if no other instance holds the named lock; the answer is empty, and the job is not
     * started, when someone else does. The lock is released when the job's stage completes, whether it
     * succeeds or fails, and automatically if this process dies. The job must return a non-null value.
     */
    <T> CompletionStage<Optional<T>> runIfAcquired(String name, Supplier<CompletionStage<T>> job);
}
