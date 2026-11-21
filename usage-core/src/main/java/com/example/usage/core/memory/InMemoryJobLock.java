package com.example.usage.core.memory;

import com.example.usage.core.spi.JobLock;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static com.example.usage.core.spi.Stages.done;

/** Single-JVM JobLock for tests and single-instance deployments: it excludes only callers in this process. */
public final class InMemoryJobLock implements JobLock {
    private final Set<String> held = new HashSet<>();

    @Override
    public <T> CompletionStage<Optional<T>> runIfAcquired(String name, Supplier<CompletionStage<T>> job) {
        synchronized (held) {
            if (!held.add(name)) return done(Optional.empty());
        }
        CompletableFuture<Optional<T>> out = new CompletableFuture<>();
        CompletionStage<T> running;
        try {
            running = job.get();
            if (running == null) running = CompletableFuture.failedFuture(new IllegalStateException("the job returned a null stage"));
        } catch (RuntimeException e) {
            running = CompletableFuture.failedFuture(e);
        }
        running.whenComplete((result, ex) -> {
            synchronized (held) {
                held.remove(name);
            }
            if (ex != null) out.completeExceptionally(ex);
            else if (result == null) out.completeExceptionally(new IllegalStateException("the job returned null"));
            else out.complete(Optional.of(result));
        });
        return out;
    }
}
