package dev.usageplatform.core;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Small helpers for the asynchronous contracts of this library. */
public final class Stages {
    private Stages() {}

    /** An already completed stage; for answers that need no I/O. */
    public static <T> CompletionStage<T> done(T value) {
        return CompletableFuture.completedFuture(value);
    }
}
