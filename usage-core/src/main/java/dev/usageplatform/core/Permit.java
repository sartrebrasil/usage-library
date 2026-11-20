package dev.usageplatform.core;

import java.util.concurrent.CompletionStage;

/** A concurrency slot. Release it in finally; releasing twice frees only once. */
public interface Permit {
    Decision decision();

    CompletionStage<Void> release();
}
