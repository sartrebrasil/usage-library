package dev.usageplatform.core.model;

import java.util.concurrent.CompletionStage;

/** A concurrency slot. Release it in finally; releasing twice frees only once. */
public interface Permit {
    Decision decision();

    CompletionStage<Void> release();
}
