package dev.usageplatform.core;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * SPI: where plans, limits and overrides come from (PostgreSQL + cache in production).
 * Failures must be reported through the returned stage, never thrown.
 */
@FunctionalInterface
public interface PolicyStore {
    CompletionStage<Optional<Policy>> policyFor(TenantId tenant);
}
