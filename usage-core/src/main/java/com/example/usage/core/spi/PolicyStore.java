package com.example.usage.core.spi;

import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;

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
