package com.example.usage.core.spi;

import com.example.usage.core.model.TenantId;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * SPI: maps whatever the host has (JWT claims, API key, message headers) to a tenant.
 * C is the host's credential type, so the core stays free of Spring and of any IdP.
 */
@FunctionalInterface
public interface TenantResolver<C> {
    CompletionStage<Optional<TenantId>> resolve(C credentials);
}
