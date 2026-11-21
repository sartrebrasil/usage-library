package dev.usageplatform.core.model;

import java.util.Objects;

public record TenantId(String value) {
    public TenantId {
        Objects.requireNonNull(value, "tenantId");
        if (value.isBlank()) throw new IllegalArgumentException("tenantId is blank");
    }
}
