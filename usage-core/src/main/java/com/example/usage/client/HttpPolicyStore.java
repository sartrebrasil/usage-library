package com.example.usage.client;

import com.example.usage.core.memory.CachedPolicyStore;
import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.spi.PolicyStore;
import com.example.usage.core.spi.UsageTelemetry;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * A {@link PolicyStore} that asks the usage API: {@code GET <base>/internal/policies/{tenant}}. The host brings the {@link RestClient}
 * (its base URL and its authentication, an OAuth client-credentials token for example). 404 is "no policy" (the engine then refuses
 * the tenant); any other failure fails the stage, and the engine answers it by its fail mode.
 * <p>
 * It does a call per lookup: wrap it with {@link #cached} so it is not asked on every request, and so the last policy read keeps
 * serving while the usage API is down.
 */
public final class HttpPolicyStore implements PolicyStore {
    private final RestClient client;
    private final Executor executor;

    /** @param executor runs the blocking call; virtual threads suit it */
    public HttpPolicyStore(RestClient client, Executor executor) {
        this.client = client;
        this.executor = executor;
    }

    @Override
    public CompletionStage<Optional<Policy>> policyFor(TenantId tenant) {
        return CompletableFuture.supplyAsync(() -> fetch(tenant), executor);
    }

    private Optional<Policy> fetch(TenantId tenant) {
        try {
            String body = client.get().uri("/internal/policies/{tenant}", tenant.value()).retrieve().body(String.class);
            return body == null || body.isBlank() ? Optional.empty() : Optional.of(PolicyJson.parse(body));
        } catch (HttpClientErrorException.NotFound e) {
            return Optional.empty();
        }
    }

    /**
     * The usual way to build one: a cache of {@code ttl} in front, serving the last policy read for up to {@code staleFor} when the
     * usage API fails. Invalidate nothing by hand: a plan change reaches the host within {@code ttl}.
     */
    public static CachedPolicyStore cached(RestClient client, Executor executor, Clock clock, Duration ttl, Duration staleFor, UsageTelemetry telemetry) {
        return new CachedPolicyStore(new HttpPolicyStore(client, executor), clock, ttl, 100_000, telemetry, staleFor);
    }
}
