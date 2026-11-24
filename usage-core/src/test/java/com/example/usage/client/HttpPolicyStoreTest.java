package com.example.usage.client;

import com.example.usage.core.model.Limit;
import com.example.usage.core.model.Limit.Enforcement;
import com.example.usage.core.model.Limit.Type;
import com.example.usage.core.model.Metric;
import com.example.usage.core.model.Metric.Mode;
import com.example.usage.core.model.Plan;
import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.spi.UsageTelemetry;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Against a real HTTP server: what the usage API answers, and how the host's cache copes when it stops answering. */
class HttpPolicyStoreTest {
    static final Policy PRO = new Policy(new Plan("PRO", 1), Instant.parse("2026-11-01T00:00:00Z"), List.of(
            new Limit(new Metric("DOCUMENTS", Mode.CONSUME_ON_CHECK), Type.QUOTA, 1000, com.example.usage.core.model.Window.months(1), Enforcement.REJECT)));

    HttpServer server;
    final AtomicInteger status = new AtomicInteger(200);
    final AtomicInteger calls = new AtomicInteger();
    final AtomicReference<String> lastPath = new AtomicReference<>();
    final AtomicReference<String> lastAuth = new AtomicReference<>();
    RestClient client;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/policies/", ex -> {
            calls.incrementAndGet();
            lastPath.set(ex.getRequestURI().getPath());
            lastAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
            int code = status.get();
            byte[] body = code == 200 ? PolicyJson.toJson(PRO).getBytes(StandardCharsets.UTF_8) : new byte[0];
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(code, code == 200 ? body.length : -1);
            if (code == 200) ex.getResponseBody().write(body);
            ex.close();
        });
        server.start();
        client = RestClient.builder().baseUrl("http://127.0.0.1:" + server.getAddress().getPort())
                .defaultHeader("Authorization", "Bearer edge-token").build();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    HttpPolicyStore store() {
        return new HttpPolicyStore(client, Executors.newVirtualThreadPerTaskExecutor());
    }

    @Test
    void readsThePolicyOfTheTenantWithTheHostsAuthentication() {
        assertEquals(Optional.of(PRO), store().policyFor(new TenantId("acme")).toCompletableFuture().join());
        assertEquals("/internal/policies/acme", lastPath.get());
        assertEquals("Bearer edge-token", lastAuth.get());
    }

    @Test
    void aTenantTheApiDoesNotKnowHasNoPolicyAndAnyOtherFailureFailsTheStage() {
        status.set(404);
        assertEquals(Optional.empty(), store().policyFor(new TenantId("ghost")).toCompletableFuture().join());
        status.set(500);
        assertThrows(CompletionException.class, () -> store().policyFor(new TenantId("acme")).toCompletableFuture().join());
    }

    @Test
    void theCachedStoreAsksOncePerTtlAndKeepsServingTheLastPolicyWhileTheApiIsDown() {
        var now = new AtomicReference<>(Instant.parse("2026-11-19T00:00:00Z"));
        Clock clock = new Clock() {
            @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId z) { return this; }
            @Override public Instant instant() { return now.get(); }
        };
        var cached = HttpPolicyStore.cached(client, Executors.newVirtualThreadPerTaskExecutor(), clock, Duration.ofMinutes(1), Duration.ofMinutes(30), UsageTelemetry.NOOP);
        var tenant = new TenantId("acme");

        cached.policyFor(tenant).toCompletableFuture().join();
        cached.policyFor(tenant).toCompletableFuture().join();
        assertEquals(1, calls.get(), "within the ttl the API is not asked again");

        status.set(503);
        now.set(now.get().plus(Duration.ofMinutes(2)));
        assertEquals(Optional.of(PRO), cached.policyFor(tenant).toCompletableFuture().join(), "the API is down: the last policy read serves");

        now.set(now.get().plus(Duration.ofMinutes(40)));
        assertThrows(CompletionException.class, () -> cached.policyFor(tenant).toCompletableFuture().join()); // older than the host accepts
    }
}
