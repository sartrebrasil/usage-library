package com.example.usage.spring;

import com.example.usage.core.UsageEngine;
import com.example.usage.core.UsageEngine.FailMode;
import com.example.usage.core.model.Limit;
import com.example.usage.core.model.Limit.Enforcement;
import com.example.usage.core.model.Limit.Type;
import com.example.usage.core.model.Metric;
import com.example.usage.core.model.Metric.Mode;
import com.example.usage.core.model.Plan;
import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.model.Window;
import com.example.usage.core.spi.PolicyStore;
import com.example.usage.redis.RedisCounterStore;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The two calls a servlet service makes, on a real Redis: the last unit goes to exactly one caller, and a rollback gives the quota back. */
@Testcontainers
class UsageAdmissionTest {
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    static final Metric DOCS = new Metric("DOCUMENTS", Mode.CONSUME_ON_CHECK);
    static RedisClient client;
    static StatefulRedisConnection<String, String> connection;
    static UsageAdmission admission;

    @BeforeAll
    static void start() {
        client = RedisClient.create("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379));
        connection = client.connect();
        Policy limited = new Policy(new Plan("PRO", 1), Instant.parse("2026-01-01T00:00:00Z"), List.of(
                new Limit(DOCS, Type.RATE, 2, Duration.ofMinutes(1), Enforcement.REJECT),
                new Limit(DOCS, Type.QUOTA, 5, Window.months(1), Enforcement.REJECT)));
        PolicyStore policies = t -> CompletableFuture.completedFuture(t.value().startsWith("nobody") ? Optional.empty() : Optional.of(limited));
        var engine = new UsageEngine(policies, new RedisCounterStore(connection.async()), (e, s) -> CompletableFuture.failedFuture(new UnsupportedOperationException()),
                e -> CompletableFuture.completedFuture(null), Clock.systemUTC(), FailMode.CLOSED, Duration.ofMinutes(15));
        admission = new UsageAdmission(engine, "DOCUMENTS", Clock.systemUTC());
    }

    @AfterAll
    static void stop() {
        connection.close();
        client.shutdown();
    }

    /** A transaction manager with nothing to commit: only the synchronizations matter here. */
    static TransactionTemplate transactions() {
        PlatformTransactionManager tm = new AbstractPlatformTransactionManager() {
            @Override protected Object doGetTransaction() { return new Object(); }
            @Override protected void doBegin(Object tx, TransactionDefinition d) { }
            @Override protected void doCommit(DefaultTransactionStatus s) { }
            @Override protected void doRollback(DefaultTransactionStatus s) { }
        };
        return new TransactionTemplate(tm);
    }

    @Test
    void theLastUnitOfTheQuotaGoesToExactlyOneCallerAmongMany() throws Exception {
        String tenant = "race-" + UUID.randomUUID();
        var pool = Executors.newFixedThreadPool(16);
        var go = new CountDownLatch(1);
        var granted = new AtomicInteger();
        var denied = new ArrayList<UsageDenied>();
        List<CompletableFuture<Void>> all = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            all.add(CompletableFuture.runAsync(() -> {
                try {
                    go.await();
                    admission.consume(tenant);
                    granted.incrementAndGet();
                } catch (UsageDenied d) {
                    synchronized (denied) { denied.add(d); }
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }, pool));
        }
        go.countDown();
        CompletableFuture.allOf(all.toArray(CompletableFuture[]::new)).join();
        pool.shutdown();

        assertEquals(5, granted.get(), "the quota is 5: no more, no less, however many ask at once");
        assertEquals(35, denied.size());
        assertTrue(denied.stream().allMatch(d -> d.kind() == UsageDenied.Kind.QUOTA_EXCEEDED));
        assertTrue(denied.get(0).retryAfterSeconds() > 0, "it says when the period ends");
    }

    @Test
    void aTransactionThatRollsBackGivesTheQuotaBackAndOneThatCommitsKeepsIt() {
        String tenant = "tx-" + UUID.randomUUID();
        var tx = transactions();
        for (int i = 0; i < 20; i++) { // far more rollbacks than the quota of 5
            tx.executeWithoutResult(status -> {
                admission.consume(tenant);
                status.setRollbackOnly();
            });
        }
        assertThrows(RuntimeException.class, () -> tx.executeWithoutResult(status -> {
            admission.consume(tenant);
            throw new IllegalStateException("the document could not be saved"); // an exception also rolls back
        }));
        for (int i = 0; i < 5; i++) tx.executeWithoutResult(status -> admission.consume(tenant)); // all five units were still there
        assertEquals(UsageDenied.Kind.QUOTA_EXCEEDED, assertThrows(UsageDenied.class, () -> tx.executeWithoutResult(status -> admission.consume(tenant))).kind());
    }

    @Test
    void aRefusedConsumeInsideATransactionTakesNothingAndTheRateLimitIsGlobal() {
        String tenant = "rate-" + UUID.randomUUID();
        admission.admit(tenant);
        admission.admit(tenant);
        var denied = assertThrows(UsageDenied.class, () -> admission.admit(tenant)); // 2 per minute
        assertEquals(UsageDenied.Kind.RATE_LIMITED, denied.kind());
        assertTrue(denied.retryAfterSeconds() >= 1 && denied.retryAfterSeconds() <= 60);
        admission.consume(tenant); // the rate checks did not touch the quota
    }

    @Test
    void aTenantWithoutASubscriptionIsRefusedEarly() {
        var denied = assertThrows(UsageDenied.class, () -> admission.admit("nobody"));
        assertEquals(UsageDenied.Kind.SUBSCRIPTION_REQUIRED, denied.kind());
        assertEquals(0, denied.retryAfterSeconds());
    }
}
