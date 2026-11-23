package com.example.usage.spring;

import com.example.usage.core.UsageEngine;
import com.example.usage.core.model.Decision;
import com.example.usage.core.model.Limit.Type;
import com.example.usage.core.model.TenantId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Clock;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * What a servlet service calls to decide with the engine, in two places of a request that accepts work:
 * <ul>
 *   <li>{@link #admit}: early, before any work is spent. Only the rate limits take part, and a tenant with no policy is refused.</li>
 *   <li>{@link #consume}: late, inside the transaction that records the accepted work. Only the quota takes part, and if the
 *       transaction rolls back the quota is given back, so a refused or failed request costs nothing.</li>
 * </ul>
 * Both throw {@link UsageDenied} on a refusal. Neither is a filter on purpose: whether a request counts depends on what the service
 * learned about it (validated, new, not a resend), which only the service knows. The counter is atomic in the store (two instances never
 * pass the cap), but it is not part of the database transaction: a crash between {@code consume} and the commit leaves it one above the
 * truth until the period ends, and the reconciler lowers it only when it is below the ledger.
 */
public final class UsageAdmission {
    private static final Logger LOG = LoggerFactory.getLogger(UsageAdmission.class);
    private static final Set<Type> RATE = EnumSet.of(Type.RATE);
    private static final Set<Type> QUOTA = EnumSet.of(Type.QUOTA);

    private final UsageEngine engine;
    private final String metric;
    private final Clock clock;

    public UsageAdmission(UsageEngine engine, String metric, Clock clock) {
        this.engine = engine;
        this.metric = metric;
        this.clock = clock;
    }

    /** Early: the rate limits of the metric. Refuses the tenant that has no policy. */
    public void admit(String tenant) {
        Decision d = engine.check(new TenantId(tenant), metric, 1, clock.instant(), RATE).toCompletableFuture().join();
        if (!d.allowed()) throw new UsageDenied(d);
    }

    /**
     * Late: the quota of the metric, taking one. Returns the instant the quota was counted at (stamp the usage event with it, so the
     * period is the same). Call it inside the transaction: when that rolls back, the quota is given back. Outside a transaction nothing
     * is given back by itself: call {@link #release}.
     */
    public Instant consume(String tenant) {
        Instant at = clock.instant();
        Decision d = engine.check(new TenantId(tenant), metric, 1, at, QUOTA).toCompletableFuture().join();
        if (!d.allowed()) throw new UsageDenied(d);
        // a degraded answer (fail mode OPEN, store down) consumed nothing: there is nothing to give back
        if (d.reason() == null && TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCompletion(int status) {
                    if (status != STATUS_COMMITTED) release(tenant, at);
                }
            });
        }
        return at;
    }

    /** Gives back what {@link #consume} took at {@code at}. Best effort: a failure is logged, never thrown. */
    public void release(String tenant, Instant at) {
        try {
            engine.undoCheck(new TenantId(tenant), metric, 1, at, QUOTA).toCompletableFuture().join();
        } catch (RuntimeException e) {
            engine.telemetry().count("admission.release.failed", 1, "metric", metric);
            LOG.warn("could not give back the quota of tenant {} for {}: {}", tenant, metric, e.toString());
        }
    }
}
