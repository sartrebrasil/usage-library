package dev.usageplatform.processor;

import dev.usageplatform.core.Decision;
import dev.usageplatform.core.Permit;
import dev.usageplatform.core.TenantId;
import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.UsageEvent;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static dev.usageplatform.core.Stages.done;

/**
 * Processor side of a result metric (e.g. ITEMS_PROCESSED). Broker-agnostic: the host hands over the
 * message headers and the work, and maps the {@link Verdict} to ack / retry-later / dead-letter.
 * <p>
 * Per message: tenant from headers (none: NO_TENANT) -> optional guard (already over quota) ->
 * concurrency permit (none free: DEFERRED) -> work -> persist result AND usage event together -> release in finally.
 * <p>
 * The usage event is handed to {@link Persist}, not sent by this class: the host writes it to its outbox in
 * the same transaction as the result, so a crash can never lose billable usage nor bill something that
 * was not saved. A relay then publishes the outbox rows through the {@code UsageSink}.
 * <p>
 * shortcuts: quantity is always 1 per item; the permit is released but never renewed, so a job longer than
 * the engine's lease loses its slot, which is the intended safety net.
 */
public final class QueueUsageAdapter {
    public enum Verdict {
        /** Work done and persisted. Ack. */
        COMPLETED,
        /** No tenant in the message headers. Dead-letter, never process an ownerless message. */
        NO_TENANT,
        /** Temporary refusal (concurrency, quota guard, store down under fail-closed). Retry later. */
        DEFERRED,
        /** Permanent refusal (tenant has no policy). Dead-letter. */
        DENIED
    }

    public record Result(Verdict verdict, String detail) {}

    /** What the work produced: the final status decides whether it is billable; finishedAt must come from the persisted fact. */
    public record Done<R>(R result, String status, Instant finishedAt) {}

    /** Host's single transaction: save the result and, when present, the usage event into the outbox. */
    @FunctionalInterface
    public interface Persist<R> {
        CompletionStage<Void> persist(R result, Optional<UsageEvent> usage);
    }

    private final UsageEngine engine;
    private final String metricCode;
    private final Set<String> countableStatuses;
    private final boolean deferWhenOverQuota;
    private final String tenantHeader;
    private final Executor executor;

    /** @param executor runs the caller's {@code work} and {@code persist}, so their blocking calls never run on the Redis I/O thread the engine completes on. */
    public QueueUsageAdapter(UsageEngine engine, String metricCode, Set<String> countableStatuses,
                             boolean deferWhenOverQuota, String tenantHeader, Executor executor) {
        this.engine = engine;
        this.metricCode = metricCode;
        this.countableStatuses = Set.copyOf(countableStatuses);
        this.deferWhenOverQuota = deferWhenOverQuota;
        this.tenantHeader = tenantHeader;
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    /**
     * @param itemId stable business id of the item: the same item always yields the same usage eventId,
     *               so redelivery or retry can never be billed twice.
     * @param work runs on the adapter's executor, so it may block (parsing, JDBC, HTTP)
     * @param persist runs on the same executor once the work is done
     * @return failed stage if the work or the persistence failed: the broker redelivers.
     */
    public <R> CompletionStage<Result> handle(Map<String, String> headers, String itemId,
                                              Supplier<CompletionStage<Done<R>>> work, Persist<R> persist) {
        Optional<TenantId> tenant = tenantFrom(headers);
        if (tenant.isEmpty()) return done(new Result(Verdict.NO_TENANT, "missing header " + tenantHeader));
        TenantId t = tenant.get();
        return guard(t).thenCompose(refusal -> refusal.isPresent()
                ? done(refusal.get())
                : admitAndRun(t, itemId, work, persist));
    }

    private CompletionStage<Optional<Result>> guard(TenantId t) {
        if (!deferWhenOverQuota) return done(Optional.empty());
        return engine.check(t, metricCode, 1)
                .thenApply(d -> d.allowed() ? Optional.empty() : Optional.of(refusal(d)));
    }

    private <R> CompletionStage<Result> admitAndRun(TenantId t, String itemId,
                                                    Supplier<CompletionStage<Done<R>>> work, Persist<R> persist) {
        return engine.acquire(t, metricCode, 1).thenCompose(permit -> permit.decision().allowed()
                ? runAndRelease(t, itemId, permit, work, persist)
                : done(refusal(permit.decision())));
    }

    private <R> CompletionStage<Result> runAndRelease(TenantId t, String itemId, Permit permit,
                                                      Supplier<CompletionStage<Done<R>>> work, Persist<R> persist) {
        CompletionStage<Result> run;
        try {
            run = CompletableFuture.supplyAsync(work::get, executor).thenCompose(stage -> stage)
                    .thenComposeAsync(d -> persist.persist(d.result(), usageFor(t, itemId, d))
                            .thenApply(v -> new Result(Verdict.COMPLETED, d.status())), executor);
        } catch (RuntimeException e) { // the executor refused the task
            run = CompletableFuture.failedFuture(e);
        }
        CompletableFuture<Result> out = new CompletableFuture<>();
        run.whenComplete((r, ex) -> permit.release().whenComplete((v, releaseEx) -> {
            // a failed release is not fatal: the engine's lease expires the slot
            if (ex != null) out.completeExceptionally(ex);
            else out.complete(r);
        }));
        return out;
    }

    private Optional<UsageEvent> usageFor(TenantId t, String itemId, Done<?> d) {
        if (!countableStatuses.contains(d.status())) return Optional.empty();
        return Optional.of(new UsageEvent(UsageEvent.idFor(t, itemId, metricCode), t, metricCode, 1, d.finishedAt()));
    }

    /** The tenant, or nothing if the header is missing or present under several casings with different values. */
    private Optional<TenantId> tenantFrom(Map<String, String> headers) {
        Set<String> values = headers.entrySet().stream()
                .filter(e -> e.getKey().equalsIgnoreCase(tenantHeader) && e.getValue() != null && !e.getValue().isBlank())
                .map(Map.Entry::getValue)
                .collect(Collectors.toSet());
        // two different tenants in one message: processing it for either could bill the wrong one
        return values.size() == 1 ? Optional.of(new TenantId(values.iterator().next())) : Optional.empty();
    }

    private static Result refusal(Decision d) {
        return new Result(Decision.NO_POLICY.equals(d.reason()) ? Verdict.DENIED : Verdict.DEFERRED, d.reason());
    }

}
