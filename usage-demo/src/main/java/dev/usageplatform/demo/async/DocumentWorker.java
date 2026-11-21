package dev.usageplatform.demo.async;

import dev.usageplatform.core.UsageEvent;
import dev.usageplatform.core.UsageSink;
import dev.usageplatform.processor.QueueUsageAdapter;
import dev.usageplatform.processor.QueueUsageAdapter.Done;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;

import static dev.usageplatform.core.Stages.done;

/**
 * The processor. An in-memory queue stands in for your broker (RabbitMQ, SQS...): in production the same
 * {@link QueueUsageAdapter#handle} call sits inside your message listener, and the verdict decides ack, retry or dead-letter.
 * <p>
 * For each message the adapter (1) checks the tenant is not already over its quota, (2) takes a concurrency slot,
 * (3) runs the work, (4) hands the result AND the usage event to {@link #save}, then (5) releases the slot. The usage
 * is counted only in step 4, and only when the final status is billable ({@code usage.processor.countable-statuses}).
 */
@Component
class DocumentWorker {
    private static final Logger log = LoggerFactory.getLogger(DocumentWorker.class);
    private static final Duration RETRY_DELAY = Duration.ofSeconds(1);
    private static final int MAX_FAILURES = 3;

    /** @param failures how many times processing blew up */
    record Job(UUID id, String tenant, String content, int failures) {}

    private final QueueUsageAdapter adapter;
    private final UsageSink sink;
    private final JdbcTemplate jdbc;
    private final LinkedBlockingQueue<Job> queue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService later = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService workers = Executors.newFixedThreadPool(4);
    private volatile boolean running = true;

    private final Duration processingTime;

    DocumentWorker(QueueUsageAdapter adapter, UsageSink sink, JdbcTemplate jdbc,
                   @Value("${demo.processing-time:500ms}") Duration processingTime) {
        this.processingTime = processingTime;
        this.adapter = adapter;
        this.sink = sink;
        this.jdbc = jdbc;
    }

    @PostConstruct
    void start() {
        for (int i = 0; i < 4; i++) workers.submit(this::loop);
    }

    @PreDestroy
    void stop() {
        running = false;
        workers.shutdownNow();
        later.shutdownNow();
    }

    void submit(Job job) {
        queue.add(job);
    }

    private void loop() {
        while (running) {
            try {
                handle(queue.take());
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                log.error("worker error", e);
            }
        }
    }

    private void handle(Job job) {
        QueueUsageAdapter.Result result;
        try {
            result = adapter.<String>handle(Map.of("x-tenant-id", job.tenant()), job.id().toString(),
                    () -> done(process(job)), (status, usage) -> save(job, status, usage)).toCompletableFuture().join();
        } catch (RuntimeException e) {
            fail(job, e instanceof CompletionException && e.getCause() != null ? e.getCause() : e); // the adapter wraps what the work threw
            return;
        }
        switch (result.verdict()) {
            case COMPLETED -> {
                // "the broker lost our ack": the same message arrives again. The usage id is derived from the document id,
                // so the ledger recognises it and the tenant is billed once.
                if (job.content().startsWith("dup") && deliveries(job) == 1) submit(job);
            }
            case DEFERRED -> {
                log.info("document {} deferred ({}), retrying in {}", job.id(), result.detail(), RETRY_DELAY);
                later.schedule(() -> submit(job), RETRY_DELAY.toMillis(), TimeUnit.MILLISECONDS);
            }
            case NO_TENANT, DENIED -> jdbc.update("UPDATE documents SET status = 'REFUSED' WHERE id = ?", job.id());
        }
    }

    /** The actual work. "reject..." ends REJECTED (a final status that is NOT billable), "boom..." throws, anything else COMPLETED. */
    private Done<String> process(Job job) {
        try {
            Thread.sleep(processingTime.toMillis()); // pretend to parse, validate, call something
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        if (job.content().startsWith("boom")) throw new IllegalStateException("processing failed");
        String status = job.content().startsWith("reject") ? "REJECTED" : "COMPLETED";
        return new Done<>(status, status, Instant.now());
    }

    /**
     * Result and usage event together. Here the "together" is two calls; with the outbox it is ONE transaction
     * ({@code OutboxUsageSink.sameTransaction}), so a crash can neither lose billable usage nor bill unsaved work.
     */
    private CompletionStage<Void> save(Job job, String status, Optional<UsageEvent> usage) {
        jdbc.update("UPDATE documents SET status = ?, deliveries = deliveries + 1, processed_at = now() WHERE id = ?", status, job.id());
        return usage.map(sink::publish).orElseGet(() -> done(null));
    }

    private int deliveries(Job job) {
        Integer n = jdbc.queryForObject("SELECT deliveries FROM documents WHERE id = ?", Integer.class, job.id());
        return n == null ? 0 : n;
    }

    private void fail(Job job, Throwable e) {
        if (job.failures() + 1 >= MAX_FAILURES) {
            log.warn("document {} failed for good: {}", job.id(), e.getMessage());
            jdbc.update("UPDATE documents SET status = 'FAILED' WHERE id = ?", job.id());
        } else {
            log.info("document {} failed ({}), retrying", job.id(), e.getMessage());
            later.schedule(() -> submit(new Job(job.id(), job.tenant(), job.content(), job.failures() + 1)),
                    RETRY_DELAY.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
