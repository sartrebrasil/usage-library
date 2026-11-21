package dev.usageplatform.demo.async;

import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.model.Decision;
import dev.usageplatform.core.model.TenantId;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.UUID;

/**
 * The asynchronous side of the demo. A request is ACCEPTED here, but what the tenant is billed for (the metric
 * ITEMS_PROCESSED) is only counted later, by {@link DocumentWorker}, once the document reaches a billable final status.
 * <p>
 * The gateway already counted the HTTP call itself (API_REQUESTS, an access metric). Here, before queueing, the
 * result metric is only GUARDED: {@code engine.check} on a result metric reads what was already processed and
 * consumes nothing, so a tenant that is over its daily quota is refused with 429 without being charged for the attempt.
 */
@RestController
class DocumentController {
    static final String METRIC = "ITEMS_PROCESSED";

    private final UsageEngine engine;
    private final JdbcTemplate jdbc;
    private final DocumentWorker worker;

    DocumentController(UsageEngine engine, JdbcTemplate jdbc, DocumentWorker worker) {
        this.engine = engine;
        this.jdbc = jdbc;
        this.worker = worker;
    }

    @PostMapping("/upstream/documents")
    Mono<ResponseEntity<Map<String, Object>>> submit(@RequestHeader("X-Tenant-Id") String tenant, @RequestBody String content) {
        return Mono.fromCompletionStage(() -> engine.check(new TenantId(tenant), METRIC, 1))
                .flatMap(decision -> {
                    // Only the quota refuses here. A full concurrency limit is not a reason to turn the document away:
                    // it just waits in the queue until a slot is free.
                    if (!decision.allowed() && Decision.QUOTA_EXCEEDED.equals(decision.reason())) {
                        return Mono.just(ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).<Map<String, Object>>body(Map.of("error", decision.reason())));
                    }
                    return Mono.fromCallable(() -> accept(tenant, content)).subscribeOn(Schedulers.boundedElastic());
                });
    }

    private ResponseEntity<Map<String, Object>> accept(String tenant, String content) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO documents (id, tenant_id, content, status) VALUES (?, ?, ?, 'PENDING')", id, tenant, content);
        worker.submit(new DocumentWorker.Job(id, tenant, content, 0));
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("id", id.toString(), "status", "PENDING"));
    }

    @GetMapping("/upstream/documents/{id}")
    Mono<ResponseEntity<Map<String, Object>>> status(@PathVariable UUID id) {
        return Mono.fromCallable(() -> jdbc.query("SELECT status, deliveries FROM documents WHERE id = ?",
                        (rs, i) -> Map.<String, Object>of("id", id.toString(), "status", rs.getString("status"), "deliveries", rs.getInt("deliveries")), id))
                .subscribeOn(Schedulers.boundedElastic())
                .map(rows -> rows.isEmpty() ? ResponseEntity.<Map<String, Object>>notFound().build() : ResponseEntity.ok(rows.get(0)));
    }
}
