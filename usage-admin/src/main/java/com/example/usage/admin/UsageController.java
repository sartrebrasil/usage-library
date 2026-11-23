package com.example.usage.admin;

import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

/** Read-only: what was consumed, and who changed what. */
@RestController
@RequestMapping("${usage.admin.base-path:/v1/usage}")
class UsageController {
    private final UsageQueries queries;
    private final Audit audit;

    UsageController(UsageQueries queries, Audit audit) {
        this.queries = queries;
        this.audit = audit;
    }

    @GetMapping("/summaries")
    Page<UsageQueries.Summary> summaries(@RequestParam(required = false) String tenantId, @RequestParam(required = false) String metric,
                                         @RequestParam(required = false) String windowKey, @RequestParam(required = false) String status,
                                         @RequestParam(required = false) @DateTimeFormat(iso = ISO.DATE_TIME) Instant from,
                                         @RequestParam(required = false) @DateTimeFormat(iso = ISO.DATE_TIME) Instant to,
                                         @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "50") int size) {
        return queries.summaries(tenantId, metric, windowKey, status, from, to, new Page.Request(page, size));
    }

    @GetMapping("/events")
    Page<UsageQueries.Event> events(@RequestParam(required = false) String tenantId, @RequestParam(required = false) String metric,
                                    @RequestParam(required = false) String type,
                                    @RequestParam(required = false) @DateTimeFormat(iso = ISO.DATE_TIME) Instant from,
                                    @RequestParam(required = false) @DateTimeFormat(iso = ISO.DATE_TIME) Instant to,
                                    @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "50") int size) {
        return queries.events(tenantId, metric, type, from, to, new Page.Request(page, size));
    }

    @GetMapping("/events/{id}")
    UsageQueries.Event event(@PathVariable UUID id) {
        return queries.event(id);
    }

    @GetMapping("/audit")
    Page<Audit.Entry> audit(@RequestParam(required = false) String entityType, @RequestParam(required = false) String entityId,
                            @RequestParam(required = false) String tenantId, @RequestParam(required = false) String actor,
                            @RequestParam(required = false) @DateTimeFormat(iso = ISO.DATE_TIME) Instant from,
                            @RequestParam(required = false) @DateTimeFormat(iso = ISO.DATE_TIME) Instant to,
                            @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "50") int size) {
        return audit.search(entityType, entityId, tenantId, actor, from, to, new Page.Request(page, size));
    }
}
