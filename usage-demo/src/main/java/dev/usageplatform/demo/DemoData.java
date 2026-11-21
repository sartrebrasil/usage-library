package dev.usageplatform.demo;

import dev.usageplatform.core.TenantId;
import dev.usageplatform.postgres.JdbcSubscriptions;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Seeds the catalogue the platform reads: one metric, two plans, two tenants. Idempotent, so restarting is safe.
 * In a real system an admin API or your billing system owns this data; the platform only reads it.
 * <p>
 * Two metrics: API_REQUESTS (an access metric, counted by the gateway when the request arrives) and ITEMS_PROCESSED (a
 * result metric, counted by the worker only after a document is processed; see the async package).
 * <p>
 * FREE: 3 requests/min, 20/day; 5 documents/day, 1 at a time.
 * PRO:  100 requests/min, 1000/day; 1000 documents/day, 4 at a time.
 * TRIAL: 100 requests/min, 1000/day; 3 documents/day, 1 at a time (small on purpose, to show the quota).
 */
@Component
class DemoData implements ApplicationRunner {
    private final JdbcTemplate jdbc;
    private final JdbcSubscriptions subscriptions;

    DemoData(JdbcTemplate jdbc, JdbcSubscriptions subscriptions) {
        this.jdbc = jdbc;
        this.subscriptions = subscriptions;
    }

    @Override
    public void run(ApplicationArguments args) {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    id UUID PRIMARY KEY, tenant_id TEXT NOT NULL, content TEXT NOT NULL, status VARCHAR(20) NOT NULL,
                    deliveries INT NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), processed_at TIMESTAMPTZ)""");
        jdbc.update("""
                INSERT INTO usage.metric (id, code, unit, count_trigger) VALUES (gen_random_uuid(), 'API_REQUESTS', 'REQUEST', 'ON_REQUEST')
                ON CONFLICT (code) DO NOTHING""");
        jdbc.update("""
                INSERT INTO usage.metric (id, code, unit, count_trigger) VALUES (gen_random_uuid(), 'ITEMS_PROCESSED', 'DOCUMENT', 'ON_FINAL_STATUS')
                ON CONFLICT (code) DO NOTHING""");
        plan(new PlanSpec("FREE", "Free", 3, 20, 5, 1));
        plan(new PlanSpec("PRO", "Pro", 100, 1000, 1000, 4));
        plan(new PlanSpec("TRIAL", "Trial", 100, 1000, 3, 1));
        tenant("acme", "Acme (free)", "FREE");
        tenant("globex", "Globex (pro)", "PRO");
        tenant("initech", "Initech (trial)", "TRIAL");
    }

    private record PlanSpec(String code, String name, long requestsPerMinute, long requestsPerDay, long documentsPerDay, long documentsAtOnce) {}

    private void plan(PlanSpec p) {
        jdbc.update("INSERT INTO usage.plan (id, code, name, version) VALUES (gen_random_uuid(), ?, ?, 1) ON CONFLICT (code, version) DO NOTHING", p.code(), p.name());
        limit(p.code(), "API_REQUESTS", "RATE", p.requestsPerMinute(), 60L);
        limit(p.code(), "API_REQUESTS", "QUOTA", p.requestsPerDay(), 86400L);
        limit(p.code(), "ITEMS_PROCESSED", "QUOTA", p.documentsPerDay(), 86400L);
        limit(p.code(), "ITEMS_PROCESSED", "CONCURRENCY", p.documentsAtOnce(), null); // a concurrency limit has no window
    }

    private void limit(String plan, String metric, String type, long value, Long windowSeconds) {
        jdbc.update("""
                INSERT INTO usage.plan_limit (id, plan_id, metric_id, limit_type, limit_value, window_seconds, enforcement)
                SELECT gen_random_uuid(), p.id, m.id, ?, ?, ?, 'REJECT'
                  FROM usage.plan p, usage.metric m WHERE p.code = ? AND p.version = 1 AND m.code = ?
                ON CONFLICT DO NOTHING""", type, value, windowSeconds, plan, metric);
    }

    private void tenant(String id, String name, String plan) {
        jdbc.update("INSERT INTO usage.tenant (id, name) VALUES (?, ?) ON CONFLICT (id) DO NOTHING", id, name);
        Integer open = jdbc.queryForObject("SELECT count(*) FROM usage.tenant_subscription WHERE tenant_id = ? AND ended_at IS NULL", Integer.class, id);
        // the billing cycle of the subscription starts today at 00:00 UTC; daily windows follow from it
        if (open != null && open == 0) subscriptions.subscribe(new TenantId(id), plan, 1, Instant.now().truncatedTo(ChronoUnit.DAYS));
    }
}
