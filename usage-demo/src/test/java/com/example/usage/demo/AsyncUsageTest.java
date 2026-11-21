package com.example.usage.demo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The asynchronous flow: the request is accepted at once, and ITEMS_PROCESSED is counted only after the worker
 * finishes the document with a billable final status.
 */
class AsyncUsageTest extends DemoTestBase {
    @Autowired
    JdbcTemplate jdbc;

    static String submit(String key, String content) throws Exception {
        var r = post("/api/documents", key, content);
        assertEquals(202, r.statusCode(), r.body());
        Matcher m = Pattern.compile("\"id\":\"([0-9a-f-]{36})\"").matcher(r.body());
        assertTrue(m.find(), r.body());
        return m.group(1);
    }

    String status(String id) {
        return jdbc.queryForObject("SELECT status FROM documents WHERE id = ?::uuid", String.class, id);
    }

    /** What billing would read for the tenant: the durable total of ITEMS_PROCESSED (0 if nothing was counted yet). */
    long billed(String tenant) {
        Long total = jdbc.queryForObject("SELECT COALESCE(sum(total), 0) FROM usage.usage_summary WHERE tenant_id = ? AND metric_code = 'ITEMS_PROCESSED' AND window_key = '86400'",
                Long.class, tenant);
        return total == null ? 0 : total;
    }

    static void eventually(String what, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(100);
        }
        fail("timed out waiting for: " + what);
    }

    @Test
    void theUsageIsCountedOnlyAfterTheProcessingFinishes() throws Exception {
        long before = billed("globex");
        String id = submit("key-globex", "ok: a document");

        // accepted, not processed yet (the test worker takes 2 s): nothing is billed
        assertEquals("PENDING", status(id));
        assertEquals(before, billed("globex"));

        eventually("the document is COMPLETED", () -> "COMPLETED".equals(status(id)));
        eventually("the usage is counted", () -> billed("globex") == before + 1);
    }

    @Test
    void aFinalStatusThatIsNotBillableIsSavedButNotCounted() throws Exception {
        long before = billed("globex");
        String id = submit("key-globex", "reject: not a valid document");
        eventually("the document is REJECTED", () -> "REJECTED".equals(status(id)));
        Thread.sleep(500);
        assertEquals(before, billed("globex"), "a rejected document is not billed");
    }

    @Test
    void aRedeliveredMessageIsBilledOnce() throws Exception {
        long before = billed("globex");
        String id = submit("key-globex", "dup: the broker will deliver this twice");
        eventually("it was handled twice", () -> jdbc.queryForObject("SELECT deliveries FROM documents WHERE id = ?::uuid", Integer.class, id) >= 2);
        eventually("the usage is counted", () -> billed("globex") == before + 1);
        Thread.sleep(500);
        assertEquals(before + 1, billed("globex"), "two deliveries, one bill");
    }

    @Test
    void aTenantOverItsQuotaIsRefusedAtTheDoorAndItsBacklogWaits() throws Exception {
        // TRIAL: 3 documents a day, one at a time. Five are accepted (nothing counted yet, so the guard lets them in).
        call("GET", "/api/orders", "key-initech"); // warm-up: the first call through a cold gateway is slow, and the backlog below must build before the first document finishes
        List<String> ids = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) ids.add(submit("key-initech", "ok: document " + i));

        // one at a time: the others wait for a free slot (DEFERRED) and are retried; three get through, then the quota stops the rest
        eventually("three documents billed", () -> billed("initech") == 3);
        eventually("the first three are COMPLETED", () -> ids.stream().filter(i -> "COMPLETED".equals(status(i))).count() == 3);
        Thread.sleep(3500); // the last two keep being retried and keep being deferred: they are never processed over the quota
        assertEquals(3, billed("initech"));
        assertEquals(2, ids.stream().filter(i -> "PENDING".equals(status(i))).count());

        // and a new submission is refused at the door
        var refused = post("/api/documents", "key-initech", "ok: one more");
        assertEquals(429, refused.statusCode());
        assertTrue(refused.body().contains("quota_exceeded"), refused.body());
    }
}
