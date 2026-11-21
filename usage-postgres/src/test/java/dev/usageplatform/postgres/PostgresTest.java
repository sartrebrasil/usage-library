package dev.usageplatform.postgres;

import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.UsageEngine.Compensate;
import dev.usageplatform.core.UsageEngine.FailMode;
import dev.usageplatform.core.UsageEngine.Ingest;
import dev.usageplatform.core.memory.InMemoryCounterStore;
import dev.usageplatform.core.model.Compensation;
import dev.usageplatform.core.model.Limit;
import dev.usageplatform.core.model.Limit.Enforcement;
import dev.usageplatform.core.model.Limit.Type;
import dev.usageplatform.core.model.Metric;
import dev.usageplatform.core.model.Metric.Mode;
import dev.usageplatform.core.model.Period;
import dev.usageplatform.core.model.Policy;
import dev.usageplatform.core.model.TenantId;
import dev.usageplatform.core.model.UsageEvent;
import dev.usageplatform.core.model.Window;
import dev.usageplatform.core.spi.Ledger;
import dev.usageplatform.core.spi.SummaryStore.Summary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class PostgresTest {
    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:16-alpine");

    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-19T00:00:00Z"), ZoneOffset.UTC);
    static final Instant ANCHOR = Instant.parse("2026-11-01T00:00:00Z");
    static final Metric API = new Metric("API_REQUESTS", Mode.CONSUME_ON_CHECK);
    static final Metric ITEMS = new Metric("ITEMS_PROCESSED", Mode.REPORT_ONLY);

    static DataSource ds;
    static JdbcPolicyStore policies;
    static JdbcLedger ledger;
    static java.util.concurrent.ExecutorService executor;

    @BeforeAll
    static void setUp() throws SQLException {
        PGSimpleDataSource d = new PGSimpleDataSource();
        d.setUrl(PG.getJdbcUrl());
        d.setUser(PG.getUsername());
        d.setPassword(PG.getPassword());
        ds = d;
        UsageSchema.migrate(ds);
        executor = Executors.newVirtualThreadPerTaskExecutor();
        policies = new JdbcPolicyStore(ds, executor, CLOCK);
        ledger = new JdbcLedger(ds, executor);
        seed();
    }

    static void seed() throws SQLException {
        exec("""
                INSERT INTO metric (id, code, unit, count_trigger) VALUES
                  (gen_random_uuid(), 'API_REQUESTS', 'REQUEST', 'ON_REQUEST'),
                  (gen_random_uuid(), 'ITEMS_PROCESSED', 'ITEM', 'ON_FINAL_STATUS');
                INSERT INTO plan (id, code, name, version) VALUES
                  (gen_random_uuid(), 'PRO', 'Pro', 1), (gen_random_uuid(), 'EMPTY', 'No limits', 1), (gen_random_uuid(), 'BASIC', 'Basic', 1);
                INSERT INTO plan_limit (id, plan_id, metric_id, limit_type, limit_value, window_seconds, enforcement)
                  SELECT gen_random_uuid(), p.id, m.id, 'QUOTA', 10, 86400, 'REJECT'
                    FROM plan p, metric m WHERE p.code = 'BASIC' AND m.code = 'API_REQUESTS';
                INSERT INTO plan_limit (id, plan_id, metric_id, limit_type, limit_value, window_seconds, window_months, enforcement)
                  SELECT gen_random_uuid(), p.id, m.id, x.t, x.v, x.w, x.mo, x.e
                    FROM plan p
                    JOIN (VALUES ('API_REQUESTS', 'QUOTA', 100, 86400, NULL, 'REJECT'),
                                 ('API_REQUESTS', 'QUOTA', 1000, NULL, 1, 'REJECT'),
                                 ('ITEMS_PROCESSED', 'QUOTA', 10, 86400, NULL, 'REJECT'),
                                 ('ITEMS_PROCESSED', 'CONCURRENCY', 5, NULL, NULL, 'REJECT')) AS x(mc, t, v, w, mo, e) ON true
                    JOIN metric m ON m.code = x.mc
                   WHERE p.code = 'PRO';
                INSERT INTO tenant (id, name, status) VALUES
                  ('acme', 'Acme', 'ACTIVE'), ('frozen', 'Frozen', 'SUSPENDED'), ('expired', 'Expired', 'ACTIVE'),
                  ('noplan', 'No plan', 'ACTIVE'), ('bare', 'Bare', 'ACTIVE'), ('ledgerco', 'Ledger Co', 'ACTIVE'), ('compco', 'Comp Co', 'ACTIVE'),
                  ('mover', 'Mover', 'ACTIVE'), ('fresh', 'Fresh', 'ACTIVE'), ('planco', 'Plan Co', 'ACTIVE');
                INSERT INTO tenant_subscription (id, tenant_id, cycle_anchor, started_at, ended_at)
                  SELECT gen_random_uuid(), x.t, '2026-11-01T00:00:00Z', x.f::timestamptz, x.v::timestamptz
                    FROM (VALUES ('acme', '2026-11-01T00:00:00Z', NULL),
                                 ('frozen', '2026-11-01T00:00:00Z', NULL),
                                 ('expired', '2026-01-01T00:00:00Z', '2026-06-01T00:00:00Z'),
                                 ('ledgerco', '2026-11-01T00:00:00Z', NULL),
                                 ('compco', '2026-11-01T00:00:00Z', NULL),
                                 ('planco', '2026-11-01T00:00:00Z', NULL),
                                 ('bare', '2026-11-01T00:00:00Z', NULL)) AS x(t, f, v);
                INSERT INTO tenant_plan (id, tenant_id, plan_id, valid_from, valid_to)
                  SELECT gen_random_uuid(), x.t, p.id, x.f::timestamptz, x.v::timestamptz
                    FROM (VALUES ('acme', 'PRO', '2026-11-01T00:00:00Z', NULL),
                                 ('frozen', 'PRO', '2026-11-01T00:00:00Z', NULL),
                                 ('expired', 'PRO', '2026-01-01T00:00:00Z', '2026-06-01T00:00:00Z'),
                                 ('ledgerco', 'PRO', '2026-11-01T00:00:00Z', NULL),
                                 ('compco', 'PRO', '2026-11-01T00:00:00Z', NULL),
                                 ('planco', 'PRO', '2026-11-01T00:00:00Z', NULL),
                                 ('bare', 'EMPTY', '2026-11-01T00:00:00Z', NULL)) AS x(t, pc, f, v)
                    JOIN plan p ON p.code = x.pc;
                INSERT INTO tenant_limit_override (id, tenant_plan_id, plan_limit_id, limit_value, reason)
                  SELECT gen_random_uuid(), tp.id, pl.id, 7, 'negotiated'
                    FROM tenant_plan tp JOIN plan_limit pl ON pl.plan_id = tp.plan_id
                    JOIN metric m ON m.id = pl.metric_id
                   WHERE tp.tenant_id = 'acme' AND m.code = 'API_REQUESTS' AND pl.window_seconds IS NOT NULL;
                """);
    }

    static void exec(String sql) throws SQLException {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    static <X> X join(CompletionStage<X> s) { return s.toCompletableFuture().join(); }

    static UsageEvent event(String item, long qty) {
        return eventFor("acme", item, qty);
    }

    static UsageEvent eventFor(String tenant, String item, long qty) {
        return new UsageEvent(UsageEvent.idFor(new TenantId(tenant), item, "ITEMS_PROCESSED"), new TenantId(tenant),
                "ITEMS_PROCESSED", qty, Instant.parse("2026-11-19T00:00:05Z"));
    }

    @Test
    void policyAppliesOverrideAndMapsMetricModes() {
        Policy p = join(policies.policyFor(new TenantId("acme"))).orElseThrow();

        assertEquals(ANCHOR, p.anchor());
        assertEquals(new dev.usageplatform.core.model.Plan("PRO", 1), p.plan());
        assertEquals(4, p.limits().size());
        assertTrue(p.limits().contains(new Limit(API, Type.QUOTA, 7, Duration.ofDays(1), Enforcement.REJECT))); // override, not 100
        assertTrue(p.limits().contains(new Limit(API, Type.QUOTA, 1000, Window.months(1), Enforcement.REJECT))); // calendar month, coexists with the daily quota
        assertTrue(p.limits().contains(new Limit(ITEMS, Type.QUOTA, 10, Duration.ofDays(1), Enforcement.REJECT)));
        assertTrue(p.limits().contains(Limit.concurrency(ITEMS, 5)));
    }

    @Test
    void suspendedExpiredPlanlessAndUnknownTenantsHaveNoPolicy() {
        for (String t : List.of("frozen", "expired", "noplan", "ghost")) {
            assertEquals(Optional.empty(), join(policies.policyFor(new TenantId(t))), t);
        }
    }

    @Test
    void planWithoutLimitsIsAPolicyWithNoLimits() {
        Policy p = join(policies.policyFor(new TenantId("bare"))).orElseThrow();
        assertTrue(p.limits().isEmpty());
    }

    @Test
    void ledgerDeduplicatesByEventId() {
        UsageEvent e = event("ledger-dup", 1);
        assertTrue(join(ledger.record(e, List.of())));
        assertFalse(join(ledger.record(e, List.of())));
    }

    @Test
    void aRedeliveryWithAnotherTimestampIsStillADuplicate() {
        UUID id = UUID.randomUUID();
        TenantId t = new TenantId("acme");
        var first = new UsageEvent(id, t, "ITEMS_PROCESSED", 1, Instant.parse("2026-11-19T00:00:05Z"));
        var restamped = new UsageEvent(id, t, "ITEMS_PROCESSED", 1, Instant.parse("2026-11-19T00:07:00Z"));
        assertTrue(join(ledger.record(first, List.of())));
        assertFalse(join(ledger.record(restamped, List.of())), "usage_event's key includes occurred_at; the id gate must not");
    }

    @Test
    void retentionDeletesOldIdsAndSlotsAndLeavesRecentOnes() throws Exception {
        var retention = new JdbcRetention(ds, null);
        UsageEvent old = eventFor("acme", "retention-old", 1);
        join(ledger.record(old, List.of(new Ledger.Slot("86400", new Period(Instant.parse("2020-01-01T00:00:00Z"), Instant.parse("2020-01-02T00:00:00Z")),
                new dev.usageplatform.core.model.Plan("PRO", 1)))));
        exec("UPDATE usage_event_id SET recorded_at = '2020-01-01T00:00:00Z' WHERE event_id = '" + old.eventId() + "'");
        UsageEvent recent = eventFor("acme", "retention-recent", 1);
        join(ledger.record(recent, List.of()));

        var purged = retention.purgeOlderThan(Instant.parse("2021-01-01T00:00:00Z")); // after the old ones, long before the recent id
        assertTrue(purged.ids() >= 1 && purged.slots() >= 1, purged.toString());
        assertEquals(0, count("SELECT count(*) FROM usage_event_id WHERE event_id = ?", old.eventId()));
        assertEquals(0, count("SELECT count(*) FROM usage_event_slot WHERE event_id = ?", old.eventId()));
        assertEquals(1, count("SELECT count(*) FROM usage_event_id WHERE event_id = ?", recent.eventId()));
    }

    @Test
    void anInstallationOnV1KeepsDeduplicatingItsOldEventsAfterUpgrading() throws Exception {
        String schema = "upgrade_check";
        org.flywaydb.core.Flyway.configure().dataSource(ds).locations("classpath:db/usage").table("usage_schema_history")
                .schemas(schema).defaultSchema(schema).createSchemas(true).target("1").load().migrate();
        UUID id = UUID.randomUUID();
        exec("INSERT INTO " + schema + ".usage_event (event_id, occurred_at, tenant_id, metric_code, quantity) VALUES ('" + id + "', now(), 'old', 'M', 1)");
        UsageSchema.migrate(ds, schema); // now V2: the backfill must have claimed the id
        var upgraded = new JdbcLedger(ds, executor, schema);
        assertFalse(join(upgraded.record(new UsageEvent(id, new TenantId("old"), "M", 1, Instant.parse("2030-01-01T00:00:00Z")), List.of())));
    }

    @Test
    void engineOverRealPolicyAndLedger() {
        var engine = new UsageEngine(policies, new InMemoryCounterStore(CLOCK), ledger, e -> java.util.concurrent.CompletableFuture.completedFuture(null),
                CLOCK, FailMode.CLOSED, Duration.ofMinutes(15));
        TenantId acme = new TenantId("acme");

        assertTrue(join(engine.check(acme, "ITEMS_PROCESSED", 1)).allowed());
        UsageEvent big = event("engine-big", 10);
        assertEquals(Ingest.APPLIED, join(engine.ingest(big)));
        assertEquals(Ingest.DUPLICATE, join(engine.ingest(big)));
        assertEquals("quota_exceeded", join(engine.check(acme, "ITEMS_PROCESSED", 1)).reason());
        assertEquals("no_policy", join(engine.check(new TenantId("frozen"), "API_REQUESTS", 1)).reason());
    }

    @Test
    void summaryAccumulatesWithTheEventThenClosesAndKeepsLateUsageApart() {
        var summaries = new JdbcSummaryStore(ds, executor);
        var engine = new UsageEngine(policies, new InMemoryCounterStore(CLOCK), ledger,
                e -> java.util.concurrent.CompletableFuture.completedFuture(null), CLOCK, FailMode.CLOSED, Duration.ofMinutes(15));
        TenantId t = new TenantId("ledgerco");
        Instant day = Instant.parse("2026-11-19T00:00:00Z");

        UsageEvent first = eventFor("ledgerco", "s1", 4);
        assertEquals(Ingest.APPLIED, join(engine.ingest(first)));
        assertEquals(Ingest.DUPLICATE, join(engine.ingest(first)));
        Summary open = join(summaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow();
        assertEquals(4, open.total());
        assertEquals(day.plus(Duration.ofDays(1)), open.period().end());
        assertFalse(open.closed());
        assertTrue(join(summaries.find(t, "ITEMS_PROCESSED", "m1", Instant.parse("2026-11-01T00:00:00Z"))).isEmpty()); // no monthly quota on ITEMS

        assertTrue(join(summaries.closeEndedBefore(Instant.parse("2026-11-20T00:00:00Z"))) >= 1);
        join(engine.ingest(eventFor("ledgerco", "s2", 2)));
        Summary closed = join(summaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow();
        assertTrue(closed.closed());
        assertEquals(4, closed.total());
        assertEquals(2, closed.lateTotal());
    }

    @Test
    void failedSummaryWriteRollsBackTheEventSoRetryIsNotADuplicate() {
        UsageEvent e = eventFor("ledgerco", "atomic", 1);
        Instant s = Instant.parse("2026-11-19T00:00:00Z");
        var invalidPeriod = List.of(new Ledger.Slot("86400", new Period(s, s), dev.usageplatform.core.model.Plan.UNSPECIFIED)); // violates the period check
        assertThrows(CompletionException.class, () -> join(ledger.record(e, invalidPeriod)));
        assertTrue(join(ledger.record(e, List.of())));
    }

    @Test
    void openSettledSummariesAreListedOnlyOnceTheyHaveBeenQuiet() {
        var summaries = new JdbcSummaryStore(ds, executor);
        var engine = new UsageEngine(policies, new InMemoryCounterStore(CLOCK), ledger,
                e -> java.util.concurrent.CompletableFuture.completedFuture(null), CLOCK, FailMode.CLOSED, Duration.ofMinutes(15));
        Instant farFuturePeriod = Instant.parse("2027-03-10T00:00:00Z"); // never closed by the other tests
        join(engine.ingest(new UsageEvent(UsageEvent.idFor(new TenantId("acme"), "settle-1", "ITEMS_PROCESSED"), new TenantId("acme"),
                "ITEMS_PROCESSED", 2, farFuturePeriod.plusSeconds(5))));

        boolean listedWhenQuiet = join(summaries.openSettledBefore(Instant.now().plus(Duration.ofHours(1)))).stream()
                .anyMatch(s -> s.tenant().value().equals("acme") && s.period().start().equals(farFuturePeriod) && s.total() == 2);
        boolean listedWhenJustWritten = join(summaries.openSettledBefore(Instant.parse("2020-01-01T00:00:00Z"))).stream()
                .anyMatch(s -> s.period().start().equals(farFuturePeriod));
        assertTrue(listedWhenQuiet);
        assertFalse(listedWhenJustWritten);
    }

    static long count(String sql, Object param) throws SQLException {
        try (Connection c = ds.getConnection(); var ps = c.prepareStatement(sql)) {
            ps.setObject(1, param);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    @Test
    void compensationReversesTheOriginalsPeriodAtomicallyAndOnlyOnce() throws SQLException {
        var summaries = new JdbcSummaryStore(ds, executor);
        var engine = new UsageEngine(policies, new InMemoryCounterStore(CLOCK), ledger,
                e -> java.util.concurrent.CompletableFuture.completedFuture(null), CLOCK, FailMode.CLOSED, Duration.ofMinutes(15));
        TenantId t = new TenantId("compco");
        Instant day = Instant.parse("2026-12-05T00:00:00Z");
        UsageEvent original = new UsageEvent(UsageEvent.idFor(t, "c1", "ITEMS_PROCESSED"), t, "ITEMS_PROCESSED", 5, day.plusSeconds(5));
        join(engine.ingest(original));
        assertEquals(original, join(ledger.find(original.eventId())).orElseThrow());

        // reversed three days later, still charged to the original's day
        Compensation c = Compensation.of(original.eventId(), t, "ITEMS_PROCESSED", 2, day.plus(Duration.ofDays(3)), "RESULT_REVERTED");
        assertEquals(Compensate.APPLIED, join(engine.compensate(c)));
        assertEquals(Compensate.DUPLICATE, join(engine.compensate(c)));
        assertEquals(3, join(summaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().total());
        assertTrue(join(ledger.find(c.eventId())).isEmpty()); // a compensation is never an original

        // a second, different reversal of the same original leaves no trace at all
        Compensation other = new Compensation(UUID.randomUUID(), t, "ITEMS_PROCESSED", 1, day.plusSeconds(60), original.eventId(), "AGAIN");
        assertEquals(Compensate.ALREADY_COMPENSATED, join(engine.compensate(other)));
        assertEquals(0, count("SELECT count(*) FROM usage_event WHERE event_id = ?", other.eventId()));
        assertEquals(3, join(summaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().total());

        // after the period closes a reversal becomes a credit and the total stays frozen
        UsageEvent second = new UsageEvent(UsageEvent.idFor(t, "c2", "ITEMS_PROCESSED"), t, "ITEMS_PROCESSED", 4, day.plusSeconds(10));
        join(engine.ingest(second));
        join(summaries.closeEndedBefore(Instant.parse("2026-12-06T00:00:00Z")));
        assertEquals(Compensate.APPLIED, join(engine.compensate(Compensation.of(second.eventId(), t, "ITEMS_PROCESSED", 1,
                Instant.parse("2026-12-07T00:00:00Z"), "RESULT_REVERTED"))));
        Summary closed = join(summaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow();
        assertTrue(closed.closed());
        assertEquals(7, closed.total());
        assertEquals(1, closed.credits());
    }

    @Test
    void changingPlanSwapsTheLimitsButKeepsTheCycleAnchor() {
        var subscriptions = new JdbcSubscriptions(ds);
        TenantId mover = new TenantId("mover");
        subscriptions.subscribe(mover, "PRO", 1, Instant.parse("2026-11-01T00:00:00Z"));
        Policy pro = join(policies.policyFor(mover)).orElseThrow();
        assertEquals(ANCHOR, pro.anchor());
        assertEquals(4, pro.limits().size());

        subscriptions.changePlan(mover, "BASIC", 1, Instant.parse("2026-11-10T00:00:00Z")); // mid-cycle
        Policy basic = join(policies.policyFor(mover)).orElseThrow();
        assertEquals(ANCHOR, basic.anchor()); // the cycle did not move
        assertEquals(new dev.usageplatform.core.model.Plan("BASIC", 1), basic.plan());
        assertEquals(List.of(new Limit(API, Type.QUOTA, 10, Duration.ofDays(1), Enforcement.REJECT)), basic.limits());
    }

    @Test
    void invalidSubscriptionChangesLeaveEverythingAsItWas() {
        var subscriptions = new JdbcSubscriptions(ds);
        TenantId fresh = new TenantId("fresh");
        subscriptions.subscribe(fresh, "PRO", 1, Instant.parse("2026-11-01T00:00:00Z"));

        assertThrows(IllegalStateException.class, () -> subscriptions.subscribe(fresh, "PRO", 1, Instant.parse("2026-11-05T00:00:00Z"))); // already subscribed
        assertThrows(IllegalArgumentException.class, () -> subscriptions.changePlan(fresh, "NOPE", 1, Instant.parse("2026-11-10T00:00:00Z"))); // unknown plan
        assertThrows(IllegalStateException.class, () -> subscriptions.changePlan(fresh, "BASIC", 1, Instant.parse("2026-11-01T00:00:00Z"))); // not after the current start
        assertThrows(IllegalStateException.class, () -> subscriptions.changePlan(new TenantId("noplan"), "BASIC", 1, Instant.parse("2026-11-10T00:00:00Z"))); // nothing to change

        Policy still = join(policies.policyFor(fresh)).orElseThrow(); // the failed unknown-plan change did not close PRO
        assertEquals(4, still.limits().size());
    }

    @Test
    void jobLockExcludesAnotherHolderAndIsFreedWhenTheJobEndsOrFails() {
        var lock = new JdbcJobLock(ds, executor);
        java.util.concurrent.CompletableFuture<String> working = new java.util.concurrent.CompletableFuture<>();

        var started = new java.util.concurrent.CountDownLatch(1);
        var first = lock.runIfAcquired("test-lock-1", () -> { started.countDown(); return working; });
        assertDoesNotThrow(() -> assertTrue(started.await(5, java.util.concurrent.TimeUnit.SECONDS))); // the first holder really holds it
        // while the first holder is running, a second attempt (even from another connection) must not start
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var second = join(lock.runIfAcquired("test-lock-1", () -> { calls.incrementAndGet(); return java.util.concurrent.CompletableFuture.completedFuture("x"); }));
        assertTrue(second.isEmpty());
        assertEquals(0, calls.get());
        assertTrue(join(lock.runIfAcquired("test-lock-other-name", () -> java.util.concurrent.CompletableFuture.completedFuture("ok"))).isPresent());

        working.complete("done");
        assertEquals(Optional.of("done"), join(first));
        assertEquals(Optional.of("again"), join(lock.runIfAcquired("test-lock-1", () -> java.util.concurrent.CompletableFuture.completedFuture("again"))));

        var failing = lock.runIfAcquired("test-lock-1", () -> java.util.concurrent.CompletableFuture.<String>failedFuture(new IllegalStateException("boom")));
        assertThrows(CompletionException.class, () -> join(failing));
        assertTrue(join(lock.runIfAcquired("test-lock-1", () -> java.util.concurrent.CompletableFuture.completedFuture("free"))).isPresent());
    }

    @Test
    void maintenanceReallyClosesAndReconcilesAgainstTheRealDatabase() {
        var summaries = new JdbcSummaryStore(ds, executor);
        var counters = new InMemoryCounterStore(CLOCK);
        var reconciler = new dev.usageplatform.core.CounterReconciler(policies, counters, summaries, CLOCK, Duration.ofMinutes(5));
        var maintenance = new dev.usageplatform.core.PeriodMaintenance(summaries, reconciler, CLOCK, Duration.ofHours(24));
        var scheduler = new dev.usageplatform.core.MaintenanceScheduler(new JdbcJobLock(ds, executor), maintenance, o -> {});

        // a day that ended long before CLOCK minus the 24 h tolerance
        TenantId t = new TenantId("ledgerco");
        Instant day = Instant.parse("2026-10-05T00:00:00Z");
        join(ledger.record(new UsageEvent(UsageEvent.idFor(t, "old-day", "ITEMS_PROCESSED"), t, "ITEMS_PROCESSED", 3, day.plusSeconds(5)),
                List.of(new Ledger.Slot("86400", new Period(day, day.plus(Duration.ofDays(1))), dev.usageplatform.core.model.Plan.UNSPECIFIED))));
        assertFalse(join(summaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().closed());

        var outcome = join(scheduler.runOnce());

        var completed = assertInstanceOf(dev.usageplatform.core.MaintenanceScheduler.RunOutcome.Completed.class, outcome);
        assertTrue(completed.result().closed() >= 1);
        assertTrue(join(summaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().closed());
    }
    @Test
    void theSummaryRecordsThePlanAndFreezesItWhenThePeriodCloses() {
        var subscriptions = new JdbcSubscriptions(ds);
        var summaries = new JdbcSummaryStore(ds, executor);
        var engine = new UsageEngine(policies, new InMemoryCounterStore(CLOCK), ledger,
                e -> java.util.concurrent.CompletableFuture.completedFuture(null), CLOCK, FailMode.CLOSED, Duration.ofMinutes(15));
        TenantId t = new TenantId("planco");
        Instant day = Instant.parse("2026-12-20T00:00:00Z");
        java.util.function.Function<String, UsageEvent> api = item ->
                new UsageEvent(UsageEvent.idFor(t, item, "API_REQUESTS"), t, "API_REQUESTS", 1, day.plusSeconds(5));
        java.util.function.Supplier<dev.usageplatform.core.spi.SummaryStore.Summary> daily = () ->
                join(summaries.find(t, "API_REQUESTS", "86400", day)).orElseThrow();

        join(engine.ingest(api.apply("p1")));
        assertEquals(new dev.usageplatform.core.model.Plan("PRO", 1), daily.get().plan());

        subscriptions.changePlan(t, "BASIC", 1, Instant.parse("2026-11-10T00:00:00Z")); // moves mid-period
        join(engine.ingest(api.apply("p2")));
        assertEquals(new dev.usageplatform.core.model.Plan("BASIC", 1), daily.get().plan());

        join(summaries.closeEndedBefore(Instant.parse("2026-12-21T00:00:00Z")));
        subscriptions.changePlan(t, "PRO", 1, Instant.parse("2026-11-15T00:00:00Z"));
        join(engine.ingest(api.apply("p3"))); // a late event, under yet another plan

        var closed = daily.get();
        assertTrue(closed.closed());
        assertEquals(new dev.usageplatform.core.model.Plan("BASIC", 1), closed.plan()); // frozen at the plan it closed under
        assertEquals(2, closed.total());
        assertEquals(1, closed.lateTotal());
    }

    // ---- concurrency and ordering --------------------------------------------------------------------------

    private static Ledger.Slot daySlot(Instant day) {
        return new Ledger.Slot("86400", new Period(day, day.plus(Duration.ofDays(1))), new dev.usageplatform.core.model.Plan("PRO", 1));
    }

    private static Ledger.Slot monthSlot(Instant monthStart, Instant monthEnd) {
        return new Ledger.Slot("m1", new Period(monthStart, monthEnd), new dev.usageplatform.core.model.Plan("PRO", 1));
    }

    @Test
    void manyConsumersRecordingTheSameEventCountItOnce() throws Exception {
        TenantId t = new TenantId("compco");
        Instant day = Instant.parse("2029-03-10T00:00:00Z");
        UsageEvent e = new UsageEvent(UsageEvent.idFor(t, "same-event", "ITEMS_PROCESSED"), t, "ITEMS_PROCESSED", 4, day.plusSeconds(5));
        var calls = new java.util.ArrayList<java.util.concurrent.CompletableFuture<Boolean>>();
        for (int i = 0; i < 16; i++) calls.add(ledger.record(e, List.of(daySlot(day))).toCompletableFuture());

        assertEquals(1, calls.stream().map(java.util.concurrent.CompletableFuture::join).filter(Boolean::booleanValue).count());
        assertEquals(4, join(new JdbcSummaryStore(ds, executor).find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().total());
    }

    @Test
    void twoDifferentReversalsOfOneOriginalLeaveExactlyOneApplied() {
        TenantId t = new TenantId("compco");
        Instant day = Instant.parse("2029-03-20T00:00:00Z");
        UsageEvent original = new UsageEvent(UsageEvent.idFor(t, "race-original", "ITEMS_PROCESSED"), t, "ITEMS_PROCESSED", 6, day.plusSeconds(5));
        join(ledger.record(original, List.of(daySlot(day))));

        var first = ledger.compensate(new Compensation(UUID.randomUUID(), t, "ITEMS_PROCESSED", 6, day.plusSeconds(60), original.eventId(), "A"), original).toCompletableFuture();
        var second = ledger.compensate(new Compensation(UUID.randomUUID(), t, "ITEMS_PROCESSED", 6, day.plusSeconds(61), original.eventId(), "B"), original).toCompletableFuture();

        var results = java.util.stream.Stream.of(first, second).map(java.util.concurrent.CompletableFuture::join).map(Ledger.Compensated::result).toList();
        assertEquals(1, results.stream().filter(r -> r == Ledger.CompensationResult.APPLIED).count());
        assertEquals(1, results.stream().filter(r -> r == Ledger.CompensationResult.ALREADY_COMPENSATED).count());
        assertEquals(0, join(new JdbcSummaryStore(ds, executor).find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().total()); // reversed once, not twice
    }

    @Test
    void aReversalUndoesTheSlotsTheOriginalWasRecordedInNotTheOnesRecordedLater() {
        var summaries = new JdbcSummaryStore(ds, executor);
        var engine = new UsageEngine(policies, new InMemoryCounterStore(CLOCK), ledger,
                e -> java.util.concurrent.CompletableFuture.completedFuture(null), CLOCK, FailMode.CLOSED, Duration.ofMinutes(15));
        TenantId t = new TenantId("compco");
        Instant day = Instant.parse("2029-05-10T00:00:00Z");
        Instant month = Instant.parse("2029-05-01T00:00:00Z");
        UsageEvent first = new UsageEvent(UsageEvent.idFor(t, "slots-1", "ITEMS_PROCESSED"), t, "ITEMS_PROCESSED", 5, day.plusSeconds(5));
        UsageEvent second = new UsageEvent(UsageEvent.idFor(t, "slots-2", "ITEMS_PROCESSED"), t, "ITEMS_PROCESSED", 7, day.plusSeconds(10));
        join(ledger.record(first, List.of(daySlot(day))));                                                              // before the monthly quota existed
        join(ledger.record(second, List.of(daySlot(day), monthSlot(month, Instant.parse("2029-06-01T00:00:00Z"))))); // after

        assertEquals(Compensate.APPLIED, join(engine.compensate(Compensation.of(first.eventId(), t, "ITEMS_PROCESSED", 5, day.plus(Duration.ofHours(1)), "X"))));

        assertEquals(7, join(summaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().total());
        assertEquals(7, join(summaries.find(t, "ITEMS_PROCESSED", "m1", month)).orElseThrow().total()); // untouched: the first event was never in it
    }

    @Test
    void consumersWritingTheSameRowsInOppositeOrdersNeverDeadlock() {
        TenantId t = new TenantId("compco");
        Instant day = Instant.parse("2029-02-10T00:00:00Z");
        Instant month = Instant.parse("2029-02-01T00:00:00Z");
        var calls = new java.util.ArrayList<java.util.concurrent.CompletableFuture<Boolean>>();
        for (int i = 0; i < 40; i++) {
            UsageEvent e = new UsageEvent(UsageEvent.idFor(t, "order-" + i, "ITEMS_PROCESSED"), t, "ITEMS_PROCESSED", 1, day.plusSeconds(5 + i));
            List<Ledger.Slot> slots = i % 2 == 0
                    ? List.of(daySlot(day), monthSlot(month, Instant.parse("2029-03-01T00:00:00Z")))
                    : List.of(monthSlot(month, Instant.parse("2029-03-01T00:00:00Z")), daySlot(day)); // the order a policy might list them in
            calls.add(ledger.record(e, slots).toCompletableFuture());
        }
        calls.forEach(java.util.concurrent.CompletableFuture::join); // a deadlock would surface here as a failed future
        var summaries = new JdbcSummaryStore(ds, executor);
        assertEquals(40, join(summaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().total());
        assertEquals(40, join(summaries.find(t, "ITEMS_PROCESSED", "m1", month)).orElseThrow().total());
    }

    @Test
    void closingNeverWaitsForARowAnotherTransactionIsWriting() throws Exception {
        var summaries = new JdbcSummaryStore(ds, executor);
        TenantId t = new TenantId("compco");
        Instant day = Instant.parse("2029-04-10T00:00:00Z");
        join(ledger.record(new UsageEvent(UsageEvent.idFor(t, "locked-row", "ITEMS_PROCESSED"), t, "ITEMS_PROCESSED", 2, day.plusSeconds(5)), List.of(daySlot(day))));

        try (Connection writer = ds.getConnection()) {
            writer.setAutoCommit(false);
            try (var lock = writer.prepareStatement("SELECT 1 FROM usage_summary WHERE tenant_id = 'compco' AND metric_code = 'ITEMS_PROCESSED' AND window_key = '86400' AND period_start = ? FOR UPDATE")) {
                lock.setObject(1, day.atOffset(ZoneOffset.UTC));
                lock.executeQuery();
            }
            // the row is locked by the writer: closing must return promptly and leave it open
            join(summaries.closeEndedBefore(Instant.parse("2029-04-11T00:00:00Z")).toCompletableFuture().orTimeout(5, java.util.concurrent.TimeUnit.SECONDS));
            assertFalse(join(summaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().closed());
            writer.commit();
        }
        assertTrue(join(summaries.closeEndedBefore(Instant.parse("2029-04-11T00:00:00Z"))) >= 1); // the next run closes it
        assertTrue(join(summaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().closed());
    }

    @Test
    void aJobLockJobThatReturnsNothingFailsAndFreesTheLock() {
        var lock = new JdbcJobLock(ds, executor);
        assertThrows(CompletionException.class, () -> join(lock.<String>runIfAcquired("null-job", () -> null)));
        assertThrows(CompletionException.class, () -> join(lock.<String>runIfAcquired("null-job", () -> java.util.concurrent.CompletableFuture.completedFuture(null))));
        assertEquals(Optional.of("ok"), join(lock.runIfAcquired("null-job", () -> java.util.concurrent.CompletableFuture.completedFuture("ok"))));
    }

    // ---- a schema of its own ---------------------------------------------------------------------------------

    @Test
    void aDedicatedSchemaCoexistsWithAHostThatAlreadyHasTablesOfTheSameNames() throws Exception {
        // the host's schema is NOT empty and owns tables named like ours, with other columns; its search_path points there
        exec("CREATE SCHEMA hostapp; CREATE TABLE hostapp.tenant (id BIGINT PRIMARY KEY, label TEXT); CREATE TABLE hostapp.plan (code TEXT);"
                + " INSERT INTO hostapp.tenant VALUES (1, 'the host''s own tenant')");
        PGSimpleDataSource hostDs = new PGSimpleDataSource();
        hostDs.setUrl(PG.getJdbcUrl());
        hostDs.setUser(PG.getUsername());
        hostDs.setPassword(PG.getPassword());
        hostDs.setCurrentSchema("hostapp");

        // negative control: migrating into the host's own schema is exactly what used to fail
        var refused = assertThrows(RuntimeException.class, () -> UsageSchema.migrate(hostDs));
        assertTrue(refused.getMessage().toLowerCase().contains("non-empty") || refused.getMessage().toLowerCase().contains("already exists"), refused.getMessage());

        UsageSchema.migrate(hostDs, "usage_dedicated"); // a schema of its own works

        String schema = "usage_dedicated";
        try (Connection c = hostDs.getConnection(); Statement st = c.createStatement()) {
            st.execute("""
                    INSERT INTO usage_dedicated.tenant (id, name) VALUES ('dedic', 'Dedicated');
                    INSERT INTO usage_dedicated.metric (id, code, unit, count_trigger) VALUES (gen_random_uuid(), 'ITEMS_PROCESSED', 'ITEM', 'ON_FINAL_STATUS');
                    INSERT INTO usage_dedicated.plan (id, code, name, version) VALUES (gen_random_uuid(), 'DED', 'Dedicated', 1), (gen_random_uuid(), 'DED2', 'Dedicated 2', 1);
                    INSERT INTO usage_dedicated.plan_limit (id, plan_id, metric_id, limit_type, limit_value, window_seconds, enforcement)
                      SELECT gen_random_uuid(), p.id, m.id, 'QUOTA', 10, 86400, 'REJECT'
                        FROM usage_dedicated.plan p, usage_dedicated.metric m WHERE p.code = 'DED' AND m.code = 'ITEMS_PROCESSED';
                    """);
        }
        var executor2 = Executors.newVirtualThreadPerTaskExecutor();
        var dedicatedPolicies = new JdbcPolicyStore(hostDs, executor2, CLOCK, schema);
        var dedicatedLedger = new JdbcLedger(hostDs, executor2, schema);
        var dedicatedSummaries = new JdbcSummaryStore(hostDs, executor2, schema);
        var subscriptions = new JdbcSubscriptions(hostDs, schema);
        TenantId t = new TenantId("dedic");

        subscriptions.subscribe(t, "DED", 1, ANCHOR);
        Policy policy = join(dedicatedPolicies.policyFor(t)).orElseThrow();
        assertEquals(new dev.usageplatform.core.model.Plan("DED", 1), policy.plan());
        assertEquals(1, policy.limits().size());

        var engine = new UsageEngine(dedicatedPolicies, new InMemoryCounterStore(CLOCK), dedicatedLedger,
                e -> java.util.concurrent.CompletableFuture.completedFuture(null), CLOCK, FailMode.CLOSED, Duration.ofMinutes(15));
        Instant day = Instant.parse("2026-11-19T00:00:00Z");
        UsageEvent usage = new UsageEvent(UsageEvent.idFor(t, "d1", "ITEMS_PROCESSED"), t, "ITEMS_PROCESSED", 4, day.plusSeconds(5));
        assertEquals(Ingest.APPLIED, join(engine.ingest(usage)));
        assertEquals(4, join(dedicatedSummaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().total());

        assertEquals(Compensate.APPLIED, join(engine.compensate(Compensation.of(usage.eventId(), t, "ITEMS_PROCESSED", 1, day.plusSeconds(60), "X"))));
        assertEquals(3, join(dedicatedSummaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().total()); // slots read from the dedicated schema

        assertTrue(join(dedicatedSummaries.closeEndedBefore(Instant.parse("2026-11-20T00:00:00Z"))) >= 1);
        assertTrue(join(dedicatedSummaries.find(t, "ITEMS_PROCESSED", "86400", day)).orElseThrow().closed());

        subscriptions.changePlan(t, "DED2", 1, Instant.parse("2026-11-10T00:00:00Z"));
        assertEquals(new dev.usageplatform.core.model.Plan("DED2", 1), join(dedicatedPolicies.policyFor(t)).orElseThrow().plan());

        // the host's own tables were not touched by any of it
        try (Connection c = hostDs.getConnection(); Statement st = c.createStatement()) {
            var rs = st.executeQuery("SELECT label FROM hostapp.tenant");
            assertTrue(rs.next());
            assertEquals("the host's own tenant", rs.getString(1));
            assertFalse(rs.next());
            var tables = st.executeQuery("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'hostapp'");
            tables.next();
            assertEquals(2, tables.getInt(1)); // still only the host's two tables
        }
    }

    @Test
    void theSchemaNameGoesIntoSqlSoOnlyPlainIdentifiersAreAccepted() {
        for (String bad : new String[]{"usage; DROP TABLE tenant", "Usage", "a-b", "\"quoted\"", "x".repeat(64), "1abc", "a.b"}) {
            assertThrows(IllegalArgumentException.class, () -> new JdbcLedger(ds, executor, bad), bad);
            assertThrows(IllegalArgumentException.class, () -> new JdbcPolicyStore(ds, executor, CLOCK, bad), bad);
            assertThrows(IllegalArgumentException.class, () -> new JdbcSummaryStore(ds, executor, bad), bad);
            assertThrows(IllegalArgumentException.class, () -> new JdbcSubscriptions(ds, bad), bad);
            assertThrows(IllegalArgumentException.class, () -> UsageSchema.migrate(ds, bad), bad);
        }
        assertDoesNotThrow(() -> new JdbcLedger(ds, executor, "usage_2"));
    }
}
