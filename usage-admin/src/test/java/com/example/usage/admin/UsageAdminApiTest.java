package com.example.usage.admin;

import com.example.usage.core.UsageEngine;
import com.example.usage.core.UsageEngine.FailMode;
import com.example.usage.core.memory.CachedPolicyStore;
import com.example.usage.core.memory.InMemoryCounterStore;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.model.UsageEvent;
import com.example.usage.postgres.JdbcLedger;
import com.example.usage.postgres.JdbcPolicyStore;
import com.example.usage.postgres.UsageSchema;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** The whole API against a real PostgreSQL: roles, the journey from catalog to billing, the rules, the audit trail and the policy the engine reads. */
@Testcontainers
@SpringBootTest(classes = UsageAdminApiTest.App.class, properties = "spring.main.web-application-type=servlet")
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class UsageAdminApiTest {
    @Container
    static final PostgreSQLContainer PG = new PostgreSQLContainer("postgres:16-alpine");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", PG::getJdbcUrl);
        r.add("spring.datasource.username", PG::getUsername);
        r.add("spring.datasource.password", PG::getPassword);
    }

    @Configuration
    @EnableAutoConfiguration
    static class App {
        @Bean
        SecurityFilterChain security(HttpSecurity http) {
            return http.csrf(AbstractHttpConfigurer::disable)
                    .authorizeHttpRequests(a -> {
                        UsageAdminRoutes.apply(a);
                        a.anyRequest().denyAll();
                    })
                    .oauth2ResourceServer(o -> o.jwt(Customizer.withDefaults()))
                    .build();
        }

        @Bean
        JwtDecoder jwtDecoder() {
            return token -> { throw new BadJwtException("tests send no real token"); };
        }

        /** What the starter would wire: the platform's stores and an engine, with the policy cache the admin must invalidate. */
        @Bean
        CachedPolicyStore policies(DataSource ds) {
            UsageSchema.migrate(ds, "usage");
            return new CachedPolicyStore(new JdbcPolicyStore(ds, Executors.newVirtualThreadPerTaskExecutor(), Clock.systemUTC(), "usage"),
                    Clock.systemUTC(), Duration.ofHours(1)); // an hour: only an explicit invalidation can make a change visible
        }

        @Bean
        UsageEngine engine(DataSource ds, CachedPolicyStore policies) {
            return new UsageEngine(policies, new InMemoryCounterStore(Clock.systemUTC()),
                    new JdbcLedger(ds, Executors.newVirtualThreadPerTaskExecutor(), "usage"),
                    message -> java.util.concurrent.CompletableFuture.completedFuture(null), Clock.systemUTC(), FailMode.CLOSED, Duration.ofMinutes(15));
        }
    }

    @Autowired MockMvc mvc;
    @Autowired UsageEngine engine;

    static String planFree, planPro, limitId;
    static String eventId;

    static RequestPostProcessor as(String... authorities) {
        return SecurityMockMvcRequestPostProcessors.jwt().authorities(java.util.Arrays.stream(authorities).map(SimpleGrantedAuthority::new).toArray(SimpleGrantedAuthority[]::new));
    }

    static final RequestPostProcessor ADMIN = as("usage:read", "usage:write", "usage:compensate");

    String body(MockHttpServletRequestBuilder req, int status) throws Exception {
        MvcResult r = mvc.perform(req.with(ADMIN)).andExpect(status().is(status)).andReturn();
        return r.getResponse().getContentAsString();
    }

    MockHttpServletRequestBuilder json(MockHttpServletRequestBuilder b, String content) {
        return b.contentType(MediaType.APPLICATION_JSON).content(content);
    }

    String id(String json) { return JsonPath.read(json, "$.id"); }

    // ---- roles ---------------------------------------------------------------------------------

    @Test @Order(1)
    void everyRouteNeedsItsRole() throws Exception {
        mvc.perform(get("/v1/usage/tenants")).andExpect(status().isUnauthorized());
        mvc.perform(get("/v1/usage/tenants").with(as("usage:write"))).andExpect(status().isForbidden());
        mvc.perform(get("/v1/usage/tenants").with(as("usage:read"))).andExpect(status().isOk());
        mvc.perform(json(post("/v1/usage/tenants"), "{\"id\":\"nope\",\"name\":\"x\"}").with(as("usage:read"))).andExpect(status().isForbidden());
        mvc.perform(json(post("/v1/usage/compensations"), "{}").with(as("usage:read", "usage:write"))).andExpect(status().isForbidden());
        mvc.perform(get("/other").with(ADMIN)).andExpect(status().isForbidden()); // anything the rules do not name stays denied
    }

    // ---- the journey: catalog -> subscription -> decisions -> billing -----------------------------

    @Test @Order(2)
    void catalogRulesAreEnforced() throws Exception {
        body(json(post("/v1/usage/metrics"), "{\"code\":\"API_REQUESTS\",\"unit\":\"request\",\"countTrigger\":\"ON_REQUEST\"}"), 201);
        body(json(post("/v1/usage/metrics"), "{\"code\":\"API_REQUESTS\",\"unit\":\"request\"}"), 409);
        body(json(post("/v1/usage/metrics"), "{\"code\":\"lower\",\"unit\":\"x\"}"), 400);
        body(json(post("/v1/usage/metrics"), "{\"code\":\"ITEMS_PROCESSED\",\"unit\":\"item\",\"countTrigger\":\"ON_FINAL_STATUS\"}"), 201);

        planFree = id(body(json(post("/v1/usage/plans"), "{\"code\":\"FREE\",\"name\":\"Free\"}"), 201));
        String limit = body(json(post("/v1/usage/plans/" + planFree + "/limits"),
                "{\"metric\":\"API_REQUESTS\",\"type\":\"QUOTA\",\"value\":2,\"windowSeconds\":86400}"), 201);
        limitId = id(limit);
        // the platform's own rule: RATE is for access metrics only
        body(json(post("/v1/usage/plans/" + planFree + "/limits"),
                "{\"metric\":\"ITEMS_PROCESSED\",\"type\":\"RATE\",\"value\":5,\"windowSeconds\":60}"), 400);
        body(json(post("/v1/usage/plans/" + planFree + "/limits"),
                "{\"metric\":\"API_REQUESTS\",\"type\":\"QUOTA\",\"value\":2,\"windowSeconds\":86400}"), 409); // same limit twice
        body(json(post("/v1/usage/plans/" + planFree + "/limits"),
                "{\"metric\":\"NOPE\",\"type\":\"QUOTA\",\"value\":2,\"windowSeconds\":60}"), 404);

        planPro = id(body(json(post("/v1/usage/plans"), "{\"code\":\"PRO\",\"name\":\"Pro\"}"), 201));
        body(json(post("/v1/usage/plans/" + planPro + "/limits"),
                "{\"metric\":\"API_REQUESTS\",\"type\":\"QUOTA\",\"value\":5,\"windowSeconds\":86400}"), 201);
        // a second plan of the same code is the next version
        String v2 = body(json(post("/v1/usage/plans"), "{\"code\":\"FREE\",\"name\":\"Free v2\"}"), 201);
        org.junit.jupiter.api.Assertions.assertEquals(2, (int) JsonPath.read(v2, "$.version"));
    }

    @Test @Order(3)
    void aTenantSubscribesAndTheEngineReadsThePolicyTheApiWrote() throws Exception {
        body(json(post("/v1/usage/tenants"), "{\"id\":\"Bad Id\",\"name\":\"x\"}"), 400);
        body(json(post("/v1/usage/tenants"), "{\"id\":\"acme\",\"name\":\"Acme\"}"), 201);
        body(json(post("/v1/usage/tenants"), "{\"id\":\"acme\",\"name\":\"Acme\"}"), 409);
        body(json(post("/v1/usage/tenants/acme/subscriptions"), "{\"planCode\":\"FREE\",\"planVersion\":1}"), 201);
        body(json(post("/v1/usage/tenants/acme/subscriptions"), "{\"planCode\":\"FREE\",\"planVersion\":1}"), 409);
        body(json(post("/v1/usage/tenants/ghost/subscriptions"), "{\"planCode\":\"FREE\",\"planVersion\":1}"), 404);

        TenantId acme = new TenantId("acme");
        for (int i = 0; i < 2; i++) org.junit.jupiter.api.Assertions.assertTrue(engine.check(acme, "API_REQUESTS", 1).toCompletableFuture().join().allowed());
        org.junit.jupiter.api.Assertions.assertEquals("quota_exceeded", engine.check(acme, "API_REQUESTS", 1).toCompletableFuture().join().reason());
    }

    @Test @Order(4)
    void changingThePlanIsVisibleAtOnceDespiteTheCache() throws Exception {
        // the policy is cached for an hour: only the invalidation done by the API lets the new plan act now
        body(json(put("/v1/usage/tenants/acme/plan"), "{\"planCode\":\"PRO\",\"planVersion\":1}"), 200);
        org.junit.jupiter.api.Assertions.assertTrue(engine.check(new TenantId("acme"), "API_REQUESTS", 1).toCompletableFuture().join().allowed());
        String history = body(get("/v1/usage/tenants/acme/plans"), 200);
        org.junit.jupiter.api.Assertions.assertEquals(2, (int) JsonPath.read(history, "$.length()"));
    }

    @Test @Order(5)
    void aPlanWithSubscribersIsFrozenAndCloneIsTheWayOut() throws Exception {
        body(json(post("/v1/usage/plans/" + planFree + "/limits"),
                "{\"metric\":\"ITEMS_PROCESSED\",\"type\":\"QUOTA\",\"value\":9,\"windowMonths\":1}"), 409);
        body(delete("/v1/usage/plans/" + planFree + "/limits/" + limitId), 409);
        String clone = body(json(post("/v1/usage/plans/" + planFree + "/clone"), "{}"), 201);
        org.junit.jupiter.api.Assertions.assertEquals(1, (int) JsonPath.read(clone, "$.limits.length()"));
        String cloneId = JsonPath.read(clone, "$.plan.id");
        body(json(post("/v1/usage/plans/" + cloneId + "/limits"),
                "{\"metric\":\"ITEMS_PROCESSED\",\"type\":\"QUOTA\",\"value\":9,\"windowMonths\":1}"), 201); // the copy is editable
        body(json(patch("/v1/usage/plans/" + planFree), "{\"status\":\"DEPRECATED\"}"), 200);
        body(json(post("/v1/usage/tenants"), "{\"id\":\"globex\",\"name\":\"Globex\"}"), 201);
        body(json(post("/v1/usage/tenants/globex/subscriptions"), "{\"planCode\":\"FREE\",\"planVersion\":1}"), 409); // deprecated
    }

    @Test @Order(6)
    void anOverrideChangesTheLimitOfOneTenant() throws Exception {
        String proLimits = body(get("/v1/usage/plans/" + planPro + "/limits"), 200);
        String proLimit = JsonPath.read(proLimits, "$[0].id");
        body(json(post("/v1/usage/tenants/acme/overrides"), "{\"limitId\":\"" + proLimit + "\",\"value\":1000}"), 400); // a reason is required
        String o = body(json(post("/v1/usage/tenants/acme/overrides"), "{\"limitId\":\"" + proLimit + "\",\"value\":1000,\"reason\":\"contract 42\"}"), 201);
        org.junit.jupiter.api.Assertions.assertEquals(5, (int) JsonPath.read(o, "$.planValue"));
        body(json(post("/v1/usage/tenants/acme/overrides"), "{\"limitId\":\"" + limitId + "\",\"value\":1,\"reason\":\"other plan\"}"), 404);
        body(json(put("/v1/usage/tenants/acme/overrides/" + id(o)), "{\"value\":2000}"), 200);
        body(delete("/v1/usage/tenants/acme/overrides/" + id(o)), 204);
    }

    @Test @Order(7)
    void usageIsSearchableAndACompensationIsAppliedOnce() throws Exception {
        UUID event = UUID.randomUUID();
        engine.ingest(new UsageEvent(event, new TenantId("acme"), "API_REQUESTS", 3, Instant.now())).toCompletableFuture().join();
        eventId = event.toString();

        String summaries = body(get("/v1/usage/summaries?tenantId=acme&metric=API_REQUESTS"), 200);
        org.junit.jupiter.api.Assertions.assertEquals(3, (int) JsonPath.read(summaries, "$.items[0].net"));
        body(get("/v1/usage/events"), 400); // the ledger needs a filter
        body(get("/v1/usage/events?tenantId=acme&size=500"), 400);
        org.junit.jupiter.api.Assertions.assertEquals(1, (int) JsonPath.read(body(get("/v1/usage/events?tenantId=acme"), 200), "$.items.length()"));

        body(json(post("/v1/usage/compensations"), "{\"originalEventId\":\"" + eventId + "\",\"quantity\":9,\"reason\":\"too much\"}"), 400);
        String done = body(json(post("/v1/usage/compensations"), "{\"originalEventId\":\"" + eventId + "\",\"reason\":\"billing error\"}"), 201);
        org.junit.jupiter.api.Assertions.assertEquals("APPLIED", JsonPath.read(done, "$.result"));
        org.junit.jupiter.api.Assertions.assertEquals("DUPLICATE",
                JsonPath.read(body(json(post("/v1/usage/compensations"), "{\"originalEventId\":\"" + eventId + "\",\"reason\":\"billing error\"}"), 201), "$.result"));

        org.junit.jupiter.api.Assertions.assertEquals(0,
                (int) JsonPath.read(body(get("/v1/usage/summaries?tenantId=acme&metric=API_REQUESTS"), 200), "$.items[0].net"));
        org.junit.jupiter.api.Assertions.assertNotNull(JsonPath.read(body(get("/v1/usage/events/" + eventId), 200), "$.compensatedBy"));
    }

    @Test @Order(8)
    void everyChangeLeftATrailWithWhoDidIt() throws Exception {
        String trail = body(get("/v1/usage/audit?tenantId=acme&size=200"), 200);
        java.util.List<String> actions = JsonPath.read(trail, "$.items[*].action");
        org.junit.jupiter.api.Assertions.assertTrue(actions.containsAll(java.util.List.of("CREATE", "SUBSCRIBE", "CHANGE_PLAN", "COMPENSATE")), actions.toString());
        org.junit.jupiter.api.Assertions.assertTrue(((java.util.List<?>) JsonPath.read(trail, "$.items[*].actor")).stream().noneMatch("unknown"::equals));
        String change = body(get("/v1/usage/audit?entityType=tenant_plan&tenantId=acme"), 200);
        org.junit.jupiter.api.Assertions.assertNotNull(JsonPath.read(change, "$.items[0].before.planCode"));
        org.junit.jupiter.api.Assertions.assertNotNull(JsonPath.read(change, "$.items[0].after.planCode"));
    }

    @Test @Order(9)
    void aTenantCanBeSuspendedAndItsSubscriptionEnded() throws Exception {
        body(json(patch("/v1/usage/tenants/acme"), "{\"status\":\"SUSPENDED\"}"), 200);
        org.junit.jupiter.api.Assertions.assertEquals("no_policy", engine.check(new TenantId("acme"), "API_REQUESTS", 1).toCompletableFuture().join().reason());
        body(json(patch("/v1/usage/tenants/acme"), "{\"status\":\"ACTIVE\"}"), 200);
        body(delete("/v1/usage/tenants/acme/subscriptions/open"), 200);
        org.junit.jupiter.api.Assertions.assertEquals("no_policy", engine.check(new TenantId("acme"), "API_REQUESTS", 1).toCompletableFuture().join().reason());
        body(delete("/v1/usage/tenants/acme/subscriptions/open"), 404); // nothing open anymore
    }

    @Test @Order(10)
    void aMetricThatHasUsageKeepsItsTrigger() throws Exception {
        String metrics = body(get("/v1/usage/metrics?q=api"), 200);
        String id = JsonPath.read(metrics, "$.items[0].id");
        body(json(patch("/v1/usage/metrics/" + id), "{\"countTrigger\":\"ON_FINAL_STATUS\"}"), 409);
        body(json(patch("/v1/usage/metrics/" + id), "{\"description\":\"Calls to the API\"}"), 200);
    }
    // ---- what the code review found --------------------------------------------------------------

    @Test @Order(11)
    void aPlanChangeCannotBeDatedInTheFutureAndEndingNeverStrandsAPlan() throws Exception {
        Instant now = Instant.now();
        body(json(post("/v1/usage/tenants"), "{\"id\":\"initech\",\"name\":\"Initech\"}"), 201);
        body(json(post("/v1/usage/tenants/initech/subscriptions"),
                "{\"planCode\":\"PRO\",\"planVersion\":1,\"startsAt\":\"" + now.minus(Duration.ofDays(3)) + "\"}"), 201);
        body(json(put("/v1/usage/tenants/initech/plan"), "{\"planCode\":\"FREE\",\"planVersion\":2,\"at\":\"" + now.plus(Duration.ofDays(1)) + "\"}"), 400);
        body(json(put("/v1/usage/tenants/initech/plan"), "{\"planCode\":\"FREE\",\"planVersion\":2,\"at\":\"" + now.minus(Duration.ofDays(1)) + "\"}"), 200);

        // ending before the current plan started would leave that plan open for good
        body(json(delete("/v1/usage/tenants/initech/subscriptions/open"), "{\"at\":\"" + now.minus(Duration.ofDays(2)) + "\"}"), 400);
        body(json(delete("/v1/usage/tenants/initech/subscriptions/open"), "{\"at\":\"" + now.plus(Duration.ofDays(1)) + "\"}"), 400);
        body(delete("/v1/usage/tenants/initech/subscriptions/open"), 200);
        body(json(post("/v1/usage/tenants/initech/subscriptions"), "{\"planCode\":\"PRO\",\"planVersion\":1}"), 201); // free to subscribe again
    }

    @Test @Order(12)
    void aRetriedCompensationCompletesATrailThatWasCutShortAndEchoesWhatIsStored() throws Exception {
        UUID event = UUID.randomUUID();
        TenantId initech = new TenantId("initech");
        engine.ingest(new UsageEvent(event, initech, "API_REQUESTS", 4, Instant.now())).toCompletableFuture().join();
        // applied by the engine without going through the API: as if the trail write had failed after the ledger committed
        engine.compensate(com.example.usage.core.model.Compensation.of(event, initech, "API_REQUESTS", 4, Instant.now(), "first try"))
                .toCompletableFuture().join();
        org.junit.jupiter.api.Assertions.assertEquals(0, (int) JsonPath.read(body(get("/v1/usage/audit?entityType=compensation&tenantId=initech"), 200), "$.items.length()"));

        String retry = body(json(post("/v1/usage/compensations"), "{\"originalEventId\":\"" + event + "\",\"quantity\":2,\"reason\":\"retry\"}"), 201);
        org.junit.jupiter.api.Assertions.assertEquals("DUPLICATE", JsonPath.read(retry, "$.result"));
        org.junit.jupiter.api.Assertions.assertEquals(4, (int) JsonPath.read(retry, "$.quantity")); // what the ledger holds, not what the retry asked
        org.junit.jupiter.api.Assertions.assertEquals(1, (int) JsonPath.read(body(get("/v1/usage/audit?entityType=compensation&tenantId=initech"), 200), "$.items.length()"));
        body(json(post("/v1/usage/compensations"), "{\"originalEventId\":\"" + event + "\",\"reason\":\"again\"}"), 201);
        org.junit.jupiter.api.Assertions.assertEquals(1, (int) JsonPath.read(body(get("/v1/usage/audit?entityType=compensation&tenantId=initech"), 200), "$.items.length()")); // not twice
    }

    @Test @Order(13)
    void aHugePageIsRefusedNotAnInternalError() throws Exception {
        body(get("/v1/usage/tenants?page=2147483647&size=200"), 400);
        body(get("/v1/usage/audit?page=1000&size=200"), 400); // 200,000 rows deep
    }
}
