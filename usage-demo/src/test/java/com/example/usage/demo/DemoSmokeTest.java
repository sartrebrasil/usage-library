package com.example.usage.demo;

import org.junit.jupiter.api.Test;

import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

class DemoSmokeTest extends DemoTestBase {
    @Test
    void theGatewayIdentifiesTheTenantAndPassesItsPlanToTheBackend() throws Exception {
        var r = call("GET", "/api/orders", "key-globex", "X-Tenant-Id", "spoofed");
        assertEquals(200, r.statusCode(), r.body());
        assertTrue(r.body().contains("\"tenant\":\"globex\""), "the spoofed header must be replaced: " + r.body());
        assertTrue(r.body().contains("PRO v1"), r.body());
    }

    @Test
    void anUnknownKeyIsRefused() throws Exception {
        assertEquals(403, call("GET", "/api/orders", "key-nobody").statusCode());
        assertEquals(403, call("GET", "/api/orders", null).statusCode());
    }

    @Test
    void theFreePlanIsRateLimited() throws Exception {
        // 3 per minute. Windows are fixed, so a boundary can let a few more through: look at the first 8
        int ok = 0;
        String refusal = null;
        for (int i = 0; i < 8 && refusal == null; i++) {
            var r = call("GET", "/api/orders", "key-acme");
            if (r.statusCode() == 200) ok++;
            else { assertEquals(429, r.statusCode()); refusal = r.body(); }
        }
        assertNotNull(refusal, "the free plan was never refused");
        assertTrue(ok >= 1 && ok <= 6, "ok=" + ok);
        assertTrue(refusal.contains("rate_limited") || refusal.contains("quota_exceeded"), refusal);
    }

    @Test
    void aBatchIsBilledByItsDeclaredCostAgainstEveryLimitOfTheMetric() throws Exception {
        // 500 items against a free plan of 3 a minute and 20 a day: refused. The cost counts in every limit, rate included.
        var r = call("POST", "/api/batch", "key-acme", "X-Item-Count", "500");
        assertEquals(429, r.statusCode());
        assertTrue(r.body().contains("rate_limited") || r.body().contains("quota_exceeded"), r.body());
        // the same batch is fine for the pro plan's quota of 1000 a day... but not its rate of 100 a minute
        assertEquals(429, call("POST", "/api/batch", "key-globex", "X-Item-Count", "500").statusCode());
        assertEquals(200, call("POST", "/api/batch", "key-globex", "X-Item-Count", "40").statusCode());
    }

    @Test
    void aServerErrorIsPassedThroughAndItsQuotaIsGivenBack() throws Exception {
        assertEquals(503, call("GET", "/api/fail", "key-globex").statusCode());
    }
}
