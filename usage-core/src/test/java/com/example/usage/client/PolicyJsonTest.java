package com.example.usage.client;

import com.example.usage.core.model.Limit;
import com.example.usage.core.model.Limit.Enforcement;
import com.example.usage.core.model.Limit.Type;
import com.example.usage.core.model.Metric;
import com.example.usage.core.model.Metric.Mode;
import com.example.usage.core.model.Plan;
import com.example.usage.core.model.Policy;
import com.example.usage.core.model.Window;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PolicyJsonTest {
    static final Metric DOCS = new Metric("DOCUMENTS", Mode.CONSUME_ON_CHECK);
    static final Metric BILLED = new Metric("DOCUMENTS_NFE", Mode.REPORT_ONLY);

    @Test
    void everyKindOfLimitSurvivesTheRoundTrip() {
        var policy = new Policy(new Plan("PRO", 2), Instant.parse("2026-11-01T00:00:00Z"), List.of(
                new Limit(DOCS, Type.RATE, 5, Duration.ofSeconds(1), Enforcement.REJECT),
                new Limit(DOCS, Type.QUOTA, 1000, Window.months(1), Enforcement.ALLOW_OVERAGE),
                new Limit(DOCS, Type.QUOTA, 50, Duration.ofDays(1), Enforcement.REJECT),
                Limit.concurrency(BILLED, 3),
                new Limit(BILLED, Type.METER, 0, Window.months(1), Enforcement.REJECT)));

        assertEquals(policy, PolicyJson.parse(PolicyJson.toJson(policy)));
    }

    @Test
    void unknownFieldsAreIgnoredAndWhatIsNotAPolicyIsRefused() {
        var json = PolicyJson.toJson(new Policy(new Plan("PRO", 1), Instant.parse("2026-11-01T00:00:00Z"), List.of()));
        assertEquals(new Plan("PRO", 1), PolicyJson.parse(json.replace("\"limits\"", "\"future\":1,\"limits\"")).plan());

        assertThrows(IllegalArgumentException.class, () -> PolicyJson.parse("not json"));
        assertThrows(IllegalArgumentException.class, () -> PolicyJson.parse("{\"limits\":[]}")); // no plan
        assertThrows(IllegalArgumentException.class, () -> PolicyJson.parse(json.replace("2026-11-01T00:00:00Z", "yesterday")));
        // the core's own rules apply: a METER carries no value
        assertThrows(IllegalArgumentException.class, () -> PolicyJson.parse(
                "{\"plan\":{\"code\":\"P\",\"version\":1},\"anchor\":\"2026-11-01T00:00:00Z\",\"limits\":[{\"metric\":\"X\",\"mode\":\"REPORT_ONLY\","
                        + "\"type\":\"METER\",\"value\":5,\"windowMonths\":1,\"enforcement\":\"REJECT\"}]}"));
    }
}
