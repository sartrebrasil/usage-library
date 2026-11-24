package com.example.usage.client;

import com.example.usage.core.model.Limit;
import com.example.usage.core.model.Metric;
import com.example.usage.core.model.Plan;
import com.example.usage.core.model.Policy;
import com.example.usage.core.model.Window;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The wire form of a {@link Policy}: what the usage API serves and {@link HttpPolicyStore} reads. Readers are tolerant of unknown
 * fields; anything that does not make a valid policy is refused with an {@link IllegalArgumentException} (the core's own rules apply:
 * a METER carries no value, a RATE needs a fixed window...).
 * <pre>{@code
 * {"plan": {"code": "PRO", "version": 1}, "anchor": "2026-11-01T00:00:00Z",
 *  "limits": [{"metric": "DOCUMENTS", "mode": "CONSUME_ON_CHECK", "type": "QUOTA", "value": 1000,
 *              "windowMonths": 1, "enforcement": "ALLOW_OVERAGE"}]}
 * }</pre>
 */
public final class PolicyJson {
    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private PolicyJson() {}

    public static String toJson(Policy policy) {
        ObjectNode root = MAPPER.createObjectNode();
        root.putObject("plan").put("code", policy.plan().code()).put("version", policy.plan().version());
        root.put("anchor", policy.anchor().toString());
        ArrayNode limits = root.putArray("limits");
        for (Limit l : policy.limits()) {
            ObjectNode n = limits.addObject();
            n.put("metric", l.metric().code()).put("mode", l.metric().mode().name()).put("type", l.type().name()).put("value", l.value());
            if (l.window() instanceof Window.Fixed f) n.put("windowSeconds", f.length().toSeconds());
            if (l.window() instanceof Window.Months m) n.put("windowMonths", m.count());
            n.put("enforcement", l.enforcement().name());
        }
        return root.toString();
    }

    public static Policy parse(String json) {
        try {
            JsonNode root = MAPPER.readTree(json);
            JsonNode plan = required(root, "plan");
            List<Limit> limits = new ArrayList<>();
            for (JsonNode n : required(root, "limits")) {
                Window window = n.hasNonNull("windowSeconds") ? Window.fixed(Duration.ofSeconds(n.get("windowSeconds").asLong()))
                        : n.hasNonNull("windowMonths") ? Window.months(n.get("windowMonths").asInt()) : null;
                limits.add(new Limit(new Metric(text(n, "metric"), Metric.Mode.valueOf(text(n, "mode"))), Limit.Type.valueOf(text(n, "type")),
                        required(n, "value").asLong(), window, Limit.Enforcement.valueOf(text(n, "enforcement"))));
            }
            return new Policy(new Plan(text(plan, "code"), required(plan, "version").asInt()), Instant.parse(text(root, "anchor")), List.copyOf(limits));
        } catch (JacksonException | java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException("not a policy: " + e.getMessage(), e);
        }
    }

    private static JsonNode required(JsonNode parent, String field) {
        JsonNode n = parent.get(field);
        if (n == null || n.isNull()) throw new IllegalArgumentException("not a policy: " + field + " is missing");
        return n;
    }

    private static String text(JsonNode parent, String field) {
        return required(parent, field).asString();
    }
}
