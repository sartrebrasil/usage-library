package com.example.usage.postgres;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Qualifies the platform's tables with a schema so they never collide with the host's own tables of the same
 * name (tenant, metric, plan...) and never depend on the connection's search_path. SQL is written with
 * {@code {table}} placeholders; {@link #sql} turns them into {@code "schema"."table"}, or into the bare table name
 * when no schema is configured (the tables then resolve through the search_path, as before).
 * <p>
 * The schema name comes from configuration and goes into SQL text, so it must be a plain lowercase identifier:
 * anything else is refused instead of escaped.
 */
final class Tables {
    private static final Pattern IDENTIFIER = Pattern.compile("[a-z_][a-z0-9_]{0,62}");
    private static final List<String> NAMES = List.of(
            "tenant", "metric", "plan", "plan_limit", "tenant_subscription", "tenant_plan", "tenant_limit_override",
            "usage_event", "usage_event_id", "usage_event_slot", "usage_compensation", "usage_summary");

    private final String prefix;
    private final Map<String, String> resolved = new ConcurrentHashMap<>();

    private Tables(String prefix) {
        this.prefix = prefix;
    }

    /** @param schema null or blank for "no schema": the tables are found through the search_path. */
    static Tables of(String schema) {
        if (schema == null || schema.isBlank()) return new Tables("");
        validate(schema);
        return new Tables("\"" + schema + "\".");
    }

    static void validate(String schema) {
        if (!IDENTIFIER.matcher(schema).matches()) {
            throw new IllegalArgumentException("schema must be a lowercase identifier (letters, digits, underscore), got: " + schema);
        }
    }

    String sql(String template) {
        return resolved.computeIfAbsent(template, t -> {
            String out = t;
            for (String name : NAMES) out = out.replace("{" + name + "}", prefix + name);
            return out;
        });
    }
}
