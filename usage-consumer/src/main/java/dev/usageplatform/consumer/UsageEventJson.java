package dev.usageplatform.consumer;

import dev.usageplatform.core.model.Compensation;
import dev.usageplatform.core.model.TenantId;
import dev.usageplatform.core.model.UsageEvent;
import dev.usageplatform.core.model.UsageMessage;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The wire contract of usage events (design doc section 13): USAGE and COMPENSATION. Readers are
 * tolerant: unknown fields are ignored, a missing eventType is USAGE and a missing schemaVersion is 1.
 * Anything the consumer cannot safely count is rejected with a short reason code, never guessed.
 * shortcut: clientId, source and attributes are accepted but not carried by the core events yet.
 */
public final class UsageEventJson {
    public static final int SCHEMA_VERSION = 2;

    /** Limits that match the database columns, so a message the database would reject is refused here with a clear reason. */
    static final int MAX_CODE_LENGTH = 60;
    static final int MAX_TENANT_ID_LENGTH = 128;

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();

    private UsageEventJson() {}

    /** What a message body turned out to be. */
    public sealed interface Parsed {
        record Usage(UsageEvent event) implements Parsed {}

        record Reversal(Compensation compensation) implements Parsed {}
    }

    public static final class InvalidEventException extends RuntimeException {
        private final String reason;

        public InvalidEventException(String reason) {
            super(reason);
            this.reason = reason;
        }

        public String reason() { return reason; }
    }

    /** Serializes either kind of message: what a {@code UsageSink} usually needs. */
    public static String toJson(UsageMessage message) {
        return switch (message) {
            case UsageEvent e -> toJson(e);
            case Compensation c -> toJson(c);
        };
    }

    public static String toJson(UsageEvent e) {
        Map<String, Object> m = common("USAGE", e.eventId(), e.tenant(), e.metricCode(), e.quantity(), e.occurredAt());
        return write(m);
    }

    public static String toJson(Compensation c) {
        Map<String, Object> m = common("COMPENSATION", c.eventId(), c.tenant(), c.metricCode(), c.quantity(), c.occurredAt());
        m.put("compensates", c.compensates().toString());
        m.put("reason", c.reason());
        return write(m);
    }

    /** Parses a USAGE message; a COMPENSATION is rejected as unsupported here (use {@link #parse}). */
    public static UsageEvent fromJson(String json) {
        if (parse(json) instanceof Parsed.Usage u) return u.event();
        throw new InvalidEventException("unsupported_event_type");
    }

    public static Parsed parse(String json) {
        JsonNode n = readObject(json);
        String type = text(n, "eventType", "USAGE");
        if (!type.equals("USAGE") && !type.equals("COMPENSATION")) throw new InvalidEventException("unsupported_event_type");
        JsonNode version = n.path("schemaVersion");
        if (!version.isMissingNode() && !(version.isIntegralNumber() && version.canConvertToInt())
                || version.asInt(1) > SCHEMA_VERSION) {
            throw new InvalidEventException("unsupported_schema_version");
        }

        String tenant = text(n, "tenantId", null);
        if (tenant == null || tenant.isBlank()) throw new InvalidEventException("missing_tenant_id");
        if (tenant.length() > MAX_TENANT_ID_LENGTH) throw new InvalidEventException("invalid_tenant_id");
        String metric = text(n, "metricCode", null);
        if (metric == null || metric.isBlank()) throw new InvalidEventException("missing_metric_code");
        if (metric.length() > MAX_CODE_LENGTH) throw new InvalidEventException("invalid_metric_code");
        UUID id = uuid(n, "eventId", "invalid_event_id");
        long quantity = quantity(n);
        Instant at = occurredAt(n);

        if (type.equals("USAGE")) return new Parsed.Usage(new UsageEvent(id, new TenantId(tenant), metric, quantity, at));

        UUID compensates = uuid(n, "compensates", "invalid_compensates");
        String reason = text(n, "reason", null);
        if (reason == null || reason.isBlank()) throw new InvalidEventException("missing_reason");
        if (reason.length() > MAX_CODE_LENGTH) throw new InvalidEventException("invalid_reason");
        return new Parsed.Reversal(new Compensation(id, new TenantId(tenant), metric, quantity, at, compensates, reason));
    }

    private static Map<String, Object> common(String type, UUID id, TenantId tenant, String metric, long quantity, Instant at) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", id.toString());
        m.put("eventType", type);
        m.put("schemaVersion", SCHEMA_VERSION);
        m.put("tenantId", tenant.value());
        m.put("metricCode", metric);
        m.put("quantity", quantity);
        m.put("occurredAt", at.toString());
        return m;
    }

    private static String write(Map<String, Object> m) {
        try {
            return MAPPER.writeValueAsString(m);
        } catch (JacksonException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static JsonNode readObject(String json) {
        try {
            JsonNode n = json == null ? null : MAPPER.readTree(json);
            if (n == null || !n.isObject()) throw new InvalidEventException("invalid_json");
            return n;
        } catch (JacksonException ex) {
            throw new InvalidEventException("invalid_json");
        }
    }

    private static UUID uuid(JsonNode n, String field, String reason) {
        try {
            return UUID.fromString(text(n, field, ""));
        } catch (IllegalArgumentException ex) {
            throw new InvalidEventException(reason);
        }
    }

    private static long quantity(JsonNode n) {
        JsonNode q = n.path("quantity");
        if (!q.isMissingNode() && !(q.isIntegralNumber() && q.canConvertToLong())) throw new InvalidEventException("invalid_quantity");
        long quantity = q.asLong(1);
        if (quantity <= 0 || quantity > UsageEvent.MAX_QUANTITY) throw new InvalidEventException("invalid_quantity");
        return quantity;
    }

    private static Instant occurredAt(JsonNode n) {
        try {
            return Instant.parse(text(n, "occurredAt", ""));
        } catch (DateTimeParseException ex) {
            throw new InvalidEventException("invalid_occurred_at");
        }
    }

    private static String text(JsonNode n, String field, String fallback) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? fallback : v.asText();
    }
}
