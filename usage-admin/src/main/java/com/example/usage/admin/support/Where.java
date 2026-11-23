package com.example.usage.admin.support;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Builds the WHERE of a search from the filters that were actually given, with bound parameters only. */
public final class Where {
    private final List<String> conditions = new ArrayList<>();
    private final Map<String, Object> params = new HashMap<>();

    /** column = value, when a value is given. {@code column} is a constant of the caller, never user input. */
    public Where eq(String column, Object value) {
        if (value != null && !(value instanceof String s && s.isBlank())) {
            String name = "p" + params.size();
            conditions.add(column + " = :" + name);
            params.put(name, value);
        }
        return this;
    }

    public Where prefix(String column, String value) {
        if (value != null && !value.isBlank()) {
            String name = "p" + params.size();
            conditions.add("lower(" + column + ") LIKE :" + name);
            params.put(name, value.toLowerCase().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%");
        }
        return this;
    }

    public Where since(String column, Instant from) {
        if (from != null) {
            String name = "p" + params.size();
            conditions.add(column + " >= :" + name);
            params.put(name, AdminContext.ts(from));
        }
        return this;
    }

    public Where before(String column, Instant to) {
        if (to != null) {
            String name = "p" + params.size();
            conditions.add(column + " < :" + name);
            params.put(name, AdminContext.ts(to));
        }
        return this;
    }

    public boolean isEmpty() { return conditions.isEmpty(); }

    public String sql() { return conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions); }

    public Map<String, Object> params() { return params; }
}
