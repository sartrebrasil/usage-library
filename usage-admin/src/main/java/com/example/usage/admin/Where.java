package com.example.usage.admin;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Builds the WHERE of a search from the filters that were actually given, with bound parameters only. */
final class Where {
    private final List<String> conditions = new ArrayList<>();
    private final Map<String, Object> params = new HashMap<>();

    /** column = value, when a value is given. {@code column} is a constant of the caller, never user input. */
    Where eq(String column, Object value) {
        if (value != null && !(value instanceof String s && s.isBlank())) {
            String name = "p" + params.size();
            conditions.add(column + " = :" + name);
            params.put(name, value);
        }
        return this;
    }

    Where prefix(String column, String value) {
        if (value != null && !value.isBlank()) {
            String name = "p" + params.size();
            conditions.add("lower(" + column + ") LIKE :" + name);
            params.put(name, value.toLowerCase().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%");
        }
        return this;
    }

    Where since(String column, Instant from) {
        if (from != null) {
            String name = "p" + params.size();
            conditions.add(column + " >= :" + name);
            params.put(name, AdminContext.ts(from));
        }
        return this;
    }

    Where before(String column, Instant to) {
        if (to != null) {
            String name = "p" + params.size();
            conditions.add(column + " < :" + name);
            params.put(name, AdminContext.ts(to));
        }
        return this;
    }

    boolean isEmpty() { return conditions.isEmpty(); }

    String sql() { return conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions); }

    Map<String, Object> params() { return params; }
}
