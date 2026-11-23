package com.example.usage.admin.support;

import com.example.problem.ApiException;

import java.util.Set;

/** Input checks that answer 400 with a code naming the field. */
public final class Check {
    private Check() {}

    public static String text(String value, String field, int max) {
        if (value == null || value.isBlank()) throw ApiException.badRequest("FIELD_REQUIRED", field + " is required");
        if (value.length() > max) throw ApiException.badRequest("FIELD_TOO_LONG", field + " is longer than " + max);
        return value.strip();
    }

    public static String optionalText(String value, String field, int max) {
        return value == null || value.isBlank() ? null : text(value, field, max);
    }

    public static String oneOf(String value, Set<String> allowed, String field) {
        if (value == null || !allowed.contains(value)) throw ApiException.badRequest("INVALID_" + field.toUpperCase(), field + " must be one of " + allowed);
        return value;
    }
}
