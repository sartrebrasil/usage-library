package com.example.usage.admin.support;

import org.springframework.http.HttpStatus;

/** A refusal the caller can act on: the HTTP status, a stable {@code code} to branch on, and a message for people. */
public final class AdminException extends RuntimeException {
    private final HttpStatus status;
    private final String code;

    private AdminException(HttpStatus status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public HttpStatus status() { return status; }

    public String code() { return code; }

    public static AdminException invalid(String code, String message) { return new AdminException(HttpStatus.BAD_REQUEST, code, message); }

    public static AdminException notFound(String code, String message) { return new AdminException(HttpStatus.NOT_FOUND, code, message); }

    public static AdminException conflict(String code, String message) { return new AdminException(HttpStatus.CONFLICT, code, message); }
}
