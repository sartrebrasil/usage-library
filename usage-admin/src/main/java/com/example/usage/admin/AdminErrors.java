package com.example.usage.admin;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Errors of the admin controllers as RFC 9457 problem details with a stable {@code code}. Scoped to this package: the host's own controllers are untouched. */
@RestControllerAdvice(basePackageClasses = AdminErrors.class)
class AdminErrors {

    @ExceptionHandler(AdminException.class)
    ProblemDetail admin(AdminException e) {
        return problem(e.status(), e.code(), e.getMessage());
    }

    @ExceptionHandler(DuplicateKeyException.class)
    ProblemDetail duplicate(DuplicateKeyException e) {
        return problem(HttpStatus.CONFLICT, "ALREADY_EXISTS", "a record with the same key already exists");
    }

    /** A foreign key or check the service did not foresee: still the caller's state, not a server fault. */
    @ExceptionHandler(DataIntegrityViolationException.class)
    ProblemDetail integrity(DataIntegrityViolationException e) {
        return problem(HttpStatus.CONFLICT, "INTEGRITY", "the change conflicts with existing data");
    }

    private static ProblemDetail problem(HttpStatus status, String code, String detail) {
        ProblemDetail p = ProblemDetail.forStatusAndDetail(status, detail);
        p.setProperty("code", code);
        return p;
    }
}
