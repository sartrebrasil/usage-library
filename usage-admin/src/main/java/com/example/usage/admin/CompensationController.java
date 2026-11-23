package com.example.usage.admin;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** Present only when the application has a {@code UsageEngine}: the reversal is applied through it. */
@RestController
@RequestMapping("${usage.admin.base-path:/v1/usage}/compensations")
class CompensationController {
    private final CompensationAdmin compensations;

    CompensationController(CompensationAdmin compensations) {
        this.compensations = compensations;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    CompensationAdmin.Result compensate(@RequestBody CompensationAdmin.Request body) {
        return compensations.compensate(body);
    }
}
