package com.example.usage.admin.web;

import com.example.problem.ApiException;
import com.example.usage.client.PolicyJson;
import com.example.usage.core.model.Policy;
import com.example.usage.core.model.TenantId;
import com.example.usage.core.spi.PolicyStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;
import java.util.concurrent.CompletionException;

/**
 * The policy of a tenant in force now, for a service that decides with the engine but keeps no plans (its
 * {@code HttpPolicyStore} reads this). Not part of the management API: its own role, {@code usage:policy-read}, which opens
 * nothing else. 404 when the tenant has no policy (no subscription, or suspended): the reader then refuses it.
 */
@RestController
public class PolicyController {
    private final PolicyStore policies;

    PolicyController(PolicyStore policies) {
        this.policies = policies;
    }

    @GetMapping(value = "/internal/policies/{tenant}", produces = MediaType.APPLICATION_JSON_VALUE)
    String policy(@PathVariable String tenant) {
        TenantId id;
        try {
            id = new TenantId(tenant);
        } catch (IllegalArgumentException e) {
            throw ApiException.badRequest("INVALID_TENANT_ID", e.getMessage());
        }
        Optional<Policy> policy;
        try {
            policy = policies.policyFor(id).toCompletableFuture().join();
        } catch (CompletionException e) {
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "POLICY_UNAVAILABLE", "the policy of " + tenant + " could not be read", e.getCause());
        }
        return PolicyJson.toJson(policy.orElseThrow(() -> ApiException.notFound("POLICY_NOT_FOUND", "tenant " + tenant + " has no policy in force")));
    }
}
