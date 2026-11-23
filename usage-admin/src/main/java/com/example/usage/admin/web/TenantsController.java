package com.example.usage.admin.web;

import com.example.usage.admin.support.Page;
import com.example.usage.admin.tenant.OverrideAdmin;
import com.example.usage.admin.tenant.SubscriptionAdmin;
import com.example.usage.admin.tenant.TenantAdmin;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/** Tenants, their subscription, their plan and their limit overrides. */
@RestController
@RequestMapping("${usage.admin.base-path:/v1/usage}/tenants")
public class TenantsController {
    private final TenantAdmin tenants;
    private final SubscriptionAdmin subscriptions;
    private final OverrideAdmin overrides;

    TenantsController(TenantAdmin tenants, SubscriptionAdmin subscriptions, OverrideAdmin overrides) {
        this.tenants = tenants;
        this.subscriptions = subscriptions;
        this.overrides = overrides;
    }

    @GetMapping
    Page<TenantAdmin.View> list(@RequestParam(required = false) String q, @RequestParam(required = false) String status,
                                @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "50") int size) {
        return tenants.list(q, status, new Page.Request(page, size));
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    TenantAdmin.View create(@RequestBody TenantAdmin.Create body) {
        return tenants.create(body);
    }

    @GetMapping("/{id}")
    TenantAdmin.View get(@PathVariable String id) {
        return tenants.get(id);
    }

    @PatchMapping("/{id}")
    TenantAdmin.View update(@PathVariable String id, @RequestBody TenantAdmin.Update body) {
        return tenants.update(id, body);
    }

    // ---- subscription and plan ---------------------------------------------------------------

    @GetMapping("/{id}/subscriptions")
    List<SubscriptionAdmin.Subscription> subscriptions(@PathVariable String id) {
        return subscriptions.subscriptions(id);
    }

    @PostMapping("/{id}/subscriptions")
    @ResponseStatus(HttpStatus.CREATED)
    SubscriptionAdmin.Subscription subscribe(@PathVariable String id, @RequestBody SubscriptionAdmin.Subscribe body) {
        return subscriptions.subscribe(id, body);
    }

    /** Ends the open subscription (and the plan on it) now, or at {@code at}. */
    @DeleteMapping("/{id}/subscriptions/open")
    SubscriptionAdmin.Subscription end(@PathVariable String id, @RequestBody(required = false) SubscriptionAdmin.End body) {
        return subscriptions.end(id, body);
    }

    @GetMapping("/{id}/plans")
    List<SubscriptionAdmin.PlanAssignment> planHistory(@PathVariable String id) {
        return subscriptions.planHistory(id);
    }

    /** Moves the tenant to another plan: swaps the limits, keeps the cycle and the counters of the running period. */
    @PutMapping("/{id}/plan")
    SubscriptionAdmin.PlanAssignment changePlan(@PathVariable String id, @RequestBody SubscriptionAdmin.ChangePlan body) {
        return subscriptions.changePlan(id, body);
    }

    // ---- overrides ---------------------------------------------------------------------------

    @GetMapping("/{id}/overrides")
    List<OverrideAdmin.View> overrides(@PathVariable String id) {
        return overrides.list(id);
    }

    @PostMapping("/{id}/overrides")
    @ResponseStatus(HttpStatus.CREATED)
    OverrideAdmin.View createOverride(@PathVariable String id, @RequestBody OverrideAdmin.Create body) {
        return overrides.create(id, body);
    }

    @PutMapping("/{id}/overrides/{overrideId}")
    OverrideAdmin.View updateOverride(@PathVariable String id, @PathVariable UUID overrideId, @RequestBody OverrideAdmin.Update body) {
        return overrides.update(id, overrideId, body);
    }

    @DeleteMapping("/{id}/overrides/{overrideId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void deleteOverride(@PathVariable String id, @PathVariable UUID overrideId) {
        overrides.delete(id, overrideId);
    }
}
