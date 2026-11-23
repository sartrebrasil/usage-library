package com.example.usage.admin;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/** The catalog: metrics, plans and their limits. */
@RestController
@RequestMapping("${usage.admin.base-path:/v1/usage}")
class CatalogController {
    private final MetricAdmin metrics;
    private final PlanAdmin plans;

    CatalogController(MetricAdmin metrics, PlanAdmin plans) {
        this.metrics = metrics;
        this.plans = plans;
    }

    @GetMapping("/metrics")
    Page<MetricAdmin.View> metrics(@RequestParam(required = false) String q,
                                   @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "50") int size) {
        return metrics.list(q, new Page.Request(page, size));
    }

    @PostMapping("/metrics")
    @ResponseStatus(HttpStatus.CREATED)
    MetricAdmin.View createMetric(@RequestBody MetricAdmin.Create body) {
        return metrics.create(body);
    }

    @GetMapping("/metrics/{id}")
    MetricAdmin.View metric(@PathVariable UUID id) {
        return metrics.get(id);
    }

    @PatchMapping("/metrics/{id}")
    MetricAdmin.View updateMetric(@PathVariable UUID id, @RequestBody MetricAdmin.Update body) {
        return metrics.update(id, body);
    }

    // ---- plans -------------------------------------------------------------------------------

    @GetMapping("/plans")
    Page<PlanAdmin.PlanView> plans(@RequestParam(required = false) String code, @RequestParam(required = false) String status,
                                   @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "50") int size) {
        return plans.list(code, status, new Page.Request(page, size));
    }

    @PostMapping("/plans")
    @ResponseStatus(HttpStatus.CREATED)
    PlanAdmin.PlanView createPlan(@RequestBody PlanAdmin.CreatePlan body) {
        return plans.create(body);
    }

    @GetMapping("/plans/{id}")
    PlanAdmin.PlanDetail plan(@PathVariable UUID id) {
        return plans.detail(id);
    }

    @PatchMapping("/plans/{id}")
    PlanAdmin.PlanView updatePlan(@PathVariable UUID id, @RequestBody PlanAdmin.UpdatePlan body) {
        return plans.update(id, body);
    }

    /** The next version, with the limits copied: the way to change a plan that already has subscribers. */
    @PostMapping("/plans/{id}/clone")
    @ResponseStatus(HttpStatus.CREATED)
    PlanAdmin.PlanDetail clonePlan(@PathVariable UUID id, @RequestBody(required = false) PlanAdmin.CloneRequest body) {
        return plans.cloneToNextVersion(id, body);
    }

    @GetMapping("/plans/{id}/limits")
    List<PlanAdmin.LimitView> limits(@PathVariable UUID id) {
        return plans.limits(id);
    }

    @PostMapping("/plans/{id}/limits")
    @ResponseStatus(HttpStatus.CREATED)
    PlanAdmin.LimitView addLimit(@PathVariable UUID id, @RequestBody PlanAdmin.LimitRequest body) {
        return plans.addLimit(id, body);
    }

    @PutMapping("/plans/{id}/limits/{limitId}")
    PlanAdmin.LimitView updateLimit(@PathVariable UUID id, @PathVariable UUID limitId, @RequestBody PlanAdmin.LimitRequest body) {
        return plans.updateLimit(id, limitId, body);
    }

    @DeleteMapping("/plans/{id}/limits/{limitId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void deleteLimit(@PathVariable UUID id, @PathVariable UUID limitId) {
        plans.deleteLimit(id, limitId);
    }
}
