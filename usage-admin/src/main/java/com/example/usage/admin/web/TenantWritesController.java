package com.example.usage.admin.web;

import com.example.usage.admin.tenant.TenantAdmin;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

/**
 * Creating and changing tenants. Registered unless {@code usage.admin.tenants.write=false}, for a host that owns tenant
 * creation itself (it creates them, and this API only lists them).
 */
@RestController
@RequestMapping("${usage.admin.base-path:/v1/usage}/tenants")
public class TenantWritesController {
    private final TenantAdmin tenants;

    TenantWritesController(TenantAdmin tenants) {
        this.tenants = tenants;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    TenantAdmin.View create(@RequestBody TenantAdmin.Create body) {
        return tenants.create(body);
    }

    @PatchMapping("/{id}")
    TenantAdmin.View update(@PathVariable String id, @RequestBody TenantAdmin.Update body) {
        return tenants.update(id, body);
    }
}
