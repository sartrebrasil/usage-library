package dev.usageplatform.demo;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * The "backend" behind the gateway. The route in application.yml sends /api/** here, so the demo needs no second
 * process. It echoes the trusted headers the gateway sets, which is what a real service would read.
 */
@RestController
class UpstreamController {

    @GetMapping("/upstream/orders")
    Map<String, String> orders(@RequestHeader(value = "X-Tenant-Id", required = false) String tenant,
                               @RequestHeader(value = "X-Plan-Code", required = false) String plan,
                               @RequestHeader(value = "X-Plan-Version", required = false) String version) {
        return Map.of("tenant", String.valueOf(tenant), "plan", plan + " v" + version);
    }

    /** Items in a batch: the gateway bills X-Item-Count (a client claim: see the README about trusting it). */
    @PostMapping("/upstream/batch")
    Map<String, String> batch(@RequestHeader(value = "X-Item-Count", defaultValue = "1") String items) {
        return Map.of("accepted", items);
    }

    /** A server error: the gateway gives the quota of this request back. */
    @GetMapping("/upstream/fail")
    ResponseEntity<Map<String, String>> fail() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", "backend unavailable"));
    }
}
