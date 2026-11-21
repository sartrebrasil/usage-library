package dev.usageplatform.gateway;

import dev.usageplatform.core.Compensation;
import dev.usageplatform.core.Decision;
import dev.usageplatform.core.Plan;
import dev.usageplatform.core.TenantId;
import dev.usageplatform.core.TenantResolver;
import dev.usageplatform.core.UsageEngine;
import dev.usageplatform.core.UsageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

/**
 * Edge enforcement: resolve tenant, ask the engine, forward or refuse, then report usage.
 * Run it after JWT validation (Spring Security's WebFilter already runs before any GlobalFilter).
 * <p>
 * A request that ends in a server error (5xx by default, or an upstream exception, which counts as a 500) is not billed: for every
 * metric the gateway counts, a COMPENSATION of that usage is published right after the usage event, so the
 * quota comes back and the summaries net to zero while the ledger keeps both facts. The refund is sent only
 * when the usage event itself was published, never for an event the platform does not know.
 * <p>
 * Several metrics are checked guards first (result metrics the gateway only reads), then access metrics in the
 * configured order. If one is refused, whatever the earlier access metrics consumed is given back, so a
 * refused request costs nothing. A malformed, repeated or out-of-range cost is a 400, never a guess.
 * <p>
 * A client that drops the connection after the work started is billed as a normal request (no refund): the
 * backend did the work, and a cancelled response must not become a way to avoid metering.
 * <p>
 * shortcuts: rate-limit tokens are not refunded after a 5xx; if the refund event is lost the counter stays high
 * until the period ends (reconciliation never lowers an access counter); 429 carries no Retry-After; a failing
 * TenantResolver surfaces as a plain error (500) instead of 503.
 */
public final class UsageGatewayFilter implements GlobalFilter, Ordered {
    private static final Logger log = LoggerFactory.getLogger(UsageGatewayFilter.class);
    /** Identity headers the gateway owns: whatever a client sends under these prefixes is dropped. */
    private static final List<String> OWNED_HEADER_PREFIXES = List.of("X-Tenant-", "X-Plan-");

    private final UsageEngine engine;
    private final TenantResolver<ServerWebExchange> tenants;
    private final List<MeteredMetric> metrics;
    private final Predicate<ServerWebExchange> internalTraffic;
    private final Clock clock;
    private final IntPredicate refundStatus;

    /** @param internalTraffic true for service-to-service calls that must not be metered (see identity provider marker). */
    public UsageGatewayFilter(UsageEngine engine, TenantResolver<ServerWebExchange> tenants,
                              List<MeteredMetric> metrics, Predicate<ServerWebExchange> internalTraffic, Clock clock) {
        this(engine, tenants, metrics, internalTraffic, clock, status -> status >= 500);
    }

    /** @param refundStatus which response statuses are not billed (default: every 5xx). */
    public UsageGatewayFilter(UsageEngine engine, TenantResolver<ServerWebExchange> tenants,
                              List<MeteredMetric> metrics, Predicate<ServerWebExchange> internalTraffic, Clock clock,
                              IntPredicate refundStatus) {
        this.refundStatus = refundStatus;
        this.engine = engine;
        this.tenants = tenants;
        if (metrics.isEmpty()) throw new IllegalArgumentException("at least one metric is required");
        // guards first: refuse on a result metric before any access counter is touched
        this.metrics = metrics.stream().sorted(Comparator.comparing(MeteredMetric::emitsUsage)).toList();
        this.internalTraffic = internalTraffic;
        this.clock = clock;
    }

    @Override
    public int getOrder() {
        return -50;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        if (internalTraffic.test(exchange)) return chain.filter(exchange);
        return Mono.fromCompletionStage(() -> tenants.resolve(exchange))
                .flatMap(opt -> opt.isPresent()
                        ? meter(opt.get(), exchange, chain)
                        : reject(exchange, HttpStatus.FORBIDDEN, "client_not_linked"));
    }

    private Mono<Void> meter(TenantId tenant, ServerWebExchange exchange, GatewayFilterChain chain) {
        Instant at = clock.instant();
        long[] costs;
        try {
            costs = costsOf(exchange);
        } catch (IllegalArgumentException e) {
            return reject(exchange, HttpStatus.BAD_REQUEST, "invalid_cost");
        }
        long[] cost = costs;
        return Flux.range(0, metrics.size())
                .concatMap(i -> Mono.fromCompletionStage(() -> engine.check(tenant, metrics.get(i).code(), cost[i], at)))
                .takeUntil(d -> !d.allowed())
                .collectList()
                .flatMap(decisions -> {
                    int last = decisions.size() - 1;
                    if (!decisions.get(last).allowed()) {
                        String reason = decisions.get(last).reason();
                        return giveBack(tenant, last, cost, at, decisions).then(Mono.defer(() -> reject(exchange, statusFor(reason), reason)));
                    }
                    Plan plan = decisions.stream().map(Decision::plan).filter(Objects::nonNull).findFirst().orElse(null);
                    return forward(tenant, exchange, chain, decisions.stream().anyMatch(Decision::overage), plan, at, cost);
                });
    }

    /** The cost of each metric for this request. Throws IllegalArgumentException for anything unusable. */
    private long[] costsOf(ServerWebExchange exchange) {
        long[] costs = new long[metrics.size()];
        for (int i = 0; i < costs.length; i++) {
            costs[i] = metrics.get(i).cost().applyAsLong(exchange);
            if (costs[i] < 1 || costs[i] > UsageEngine.MAX_COST) throw new IllegalArgumentException("cost out of range");
        }
        return costs;
    }

    /** Returns what the access metrics before the refused one consumed. Best effort, logged if it fails. */
    private Mono<Void> giveBack(TenantId tenant, int refusedIndex, long[] costs, Instant at, List<Decision> decisions) {
        return Flux.range(0, refusedIndex)
                // a degraded allow (the store failed, nothing was charged) must not be undone: that would subtract a charge
                // that never happened and leave the counter below the truth, which reconciliation never raises for access
                .filter(i -> metrics.get(i).emitsUsage() && decisions.get(i).reason() == null)
                .concatMap(i -> Mono.fromCompletionStage(() -> engine.undoCheck(tenant, metrics.get(i).code(), costs[i], at))
                        .onErrorResume(e -> {
                            log.error("could not give back quota for tenant={} metric={}", tenant.value(), metrics.get(i).code(), e);
                            return Mono.empty();
                        }))
                .then();
    }

    private Mono<Void> forward(TenantId tenant, ServerWebExchange exchange, GatewayFilterChain chain,
                               boolean overage, Plan plan, Instant at, long[] costs) {
        // never trust identity headers coming from the client
        ServerWebExchange mutated = exchange.mutate().request(r -> r.headers(h -> {
            List.copyOf(h.headerNames()).stream()
                    .filter(k -> OWNED_HEADER_PREFIXES.stream().anyMatch(prefix -> k.regionMatches(true, 0, prefix, 0, prefix.length())))
                    .forEach(h::remove);
            h.remove("X-Overage");
            h.set("X-Tenant-Id", tenant.value());
            if (plan != null && plan.isSpecified()) {
                h.set("X-Plan-Code", plan.code());
                h.set("X-Plan-Version", Integer.toString(plan.version()));
            }
            if (overage) h.set("X-Overage", "true");
        })).build();

        // exactly one report per request, whichever way it ends: finished, failed or cancelled by the client
        AtomicBoolean reported = new AtomicBoolean();
        Function<Boolean, Mono<Void>> reportOnce = refund -> Mono.defer(
                () -> reported.compareAndSet(false, true) ? report(tenant, costs, at, refund) : Mono.empty());
        return chain.filter(mutated)
                .then(Mono.defer(() -> reportOnce.apply(serverFailed(exchange))))
                // an upstream exception ends as a 500: refunded exactly when the configured predicate refunds a 500
                // (so refund-from-status=600 turns it off too), and the error still propagates
                .onErrorResume(e -> reportOnce.apply(refundStatus.test(500)).then(Mono.error(e)))
                .doOnCancel(() -> reportOnce.apply(false).subscribe());
    }

    private boolean serverFailed(ServerWebExchange exchange) {
        HttpStatusCode status = exchange.getResponse().getStatusCode();
        return status != null && refundStatus.test(status.value());
    }

    /** Best effort: the response never waits for or fails because of the usage pipeline. */
    private Mono<Void> report(TenantId tenant, long[] costs, Instant at, boolean refund) {
        return Flux.range(0, metrics.size())
                .filter(i -> metrics.get(i).emitsUsage())
                .concatMap(i -> reportOne(tenant, metrics.get(i), costs[i], at, refund))
                .then();
    }

    private Mono<Void> reportOne(TenantId tenant, MeteredMetric metric, long cost, Instant at, boolean refund) {
        UsageEvent usage = new UsageEvent(UUID.randomUUID(), tenant, metric.code(), cost, at);
        Mono<Void> publishUsage = Mono.fromCompletionStage(() -> engine.report(usage));
        Mono<Void> publishRefund = Mono.defer(() -> Mono.fromCompletionStage(() -> engine.reportCompensation(
                Compensation.of(usage.eventId(), tenant, metric.code(), cost, clock.instant(), "UPSTREAM_ERROR"))));
        return (refund ? publishUsage.then(publishRefund) : publishUsage).onErrorResume(e -> {
            log.error("usage event or refund lost for tenant={} metric={}", tenant.value(), metric.code(), e);
            return Mono.empty();
        });
    }

    private static HttpStatus statusFor(String reason) {
        return switch (reason) {
            case Decision.RATE_LIMITED, Decision.QUOTA_EXCEEDED, Decision.CONCURRENCY_EXCEEDED -> HttpStatus.TOO_MANY_REQUESTS;
            case Decision.NO_POLICY -> HttpStatus.FORBIDDEN;
            default -> HttpStatus.SERVICE_UNAVAILABLE;
        };
    }

    private static Mono<Void> reject(ServerWebExchange exchange, HttpStatus status, String code) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        byte[] body = ("{\"error\":\"" + code + "\"}").getBytes(StandardCharsets.UTF_8);
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }
}
