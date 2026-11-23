# Plano: a decisão da cota do engine passa para a biblioteca (sai o `Quotas`)

> Estado: **plano, nada foi alterado.** Pedido do usuário (2026-11-23): remover do código do engine os detalhes de planos e de uso, para a plataforma ser adotada de fato.
> Retoma a "rota B" do [`plano-integracao-engine.md`](plano-integracao-engine.md). **Decisões do usuário (2026-11-23): Redis como contador; ciclo da assinatura com âncora no dia 1 UTC; sem assinatura não emite; limite por segundo global no Redis; alerta pela razão uso/limite no `check`.** O desenho abaixo é o dessas decisões (a versão anterior deste plano, com contador em JDBC, foi descartada).

## 1. O que sai do engine

| Peça | Onde | O que faz hoje |
|---|---|---|
| `Quotas` (+ `QuotaException`, `QuotaAdvice`) | outcome-api | resolve o plano, balde por segundo, contador mensal, `429` com `Retry-After`, métricas `xmloutcome.quota.*` |
| `TenantClient` (+ `Plan`, `Overage`) | outcome-api | lê o plano em `/internal/tenants/{id}/plan` |
| `TenantUsageRepository` + tabela `tenant_usage` | outcome-api (schema `outcome`) | `incrementBelow`: o contador exato, na transação do documento |
| `PlanProjection`, `Plan`, `Overage`, `ClientService.plan`, a rota interna e a role `tenant:plan-read` | usage-api | traduzem a política de uso para o JSON do `Quotas` |
| `outcome.plan-cache-ttl`, alerta `QuotaNearLimit`, painéis da cota | config e ops | |

Fica no outcome-api só um adaptador fino: pergunta à plataforma se pode, e traduz a recusa em `429`.

## 2. O que a biblioteca faz hoje e por que não basta

O `UsageEngine.check(tenant, metric, custo)` já decide RATE e QUOTA por tenant (`REJECT` ou `ALLOW_OVERAGE`), com política vinda de um `PolicyStore` e contadores de um `CounterStore`. Falta, para o caso do engine:

1. **Contador no Redis, fora da transação do documento.** Hoje o `incrementBelow` é o mesmo commit que grava o documento (recusa não deixa rastro). Com Redis o `add` é atômico (Lua, com teto): dois pods nunca passam do limite, mas a gravação do documento é outro sistema. A recusa e a corrida pelo mesmo número precisam **devolver** a cota (`undoCheck`, e um gancho de rollback da transação do Spring), e uma queda entre o `add` e o commit deixa o contador **acima** do real (conservador) até o fim do período. O engine **não tem Redis hoje**: entra na infra.
2. **A política vem de outro serviço.** O schema `usage` é do usage-api (regra D2: nenhum serviço lê tabela de outro). O outcome-api precisa de um `PolicyStore` remoto, com cache e "vale o último lido" quando o usage-api cai.
3. **`Retry-After`.** O `Decision` não diz até quando esperar; o `Quotas` calcula (virada do mês, 1 s). É também um limite conhecido do `UsageGatewayFilter` ("429 sem `Retry-After`").
4. **Duas etapas.** Hoje `admit` (plano e limite por segundo) roda **cedo**, antes de gastar número e storage, e `consume` roda **tarde**, dentro da transação. O `check` avalia tudo de uma vez.

## 3. Desenho (Redis, ciclo da assinatura)

```
outcome-api                                                         usage-api
  UsageEngine (decisão)  ── check RATE (cedo) / check QUOTA (tarde) ──┐
    ├─ PolicyStore  = HttpPolicyStore ── HTTP, cache, stale-if-error ──▶ GET /internal/policies/{tenant}
    └─ CounterStore = RedisCounterStore  ◀── o mesmo Redis ──▶ CounterReconciler (manutenção, Redis + ledger)
  evento de uso DOCUMENTS (mesma transação do documento) ─ outbox ─ fila ─▶ consumidor → ledger e resumo
```

- **Cedo (`admit`):** `check` só de `RATE` na métrica `DOCUMENTS`. Agora **global** (Redis), não por pod.
- **Tarde (`register`):** `check` só de `QUOTA`, imediatamente antes da transação do documento, com **um `at`** (o instante usado no `check`, no evento de uso e no estorno). Se a transação falhar ou voltar, a cota é devolvida (`undoCheck` com o mesmo `at`; um gancho de sincronização do Spring cobre o rollback).
- **O evento de uso `DOCUMENTS`** (métrica de acesso) é gravado no outbox **na mesma transação do documento** (o `OutboxUsageSink`, `ordering=EVENT`) e consumido pelo usage-api: dá o resumo da cota, que o **reconciliador** usa para consertar o contador do Redis se ele for perdido (o contador de acesso só sobe: se o Redis zera, volta ao valor do resumo). É a razão de o contador poder ser aproximado.
- **Compensação manual passa a ter efeito na cota:** estornar um evento `DOCUMENTS` baixa o contador do Redis (o M3 do plano de compensação deixa de ser uma limitação).
- **Sem assinatura, não emite:** `no_policy` vira `403` (código próprio, `SUBSCRIPTION_REQUIRED`). Falha do Redis ou da política segue o `fail-mode` (`OPEN`: a borda não cai por causa da cota); o stale-if-error cobre "vale o último plano lido".

### Mudanças na biblioteca

| # | Mudança | Notas |
|---|---|---|
| L1 | `Decision.retryAfter` (fim do período da cota ou da janela da taxa) | corrige também o `UsageGatewayFilter` (429 sem `Retry-After`) |
| L2 | `UsageEngine.check(..., Set<Limit.Type> só)`: só `RATE` (cedo) ou só `QUOTA` (tarde) | uma métrica `DOCUMENTS` com os dois limites |
| L3 | `HttpPolicyStore` (cliente) e `PolicyJson` (o JSON da política) | token Bearer por `Supplier`; o servidor é a rota do usage-api |
| L4 | `CachedPolicyStore` com **stale-if-error** (vale o último lido por até X) | hoje a falha não é guardada |
| L5 | Starter: o `Ledger` passa a ser opcional (motor só de decisão: sem `usage-postgres`, o estorno e o ingest recusam) | o outcome-api usa o starter com Redis e sem Postgres |
| L6 | Telemetria: razão uso/limite da cota no `check` (distribuição) | substitui o `QuotaNearLimit` |
| L7 | A manutenção (`CounterReconciler`) no usage-api com `usage.counters.type=redis` | hoje o usage-api usa contador em memória (e o aviso do starter some) |

### usage-api
`GET /internal/policies/{tenant}` serve o `PolicyJson` a partir do `PolicyStore` do starter (role `usage:policy-read`, no lugar de `tenant:plan-read`); saem `PlanProjection`, `Plan`, `Overage` e a rota do plano; `usage.counters.type=redis` e `usage.maintenance.enabled=true`; `known-metrics` ganha `DOCUMENTS`.

### outcome-api
`UsageAdmission` (adaptador fino): `admit(tenant)` e `consume(tenant, at)`, `Decision` negada vira `429` (`RATE_LIMITED`, `QUOTA_EXCEEDED`, com `Retry-After`) ou `403 SUBSCRIPTION_REQUIRED`; o evento `DOCUMENTS` no outbox; `usage.redis.uri`. **Saem** `Quotas`, `TenantClient`, `TenantUsageRepository`, a tabela `tenant_usage`, o balde em memória, `outcome.plan-cache-ttl`.

### Infra e ops
Redis no `docker-compose` (e `host.docker.internal:6379` no kind, como Postgres e LocalStack); `USAGE_REDIS_URI` no `local.env` e nos ConfigMaps dos dois serviços; Keycloak: `usage:policy-read` e a troca no client do outcome-api; alertas e painéis da cota pelas métricas da biblioteca (`usage.check`, razão); `ops/demo` já cria o plano com `DOCUMENTS`; o demo e os tenants de teste **precisam de assinatura**.

## 4. Pontos de atenção

1. **Exatidão.** O limite mensal deixa de ser estrito por transação: continua atômico no Redis (nunca passa do teto por corrida), mas pode ficar **acima** do real por queda entre o `add` e o commit, e **abaixo** se o Redis perder dados (o reconciliador sobe até o resumo). Os testes do T-31 (a corrida dá exatamente um `429`; recusa não deixa documento nem evento) continuam, agora sobre o Redis e o `undoCheck`.
2. **Redis é infra nova do engine**, com disponibilidade própria (`fail-mode` decide); em produção, ElastiCache; Cluster não foi testado pela biblioteca (as chaves levam hash tag).
3. **Assinatura obrigatória** quebra o que hoje emite sem plano: tenants e testes precisam de assinatura; um tenant novo só emite depois de assinar (decisão do usuário).
4. **Ciclo da assinatura:** a âncora no dia 1, 00:00 UTC é regra de correção (demo, documentação e, se o usuário quiser, validação no admin).
5. **Latência.** Dois idas e voltas ao Redis por documento aceito (taxa cedo, cota tarde) e a política em cache; medir com `load-tests`.
6. **Dois sistemas de contagem coexistem por pouco tempo:** na troca, o `tenant_usage` do mês corrente não migra (sem produção).

## 5. Fases

| Fase | Entrega | Pronto quando |
|---|---|---|
| F1 | Biblioteca L1 a L7, com testes (Redis real: a corrida pelo último item dá exatamente um `429`; `undoCheck` e o rollback devolvem a cota; o reconciliador conserta um Redis zerado; stale-if-error; `Retry-After`) | `mvn test` do usage-platform verde |
| F2 | usage-api: a rota de política e a role, `counters=redis`, manutenção ligada, `DOCUMENTS` nas métricas conhecidas; saem o plano e a projeção | `UsageApiTest` verde |
| F3 | outcome-api: o adaptador, o evento `DOCUMENTS`, as migrations (some `tenant_usage`), a troca nos dois pontos; os testes de cota do `OutcomeApiTest` reescritos com **os mesmos cenários** (reenvio não conta, recusa não deixa rascunho nem evento, corrida, excedente, política fora com e sem plano lido, **sem assinatura 403**) | `OutcomeApiTest` verde |
| F4 | Infra e ops: Redis, variáveis, Keycloak, alertas e painéis, docs; demo e carga | demo no kind verde e `load-tests` sem regressão relevante de p99 |

## 6. Decisões tomadas

1. **Contador:** Redis. 2. **Período:** ciclo da assinatura, âncora no dia 1 UTC. 3. **Sem política:** exige assinatura. 4. **Por segundo:** global, no Redis. 5. **Alerta:** razão uso/limite no `check`.

### Decisões da F1 (2026-11-23)

6. **Nenhum filter nem interceptor.** O `outcome-api` chama a biblioteca de forma explícita, em dois pontos: `admit(tenant)` (cedo, só `RATE`; também recusa o tenant sem assinatura) e `consume(tenant)` (tarde, dentro da transação do documento, só `QUOTA`, devolvendo o `at` do evento de uso e registrando o `undoCheck` se a transação voltar). Um `HandlerInterceptor` só do limite por segundo fica como evolução, a pedido.
7. **`UsageAdmission` e `UsageDenied` moram no starter** (`spring-tx` já é dependência opcional dele). O `outcome-api` traduz o `UsageDenied` no seu `OutcomeExceptionHandler` (mantém o contrato de erro e o `Retry-After`); a biblioteca traz só um advice opcional.
8. **`HttpPolicyStore` e `PolicyJson` num módulo novo `usage-client`** (leve: `RestClient` do Spring e Jackson), para o `outcome-api` não puxar o admin; o `HttpPolicyStore` recebe o `RestClient` do host (o `ServiceClient` OAuth do engine).
9. **A rota de política no `usage-admin`**, `GET /internal/policies/{tenant}`, com a role própria **`usage:policy-read`** (não reaproveita `usage:read`, que abre tudo); `UsageAdminRoutes` ganha a regra.

## 7. F1 feita (2026-11-23, sem commit)

Implementado na biblioteca, com testes: `Decision.retryAfter` (também no `UsageGatewayFilter`, que responde `Retry-After`); `check`/`undoCheck` por tipo de limite; `CachedPolicyStore` com stale-if-error; módulo
`usage-client` (`PolicyJson`, `HttpPolicyStore.cached`); `GET /internal/policies/{tenant}` com a role `usage:policy-read` no `usage-admin`; no starter, `UsageAdmission`, `UsageDenied` e `usage.admission.metric`,
o `Ledger` opcional (motor só de decisão) e a métrica `usage.quota.ratio`. A corrida pelo último item (40 chamadas simultâneas para 5 unidades dá exatamente 5) e a devolução no rollback foram testadas contra um Redis real.
Falta da F1 só o L7, que é configuração do usage-api (F2: `counters=redis` e a manutenção ligada).

