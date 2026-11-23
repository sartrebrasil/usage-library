# Plano: módulo `usage-admin` (gestão por API)

> Estado: **fases 1 a 5 implementadas** no módulo `usage-admin` (leitura, auditoria, catálogo, contratação e compensação).
> Falta a fase 6, a integração no engine, que é trabalho do outro projeto. Decisões na seção 1 e em "Decisões da rodada 2".

## 1. Decisões já tomadas

| Tema | Decisão |
|---|---|
| Stack | Spring **WebMVC** |
| Forma | Módulo **autoconfigurável**: quem declara a dependência recebe os endpoints. Serviço à parte do gateway (fora do caminho quente) |
| Primeiro consumidor | `tenant-api` do projeto engine (ela associa tenant ↔ client do Keycloak) |
| Autenticação | Do host (Keycloak). O módulo **só valida roles**, no padrão `<domínio>:<ação>` do engine; novas roles entram no `docker/keycloak/realm-fiscal.json` do engine |
| Auditoria | **Entra** (tabela nova, migration `V3`; o V1 está congelado) |
| Compensação manual | **Entra** no escopo |

## 2. O que o engine nos impõe (levantado em `engine/`)

1. **A cadeia de segurança é do host.** `ApiSecurity.chain` termina em `anyRequest().denyAll()`: rotas novas são negadas
   até o host liberá-las. Um módulo de biblioteca não pode contar com o `hasAuthority` dele. Solução: o módulo publica
   as regras prontas (`UsageAdminRoutes.rules(...)`) e a `tenant-api` as aplica com **uma linha** no seu
   `SecurityConfiguration`, ao lado de `tenant:admin`. Não criamos `SecurityFilterChain` próprio: perderíamos o formato
   de erro (`ApiProblem`) e a validação de emissor/audiência que o `ApiSecurity` já dá.
2. **Roles do engine:** `<domínio>:<ação>` (`tenant:admin`, `numbering:write`, `issuer:grant`). `tenant:admin` é role de
   operador, fora do `TenantService.ALLOWED_ROLES`: um client de tenant não a recebe. As roles novas seguem a regra:
   são de **plataforma**, nunca entram no `ALLOWED_ROLES`.
3. **Já existem `tenant` e `tenant_plan` na `tenant-api`** (schema `tenant`; `tenant_plan` com `monthly_documents`,
   `documents_per_second`, `overage`, usado por `/internal/tenants/*/plan`, T-31). As nossas tabelas homônimas ficam
   no schema `usage`, então não colidem, mas **o conceito está duplicado** (ver pergunta A).
4. **Migrations do engine rodam por imagem própria** (`Dockerfile.migrations`), não pelo app (`spring.flyway.enabled:
   false`; usuário do banco só do schema `tenant`). O `usage.postgres.migrate=true` não é o caminho lá (ver pergunta B).
5. O cache de política é **por processo**: uma escrita feita na `tenant-api` só chega ao gateway/outcome-api depois do
   `usage.policy-cache-ttl` (padrão 1 min). Para o admin isso é aceitável se estiver documentado.

## 3. Roles propostas

| Role | Dá acesso a |
|---|---|
| `usage:read` | Todas as listagens e pesquisas, inclusive `usage_summary`, `usage_event` e auditoria |
| `usage:write` | Escrita de catálogo (métrica, plano, limites), tenants, assinaturas, troca de plano, overrides |
| `usage:compensate` | Compensação manual (altera o que será cobrado: separada de propósito) |

Adicionar as três em `realm-fiscal.json` (`roles.realm`) e ao client `dev-platform-admin`. Ficam **fora** do
`TenantService.ALLOWED_ROLES`.

## 4. Endpoints (prefixo `/v1/usage`, configurável em `usage.admin.base-path`)

**Leitura e pesquisa (`usage:read`)**: paginação obrigatória em todas as listas.

| Recurso | Operações |
|---|---|
| `/summaries` | listar por tenant, métrica, janela, período, status |
| `/events` | pesquisar por tenant, métrica, intervalo; `/events/{id}` (com a compensação, se houver) |
| `/audit` | pesquisar por entidade, tenant, ator, intervalo |

**Escrita (`usage:write`)**

| Recurso | Operações | Regra |
|---|---|---|
| `/tenants` | `POST`, `PATCH` (nome, status), `GET` | sem `DELETE`; suspender |
| `/metrics` | `POST`, `PATCH` (descrição, unidade), `GET` | `code` imutável; `count_trigger` só muda sem uso registrado |
| `/plans` | `POST` (nova versão), `PATCH` (nome, `DEPRECATED`), `GET`; `POST /plans/{id}/clone` | versão em uso é imutável |
| `/plans/{id}/limits` | `POST`, `PUT`, `DELETE`, `GET` | só em plano **sem assinantes**; senão 409 com a dica de clonar |
| `/tenants/{id}/subscription` | `POST` (assinar), `DELETE` (encerrar), `GET` | uma aberta por tenant; `cycle_anchor` imutável |
| `/tenants/{id}/plan` | `PUT` (trocar de plano), `GET` (histórico) | usa `JdbcSubscriptions.changePlan`; não é CRUD |
| `/tenants/{id}/overrides` | `POST`, `PUT`, `DELETE`, `GET` | `reason` obrigatório |

**Compensação (`usage:compensate`)**: `POST /compensations` com `{originalEventId, quantity, reason}` → `202`. Publica
pelo `UsageSink` do host; o consumidor central aplica (ledger, summary, contadores). O estado (`APPLIED`,
`ORIGINAL_NOT_FOUND`, `ALREADY_COMPENSATED`) aparece depois em `/events/{id}` e na auditoria.

**Sem endpoint:** `usage_event_id`, `usage_event_slot`, `usage_compensation` (mecanismo interno). A retenção
(`JdbcRetention`) pode virar uma operação `POST /maintenance/retention` numa fase posterior, fora deste escopo.

## 5. Auditoria (`V3__usage_audit.sql`)

`usage_audit(id, occurred_at, actor, action, entity_type, entity_id, tenant_id, before jsonb, after jsonb, reason)`.
Ator = `Authentication.getName()` do token (client ou usuário). Toda escrita grava na **mesma transação** da mudança, ou
nenhuma das duas acontece. Sem `UPDATE`/`DELETE` na tabela. A compensação grava a intenção (a aplicação é assíncrona).

## 6. Fases

| Fase | Entrega | Pronto quando |
|---|---|---|
| 0 | Resolver as perguntas A, B e C (seção 7) | Decisões escritas aqui |
| 1 | Módulo `usage-admin`, autoconfiguração, `UsageAdminRoutes`, erros em `ProblemDetail`, endpoints de **leitura** | Testes de MVC (`MockMvc` + Testcontainers) cobrindo 401/403 por role e paginação |
| 2 | `V3` de auditoria + gravação transacional + `/audit` | Toda escrita futura já nasce auditada |
| 3 | Escrita do catálogo: `tenants`, `metrics`, `plans`, `limits` (regra de plano em uso) | Testes das regras de imutabilidade |
| 4 | Contratação: assinatura, troca de plano, overrides, invalidação do cache | Teste: após trocar o plano, a próxima decisão usa o novo |
| 5 | Compensação manual | Teste ponta a ponta até o `usage_summary` (consumidor real) |
| 6 | Integração no engine: dependência na `tenant-api`, roles no `realm-fiscal.json`, regra de rotas, migrations na imagem, docs | `tenant-api` sobe e um token com `usage:read` lista; sem role, 403 |

Estrutura: `usage-admin` depende de `usage-core` e `usage-postgres` (reutiliza o schema e `JdbcSubscriptions`), de
`spring-boot-starter-webmvc` e `spring-security` (opcionais, só para `UsageAdminRoutes`). Entra no `usage-bom`.
Condições: aplicação servlet, `DataSource` presente; `usage.admin.enabled` (padrão `true`).

## 7. Pontos em aberto

- **A. Tenant e plano duplicados.** A `tenant-api` já é dona de `tenant` e do plano de T-31. Quem é a fonte? Proposta:
  a `tenant-api` continua dona do **tenant** (criar/suspender), e ao criar chama o módulo para registrar o
  `usage.tenant` na mesma operação; o `tenant_plan` de T-31 é **substituído** pelo plano de uso (e
  `/internal/tenants/*/plan` passa a ler daqui). Isso precisa de decisão e de uma migração dos dados existentes.
- **B. Banco e migrations.** Mesmo banco que o `tenant`, com schema `usage`? O usuário de banco da `tenant-api` só vê o
  schema `tenant` (D2): precisa de grants no `usage`, e as migrations do módulo entram na imagem de migração.
- **C. Compensação precisa de `UsageSink`.** A `tenant-api` não publica no outbox hoje. Ela passa a ter um sink
  (`usage.messaging.outbox.destination`)? Alternativa: aplicar a compensação direto no ledger, o que exige Redis e o
  `UsageEngine` completo na `tenant-api`. Recomendo o sink.
- **D. Propagação do cache** entre instâncias: aceitar o TTL (1 min) agora; `LISTEN/NOTIFY` fica como evolução.

## 8. Decisões da rodada 2 (valem sobre as perguntas acima)

- **A (fonte do tenant/plano):** um só schema, `usage`. A `tenant-api` vira `usage-api` e seu schema `tenant` passa a ser o
  `usage`; o `tenant` e o plano de uso deste módulo substituem o `tenant` e o `tenant_plan` de T-31. A migração dos dados e a
  mudança de nome ficam para quando a `usage-api` adotar o módulo. Nada muda na `tenant-api` agora.
- **C (compensação):** **sem outbox.** A compensação é aplicada direto pelo `UsageEngine`, na hora; o host só precisa do
  `UsageEngine` (e do `CounterStore`, normalmente Redis, para devolver a cota na hora).
- **D (cache):** 1 minuto de propagação entre instâncias é aceito.
- **B (banco e migrations):** em aberto até a integração; o módulo não roda migrations, quem as roda é o host (`V3` entra
  junto com `V1` e `V2`).

## 9. Para a fase 6 (integração no engine)

1. Dependência `usage-admin` na `tenant-api`/`usage-api` (mais o starter, para o `UsageEngine`).
2. `UsageAdminRoutes.apply(routes, "/v1/usage")` no `SecurityConfiguration`, antes do `denyAll`.
3. Roles `usage:read`, `usage:write`, `usage:compensate` no `realm-fiscal.json` e no client `dev-platform-admin`;
   **não** entram no `TenantService.ALLOWED_ROLES`.
4. Migrations `V1`..`V3` do módulo na imagem de migração, com permissão do usuário do banco no schema.
5. `usage.postgres.schema` igual ao schema onde estão as tabelas.
