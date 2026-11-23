# Plano: integração do `usage-admin` no engine (Fase 6)

> Estado: **avaliação e plano, nada foi alterado no engine.** Levantado em `engine/` (branch `main`, árvore limpa) em 2026-11-23.
> Complementa a seção 9 de [`plano-usage-admin.md`](plano-usage-admin.md).

## 1. O que o engine já tem (e que se sobrepõe)

| Peça do engine | Onde | O que faz |
|---|---|---|
| `tenant` (id, name, **created_by NOT NULL**, created_at) e `tenant_client` | `tenant-api`, schema `tenant` | tenants e o vínculo com clients do Keycloak |
| `tenant_plan` (**uma linha por tenant**: name, monthly_documents, documents_per_second, overage) | `tenant-api` | plano e limites simples (T-31) |
| `PUT/GET /v1/tenants/{id}/plan`, `GET /internal/tenants/{id}/plan` (role `tenant:plan-read`) | `tenant-api` | grava e serve o plano |
| `Quotas` + `tenant_usage` (V6) + `TenantClient` | `outcome-api` | lê o plano (cache), aplica o limite por segundo (token bucket por pod) e o mensal (**incremento condicional na transação que aceita o documento**) |
| `ApiSecurity.chain` com `denyAll` no fim, roles `<domínio>:<ação>`, `ProblemSecurityHandler` | `platform/security-core` | a cadeia de segurança de todas as APIs |
| `ProblemFactory` / `TenantExceptionHandler` | `tenant-api` | erros no formato da casa (o mesmo starter que o `usage-admin` já usa) |
| Migrations por **imagem** (`Dockerfile.migrations`), com o usuário `tenant_migrator` dono do schema e o `tenant` só com DML | `tenant-api`, `docker/postgres/init.sql` | o app não migra |
| `PackageStructureTest` | `tenant-api` | só olha o código **do módulo** (`src/main/java/.../tenant`): o jar do `usage-admin` não é varrido |

O T-31 já deixou escrito o destino: *"a verdade para cobrança futura é o evento de aceite no outbox
(`DocumentReceived`, com tenant e tipo de documento); os próximos modelos somam pelo evento, não pelo contador"*.

## 2. Achados que mudam o desenho

1. **Colisão de tabelas.** `tenant` e `tenant_plan` existem nos dois lados com **colunas diferentes** (o engine exige
   `created_by`; o nosso `tenant_plan` é um histórico com várias linhas). Colocar tudo no mesmo schema não funciona sem
   uma migração que reconcilie as duas, e o nosso `V1` não serve em cima do schema do engine.
2. **Garantia diferente.** A cota mensal do engine é **exata** (Postgres, mesma transação do aceite, com teste de corrida).
   O `check` do usage-platform é **aproximado** por projeto (Redis é cache de decisão; o ledger é que é exato, depois).
   Trocar o `Quotas` do outcome-api pelo `UsageEngine` enfraquece a garantia e exige **Redis, que o engine não tem**.
3. **Dois endpoints de tenant.** `POST /v1/tenants` (engine: grava `created_by`, associa clients) e
   `POST /v1/usage/tenants` (nosso). O nosso falharia no `created_by NOT NULL` e não conhece o Keycloak.
4. **Migrations de biblioteca ficam fora da imagem hoje.** O comentário do `Dockerfile.migrations` diz que só entram
   os diretórios `db/tenant-api/...` ("uma biblioteca fora dos locations não entra"). O `usage.postgres.migrate=true`
   também não serve (o usuário do app não tem DDL).
5. **Sem Redis no engine.** Por isso a compensação manual (que precisa do `UsageEngine` e de um `CounterStore`) **não
   existe** na `tenant-api`: a rota some sozinha sem o bean. É o comportamento correto até haver Redis.
6. **Calendário.** O engine conta o **mês UTC**; o usage-platform conta pelo **ciclo da assinatura** (âncora). Para ficar
   equivalente, a assinatura nasce com âncora no dia 1, 00:00 UTC.
7. **O renome `tenant-api` → `usage-api` e `schema tenant` → `usage` é grande e independente** da integração: toca a
   pasta e o `artifactId` do módulo, `Dockerfile*`, `devspace.yaml`, `k8s/`, `ops/containers`, o client e a audiência
   `tenant-api` no Keycloak, variáveis `TENANT_*`, os papéis `tenant`/`tenant_migrator` do `init.sql` e os pacotes
   `com.example.engine.tenant`. Misturar os dois aumenta o risco sem ganho: recomendo **depois**.

## 3. Duas rotas de integração

| | **Rota A (recomendada): administrar e medir, sem mudar a decisão** | **Rota B: trocar a decisão do outcome-api pelo `UsageEngine`** |
|---|---|---|
| Gestão (tenants, planos, assinaturas, overrides, auditoria) | pela `usage-admin` na `tenant-api` | idem |
| Quem decide a cota | continua o `Quotas` do outcome-api (exato) | `UsageEngine` (aproximado, precisa de Redis) |
| O plano do outcome-api | `GET /internal/tenants/{id}/plan` passa a ler o plano de uso e devolve o **mesmo JSON de hoje** | o outcome-api deixa de ler o plano |
| Medição para cobrança | etapa B1: consumir o `DocumentReceived` do outbox para o ledger (`usage-consumer`) | já nativa |
| Mudança no outcome-api | **nenhuma** | grande (e muda uma garantia testada) |
| Risco | baixo | alto |

A rota A deixa a porta aberta para a B depois, se a aproximação for aceitável. Hoje não há razão para pagar esse custo.

## 4. Mudanças no próprio `usage-platform` (antes de tocar o engine)

| # | Mudança | Por quê |
|---|---|---|
| L1 | `usage.admin.tenants.manage=false` (ou um bean de política): com ele, `POST/PATCH /tenants` não existem e só a **leitura** fica | o dono do tenant é a `tenant-api` (achado 3) |
| L2 | Tornar o registro do tenant um serviço público e idempotente (`TenantAdmin.register(id, name)`, sem exigir ator HTTP) | para a `tenant-api` registrar o tenant nos dois lugares ao criar |
| L3 | Documentar/publicar: `mvn install` dos módulos e o `usage-bom` no `.m2`; versão no `pom` do engine | o engine consome as libs pelo `.m2`, como faz com messaging e outbox |
| L4 | (se a decisão A abaixo for "mesmo schema") reabrir o `V1`, hoje congelado por um teste nosso | o plano vira único; a biblioteca nunca foi implantada, então o congelamento ainda é só nosso |

## 5. Plano no engine (rota A)

Pelo `CLAUDE.md` do engine, o trabalho novo entra em `docs/tasks.md` **antes** de começar, com ID novo, e commit só com
"pode commitar". Proposta de tarefas, na ordem:

| Etapa | O que | Pronto quando |
|---|---|---|
| E1 | `pom`: `usage.version`, `usage-bom` e as dependências `usage-admin` e `usage-postgres` na `tenant-api` (sem o starter, que puxaria Redis e o motor) | `mvn -pl tenant-api test` verde |
| E2 | **Banco:** schema `usage` e os papéis no `init.sql` (`usage_migrator` dono, o app com DML); **segunda imagem de migração** (`usage-postgres`: `V1`..`V3`, `FLYWAY_SCHEMAS=usage`, histórico próprio); `devspace`/k8s: Job a mais | a imagem migra um banco vazio, e os testes de integração aplicam os mesmos arquivos |
| E3 | **Segurança:** `usage:read`, `usage:write`, `usage:compensate` no `realm-fiscal.json` e no client `dev-platform-admin`; **fora** do `TenantService.ALLOWED_ROLES`; `UsageAdminRoutes.apply(routes, "/v1/usage")` no `TenantConfiguration`, antes do `denyAll` | teste: 401 sem token, 403 sem a role, 200 com ela |
| E4 | **Tenant em dois lugares:** `TenantService.create` registra também em `usage.tenant` (L2), na mesma transação; migração que preenche `usage.tenant` com os tenants existentes | criar tenant pela API antiga aparece em `GET /v1/usage/tenants` |
| E5 | **Plano compatível:** `GET /internal/tenants/{id}/plan` passa a montar o JSON atual a partir do `PolicyStore` (`JdbcPolicyStore`): `QUOTA` mensal de `DOCUMENTS` → `monthlyDocuments`, `RATE` de 1 s → `documentsPerSecond`, `ALLOW_OVERAGE` → `overage ALLOW`; sem plano → vazio (ilimitado, como hoje) | `OutcomeApiTest` passa **sem alteração** contra a `tenant-api` nova |
| E6 | **Migração dos planos:** cada linha de `tenant_plan` vira plano + assinatura (âncora dia 1 UTC) em `usage`; `PUT /v1/tenants/{id}/plan` fica como adaptador (ou é marcado como obsoleto) | tenant com plano antigo devolve os mesmos limites no endpoint interno |
| E7 | Testes da `tenant-api`: a jornada pela `usage-admin` (catálogo → assinatura → plano que o outcome-api lê) | `mvn test` verde |
| E8 | Docs do engine: `tasks.md`, `roadmap.md`, plano §3 | no mesmo commit de cada etapa |

**Depois (fora desta integração):** B1 (consumir `DocumentReceived` para o ledger e o `usage_summary` do billing),
renome para `usage-api`, e a rota B só se a medição provar que vale.

## 6. Riscos

- **Dois lugares com o tenant** (E4): se o registro nos dois falhar pela metade, o tenant existe só num. Mitigação: mesma
  transação (mesmo `DataSource`) e um teste de rollback.
- **Permissões de banco:** o usuário do app precisa de DML no schema `usage` e o migrator de DDL; esquecer isso só
  aparece em ambiente real.
- **Mudança de plano e cache:** vale em até `planCacheTtl` do outcome-api; sem mudança em relação a hoje.
- **`problem.enabled=false`** faria as `ApiException` do `usage-admin` virarem 500 (a `tenant-api` já usa o starter).
- **Versões:** o engine usa Boot 4.1.1 e o usage-platform declara 4.2.0; funcionou nos testes, mas só o build do engine confirma.

## 7. Decisões que preciso de você

1. **Rota A ou B?** (recomendo A.)
2. **Schema:** (S1) `usage` separado ao lado do `tenant`, sem renome agora, ou (S2) unificar em `usage` já, o que obriga a
   reconciliar `tenant`/`tenant_plan` e reabrir o `V1` (L4). Recomendo S1 agora e S2 junto com o renome, se ainda fizer sentido.
3. **Há dados reais** no schema `tenant` de algum ambiente (staging/produção), ou só desenvolvimento? Define o peso de E4 e E6.
4. **`PUT /v1/tenants/{id}/plan`:** manter como adaptador por um tempo, ou remover junto com a migração?
5. **O renome para `usage-api`:** depois (recomendo) ou junto?

## 8. Decisões tomadas (2026-11-23) e o que mudou

O usuário decidiu: **rota A**; **schema único `usage`** (a `tenant-api` vira `usage-api`, e as tabelas do engine e da biblioteca passam a
conviver no mesmo schema); **nada em produção**, então sem migração de dados; **`PUT /v1/tenants/{id}/plan` removido já**; **renome agora**.

Consequências no desenho:

- **Sem sincronização de tenant.** Com um schema só, a tabela `tenant` é uma: o `usage-api` cria o tenant nela, e a `usage-admin` enxerga o mesmo
  registro. O item L2 deixou de existir.
- **A migration inicial foi editada, não houve migration nova.** A diferença entre os dois `tenant` era só o `created_by`; entrou como uma coluna opcional na própria migration inicial. Depois as três migrations viraram uma só (`V202611190001__usage_core.sql`, no padrão de nome do engine `V<aaaammdd><nnnn>__descrição.sql`) e o teste que a congelava saiu: o projeto está em desenvolvimento, sem instalação a atualizar. O `tenant_plan` do engine é removido (não há o que reconciliar com o nosso).
- **`usage.admin.tenants.write=false`** (L1, implementado): o `usage-api` é dono da criação de tenants (`POST /v1/tenants`, que também associa o
  client do Keycloak), e a `usage-admin` fica só com a leitura deles.
- **Uma execução do Flyway.** As migrations da biblioteca (`db/usage`, versões 1 a 4) e as do módulo (`db/usage-api`, versões por data) rodam juntas
  no schema `usage`, com o histórico padrão do Flyway. As versões da biblioteca são menores, então rodam primeiro.

## 9. Execução (2026-11-23)

Feita no engine (sem commit): T-196 a T-200 em `engine/docs/tasks.md`. O que ficou como o plano previa, e o que mudou:

- **Renome completo** (`tenant-api` → `usage-api`, pacote `com.example.engine.usage`, client e audience no Keycloak, schema e papéis do banco, variáveis `USAGE_*`).
  O prefixo de propriedades `tenant.keycloak.*` ficou, para não colidir com o `usage.*` da biblioteca.
- **Schema único** com uma execução do Flyway (`classpath:db/usage` e `classpath:db/usage-api/{vendor}`); a imagem de migração também extrai o SQL do jar `usage-postgres`.
- **`/v1/usage` no `usage-api`** com as roles `usage:read|write|compensate` (no realm e no `dev-platform-admin`); `tenants.write=false`; a rota de compensação **não existe** lá (sem Redis nem `UsageEngine`).
- **Plano compatível:** `PlanProjection` (métrica `DOCUMENTS`: `QUOTA` de 1 mês e `RATE` de 1 s). `PUT/GET /v1/tenants/{id}/plan` e a `tenant_plan` foram removidos; o `outcome-api` não mudou a regra.
- **Pendências:** publicar o usage-platform num remoto para o `ops/install-libraries.sh` (hoje `mvn install` local); validar a imagem de migração e o deploy no kind; a etapa B1 (consumir `DocumentReceived` para o ledger) e a rota B seguem fora.
- **Cadastro de tenant (T-201):** como a tabela `tenant` é uma só, o `usage-api` deixou de ter o seu próprio cadastro (`POST /v1/tenants`, `insert` no repositório); quem cadastra e consulta tenants é
  a `usage-admin` (`/v1/usage/tenants`). O `usage-api` confere a existência com `TenantAdmin.find` e cuida só dos clients do Keycloak. A propriedade `usage.admin.tenants.write=false`
  continua disponível para quem quiser outro dono do cadastro.
