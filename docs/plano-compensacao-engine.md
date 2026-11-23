# Plano: compensação manual no engine (`usage-api`)

> Estado: **M1 (no lifecycle, T-204) e M2 feitos**; M4 (compensação por item) feito; falta só M3 (decisão: documentar). Texto original abaixo (o M1 foi para o lifecycle, §9). Antes: **plano; o M2 já está feito** (o `usage-api` usa o starter e tem o `UsageEngine`, T-203) e falta o M1, que alimenta o ledger. Levantado em 2026-11-23, depois dos commits `520d737` (usage-platform) e `91d0ce0` (engine).
> Contexto: [`plano-integracao-engine.md`](plano-integracao-engine.md).

## 1. Por que a rota não existe hoje no `usage-api`

A compensação manual (`POST /v1/usage/compensations`) só aparece quando a aplicação tem um `UsageEngine`. No `usage-api` não há, e mesmo que houvesse
**não haveria o que compensar**: a compensação reverte um evento que **já está no ledger** (`ledger.find(original)`, senão `ORIGINAL_NOT_FOUND`), e hoje nenhum
evento chega lá. O engine só conta o documento aceito no `tenant_usage` do outcome-api (o contador exato que decide a cota). O ledger do schema `usage` está vazio.
Isso é a etapa B1 que o T-31 já previa: *"a verdade para cobrança futura é o evento de aceite"*.

## 2. O que a compensação exige (conferido no código)

| Peça | Exigência | No engine hoje |
|---|---|---|
| Ledger com o evento original | `UsageIngestion.compensate` → `ledger.find` | **vazio** (falta B1) |
| `UsageEngine` | `CompensationAdmin` é condicional a ele | não há |
| `CounterStore` | a compensação baixa os contadores **dos mesmos slots** do original, inclusive de métrica de acesso | o engine **não tem Redis**, mas aqui os contadores da biblioteca não decidem nada (quem decide é o `Quotas` do outcome-api): serve um `InMemoryCounterStore`, que nunca é lido e não deixa o contador abaixo de zero |
| `UsageSink` | só `report`/`reportCompensation`; **`engine.compensate` não o usa** | não precisa |
| Roles | `usage:compensate` | **já existem** (realm e `dev-platform-admin`) |
| Auditoria | gravada pelo `usage-admin` | pronta |

Conclusão: **Redis não é necessário** (meu relato anterior estava pessimista: valia para decidir cota na biblioteca, que aqui não acontece). O que falta de verdade é alimentar o ledger.

## 3. Desenho

### M1: alimentar o ledger (B1), a base de tudo
O `outcome-api` já grava, na **mesma transação**, o documento, o `quotas.consume` e o outbox (`DocumentService.register`). Acrescentar nessa transação um
evento de uso, com o padrão que a biblioteca foi feita para isso (`OutboxUsageSink.sameTransaction`):

```
UsageEvent(eventId = UsageEvent.idFor(tenant, documentId, "DOCUMENTS"), tenant, "DOCUMENTS", 1, acceptedAt)
   → outbox (destino `usage-events`) → fila → usage-api (UsageInboxHandler → UsageEventConsumer → ledger + usage_summary)
```

- É **exatamente consistente com a cota**: se o `consume` recusa ou a transação volta, o evento volta junto; reenvio idempotente não gera evento novo.
- O `eventId` é determinístico por documento: reentrega não conta duas vezes (o ledger deduplica).
- Custa **uma mensagem a mais por documento aceito** no outbox do outcome-api (hoje uma, `DocumentReceived` para o lifecycle): medir o relay (T-108/T-114) antes de aceitar.
- Do lado do `usage-api`: `usage-messaging` (inbox), um receiver com fila, DLQ e `failed` (como o outcome-api e o lifecycle), `usage.consumer.enabled` com `known-metrics: [DOCUMENTS]`.
  Infra: filas no `localstack-init.sh`, ConfigMaps e URLs (`OUTCOME_USAGE_QUEUE_URL`, `USAGE_EVENTS_QUEUE_URL`...), permissões.

### M2: o `UsageEngine` no `usage-api`
Um bean `UsageEngine` (ledger `JdbcLedger`, a política que já existe, `InMemoryCounterStore`, sink que recusa, `FailMode.OPEN`) em `UsageApiConfiguration`. Com ele a rota
`POST /v1/usage/compensations` passa a existir sozinha. Teste no `UsageApiTest`: o evento entra pelo consumidor, a compensação o reverte, o resumo fecha em zero, a trilha registra.

### M3: o contador exato do outcome-api (decisão)
A compensação corrige o que se **cobra** (`usage_summary`), mas o `tenant_usage` do outcome-api, que **decide** `429` no plano `BLOCK`, não muda. Para um estorno dentro do mês
num plano que bloqueia, o tenant ficaria bloqueado por um uso já estornado. Três saídas:

| | O que é | Custo |
|---|---|---|
| **a** | Aceitar e documentar: o estorno corrige a cobrança; a cota do mês só normaliza na virada | zero; exige dizer isso ao operador |
| **b** | O `usage-api` chama uma rota interna do outcome-api (`POST /internal/tenants/{id}/usage/credits`, role própria, idempotente pelo id da compensação) | rota nova, tabela de créditos aplicados, falha parcial (compensou e não creditou) |
| **c** | O `usage-api` publica a compensação e o outcome-api consome e debita | volta o outbox no `usage-api`, que já foi recusado como pesado |

Recomendo **a** primeiro (e só **b** se o caso aparecer): plano `BLOCK` com estorno no mesmo mês é raro, e o custo de b é o de uma consistência entre dois serviços.

### M4: ergonomia
O operador acha o evento por `GET /events?tenantId&from` (o `eventId` é opaco). Acrescentar à compensação uma forma por item (`{tenantId, metric, itemId, reason}`): o id sai de
`UsageEvent.idFor(...)`, então "estornar o documento X" não exige procurar o evento. Pequena, na biblioteca.

## 4. Fases

| Fase | Entrega | Pronto quando |
|---|---|---|
| M1 | outcome-api publica o evento de uso; usage-api o consome | um documento aceito vira uma linha em `usage_event` e soma no `usage_summary`; reenvio e recusa não geram evento; a entrega repetida conta uma vez |
| M2 | `UsageEngine` no `usage-api` | a rota de compensação responde; o resumo e a trilha refletem o estorno |
| M3 | decisão a/b/c | registrada em `tasks.md` (a: texto; b: tarefa nova) |
| M4 | compensação por item | `POST /compensations` aceita `itemId` e dá o mesmo resultado que pelo `eventId` |

Ordem natural: M1 → M2 → M4 → (M3 conforme a decisão). M1 é a maior (dois serviços, mensageria, infra) e é a que traz também o valor de cobrança, independente da compensação.

## 5. Riscos

- **Carga do outbox** do outcome-api (+1 mensagem por documento). Medir com a carga existente (`load-tests`) antes de ligar.
- **Fila a mais**: configuração em dev (LocalStack) e no k8s; o consumidor precisa ser idempotente (é: dedup por `eventId`).
- **Divergência dos dois contadores** (ledger × `tenant_usage`): possível por erro de entrega; o reconciliador da biblioteca só vale com Redis, aqui haveria que comparar os dois por consulta periódica (fora do escopo).
- **Documentos antigos** (anteriores ao M1) não têm evento: não se compensam pela plataforma.

## 6. Decisões que preciso de você

1. Fazer **M1** agora (é pré-requisito)? Ou parar e deixar a compensação para quando o ledger for alimentado por outro motivo?
2. **M3:** a, b ou c?
3. Incluir **M4** (compensação por item)?
4. Documentos já aceitos antes do M1 ficam sem cobrança pela plataforma, e isso é aceitável (não há produção)?

## 7. Atualização (T-203)

O `usage-api` passou a usar o `usage-spring-boot-starter` com `usage.counters.type=memory`. O starter sempre cria o `UsageEngine`, então o **M2 está pronto**: a rota
`POST /v1/usage/compensations` existe (testada: 403 sem `usage:compensate`, 404 `EVENT_NOT_FOUND` com ela, porque o ledger está vazio). O que impede de estornar algo de verdade continua sendo o **M1**.

## 8. M1 refinado (2026-11-23, antes de implementar)

Conferido no código do engine e da biblioteca. O M1 do item 3 está certo no desenho e **precisa dos ajustes abaixo**.

| # | Achado | Ajuste |
|---|---|---|
| R1 | O `OutboxUsageSink` grava `aggregateId = tenant`. O outbox ordena por agregado, então todos os eventos de uso de um tenant sairiam em fila única no relay e na *lane* do consumidor, e **uma linha morta congelaria a cobrança do tenant**. O ledger deduplica por `eventId` e não precisa de ordem | `aggregateBy` no sink: `TENANT` (padrão de hoje) ou `EVENT` (`aggregateId = eventId`). O engine usa `EVENT`. Também evita colidir com os testes do outcome-api, que contam linhas do outbox por `aggregate_id = documentId` |
| R2 | O mês da cota vem de `clock.instant()` dentro de `Quotas.consume`; o evento teria o seu próprio instante. Na virada do mês, o documento podia contar num mês no `tenant_usage` e noutro no ledger | `register` calcula **um** `now` e o passa a `consume(tenant, plan, now)` e ao `occurredAt` do evento |
| R3 | O engine conta o **mês civil UTC**; o resumo da plataforma segue a **âncora da assinatura**. Uma assinatura criada no meio do mês gera períodos que não coincidem com os da cota | Plano medido por mês precisa de assinatura com `startsAt` no **dia 1, 00:00 UTC**. Demo e docs fazem isso; avaliar uma validação no `usage-admin` |
| R4 | O resumo (`usage_summary`) só existe para limite `QUOTA`. Um tenant cujo plano não tem a cota mensal de `DOCUMENTS` tem eventos no ledger e **nenhuma linha de resumo** para o billing | Todo plano cobrado leva a cota mensal de `DOCUMENTS` (com `ALLOW_OVERAGE` e valor alto, se não houver teto). Documentar |
| R5 | O `usage-api` hoje não consome fila nenhuma | Entram `usage-messaging`/`usage-consumer`, a `messaging-library` (provider AWS, `messaging-inbox`), receiver `usage-events` (fila, DLQ e `failed`), `usage.consumer.enabled` com `known-metrics: [DOCUMENTS]` e `usage.messaging.inbox.enabled`. Do outro lado, o `sender` `usage-events` no outcome-api. Infra: filas no `localstack-init.sh`, ConfigMaps, variáveis e o `ENV_PREFIXES` do DevSpace |
| R6 | Tabelas: o `usage-api` só consome (sem outbox); o outcome-api já tem as do outbox. A inbox não ganhou tabela no lifecycle (que só consome e lê o outbox) | Sem migration nova. **Confirmar** subindo o `usage-api` contra o Postgres |
| R7 | O consumidor recusa evento mais velho que `usage.consumer.max-past` (90 d) e mais novo que `max-future` (5 min) | Atraso de fila acima disso vai para a DLQ: alertar profundidade da DLQ (métricas `inbox.*`) |
| R8 | Testes: o outcome-api não tem broker nos testes (relay e consumidor desligados) | (a) outcome-api: um evento `usage.event` no outbox por documento **novo** aceito, nenhum em reenvio idempotente, recusa por validação ou `429`; payload lido por `UsageEventJson` (contrato). (b) usage-api: o corpo entra pelo consumidor e chega ao ledger e ao resumo; entrega repetida conta uma vez. (c) Fim a fim só no kind: `ops/demo/run.sh` confere o total do `t-dev` em `/v1/usage/summaries` (**não rodável aqui**) |
| R9 | +1 mensagem por documento no outbox e no relay (4 workers, T-114) | Medir com `load-tests` antes de dar por concluído; reabrir o dimensionamento se o relay encostar |
| R10 | Segurança | A fila precisa de ACL (o `tenantId` da mensagem é confiável); localmente, o LocalStack |

Ordem: biblioteca (R1) → outcome-api (R2, publicação) → usage-api (consumo) → infra e docs → demo → medir a carga.

## 9. Onde nasce o evento de uso: no lifecycle, não no aceite (proposta, aguardando decisões)

Pergunta do usuário (2026-11-23): o evento de cobrança deveria sair do **lifecycle**, quando o documento é autorizado ou cancelado, e não do outbox do aceite no outcome-api?
A resposta é **sim, para a cobrança**: são dois fatos diferentes, e o modelo da biblioteca já os separa.

| Fato | Quem mede | Quando | Para quê |
|---|---|---|---|
| **Cota** (proteger a capacidade) | `Quotas` do outcome-api, `tenant_usage` | no aceite (`202`), exato, na transação do documento (T-31) | `429`; **continua como está** |
| **Cobrança** (o que o cliente paga) | o processamento, ao terminar | quando o resultado é gravado (AUTHORIZED, etc.) | `usage_summary`; é uma **métrica de resultado** (`REPORT_ONLY`) da biblioteca, o padrão do `QueueUsageAdapter` ("conta quando o resultado chega, no mesmo commit do resultado") |

No lifecycle o ponto principal é `Stepper.publish(Process)` (**não é o único: ver 9.1**), que já grava na mesma transação do estado o `DocumentStatusChanged` no outbox. Ali entra o evento de uso, com o mesmo padrão
(`OutboxUsageSink.sameTransaction`), só para documento (não evento filho, não inutilização) em estado cobrável.

O que muda no M1:
- **Quem publica:** `lifecycle-worker` (que já tem outbox, mensageria e AWS), não o `outcome-api`. O `outcome-api` não muda. Some o R2 (instante único com a cota): são fatos diferentes.
- **Continuam:** R1 (`aggregateBy = EVENT`), R3 (âncora no dia 1 se o plano for mensal), R4 (o plano cobrado leva `QUOTA` da métrica cobrada), R5 a R10 (consumo no `usage-api`, filas, testes, carga).
- **O número da cota e o da cobrança passam a diferir de propósito:** aceitos (inclui os que a SEFAZ rejeita depois) contra autorizados.
- **O T-31 dizia** "a verdade para cobrança futura é o evento de aceite (`DocumentReceived`)": isso é **revisto** por esta decisão e precisa ser corrigido no `tasks.md` do engine.

Decisões necessárias:
1. **Quais estados cobram:** AUTHORIZED, e DENIED (denegada)? REJECTED e FAILED não.
2. **Cancelamento:** (a) não estorna (o documento foi emitido), (b) estorna, com uma `Compensation` do evento original (a biblioteca já trata "o estorno chegou antes do uso" com retry).
3. **Granularidade da métrica:** uma por modelo (`DOCUMENTS_NFE`, `DOCUMENTS_NFCE`, a partir do `documentType`) ou uma só? Plano e cobrança por modelo pedem uma por modelo.
4. **`occurredAt`:** a hora da autorização (simples; a virada do mês pode jogar o documento no mês seguinte à cota) ou a do aceite (exige levar a data no `DocumentReceived`).

### 9.1 Revisão da proposta (advisor, 2026-11-23) e o que foi conferido no código

1. **"Cancelado" tem três leituras**, não duas: (a) não cobra nada; (b) estorna a nota (`Compensation` do evento original); (c) o **cancelamento é um fato cobrável por si** (evento na SEFAZ, métrica própria, vindo do processo filho `REGISTERED`). A CC-e cai na mesma pergunta.
2. **O gancho não é único.** Conferido: `Stepper.cancelParent` grava o status do pai com um `outbox.enqueue` **próprio** (fora do `publish`), e os eventos filhos entram pelo ramo `p.child() != null`. Cobrir cancelamento ou eventos exige gancho também nesses caminhos.
3. **Identidade do evento.** O processo é único por `document_id`, não por chave de acesso. Pode haver dois documentos que chegam a AUTHORIZED com a mesma chave (reenvio e duplicidade resolvida por CONSULTING). Com `idFor(tenant, documentId, …)` cobraria duas vezes. **Usar a chave de acesso como chave natural** (`idFor(tenant, accessKey, metric)`): ela existe em todo AUTHORIZED e DENIED.
4. **`occurredAt` do fato gravado**, não de `clock.instant()`: o README da biblioteca exige isso do processador, para o reprocessamento gerar o mesmo evento. O processo tem `state_since` (muda só quando o estado muda): é o instante da entrada no estado terminal.
5. **Inbox do `usage-api`** (avisos do README, achados com broker real): `retry.retryable` precisa incluir `RetryableUsageException` (senão uma queda do banco manda o uso para `failed`); o `envelope` do receiver tem de ser `auto` ou `outbox` (confirmar como o `outbox-broker-messaging` do engine envelopa); com `aggregateBy = EVENT` não há ordem entre uso e estorno, então se o cancelamento virar `Compensation` o nível de retry do `original_not_found` precisa existir no SQS (a espera por TTL).
6. **Resumo só para `QUOTA`:** "cota enorme com `ALLOW_OVERAGE`" é um contorno de uma lacuna da biblioteca justamente no caso central (medir para cobrar). Decidir: aceitar o contorno agora e registrar como débito, **ou** dar à biblioteca um modo "só medir".
7. **Plano-exemplo completo**, para o operador não se confundir: cada plano leva `DOCUMENTS` (a cota da borda; **nunca recebe evento**, seu resumo fica vazio) e `DOCUMENTS_NFE` / `DOCUMENTS_NFCE` (a cobrança). Entra neste doc e no `ops/demo/run.sh`.
8. **Testes no lifecycle:** um `usage.event` no outbox para AUTHORIZED e para DENIED; nenhum para REJECTED, FAILED, evento filho e inutilização (salvo o que o item 1 decidir); reprocessar não gera evento com id diferente.

Decisão de negócio que não é nossa: **quais estados cobram** (AUTHORIZED e DENIED é o padrão razoável, mas é do usuário).

## 10. Feito (T-204, 2026-11-23)

Decisões do usuário: só **AUTHORIZED** cobra; eventos (cancelamento, CC-e...) cobram como **métrica própria** (`EVENTS_<modelo>`) e **cancelar não estorna**; **uma métrica por modelo**; o débito do resumo (só `QUOTA`) aceito.
Implementado: `OutboxUsageSink.orderedBy(EVENT)`; no lifecycle, `UsageMetering` (identidade pela chave de acesso; instante do `state_since`); no `usage-api`, o consumidor e o receiver; fila `xml-usage`; plano do demo.
Pendente de verificação (precisa do kind): o caminho pela fila SQS de ponta a ponta e a carga com a mensagem a mais.

### 10.1 O débito do item 6 foi pago (2026-11-23)

A biblioteca ganhou o limite **`METER`** (só mede: tem janela e nenhum valor; não decide, não tem contador, não aceita override). Faz o `usage_summary` existir para uma métrica de cobrança sem uma cota inventada
(antes: `QUOTA` de 1.000.000.000 com `ALLOW_OVERAGE`). O engine usa `METER` mensal em `DOCUMENTS_<modelo>` e `EVENTS_<modelo>`; a `QUOTA` fica só para a cota da borda (`DOCUMENTS`).
