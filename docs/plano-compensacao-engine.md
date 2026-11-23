# Plano: compensação manual no engine (`usage-api`)

> Estado: **plano; o M2 já está feito** (o `usage-api` usa o starter e tem o `UsageEngine`, T-203) e falta o M1, que alimenta o ledger. Levantado em 2026-11-23, depois dos commits `520d737` (usage-platform) e `91d0ce0` (engine).
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
