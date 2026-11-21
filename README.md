# usage-platform

> Nome e coordenadas Maven (`com.example.usage`) são provisórios.

Planos, cotas, limites e **medição durável de uso por tenant** para o ecossistema Java/Spring. Uma biblioteca
que qualquer produto com essas necessidades pode embutir: ela decide se um tenant pode consumir, registra o que foi
consumido de forma exata e entrega o total por período e janela de cota para o billing ler.

Roda no seu próprio Redis e no seu próprio PostgreSQL (self-hosted, sem serviço externo).

## O que ela resolve

- **Rate limit**, **cota por período** (dia, mês) e **limite de concorrência**, por tenant e por métrica.
- **Planos versionados** com *overrides* por tenant, e o ciclo de cobrança preso à **assinatura** (trocar de plano
  no meio do ciclo não corta o período).
- **Métricas de acesso** (contadas na entrada, sincronamente) e **métricas de resultado** (contadas quando um
  processamento assíncrono termina), com um dono de contador bem definido para cada uma.
- **Medição exata e idempotente**: evento imutável com `eventId` determinístico, ledger no banco, resumo por período
  e **compensação** (devolver cota em 5xx, reverter um resultado).
- **Operação**: fechamento de períodos, reconciliação Redis × banco, job com lock de instância única.

O que ela **não** faz: cobrança, faturas, pagamento, identidade (quem é o tenant vem do seu provedor de
identidade), painel web. O billing é um consumidor futuro do `usage_summary`.

## Como funciona

```text
  Cliente ──JWT──▶ Gateway (UsageGatewayFilter) ──▶ APIs de negócio
                      │  check(tenant, métrica, custo)         │ mensagem com tenantId
                      │  consome cota de acesso                ▼
                      │                                  Fila / tópico
                      ▼                                        │
                 ┌─────────┐                           Processador (QueueUsageAdapter)
                 │  Redis  │◀── contadores (cache)           │  acquire → trabalho → persiste
                 └─────────┘                                 │  resultado + evento (outbox)
                      ▲                                      ▼
                      │                              outbox ─▶ broker ─▶ UsageEventConsumer
                      │ repara (CounterReconciler)                         │ ledger (dedup) + resumo, atômicos
                      │                                                    ▼
                      └───────────────────────────────────────────── PostgreSQL
                                                                     usage_event · usage_summary · planos
                                                                           │
                                                                      Billing (futuro) lê usage_summary
```

Três camadas: **configuração** (planos, limites, assinatura), **decisão** (caminho quente: cache + Redis, sem
escrever no banco relacional) e **medição** (assíncrona, no banco, fonte de verdade).

## Conceitos

**Dono do contador.** Cada métrica tem exatamente um dono, definido por `Metric.Mode`:

| Modo | Exemplo | Quem move o contador da cota |
|---|---|---|
| `CONSUME_ON_CHECK` (acesso) | `API_REQUESTS` | o `check`, na entrada, de forma atômica |
| `REPORT_ONLY` (resultado) | `ITEMS_PROCESSED` | o `ingest`, quando o evento do resultado chega. O `check` só lê o que já foi consumido |

Isso evita contar duas vezes. No modo de resultado não há reserva de vaga: um lote que entra com a cota quase
cheia passa inteiro e o estouro vira excedente (`ALLOW_OVERAGE`) ou é contido pelo limite de concorrência.

**Limites** (`Limit`): `RATE` (rajada, só métricas de acesso), `QUOTA` (volume por período) e `CONCURRENCY`
(itens em andamento). O custo de um `check` é uma **quantidade**: um lote de 500 itens é um `check` de custo 500.
A cota admite `REJECT` ou `ALLOW_OVERAGE` (aceita e marca excedente).

**Períodos.** `Window.fixed(duração)` ou `Window.months(n)`, sempre em UTC, alinhados à âncora do ciclo. Mensal
começa no dia e hora da âncora; âncora no dia 31 vira 28/29 em fevereiro e **volta ao 31** quando o mês tem.
`PeriodCalculator` é o único lugar que calcula períodos: use-o em qualquer extensão.

**Plano.** `Plan(code, version)` identifica um plano versionado. A `Policy` que o motor recebe carrega o `Plan`, os
limites já com overrides e a âncora do ciclo; toda `Decision` diz sob qual plano foi tomada, e o gateway repassa
`X-Plan-Code` e `X-Plan-Version` (qualquer `X-Plan-*` do cliente é descartado; sem plano real, nada é enviado).
O `usage_summary` guarda `plan_code` e `plan_version` do plano em vigor quando o período recebeu uso pela última vez
**enquanto estava aberto**; ao fechar, o plano congela, e é com ele que o billing lê cota e preço da época. Um store
que não conhece planos devolve `Plan.UNSPECIFIED`.

**Assinatura.** O ciclo (`cycle_anchor`) pertence à `tenant_subscription`. Trocar de plano
(`JdbcSubscriptions.changePlan`) troca só os limites: contadores e resumos do período corrente continuam. Os
*overrides* pertencem ao plano e terminam com ele.

**Eventos.** `UsageEvent` (soma) e `Compensation` (reverte), ambos `UsageMessage`. A quantidade de um evento vai de 1 a
1.000.000.000. O `eventId` determinístico de um item inclui o **tenant** (`UsageEvent.idFor(tenant, item, métrica)`). Imutáveis. A compensação é debitada no período do
**original**, só pode reverter um original uma vez, e em período já fechado vira **crédito** em vez de mexer no
total. Um 5xx no gateway gera uma compensação do uso que acabou de ser registrado.

**Resumo e billing.** `usage_summary` guarda, por tenant, métrica, janela e período: `total`, `late_total` (uso que
chegou depois do fechamento) e `credits`. Consumo líquido de um período fechado:
`total + late_total − credits`.

Duas coisas que o billing precisa saber antes de ler a tabela:
- **O resumo só existe para limites de cota (`QUOTA`)**, não para rate nem concorrência, e tem **uma linha por janela**: um
  tenant com cota diária e mensal da mesma métrica gera duas linhas no mesmo período. Escolha a `window_key` que cobra
  (normalmente a mensal) ao somar; somar todas as linhas da métrica conta o consumo em dobro.
- **O custo de uma requisição do gateway é uma afirmação do cliente** quando vem de um header (`cost-header`): declarar
  `X-Item-Count: 1` num lote de 500 cobra 1. Para cobrar o que foi feito de fato, meça no backend ou use uma métrica de
  resultado emitida pelo processador, que conta o que ele processou.

## Módulos

| Módulo | Papel | Depende de |
|---|---|---|
| `usage-core` | Motor (`UsageEngine`), modelo, SPIs, `PeriodCalculator`, reconciliação, manutenção. **Sem Spring, sem broker, sem gateway.** | só o JDK |
| `usage-redis` | `CounterStore` em Redis (um script Lua por operação, Lettuce) | core |
| `usage-postgres` | `Ledger`, `PolicyStore`, `SummaryStore`, `JobLock`, assinaturas, schema Flyway | core |
| `usage-gateway` | `UsageGatewayFilter` para Spring Cloud Gateway (WebFlux) | core |
| `usage-processor` | `QueueUsageAdapter` para consumidores de fila (sem cliente de broker) | core |
| `usage-consumer` | Contrato JSON dos eventos e `UsageEventConsumer` central | core |
| `usage-messaging` | Liga a plataforma a um broker pela **sua** `messaging-library` (inbox) e `outbox-library`: um handler da inbox e um `UsageSink` sobre o outbox. Não tem código de broker | core, consumer, `messaging-inbox`, `outbox-core` |
| `usage-bom` | Só as versões de todos os módulos, para importar (sem pai, sem o BOM do Boot) | — |
| `usage-demo` | Aplicação Spring Cloud Gateway pronta para rodar, com docker-compose; veja [usage-demo/README.md](usage-demo/README.md) | o starter e os adaptadores |
| `usage-spring-boot-starter` | Auto-configuração a partir de propriedades `usage.*` | os acima (opcionais) |

As SPIs do core (`PolicyStore`, `CounterStore`, `Ledger`, `SummaryStore`, `UsageSink`, `TenantResolver`,
`JobLock`) são o ponto de extensão: troque Redis ou Postgres implementando a interface.

O `usage-core` se organiza em quatro pacotes, e a dependência vai sempre no mesmo sentido (`model` ← `spi` ← `memory` e o motor):

| Pacote | O que tem |
|---|---|
| `com.example.usage.core` | O motor (`UsageEngine`), a reconciliação de contadores e a manutenção de períodos |
| `com.example.usage.core.model` | Plano, política, limite, métrica, janela, período, tenant, eventos e as respostas do motor (`Decision`, `Permit`) |
| `com.example.usage.core.spi` | As interfaces que você implementa (as SPIs acima) |
| `com.example.usage.core.memory` | Implementações em memória (contadores, ledger, lock) e o cache de políticas, para testes e uso de uma só instância |

## Início rápido (Spring Cloud Gateway)

Importe o BOM (`usage-bom`, instalado com `mvn install`) e declare os módulos sem versão:

```xml
<dependencyManagement><dependencies>
  <dependency><groupId>com.example.usage</groupId><artifactId>usage-bom</artifactId>
    <version>0.1.0-SNAPSHOT</version><type>pom</type><scope>import</scope></dependency>
</dependencies></dependencyManagement>
```

Dependências:

```xml
<dependency><groupId>com.example.usage</groupId><artifactId>usage-spring-boot-starter</artifactId></dependency>
<dependency><groupId>com.example.usage</groupId><artifactId>usage-gateway</artifactId></dependency>
<dependency><groupId>com.example.usage</groupId><artifactId>usage-redis</artifactId></dependency>
<dependency><groupId>com.example.usage</groupId><artifactId>usage-postgres</artifactId></dependency>
```

Cada capacidade pede o seu jar. Além dos quatro acima: `usage-processor` para consumidores de fila, `usage-consumer` para o
consumidor central, e `usage-messaging` para ligar a um broker (ele já traz o `usage-consumer`). Ligar a propriedade sem o
jar **falha o startup** dizendo qual falta (veja "Propriedades").

`application.yml`:

```yaml
usage:
  fail-mode: OPEN            # OPEN deixa passar se Redis/banco caírem; CLOSED recusa
  redis:
    uri: redis://redis:6379
  postgres:
    migrate: true            # roda as migrations da plataforma, no schema "usage" (criado se não existir)
  gateway:
    enabled: true
    refund-from-status: 500  # 5xx devolve a cota (e uma exceção do upstream conta como 500); 600 desliga tudo
    metrics:
      - code: API_REQUESTS
        cost-header: X-Item-Count   # opcional: custo do lote vindo do cliente
      - code: ITEMS_PROCESSED
        mode: GUARD                 # só recusa se o tenant já estourou
  maintenance:
    enabled: true            # fecha períodos e reconcilia contadores, uma instância por vez
    close-tolerance: 24h
```

O que **você** fornece:

```java
@Bean TenantResolver<ServerWebExchange> tenants() {      // do JWT/client para o tenant, no seu provedor
    return exchange -> /* CompletionStage<Optional<TenantId>> */;
}

@Bean UsageSink usageSink() {                             // o seu outbox: precisa ser durável
    // recebe os dois tipos de mensagem: o uso e a sua compensação (devolução de cota em 5xx)
    return message -> outbox.save(UsageEventJson.toJson(message));   // ilustrativo: devolva um CompletionStage<Void>
}

@Bean InternalTraffic internal() {                        // opcional: chamadas entre serviços não contam
    return exchange -> /* reconhece service account */ false;
}
```

O filtro responde `400` (`invalid_cost`: custo malformado, repetido, zero, negativo ou acima do máximo), `429`
(`rate_limited`, `quota_exceeded`, `concurrency_exceeded`), `403` (`client_not_linked`, `no_policy`) ou `503`
(`store_unavailable` com `fail-mode: CLOSED`), remove qualquer `X-Tenant-*` e `X-Plan-*` vindo do cliente e repassa `X-Tenant-Id`,
`X-Overage`, `X-Plan-Code` e `X-Plan-Version`. Com `usage.gateway.enabled=true` o contexto **não sobe** sem exatamente um bean `UsageSink`.

Regras de custo e de cobrança do filtro:
- O custo de uma requisição vai de **1 a 1.000.000.000** (`UsageEngine.MAX_COST`). Um header de custo repetido ou
  malformado é `400`, nunca "1": senão o cliente poderia declarar menos do que consome.
- Várias métricas são checadas com os *guards* primeiro. Se uma recusa, o que as de acesso anteriores consumiram é
  devolvido: uma requisição recusada não custa cota.
- Se o cliente derruba a conexão com o trabalho em andamento, a requisição é **cobrada** (sem devolução).

Cadastro de um tenant (depois de inserir `tenant`, `metric`, `plan` e `plan_limit`):

```java
subscriptions.subscribe(new TenantId("acme"), "PRO", 1, Instant.parse("2026-11-01T00:00:00Z"));
subscriptions.changePlan(new TenantId("acme"), "ENTERPRISE", 1, Instant.parse("2027-02-10T00:00:00Z"));
policyStore.invalidate(new TenantId("acme"));   // e propague a invalidação às outras instâncias
```

## Processadores (métricas de resultado)

```yaml
usage:
  processor:
    metric: ITEMS_PROCESSED
    countable-statuses: [COMPLETED, APPROVED]
    tenant-header: x-tenant-id
    defer-when-over-quota: false
```

```java
adapter.handle(headers, itemId,
    () -> processar(),                                   // devolve Done(resultado, statusFinal, finishedAt)
    (resultado, usage) -> repo.salvarComOutbox(resultado, usage));   // MESMA transação
```

O adaptador devolve `COMPLETED`, `NO_TENANT` (vai para a DLQ), `DEFERRED` (tente depois) ou `DENIED`. O evento de uso
**não** é enviado pelo adaptador: ele é entregue ao seu `Persist` para ser gravado no outbox na mesma transação do
resultado. `finishedAt` deve vir do fato persistido, para que um reprocessamento gere exatamente o mesmo evento.

## Consumidor central

```yaml
usage:
  consumer:
    enabled: true
    known-metrics: [API_REQUESTS, ITEMS_PROCESSED]
```

`UsageEventConsumer.handle(corpo)` devolve `ACK`, `RETRY` ou `DEAD_LETTER` com um motivo curto (`applied`,
`duplicate`, `counter_drift`, `original_not_found`, `invalid_json`...). Particione o tópico por `tenantId`.

## Ligando a um broker (messaging-library e outbox-library)

A plataforma não fala com Kafka, SQS, JMS ou RabbitMQ: ela fala com a `messaging-library` (consumo) e a `outbox-library`
(publicação), e o provedor é o que estiver em `messaging.*`. O módulo `usage-messaging` é a cola, em duas classes:

**Consumo: `UsageInboxHandler`**, um `InboxResultHandler` que chama o `UsageEventConsumer`:

| Resposta do consumidor | Resultado na inbox |
|---|---|
| `ACK` (`applied`, `duplicate`, `counter_drift`) | `HandlerResult.ack()`; todo resultado passa pelo `observer`, que deve contar o `counter_drift` |
| `RETRY` `original_not_found` | `HandlerResult.retryAfter(atraso)`: a compensação espera o uso no nível atrasado |
| `RETRY` outro motivo (banco fora) | lança `RetryableUsageException`: valem o backoff, os níveis e a DLQ da própria inbox |
| `DEAD_LETTER` | `HandlerResult.reject(motivo)`: vai para `failed`, sem retry |

```java
var handler = new UsageInboxHandler<Object>(usageEventConsumer, Duration.ofSeconds(30), Duration.ofSeconds(10),
        outcome -> metrics.count(outcome.reason()));   // atraso de original_not_found, timeout, observador
```

O handler lê o corpo cru da mensagem e usa o contrato da plataforma, qualquer que seja o tipo de payload com que ele
foi registrado na inbox. Ordene ou particione o tópico por tenant.

**Publicação: `OutboxUsageSink`**, um `UsageSink` sobre o `OutboxWriter`. O `aggregateId` é o tenant (ordenação por
tenant) e o `eventType` é `usage.event` ou `usage.compensation`. O `messageId` da linha do outbox é **aleatório**, e o
`eventId` da plataforma viaja no header `usage-event-id`: o `eventId` de um item é determinístico, e
`outbox_message.id` é chave primária, então reaproveitá-lo faria a reentrega de um item já publicado falhar o `INSERT` e
abortar a transação inteira do host. Quem torna a repetição inofensiva é o ledger, que deduplica pelo `eventId` do
payload. O `OutboxWriter.enqueue` grava **na transação ativa de quem chama**, por isso há duas formas:

```java
// processador: dentro do Persist do QueueUsageAdapter, na mesma transação do resultado
var sink = OutboxUsageSink.sameTransaction(outboxWriter, "usage-events");

// gateway: não há transação de negócio depois da resposta; cada mensagem ganha uma transação própria, fora da thread reativa
var sink = OutboxUsageSink.ownTransaction(outboxWriter, "usage-events", executor,
        work -> transactionTemplate.executeWithoutResult(status -> work.run()));
```

No gateway isso põe um banco no caminho pós-resposta, mas é o único jeito de não perder uso se o broker cair. Publicar
direto pelo `MessageSender` seria mais simples e perderia o uso nesse caso: não está incluído.

**Verificado contra um broker real.** `UsageOverRealBrokerTest` sobe RabbitMQ 4.1 e PostgreSQL e percorre o caminho
inteiro: `OutboxUsageSink` → outbox (PostgreSQL) → relay → RabbitMQ → messaging-inbox → `UsageInboxHandler` → ledger.
Cobre: um evento de ponta a ponta; o mesmo evento entregue duas vezes (contado uma); uma compensação que chega antes do
uso (espera no nível de retry e é aplicada); uma mensagem impossível de contar (vai para a fila `failed`, sem travar
a seguinte); e **o broker pausado durante a escrita** (o uso fica no outbox e chega quando o broker volta).

Duas coisas que o teste mostrou e que o seu código não resolve sozinho:
- **`retry-queues` no RabbitMQ é topologia.** A fila de espera precisa de `x-message-ttl` igual ao atraso configurado e
  de dead-letter de volta para a fila principal (veja "RabbitMQ com retry-queues" no README da inbox). Sem isso, uma
  compensação adiantada fica parada na fila de espera para sempre. O `original_not_found` usa esse nível.
- **A lista de exceções repetíveis da inbox precisa conter `RetryableUsageException`.** Uma indisponibilidade do ledger
  ou do banco chega à inbox como essa exceção, e só o que está em `retry.retryable` (ou o padrão equivalente da sua
  versão da inbox) volta para a fila de retry; qualquer outra exceção vai direto para `failed` e o uso se perde. Teste
  isso com o seu `application.yml`, não só com o do exemplo.
- **Uma linha do outbox que esgota as tentativas congela aquele tenant.** O `aggregateId` é o tenant, então a ordenação
  por agregado do outbox para as mensagens seguintes dele atrás da linha morta (dead-lettered) até alguém tratá-la.
  Alerte sobre linhas mortas do outbox: é o único sinal, e o uso daquele tenant só volta a chegar depois da intervenção.
- **O provedor de consumo vem da messaging-library**, e ela tem adapters para AWS, Azure, GCP, JMS e RabbitMQ: **não
  há adapter de consumo Kafka** nela (só o `outbox-broker-kafka`, no lado de publicação). Para consumir de Kafka seria
  preciso um adapter novo na messaging-library; a plataforma não muda.

**Um só `DataSource`.** O `OutboxUsageSink.sameTransaction` só é atômico com o resultado do processador se o starter, o
`JdbcOutboxWriter` e o `TransactionTemplate`/`PlatformTransactionManager` usam o **mesmo** `DataSource`. Nada confere isso:
com dois bancos, a gravação do outbox cai numa transação que não é a do resultado e a atomicidade se perde sem erro. Se a
aplicação tem mais de um `DataSource`, aponte os três para o mesmo (`@Primary` ou beans explícitos) e teste um rollback.

**Envelope da inbox.** O receiver `usage-events` precisa de `envelope: auto` ou `outbox`. Com `raw` ou `cloudevents` o
handler não encontra o payload da plataforma e manda **toda** mensagem para a DLQ (`invalid_event_id`).

**Pelo starter, só com propriedades.** Com `usage-messaging` no classpath, duas propriedades fazem o resto:

```yaml
usage:
  consumer: { enabled: true, known-metrics: [API_REQUESTS] }
  messaging:
    outbox: { destination: usage-events }     # UsageSink sobre o outbox; o modo de transação segue o papel da aplicação
    inbox:  { enabled: true }                 # handler do receiver "usage-events"
messaging:
  receivers:
    usage-events: { destination: usage-events, failed: usage-failed, ... }   # o resto é da messaging-library
```

O handler é o bean `usageEvents`, que a inbox liga ao receiver `usage-events` (verificado com os starters reais: veja
`StarterBrokerTest`). Para um receiver de outro nome, declare você mesmo um `InboxResultHandler` com esse nome em volta
de `UsageInboxHandler`. O alias `usage-events` **não** é registrado de propósito: o outbox nomeia seus beans
`MessagePublisher` pelo destino, e um publisher para `usage-events` colidiria com ele. Um `UsageSink` seu continua
substituindo o do outbox.

Para compilar o módulo, instale antes as duas bibliotecas (`mvn install` em cada uma): são dependências locais, SNAPSHOTs
que só existem no seu `~/.m2`, e uma máquina limpa (ou o CI) não compila sem isso. Os módulos de produção usam
`com.example.messaging:messaging-inbox` e `com.example.outbox:outbox-core`; os testes e o starter também precisam de
`messaging-rabbitmq`, `messaging-spring-boot-starter`, `outbox-spring-boot-starter` e `outbox-broker-messaging`. As versões
ficam em um lugar só, o `pom.xml` raiz (`messaging-library.version`, `outbox-library.version`). Para publicar de verdade,
suba as duas bibliotecas para um repositório de artefatos e aponte o build para ele.

## Propriedades

Todas vivem sob `usage.` (classe `UsageProperties`, com autocomplete na IDE). Durações aceitam o formato do Spring
(`500ms`, `30s`, `15m`, `24h`, `90d`). Valores inválidos **falham o startup** com o nome da propriedade.

### Geral

| Propriedade | Tipo | Padrão | Descrição |
|---|---|---|---|
| `usage.fail-mode` | `OPEN` \| `CLOSED` | `OPEN` | O que fazer quando Redis ou o store de políticas estão fora. `OPEN` deixa a requisição passar (sem medir a decisão); `CLOSED` recusa (`503 store_unavailable` no gateway). Cada decisão degradada sai em WARNING (`com.example.usage.core.UsageEngine`), no máximo uma linha a cada 30 s com a contagem das omitidas. Uma política inválida sai em ERROR com o motivo `invalid_policy` |
| `usage.concurrency-lease` | duração `> 0` | `15m` | Vida de cada slot de concorrência, contada por dono. Se o processador cai sem liberar, o slot some quando o prazo acaba. Um job mais longo que o lease perde o seu slot: aumente-o |
| `usage.policy-cache-ttl` | duração `>= 0` | `1m` | Quanto tempo a política de um tenant fica em cache. Mudanças administrativas só valem depois do TTL, a menos que você invalide (`CachedPolicyStore.invalidate`) |

### Contadores e armazenamento

| Propriedade | Tipo | Padrão | Descrição |
|---|---|---|---|
| `usage.counters.type` | `redis` \| `memory` | `redis` | Onde ficam os contadores de decisão. `memory` conta dentro de uma JVM: serve para testes e uma instância só, nunca para cluster (loga WARN na subida) |
| `usage.redis.uri` | URI | — | **Obrigatória** com `counters.type=redis`, a menos que exista um bean `CounterStore` ou `RedisClient` seu. Ex.: `redis://host:6379`, `rediss://...` para TLS |
| `usage.redis.command-timeout` | duração `> 0` | `500ms` | Um comando mais lento que isso falha e o `fail-mode` decide. Com o Redis desconectado, os comandos falham na hora em vez de enfileirar. Ignorada se você declara o seu `RedisClient` |
| `usage.postgres.migrate` | boolean | `false` | `true` roda as migrations Flyway da plataforma na subida (histórico em `<schema>.usage_schema_history`). Em produção, deixe `false` se o seu pipeline já migra |
| `usage.postgres.schema` | identificador `[a-z_][a-z0-9_]*` | `usage` | Schema das tabelas da plataforma, criado pela migration. Vazio usa o schema padrão da conexão (então o Flyway recusa um banco que já tem tabelas sem histórico). Mudar depois de ter dados **não** move as tabelas |

O `DataSource` é o do Spring (`spring.datasource.*`). Se houver mais de um, marque o da plataforma como `@Primary`.

### Gateway (precisa do jar `usage-gateway`)

| Propriedade | Tipo | Padrão | Descrição |
|---|---|---|---|
| `usage.gateway.enabled` | boolean | `false` | Liga o `UsageGatewayFilter`. Exige exatamente um bean `UsageSink` e um `TenantResolver<ServerWebExchange>` |
| `usage.gateway.refund-from-status` | int `>= 100` | `500` | Respostas com esse status ou acima devolvem a cota (e uma exceção do upstream conta como 500). `600` desliga a devolução |
| `usage.gateway.metrics[].code` | texto | — | Código da métrica a checar em cada requisição (precisa existir na tabela `metric`) |
| `usage.gateway.metrics[].mode` | `ACCESS` \| `GUARD` | `ACCESS` | `ACCESS` consome a cota na entrada. `GUARD` só recusa se o tenant já estourou (para métricas de resultado, que o processador conta) |
| `usage.gateway.metrics[].cost-header` | nome de header | — | Só `ACCESS`. Header com o custo da requisição (itens do lote). Ausente = 1; repetido, malformado, zero, negativo ou acima de 1.000.000.000 = `400 invalid_cost`. É uma afirmação do cliente |

As métricas são checadas na ordem dos *guards* primeiro; se uma recusa, o que as de acesso anteriores consumiram é devolvido.

### Processador (precisa do jar `usage-processor`)

Ligado ao definir `usage.processor.metric`.

| Propriedade | Tipo | Padrão | Descrição |
|---|---|---|---|
| `usage.processor.metric` | texto | — | Métrica de **resultado** (`REPORT_ONLY`) que este processador conta |
| `usage.processor.countable-statuses` | lista | — | **Obrigatória**: status finais que geram evento de uso (ex.: `[COMPLETED]`). Qualquer outro status é gravado, mas não cobrado |
| `usage.processor.tenant-header` | nome de header | `x-tenant-id` | Header da mensagem com o tenant. Sem ele o adaptador devolve `NO_TENANT` (vai para a DLQ) |
| `usage.processor.defer-when-over-quota` | boolean | `false` | `true` adia (`DEFERRED`) o que chega com a cota já estourada, em vez de processar acima dela |

### Consumidor central (precisa do jar `usage-consumer`)

| Propriedade | Tipo | Padrão | Descrição |
|---|---|---|---|
| `usage.consumer.enabled` | boolean | `false` | Liga o `UsageEventConsumer` (grava ledger + resumo) |
| `usage.consumer.known-metrics` | lista | — | **Obrigatória** quando ligado: um evento de outra métrica vai para a DLQ |
| `usage.consumer.max-future` | duração `>= 0` | `5m` | Quanto o `occurredAt` pode estar à frente do relógio (desvio de relógio); além disso, DLQ |
| `usage.consumer.max-past` | duração `>= 0` | `90d` | Idade máxima de um evento; além disso, DLQ em vez de cair num período fechado |

### Mensageria (precisa do jar `usage-messaging`)

| Propriedade | Tipo | Padrão | Descrição |
|---|---|---|---|
| `usage.messaging.outbox.destination` | texto | — | Liga o `UsageSink` sobre o seu outbox, publicando nesse destino. Sem ele, o `UsageSink` é seu |
| `usage.messaging.outbox.transaction` | `OWN` \| `CALLER` | pelo papel | `OWN`: uma transação própria por mensagem, numa virtual thread (gateway, que não tem transação de negócio). `CALLER`: na transação aberta de quem chama (processador, dentro do `Persist`). Sem valor: `CALLER` se `usage.processor.metric` está definido, `OWN` caso contrário; quem é gateway **e** processador precisa dizer. `CALLER` com o gateway é recusado no startup; `OWN` chamado dentro de uma transação recusa em runtime. `OWN` exige exatamente um `PlatformTransactionManager` |
| `usage.messaging.inbox.enabled` | boolean | `false` | Registra o consumidor como handler da messaging-inbox, no bean `usageEvents`, que serve o receiver `usage-events`. Pede `usage.consumer.enabled=true` |
| `usage.messaging.inbox.original-not-found-delay` | duração `> 0` | `30s` | Quanto uma compensação que chegou antes do uso espera no nível de retry |
| `usage.messaging.inbox.timeout` | duração `> 0` | `10s` | Quanto esperar o consumidor antes de tratar a entrega como falha transitória |

### Manutenção

| Propriedade | Tipo | Padrão | Descrição |
|---|---|---|---|
| `usage.maintenance.enabled` | boolean | `false` | Liga o job de fechamento de períodos e reconciliação Redis × banco, com lock: uma instância por vez |
| `usage.maintenance.close-tolerance` | duração `>= 0` | `24h` | Quanto tempo depois do fim de um período ainda se aceitam eventos tardios antes de fechá-lo (24–72 h é razoável) |
| `usage.maintenance.settle` | duração `>= 0` | `5m` | Quanto tempo uma linha do resumo precisa ficar quieta para a reconciliação confiar nela. Escolha bem acima da latência do consumidor e do desvio de relógio app/banco |
| `usage.maintenance.initial-delay` | duração `>= 0` | `1m` | Espera antes da primeira execução |
| `usage.maintenance.interval` | duração `> 0` | `15m` | Intervalo entre execuções |

Cada adaptador só aparece com o seu jar no classpath. Ligar um por propriedade **sem o jar** (`usage.gateway.enabled`
sem `usage-gateway`, `usage.consumer.enabled` sem `usage-consumer`, `usage.processor.metric` sem `usage-processor`,
`usage.messaging.*` sem `usage-messaging`) **falha o startup** dizendo qual jar falta, em vez de rodar sem medir.
Qualquer bean que você definir (`UsageEngine`, `CounterStore`, `PolicyStore`...) substitui o padrão.

### Telemetria

| Propriedade | Tipo | Padrão | Descrição |
|---|---|---|---|
| `usage.telemetry.enabled` | boolean | `true` | Métricas e traces pelo Micrometer, quando ele e um `MeterRegistry` estão presentes. `false` desliga (veja "Observabilidade") |

### Exemplos de configuração

**Desenvolvimento local / testes** (sem Redis, sem broker; só o gateway medindo em memória):

```yaml
usage:
  counters: { type: memory }
  postgres: { migrate: true }
  gateway:
    enabled: true
    metrics: [{ code: API_REQUESTS }]
  consumer: { enabled: true, known-metrics: [API_REQUESTS] }   # o seu UsageSink chama o consumidor no mesmo processo
```

**Gateway em produção** (uso vai para o outbox; o consumo é de outro serviço):

```yaml
usage:
  fail-mode: CLOSED                  # produto cobrado por uso: recusar é melhor que deixar passar sem medir
  policy-cache-ttl: 30s
  redis: { uri: "rediss://${REDIS_HOST}:6379", command-timeout: 200ms }
  gateway:
    enabled: true
    metrics:
      - { code: API_REQUESTS, cost-header: X-Item-Count }
      - { code: ITEMS_PROCESSED, mode: GUARD }
  messaging:
    outbox: { destination: usage-events }     # transaction: OWN, inferido (não há processador aqui)
```

**Serviço processador** (conta `ITEMS_PROCESSED` depois do trabalho, na transação do resultado):

```yaml
usage:
  redis: { uri: "rediss://${REDIS_HOST}:6379" }
  processor:
    metric: ITEMS_PROCESSED
    countable-statuses: [COMPLETED, APPROVED]
    defer-when-over-quota: true
  messaging:
    outbox: { destination: usage-events }     # transaction: CALLER, inferido pelo processor.metric
```

**Serviço central de medição** (consome os eventos, grava o ledger e roda a manutenção):

```yaml
usage:
  redis: { uri: "rediss://${REDIS_HOST}:6379" }
  postgres: { migrate: true }
  consumer:
    enabled: true
    known-metrics: [API_REQUESTS, ITEMS_PROCESSED]
    max-past: 7d
  messaging:
    inbox: { enabled: true }
  maintenance:
    enabled: true
    close-tolerance: 48h
    interval: 10m
```

## Observabilidade (métricas e tracing)

Com Micrometer no classpath e um `MeterRegistry` na aplicação (Spring Boot Actuator, por exemplo), o starter registra
métricas e spans sozinho: nada a configurar. Sem registry, a plataforma não reporta nada (e não custa nada).
`usage.telemetry.enabled=false` desliga mesmo com o Micrometer presente. Para outra saída, declare o seu bean
`UsageTelemetry` (SPI do core, sem dependência de biblioteca de métricas).

Cada operação é uma *observation* do Micrometer: o mesmo ponto gera o **timer** (duração e contagem) e, se a aplicação
tem uma ponte de tracing (`micrometer-tracing`), o **span**. O `eventId` vai como atributo `usage.event.id` do span (e
não como tag de métrica), então um evento pode ser seguido do gateway ao ledger.

**Nunca há tag de tenant**: o id é ilimitado e derrubaria o seu Prometheus. Para "quem consumiu mais", use o ledger/SQL.
A tag `metric` é o código da métrica de uso (conjunto pequeno e conhecido).

| Métrica | Tipo | Tags | Quando alertar |
|---|---|---|---|
| `usage.check` | timer | `metric`, `result` (`allowed`/`denied`), `reason`, `overage` | `reason=store_unavailable` ou `invalid_policy` > 0 (decisões degradadas); p99 alto (é o caminho quente) |
| `usage.acquire` | timer | idem | `reason=concurrency_exceeded` constante: lease ou concorrência mal dimensionados |
| `usage.ingest` / `usage.compensate` | timer | `metric`, `result` (`applied`, `duplicate`, `counter_drift`, ...) | `counter_drift` > 0; `result` de compensação `already_compensated`/`invalid` |
| `usage.consume` | timer | `disposition`, `reason` | duração do consumidor |
| `usage.consumer.outcome` | counter | `disposition` (`ack`/`retry`/`dead_letter`), `reason` | `dead_letter` > 0; `retry` sustentado |
| `usage.consumer.lag` | timer | — | `max` alto: o billing lê dado atrasado |
| `usage.process` | timer | `metric`, `verdict` (`completed`/`deferred`/`denied`/`no_tenant`) | `no_tenant`/`denied` > 0; muito `deferred` |
| `usage.gateway.event.lost` | counter | `kind` (`usage`/`refund`), `metric` | **qualquer valor > 0**: uso ou devolução perdido (sub-cobrança) |
| `usage.gateway.refund.failed` | counter | `metric` | > 0: cota não devolvida a uma requisição recusada |
| `usage.maintenance.run` | timer | `outcome` (`completed`/`skipped`/`failed`) | `failed` > 0 |
| `usage.maintenance.last.success` | gauge | — | `time() - valor` maior que 2 intervalos (0 = nunca rodou) |
| `usage.maintenance.periods.closed` | counter | — | informativo |
| `usage.reconciler.rows` | counter | `result` (`scanned`/`repaired`/`ahead`/`skipped`/`failed`) | `repaired` alto: o Redis perde contagem; `failed` > 0 |
| `usage.policy.cache` | counter | `result` (`hit`/`miss`) | taxa de acerto baixa |
| `usage.policy.cache.size` | gauge | — | perto de 100.000 |

(No Prometheus: `usage_check_seconds_count`, `usage_gateway_event_lost_total`, e assim por diante.)

Exemplos de alerta (PromQL):

```text
sum(rate(usage_gateway_event_lost_total[5m])) > 0                                   # uso perdido
sum(rate(usage_check_seconds_count{reason=~"store_unavailable|invalid_policy"}[5m])) > 0   # decisões degradadas
sum(increase(usage_consumer_outcome_total{disposition="dead_letter"}[10m])) > 0     # DLQ
time() - usage_maintenance_last_success > 2 * 900                                   # job parado (intervalo de 15m)
```

**Tracing: o que cobre e o que não.** Cada operação é um span, e um span aberto de forma síncrona vira pai dos que ele
inicia na mesma thread (o `consume` é pai do `ingest`). Os saltos assíncronos (callbacks do Redis, a fila) **não**
propagam o contexto sozinhos, e a mensagem do outbox não leva o `traceparent`: o trace do gateway e o do consumidor
são traces separados, ligados pelo `usage.event.id`. Isso vem de a propagação por mensagem ser da `messaging-library`/
`outbox-library`, não desta.

O `usage-demo` expõe `/actuator/prometheus` e o `docker compose up` sobe um Prometheus em `localhost:9090` já
raspando o demo.

## Garantias e limites de precisão

| Dado | Precisão |
|---|---|
| `usage_event`, `usage_summary` (PostgreSQL) | **Exatos**: dedup por `eventId`, evento e resumo gravados na mesma transação |
| Contadores do Redis | **Cache de decisão**, aproximados. O reconciliador os repara a partir do resumo |
| Compensação | Reverte exatamente os *slots* em que o original foi gravado (`usage_event_slot`), mesmo que os limites tenham mudado depois |
| Rate limit | Janela fixa: uma rajada na virada da janela pode chegar a 2× o limite |
| Cota de resultado | Checa só o consumido: pequeno estouro é aceito por projeto |

Se o evento chegar duplicado, o ledger ignora. Se o Redis falhar após o ledger gravar, o consumidor devolve
`counter_drift` (ACK) e o reconciliador conserta depois. Se a política estiver fora do ar na ingestão, nada é
gravado e o broker redistribui. O dedup é pelo `eventId`: a tabela `usage_event_id` (chave só o id, não particionada) é a porta, gravada na mesma transação
do evento, então reentregar o mesmo id com **outro** `occurredAt` também conta uma vez. Para a chave primária de
`usage_event`, que inclui `occurred_at` por causa do particionamento, o mesmo id com o mesmo horário também é barrado: as duas
defesas valem. O `eventId` precisa ser determinístico por item; o horário não precisa mais ser.

## Operação

- **`RedisClient` do host.** Se a aplicação já declara um bean `RedisClient`, o starter o reutiliza e **ignora**
  `usage.redis.*` e os timeouts/`REJECT_COMMANDS` que ele configuraria: com um cliente seu, a política de timeout e de
  fila offline é a sua. Um timeout curto no cliente é o que faz uma queda do Redis virar degradação rápida em vez de
  requisições penduradas.
- **Fechamento e reconciliação**: `usage.maintenance.enabled=true`, ou chame `MaintenanceScheduler.runOnce()` do seu
  agendador. O lock é um advisory lock do Postgres por transação; ele segura uma conexão do pool durante o job.
- **Reconciliação**: resultado sobe e desce para igualar o resumo; acesso só sobe (um contador de acesso pode
  legitimamente estar à frente do ledger). Rate e concorrência não são reconciliados.
- **Lease de concorrência**: cada dono de um slot tem o **seu próprio** prazo (`usage.concurrency-lease`, 15 min por
  padrão). Um processador que cai libera só o seu slot quando o prazo acaba, mesmo com o tenant ocupado, e uma
  liberação tardia nunca remove o slot de outro. Um job mais longo que o lease perde o seu slot (é o limite
  de segurança, aumente o lease se precisar). No Redis o prazo vem do relógio do próprio Redis; as chaves levam
  `{hash tag}`, então cabem num slot do Cluster.
- **Tempo de parada**: ao encerrar, o agendador espera até 10 s pelo job em andamento antes de forçar, e o executor do
  JDBC espera as consultas terminarem; assim os stores não são fechados debaixo de um job.
- **Cache de política**: limitado a 100.000 tenants (`CachedPolicyStore`, quarto argumento); tenants desconhecidos também
  são guardados (como "sem política"), então sem o limite um fluxo de ids forjados cresceria o heap.
- **Cache de política (invalidação)**: chame `CachedPolicyStore.invalidate` após qualquer mudança administrativa e propague entre
  instâncias; do contrário vale o TTL.
- **Métricas e alertas**: veja "Observabilidade". Sem o starter, implemente `UsageTelemetry` e passe-a ao `UsageEngine`,
  ao `MaintenanceScheduler` e ao `CachedPolicyStore`; o gateway, o processador e o consumidor a usam por `engine.telemetry()`.

## Banco de dados

Migrations em `usage-postgres/src/main/resources/db/usage/` (`V1__usage_core.sql` e `V2__event_id_gate_and_indexes.sql`). **O V1 está congelado** (um teste confere o seu checksum): toda mudança de schema entra como um novo `V<n>`. Tabelas: `tenant`, `metric`, `plan`,
`plan_limit` (janela fixa em segundos ou mensal em meses), `tenant_subscription`, `tenant_plan`,
`tenant_limit_override`, `usage_event` (particionada por `occurred_at`), `usage_event_id` (o dedup por id), `usage_event_slot` (em quais janelas cada evento foi
contado, para a compensação reverter exatamente isso), `usage_compensation`, `usage_summary`.
Os ids de tenant são `TEXT`.

**Schema próprio.** Por padrão as tabelas ficam no schema `usage` (`usage.tenant`, `usage.usage_summary`...), com o
histórico do Flyway (`usage_schema_history`) dentro dele. Os nomes genéricos (`tenant`, `metric`, `plan`) não colidem com
os do seu sistema, e a migration roda ao lado de um schema que já tem tabelas, sem tocá-las. Todo SQL da biblioteca é
qualificado com esse schema, então não depende do `search_path` da conexão. O nome entra no SQL, por isso só é aceito
um identificador simples em minúsculas (`[a-z_][a-z0-9_]*`); qualquer outra coisa falha no startup. Fora do starter,
passe o mesmo schema a `UsageSchema.migrate(ds, schema)` e a cada `Jdbc*` que tem um construtor com `schema` (todos, **menos `JdbcJobLock`**,
que não toca nas tabelas: ele usa um advisory lock, cuja chave vale o **banco inteiro**, então duas instâncias da plataforma
no mesmo banco, em schemas diferentes, disputam o mesmo lock de manutenção); sem
schema, eles usam o `search_path` como antes, **mas então o Flyway migra o schema padrão da conexão e se recusa a rodar se ele já tem tabelas sem histórico do Flyway**: em um banco que já tem tabelas, use o schema próprio (o padrão do starter). Quem cadastra tenants, métricas e planos por SQL deve qualificar:
`INSERT INTO usage.tenant ...`.

## Requisitos e build

- JDK 21, Maven 3.9+, **Spring Boot 4.2.0**, Spring Cloud Gateway 5.1.0 (artefato `...-gateway-server-webflux`).
- PostgreSQL **15 ou mais novo** (`NULLS NOT DISTINCT`). Testado com PostgreSQL 16 e Redis 7.
- Os testes de integração usam Testcontainers: **precisam de Docker**.
- O `pom.xml` raiz é o pai dos módulos: versão do Boot (BOM), do Spring Cloud Gateway, dos BOMs das duas bibliotecas locais
  e dos plugins ficam nele; as versões dos módulos entre si vêm do `usage-bom`, que o pai importa. O starter gera `spring-configuration-metadata.json`, então as propriedades `usage.*`
  têm autocomplete na IDE.

```bash
cd usage-platform
mvn test            # 246 testes; Redis e PostgreSQL reais em contêineres
mvn install         # instala os módulos no repositório local
```

## Segurança: o que a biblioteca assume

- **Tópicos e filas precisam de ACL.** O `tenantId` de uma mensagem é confiável porque só serviços seus publicam;
  sem ACL, qualquer um forja uso em nome de outro tenant.
- **`InternalTraffic` deve basear-se em identidade autenticada** (service account validada), nunca em header ou IP
  que o cliente possa forjar: o tráfego marcado como interno não é medido nem tem os `X-Tenant-*` removidos.
- O `TenantResolver` e o `UsageSink` são seus; a biblioteca confia neles.
- Os limites de tamanho do contrato (`tenantId` 128, `metricCode` e `reason` 60 caracteres, quantidade até 1e9) existem
  para que o banco nunca receba o que vai recusar: mensagem acima deles vai para a DLQ com motivo, e erros de dado
  ou integridade do banco (SQLSTATE 22 e 23) também, em vez de travar a partição em retentativas.

## Limites conhecidos

- **Gateway: evento de uso perdido não é reenviado.** Se o `UsageSink` falhar, o filtro registra o erro e segue; o
  buffer local/outbox é responsabilidade do seu `UsageSink`.
- **Rate limit** não devolve tokens em 5xx e a resposta 429 não traz `Retry-After`.
- Evento tardio de uma assinatura anterior é cortado com a âncora atual. Se o evento de devolução se perder, o
  contador de acesso fica alto até o fim do período.
- Sem `THROTTLE`, sem limites por rota ou por client, sem avisos de soft limit.
- Particionamento de `usage_event` usa só a partição *default*; criação automática de partições e retenção de
  `usage_event` ficam para você (`pg_partman` ou job; descartar partições antigas é a retenção natural).
- **Retenção das tabelas auxiliares.** `usage_event_id` (dedup) e `usage_event_slot` (o que a compensação reverte) crescem
  um registro por evento. `JdbcRetention.purgeOlderThan(cutoff)` apaga, em lotes, os ids gravados e os slots de períodos
  terminados antes do `cutoff`. Escolha um `cutoff` mais antigo que tudo que ainda pode chegar: a idade máxima do consumidor
  (90 dias), a tolerância de fechamento e o maior intervalo que você aceita entre um uso e a sua compensação. Depois do
  corte, uma reentrega seria contada de novo e uma compensação não acharia o que reverter. **Nada o agenda**: chame-o de um
  job seu (a biblioteca não decide o seu prazo de retenção). Não toca em `usage_summary`, que o billing lê.
- Os locks de linha do ledger seguem uma ordem fixa `(window_key, period_start)` e o fechamento de períodos pula as
  linhas que outra transação está escrevendo (a próxima rodada as fecha): nenhum dos dois espera por linha, então não há
  deadlock entre consumidores nem com o job. `idle_in_transaction_session_timeout` menor que a duração de um job
  encerraria o lock do job antes da hora: deixe-o desligado ou maior.
- O `usage-messaging` foi verificado contra RabbitMQ (acima), com a inbox montada à mão como no contrato da
  própria messaging-library. Não foi verificado contra os outros provedores. O registro do handler pelo starter da
  messaging-library foi verificado pelo bean de mesmo nome do receiver (não por `@MessageListener`).
- Mudar `usage.postgres.schema` depois de ter dados não move as tabelas: é outro conjunto vazio no schema novo.
- O reconciliador carrega todas as linhas abertas de uma vez; pagine antes de chegar a milhões de linhas.
- Tudo em UTC. Redis Cluster e carga concorrente alta não foram testados.
- Sem tabelas de provedores de identidade, API de administração, auditoria de mudanças ou painel.
- Auto-configuração e starter são um jar só.

## Decisões em aberto

Tolerância de fechamento (24–72 h), `fail-mode` padrão por limite crítico, quais status de resposta consomem cota,
rateio na troca de plano, política de retenção de `usage_event`, modelo de licenciamento e nome do produto.
