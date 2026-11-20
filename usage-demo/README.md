# usage-demo

Aplicação Spring Cloud Gateway pronta para rodar, com a plataforma de uso ligada só por propriedades: dois planos, dois
tenants, limite de taxa, cota diária, cobrança por lote e devolução de cota em erro 5xx.

## Rodar

```bash
cd usage-platform
mvn -DskipTests install            # instala os módulos (e as suas duas bibliotecas locais, antes: veja o README raiz)
cd usage-demo
docker compose up -d               # PostgreSQL e Redis
mvn spring-boot:run
```

Na subida a aplicação cria as tabelas no schema `usage` (`usage.postgres.migrate=true`) e semeia o catálogo
([DemoData](src/main/java/dev/usageplatform/demo/DemoData.java)):

| Tenant | Chave (`X-Api-Key`) | Plano | Limites de `API_REQUESTS` |
|---|---|---|---|
| `acme` | `key-acme` | FREE | 3 por minuto, 20 por dia; 5 documentos por dia, 1 por vez |
| `globex` | `key-globex` | PRO | 100 por minuto, 1000 por dia; 1000 documentos por dia, 4 ao mesmo tempo |
| `initech` | `key-initech` | TRIAL | 100 por minuto, 1000 por dia; **3 documentos por dia, 1 por vez** |

## Experimentar

```bash
# o backend recebe o tenant e o plano que o gateway confiou (um X-Tenant-Id enviado pelo cliente é descartado)
curl -s localhost:8080/api/orders -H 'X-Api-Key: key-globex' -H 'X-Tenant-Id: outro'

# o plano FREE recusa a quarta chamada no minuto: 429 {"error":"rate_limited"}
for i in 1 2 3 4; do curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/api/orders -H 'X-Api-Key: key-acme'; done

# um lote declara o seu custo; ele conta em todos os limites da métrica, inclusive o de taxa
curl -s -XPOST localhost:8080/api/batch -H 'X-Api-Key: key-globex' -H 'X-Item-Count: 40'

# chave desconhecida: 403 {"error":"client_not_linked"}
curl -s localhost:8080/api/orders -H 'X-Api-Key: nenhuma'

# o backend falha com 503: a cota da requisição volta
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/api/fail -H 'X-Api-Key: key-globex'
```

O que foi contado, no ledger e no resumo que o billing lê:

```bash
docker compose exec postgres psql -U usage -c \
  "select tenant_id, metric_code, window_key, period_start, total from usage.usage_summary order by 1,3"
```

## Uso assíncrono: a métrica só conta depois de processar

`API_REQUESTS` é uma métrica de **acesso**: o gateway a conta quando a requisição chega. `ITEMS_PROCESSED` é uma métrica de
**resultado**: ela só é contada **depois** que um processamento assíncrono termina com um status que se cobra. Os dois lados
convivem no mesmo demo (pacote [async](src/main/java/dev/usageplatform/demo/async)):

```
POST /api/documents ──► gateway (conta a requisição: API_REQUESTS)
                          └─► DocumentController: confere a cota de documentos SEM consumir (engine.check) ─► 202 e fila
                                                                                                              │
                                  DocumentWorker (a fila do demo faz o papel do seu broker)  ◄──────────────┘
                                    QueueUsageAdapter.handle(): cota já estourada? vaga de concorrência? ─► trabalho
                                    ─► grava o resultado E o evento de uso ─► só COMPLETED conta (ITEMS_PROCESSED)
```

```bash
# aceito na hora (202); nada foi cobrado ainda
curl -s -XPOST localhost:8080/api/documents -H 'X-Api-Key: key-globex' -d 'ok: meu documento'
curl -s localhost:8080/api/documents/<id> -H 'X-Api-Key: key-globex'      # PENDING ... e depois COMPLETED

# um status final que não se cobra: REJECTED é gravado, mas não conta
curl -s -XPOST localhost:8080/api/documents -H 'X-Api-Key: key-globex' -d 'reject: inválido'

# mensagem entregue duas vezes (o broker perdeu o ack): cobrada uma vez só
curl -s -XPOST localhost:8080/api/documents -H 'X-Api-Key: key-globex' -d 'dup: entregue duas vezes'

# TRIAL: 3 por dia, 1 por vez. Cinco documentos: três passam, um de cada vez; os outros esperam (DEFERRED).
# Depois da cota, um novo POST é recusado na porta: 429 quota_exceeded
for i in 1 2 3 4 5; do curl -s -XPOST localhost:8080/api/documents -H 'X-Api-Key: key-initech' -d "ok: $i"; done
```

O que cada parte demonstra:

- **Contado depois, não antes.** `QueueUsageAdapter` entrega ao seu código (`save` no
  [DocumentWorker](src/main/java/dev/usageplatform/demo/async/DocumentWorker.java)) o resultado e o evento de uso juntos. O
  evento só existe se o status final está em `usage.processor.countable-statuses` (`COMPLETED`).
- **Reentrega não cobra duas vezes.** O id do evento deriva do id do documento; o ledger reconhece o repetido.
- **Concorrência por plano.** Sem vaga, o adaptador devolve `DEFERRED` e o worker tenta de novo em 1 s (no seu broker:
  `retryAfter`/nack).
- **Cota estourada.** Com `usage.processor.defer-when-over-quota=true` o que já está na fila espera em vez de ser
  processado acima da cota, e a porta (`DocumentController`) recusa novos pedidos com 429.
- **O que o billing lê** é o resumo, e ele só tem o que foi processado:
  `select * from usage.usage_summary where metric_code = 'ITEMS_PROCESSED'`.

## O que há aqui

- [application.yml](src/main/resources/application.yml): a rota `/api/**` para o backend do próprio demo, e tudo de
  `usage.*` (Redis, PostgreSQL, gateway, consumidor central, manutenção).
- [DemoConfiguration](src/main/java/dev/usageplatform/demo/DemoConfiguration.java): as duas coisas que o seu gateway
  precisa fornecer, **quem é o tenant** (aqui, pela chave de API) e **para onde vai o uso** (aqui, direto ao consumidor
  central no mesmo processo; sem broker).
- [UpstreamController](src/main/java/dev/usageplatform/demo/UpstreamController.java): o backend atrás do gateway.
- [DemoSmokeTest](src/test/java/dev/usageplatform/demo/DemoSmokeTest.java): sobe tudo com Redis e PostgreSQL reais e
  confere os cenários acima.

## Do demo para produção

- **A fila do demo é um stand-in.** Ela vive na memória: um reinício perde o que estava nela. Em produção a mensagem vem do
  seu broker, o `handle` fica no listener e o veredito vira ack/retry/dead-letter. Grave o resultado e o evento de uso na
  **mesma transação** (`OutboxUsageSink.sameTransaction`): aqui são duas chamadas seguidas, só para o demo não precisar de um
  outbox.
- **Uso por broker.** O `UsageSink` do demo chama o consumidor no mesmo processo, então um banco lento atrasa o fim da
  requisição. Em produção use `usage.messaging.outbox.destination` (outbox) e consuma com a messaging-library
  (veja "Ligando a um broker" no README raiz).
- **Identidade.** A chave de API fixa é só para o demo; resolva o tenant de um JWT ou de um cadastro.
- **Custo declarado pelo cliente.** `X-Item-Count` é uma afirmação do cliente: confira no backend se a cobrança importa.
- **Seed.** O `DemoData` é do demo; planos, tenants e assinaturas vêm do seu sistema (o billing é o dono).
- **Janelas de taxa.** São fixas: perto da virada de uma janela passam até o dobro das chamadas.
