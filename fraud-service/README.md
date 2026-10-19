# fraud-service

Analisa o fluxo de transferências **em tempo real** com **Kafka Streams** e emite alertas quando detecta comportamento suspeito.

## 1. Por que este serviço existe

Detecção de fraude é o caso clássico de **stream processing**: a decisão depende de *janelas de tempo* e de *estado acumulado por cliente* (ex.: "5 transferências em 1 minuto", "valor 10× acima da média"). Fazer isso com `@KafkaListener` + banco seria lento e trabalhoso; Kafka Streams oferece janelas, agregações e estado tolerante a falhas prontos.

> **Fase 5.** Construa depois que a Saga estiver estável.

## 2. Responsabilidades

- Consumir `transfer.events.v1` e aplicar regras de detecção
- Manter estado por conta (contadores em janela, média de valores)
- Publicar alertas em `fraud.alerts.v1`
- Expor consulta dos alertas (opcional)

## 3. Decisões técnicas e por quê

| Decisão | Alternativa descartada | Motivo |
|---|---|---|
| **Kafka Streams** | Flink / Spark Streaming / consumer manual | É uma biblioteca (sem cluster extra), integra bem com Spring Boot e é o que se espera de uma vaga Kafka + Java |
| **Detecção assíncrona (pós-fato) na v1** | Bloquear a transferência antes | Mais simples e não coloca o fraud-service no caminho crítico. A evolução para *pré-autorização* está descrita na etapa 8 |
| **Reaproveitar a chave `sourceAccountId`** | Repartition por `accountId` | Os eventos já chegam particionados por conta de origem: `groupByKey()` **não gera repartition topic**, economizando I/O e latência |
| **Janelas hopping/tumbling** | Contar em banco | Estado local (RocksDB) e atualização incremental; sem consulta a banco por evento |
| **State store com changelog topic** | Estado só em memória | Se a instância cair, o estado é reconstruído a partir do changelog no Kafka |
| **`GlobalKTable` para lista de bloqueio** | Consulta REST | Replicada em todas as instâncias; *join* local sem rede |
| **Regras como `Predicate`/classes pequenas** | Um `if` gigante | Cada regra testável isoladamente com `TopologyTestDriver` |
| **`processing.guarantee=exactly_once_v2`** | at_least_once | Evita alertas duplicados em caso de falha (EOS dentro do Kafka, que é exatamente o escopo onde ele funciona) |

## 4. Regras de detecção

| Regra | Descrição | Recurso do Streams |
|---|---|---|
| **R1: Velocidade** | Mais de 5 transferências da mesma conta em 1 minuto | `windowedBy(TimeWindows)` + `count()` |
| **R2: Valor atípico** | Valor > 5× a média móvel das últimas transferências da conta | `aggregate()` em `KTable` (soma e contagem) + join stream-table |
| **R3: Destinatário em lista de bloqueio** | Conta de destino marcada como suspeita | `GlobalKTable` + join |
| **R4: Fracionamento (structuring)** | Várias transferências abaixo de um limite somando acima dele em 10 min | Janela *hopping* + `reduce()` |
| **R5: Madrugada + valor alto** | Valor alto entre 0h e 5h | `filter()` simples (bom para começar) |

Comece pela **R5** (sem estado), depois R1, R2, R3 e R4.

## 5. Tópicos

### Consome
- `transfer.events.v1` (chave `sourceAccountId`): usa `TransferRequested` e `TransferCompleted`
- `fraud.blocklist.v1` (*compacted*, chave `accountId`): lista de contas suspeitas (alimentada manualmente ou por endpoint admin)

### Produz
- `fraud.alerts.v1` (chave `accountId`): `SuspiciousActivityDetected`

```json
{
  "alertId": "b3f0...",
  "accountId": "3d2e...",
  "transferId": "a91b...",
  "rule": "VELOCITY",
  "severity": "HIGH",
  "details": {"count": 7, "windowSeconds": 60},
  "detectedAt": "2026-01-10T03:12:45Z"
}
```

### Tópicos internos (criados pelo Streams)
`<application.id>-<store>-changelog` guardam o estado dos *state stores*. Deixe o Streams criá-los e **observe-os no Kafka UI**: é aprendizado direto.

## 6. Modelo de dados

O estado de processamento vive nos **state stores** (RocksDB + changelog no Kafka), **não em banco relacional**. Isso é uma decisão deliberada: estado local = baixa latência.

Um banco é opcional apenas para **consulta e revisão de alertas** pela equipe antifraude, alimentado por um consumer simples de `fraud.alerts.v1`:

```sql
CREATE TABLE fraud_alerts (
    id           UUID PRIMARY KEY,           -- alertId (dedupe)
    account_id   UUID         NOT NULL,
    transfer_id  UUID,
    rule         VARCHAR(40)  NOT NULL,
    severity     VARCHAR(10)  NOT NULL,      -- LOW | MEDIUM | HIGH
    details      JSONB        NOT NULL,
    status       VARCHAR(12)  NOT NULL DEFAULT 'OPEN',  -- OPEN | REVIEWED | FALSE_POSITIVE | CONFIRMED
    detected_at  TIMESTAMPTZ  NOT NULL,
    reviewed_at  TIMESTAMPTZ,
    reviewed_by  VARCHAR(80)
);
CREATE INDEX idx_fraud_alerts_account ON fraud_alerts (account_id, detected_at DESC);
CREATE INDEX idx_fraud_alerts_open ON fraud_alerts (status) WHERE status = 'OPEN';
```

## 7. API REST (opcional)

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/alerts?status=OPEN` | Lista alertas |
| `PATCH` | `/alerts/{id}` | Marca como revisado / falso positivo |
| `PUT` | `/blocklist/{accountId}` | Publica em `fraud.blocklist.v1` |
| `GET` | `/state/velocity/{accountId}` | *Interactive Queries*: consulta direta ao state store |

## 8. Etapas de implementação

### Etapa 1: Topologia mínima (Fase 5)
- Dependência `kafka-streams`, `@EnableKafkaStreams`, `application.id=fraud-service`
- Ler `transfer.events.v1`, filtrar `TransferRequested` e logar com `peek()`
- **Conceito:** KStream, serde, `application.id` como identidade do grupo
- **Pronto quando:** vejo os eventos passando pela topologia

### Etapa 2: Regra sem estado (R5)
- `filter()` por horário e valor → `to("fraud.alerts.v1")`
- Testes com `TopologyTestDriver` (não precisa de broker)
- **Pronto quando:** teste unitário gera alerta para madrugada + valor alto

### Etapa 3: Janela de tempo (R1: velocidade)
- `groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))).count()` → filtra `> 5` → alerta
- Entender **event time vs. processing time** e *grace period*
- Cuidado: `suppress()` para emitir só o resultado final da janela, não cada atualização
- **Conceito:** janelas tumbling/hopping/session, late events
- **Pronto quando:** 7 transferências em 1 min geram **um** alerta (e não sete)

### Etapa 4: Agregação com estado (R2: valor atípico)
- `aggregate()` mantendo `{soma, contagem}` por conta em uma `KTable`
- Join stream-table: compara o valor atual com a média
- Nomear e materializar o state store explicitamente (`Materialized.as("avg-amount-store")`)
- **Conceito:** KTable, state store, changelog topic
- **Pronto quando:** reiniciar o serviço **não** zera as médias

### Etapa 5: Enriquecimento (R3: blocklist)
- `GlobalKTable` de `fraud.blocklist.v1` (tópico compactado)
- Join do stream com a global table pela conta de destino
- **Conceito:** log compaction, GlobalKTable vs. KTable
- **Pronto quando:** adicionar uma conta à blocklist faz as próximas transferências para ela gerarem alerta

### Etapa 6: Regra composta (R4: fracionamento)
- Janela hopping de 10 min com avanço de 1 min, `reduce()` somando valores
- **Pronto quando:** 4 transferências de R$ 2.900 geram alerta com limite de R$ 10.000

### Etapa 7: Escala e tolerância a falhas
- Subir 2 instâncias, matar uma, observar o *rebalance* e a restauração do estado
- Configurar `num.standby.replicas=1`
- **Conceito:** tasks, partições, restauração de estado, standby replicas
- **Pronto quando:** consigo explicar o que acontece com o state store quando a instância morre

### Etapa 8: Evolução para pré-autorização (opcional avançada)
- Adicionar o passo `FRAUD_CHECK` na Saga: o transfer-service publica `FraudCheckRequested` e aguarda `FraudCheckApproved/Rejected` antes de debitar
- Discutir o trade-off: **segurança vs. latência/disponibilidade** (o que fazer se o fraud-service estiver fora? *fail-open* ou *fail-closed*?)
- **Pronto quando:** existe um ADR documentando a decisão

### Etapa 9: Testes e observabilidade
- `TopologyTestDriver` para todas as regras (inclusive relógio simulado para janelas)
- Métricas do Streams (`stream-thread-metrics`, `record-lateness`), alertas por regra
- **Pronto quando:** dashboard mostra alertas por regra e latência de processamento

## 9. Estrutura de pacotes sugerida

```
com.exemplo.fraud
├── topology/      (FraudTopology, uma classe por regra)
├── rules/         (VelocityRule, AtypicalAmountRule, ...)
├── serde/         (serdes Avro e de agregados)
├── alerts/        (consumer opcional + repositório)
└── api/
```

## 10. Perguntas de entrevista que este serviço responde

- *Diferença entre KStream, KTable e GlobalKTable?* → Fluxo de eventos, última versão por chave (particionada) e tabela replicada em todas as instâncias.
- *Como o Kafka Streams guarda estado e como se recupera de falhas?* → RocksDB local + changelog topic; standby replicas aceleram a recuperação.
- *O que é event time e por que importa?* → Janelas devem usar a hora do evento, não a do processamento, senão atrasos distorcem o resultado.
- *Por que `groupByKey` é melhor que `groupBy`?* → `groupBy` com nova chave força *repartition*; `groupByKey` reaproveita o particionamento.
- *O que é log compaction?* → Mantém só o último valor por chave: ideal para tabelas de referência como a blocklist.
- *Exactly-once no Streams funciona como?* → Transações Kafka (produtor + offsets atômicos), válido dentro do ecossistema Kafka.
