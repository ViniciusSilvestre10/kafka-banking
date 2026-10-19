# audit-service

Guarda uma **trilha de auditoria imutável** de tudo que acontece na plataforma, consumindo os eventos de todos os serviços.

## 1. Por que este serviço existe

Bancos precisam responder, anos depois: *"quem fez o quê, quando, e em que ordem?"*. Como o Kafka é um **log persistente** e cada consumer group lê de forma independente, o audit-service pode registrar tudo sem afetar nenhum outro serviço, e pode **reler o passado** (replay) para reconstruir a trilha se algo der errado.

> **Fase 5.** Serviço pequeno, mas excelente para demonstrar offsets, replay e retenção.

## 2. Responsabilidades

- Consumir `account.events.v1`, `transfer.events.v1` e `fraud.alerts.v1`
- Gravar cada evento em uma tabela **append-only**, com origem exata (tópico, partição, offset)
- Garantir **integridade verificável** (encadeamento por hash)
- Permitir consulta por conta, transferência e período
- Suportar **replay**: reconstruir a trilha a partir do Kafka

## 3. Decisões técnicas e por quê

| Decisão | Alternativa descartada | Motivo |
|---|---|---|
| **Consumer group exclusivo** (`audit-service`) | Reaproveitar group de outro serviço | Independência total: se a auditoria atrasar ou for reprocessada, ninguém é afetado |
| **Tabela append-only** (sem `UPDATE`/`DELETE`, reforçado por trigger/permissões) | Tabela comum | Imutabilidade é requisito de auditoria, e deve ser garantida pelo banco, não só pela aplicação |
| **Unique `(topic, partition, "offset")`** | Dedupe por `eventId` apenas | O offset identifica de forma única a posição no log: reprocessar não duplica registros |
| **Encadeamento de hash** (cada registro guarda o hash do anterior) | Sem verificação | Qualquer adulteração quebra a cadeia: *tamper-evidence*, estilo blockchain simplificado |
| **Guardar o payload bruto (JSONB)** | Guardar só campos selecionados | Auditoria precisa do evento original; novas perguntas surgem depois |
| **Commit de offset só após gravar** | Auto-commit | Nada é "consumido" sem estar persistido: at-least-once + unique = sem perda e sem duplicata |
| **Retenção longa nos tópicos de origem** (30+ dias) | Retenção padrão de 7 dias | Permite replay e reparo de lacunas |
| **Um consumer para vários tópicos** (`topicPattern` ou lista) | Um serviço por tópico | Simplicidade; a partição de cada tópico mantém sua ordem |

## 4. Eventos

### Consome (`groupId=audit-service`)
`account.events.v1`, `transfer.events.v1`, `fraud.alerts.v1`, e opcionalmente os tópicos `*-dlt` (falhas também são história).

### Produz
Nada. É um **sink**.

> Dica: leia os headers (`eventId`, `correlationId`, `eventType`, `occurredAt`) e guarde-os. O `correlationId` permite montar a linha do tempo de uma transferência atravessando serviços.

## 5. Modelo de dados (PostgreSQL + Flyway)

```sql
CREATE TABLE audit_events (
    seq             BIGSERIAL PRIMARY KEY,         -- ordem de gravação (base do encadeamento)
    event_id        UUID          NOT NULL,
    correlation_id  UUID,
    event_type      VARCHAR(80)   NOT NULL,
    aggregate_id    VARCHAR(64)   NOT NULL,        -- accountId ou transferId
    source_topic    VARCHAR(120)  NOT NULL,
    source_partition INT          NOT NULL,
    source_offset   BIGINT        NOT NULL,
    occurred_at     TIMESTAMPTZ   NOT NULL,        -- instante de negócio
    ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    payload         JSONB         NOT NULL,
    previous_hash   CHAR(64)      NOT NULL,        -- hash do registro anterior ('0'*64 no primeiro)
    hash            CHAR(64)      NOT NULL,        -- SHA-256(previous_hash || event_id || payload || occurred_at)
    CONSTRAINT uq_audit_source UNIQUE (source_topic, source_partition, source_offset)
);
CREATE INDEX idx_audit_aggregate ON audit_events (aggregate_id, occurred_at);
CREATE INDEX idx_audit_correlation ON audit_events (correlation_id);
CREATE INDEX idx_audit_occurred ON audit_events (occurred_at);

-- imutabilidade garantida pelo banco
CREATE OR REPLACE FUNCTION audit_block_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'audit_events é append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_no_update
    BEFORE UPDATE OR DELETE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION audit_block_mutation();
```

Em produção, complemente com um usuário de banco da aplicação que só tem `INSERT` e `SELECT`.

**Por que uma cadeia de hash com um único "elo" global?** Para o projeto, uma cadeia única é simples de explicar e verificar. O custo é que a gravação fica **serializada** (cada insert depende do anterior). Alternativa mais escalável: uma cadeia **por `aggregate_id`**. Documente esse trade-off em um ADR.

## 6. API REST

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/audit?aggregateId=&from=&to=` | Eventos de uma conta/transferência |
| `GET` | `/audit/correlation/{id}` | Linha do tempo completa de uma transferência entre serviços |
| `GET` | `/audit/verify?fromSeq=&toSeq=` | Recalcula os hashes e informa se a cadeia está íntegra |
| `POST` | `/admin/replay` | Dispara o replay (ver etapa 6) |

## 7. Etapas de implementação

### Etapa 1: Consumer multi-tópico (Fase 5)
- `@KafkaListener(topics = {...}, groupId = "audit-service")`, ack manual
- Persistir cada evento com `topic/partition/offset`
- **Conceito:** um grupo lendo vários tópicos; offset como identidade
- **Pronto quando:** todo evento dos três tópicos aparece em `audit_events`

### Etapa 2: Idempotência por offset
- A unique `(topic, partition, offset)` impede duplicata; capturar a violação e dar ack
- Teste: reiniciar o consumer com offset anterior (`seek`) não duplica registros
- **Pronto quando:** reprocessar 1.000 mensagens mantém a contagem inalterada

### Etapa 3: Imutabilidade
- Trigger que bloqueia `UPDATE`/`DELETE`; teste de integração tentando alterar um registro
- **Pronto quando:** a tentativa de alteração falha no banco

### Etapa 4: Encadeamento de hash
- Ao inserir, buscar o último hash (com lock, por exemplo `SELECT ... ORDER BY seq DESC LIMIT 1 FOR UPDATE` ou advisory lock) e calcular o novo
- Endpoint `/audit/verify` que percorre a cadeia e aponta o primeiro registro divergente
- Teste: alterar um registro via SQL (desabilitando o trigger só no teste) → `verify` detecta
- **Conceito:** *tamper-evidence*; trade-off entre integridade e throughput
- **Pronto quando:** adulteração é detectada pelo endpoint

### Etapa 5: Consultas e linha do tempo
- Endpoints por agregado, período e `correlationId`
- **Pronto quando:** consigo ver "tudo que aconteceu com a transferência X" em ordem, passando por vários serviços

### Etapa 6: Replay (o grande aprendizado)
- Criar um **novo consumer group** (`audit-service-replay`) ou usar `KafkaConsumer.offsetsForTimes()` + `seek()` para reler a partir de uma data
- Cenário: apagar um intervalo da tabela (em ambiente de teste) e reconstruir a partir do Kafka, validando que o resultado é idêntico
- Experimentar `kafka-consumer-groups.sh --reset-offsets` (`--to-datetime`, `--to-earliest`)
- **Conceito:** offsets, retenção, replay, diferença entre log e fila
- **Pronto quando:** reconstruo a trilha após uma "perda" simulada e `verify` retorna íntegro

### Etapa 7: Retenção e armazenamento
- Configurar `retention.ms` dos tópicos de origem e justificar o valor em um ADR
- Discutir arquivamento (ex.: exportar partições antigas de `audit_events` para storage barato) e **LGPD** (como tratar dados pessoais em registros imutáveis: pseudonimização/token em vez do dado bruto)
- **Pronto quando:** existe um ADR sobre retenção e privacidade

### Etapa 8: Métricas e testes
- Métricas: lag do grupo de auditoria, eventos gravados/s, resultado da última verificação de integridade
- Testes: Testcontainers (Kafka + Postgres) para ingestão, duplicata, replay e adulteração
- **Pronto quando:** alerta dispara se o lag do `audit-service` crescer ou se `verify` falhar

## 8. Estrutura de pacotes sugerida

```
com.exemplo.audit
├── api/
├── application/    (IngestEvent, VerifyChain, ReplayService)
├── domain/         (AuditEvent, HashChain)
└── infrastructure/
    ├── messaging/  (listener multi-tópico)
    └── persistence/
```

## 9. Perguntas de entrevista que este serviço responde

- *O que é um offset e como você reprocessa mensagens antigas?* → Posição na partição; `seek`, `offsetsForTimes` ou reset de offsets do grupo.
- *Por que o Kafka é bom para auditoria e RabbitMQ não tanto?* → Log persistente e relível por múltiplos consumers; fila apaga ao consumir.
- *Como garantir que ninguém adulterou o histórico?* → Append-only no banco + cadeia de hash verificável.
- *Como evitar duplicatas ao reprocessar?* → Unique `(topic, partition, offset)`.
- *Como conciliar auditoria imutável com LGPD?* → Pseudonimização e minimização de dados pessoais nos eventos.
- *O que acontece se o audit-service ficar parado por um dia?* → Nada se perde: o lag cresce e ele recupera ao voltar, desde que a retenção cubra o período.
