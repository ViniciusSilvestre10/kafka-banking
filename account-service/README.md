# account-service

Dono da verdade sobre **contas e saldos**. Recebe comandos (debitar, creditar, estornar), aplica regras de negócio e publica eventos com o resultado.

## 1. Por que este serviço existe

Saldo é o dado mais sensível do banco. Centralizar a escrita em um único serviço garante que **só existe um lugar** onde o saldo muda, o que simplifica regras (saldo insuficiente, conta bloqueada) e auditoria. Outros serviços nunca alteram o saldo diretamente; eles pedem via comando e reagem aos eventos.

## 2. Responsabilidades

- Abrir, consultar e bloquear/desbloquear contas
- Processar comandos `DebitAccount`, `CreditAccount`, `ReverseDebit`
- Manter o **ledger** (extrato imutável) e o saldo consistentes
- Publicar eventos de domínio via **Outbox**
- Garantir **idempotência**: o mesmo comando processado duas vezes não pode mexer no saldo duas vezes

## 3. Decisões técnicas e por quê

| Decisão | Alternativa descartada | Motivo |
|---|---|---|
| **Ledger + saldo materializado** (`ledger_entries` + `accounts.balance`) | Event Sourcing puro | Mais simples de implementar e de consultar, mantém histórico imutável e permite reconstruir o saldo. Event Sourcing fica como evolução opcional (ADR-005) |
| **Optimistic locking** (`@Version`) | Lock pessimista | Conflitos por conta são raros e a ordem já é garantida pela partição; evita contenção de banco |
| **Comandos em tópico próprio** (`account.commands.v1`) | Chamada REST do transfer-service | Desacopla disponibilidade: se account-service cair, os comandos esperam no Kafka em vez de falhar |
| **Chave = `accountId`** | Chave = `transferId` | Garante que operações da mesma conta sejam sequenciais, evitando corrida no saldo |
| **Outbox transacional** | Publicar direto no Kafka após `save()` | Se o app cair entre o commit e o `send()`, o evento se perde. O outbox grava evento e saldo **na mesma transação** |
| **Idempotência via `processed_messages`** | Confiar em exactly-once do Kafka | Exactly-once do Kafka vale só entre Kafka e Kafka. Efeitos no banco precisam de dedupe própria |
| **Constraint única no ledger** `(transfer_id, account_id, type)` | Só checar na aplicação | Última linha de defesa: o banco rejeita duplicata mesmo com bug na aplicação |
| **Valores monetários em `NUMERIC(19,2)` / `BigDecimal`** | `double` | Ponto flutuante gera erro de arredondamento |

## 4. Eventos

### Consome: `account.commands.v1`

| Comando | Campos principais | Regra |
|---|---|---|
| `DebitAccount` | `transferId`, `accountId`, `amount` | Rejeita se saldo insuficiente ou conta bloqueada |
| `CreditAccount` | `transferId`, `accountId`, `amount` | Rejeita se conta inexistente ou encerrada |
| `ReverseDebit` | `transferId`, `accountId`, `amount` | Compensação da Saga: devolve o valor debitado |

### Produz: `account.events.v1`

`AccountOpened`, `AccountBlocked`, `AccountUnblocked`, `DebitCompleted`, `DebitRejected`, `CreditCompleted`, `CreditRejected`, `DebitReversed`.

Exemplo (Avro, representado em JSON):

```json
{
  "eventId": "6c1f...",
  "transferId": "a91b...",
  "accountId": "3d2e...",
  "amount": "150.00",
  "balanceAfter": "850.00",
  "occurredAt": "2026-01-10T14:32:11Z"
}
```

> `DebitRejected` e `CreditRejected` carregam `reason` (`INSUFFICIENT_FUNDS`, `ACCOUNT_BLOCKED`, `ACCOUNT_NOT_FOUND`). **Rejeição de negócio é um evento, não uma exceção**: ela não deve ir para a DLT.

## 5. Modelo de dados (PostgreSQL + Flyway)

```sql
-- V1__init.sql
CREATE TABLE accounts (
    id            UUID PRIMARY KEY,
    customer_id   UUID           NOT NULL,
    agency        VARCHAR(4)     NOT NULL,
    number        VARCHAR(12)    NOT NULL,
    holder_name   VARCHAR(120)   NOT NULL,
    holder_email  VARCHAR(160)   NOT NULL,
    holder_phone  VARCHAR(20),
    status        VARCHAR(16)    NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE | BLOCKED | CLOSED
    balance       NUMERIC(19,2)  NOT NULL DEFAULT 0,
    version       BIGINT         NOT NULL DEFAULT 0,          -- optimistic lock
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT uq_account_number UNIQUE (agency, number),
    CONSTRAINT ck_balance_non_negative CHECK (balance >= 0)
);

CREATE TABLE ledger_entries (
    id             UUID PRIMARY KEY,
    account_id     UUID           NOT NULL REFERENCES accounts(id),
    transfer_id    UUID           NOT NULL,
    type           VARCHAR(16)    NOT NULL,   -- DEBIT | CREDIT | REVERSAL
    amount         NUMERIC(19,2)  NOT NULL CHECK (amount > 0),
    balance_after  NUMERIC(19,2)  NOT NULL,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT uq_ledger_idempotency UNIQUE (transfer_id, account_id, type)
);
CREATE INDEX idx_ledger_account_created ON ledger_entries (account_id, created_at DESC);

-- dedupe de mensagens consumidas
CREATE TABLE processed_messages (
    message_id     UUID         NOT NULL,
    consumer_name  VARCHAR(80)  NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, consumer_name)
);

-- Transactional Outbox
CREATE TABLE outbox_events (
    id             UUID PRIMARY KEY,
    aggregate_type VARCHAR(40)  NOT NULL,        -- 'Account'
    aggregate_id   VARCHAR(64)  NOT NULL,        -- vira a chave Kafka
    event_type     VARCHAR(80)  NOT NULL,
    topic          VARCHAR(120) NOT NULL,
    payload        BYTEA        NOT NULL,        -- Avro serializado
    headers        JSONB        NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ                   -- NULL = pendente
);
CREATE INDEX idx_outbox_pending ON outbox_events (created_at) WHERE published_at IS NULL;
```

**Por que `CHECK (balance >= 0)`?** Segunda barreira contra saldo negativo caso a regra da aplicação falhe.
**Por que `holder_email` aqui?** O notification-service mantém uma réplica local de contatos alimentada por `AccountOpened`; a fonte original dos dados é esta tabela.

## 6. API REST (apenas para cadastro e consulta)

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/accounts` | Abre conta (publica `AccountOpened`) |
| `GET` | `/accounts/{id}` | Consulta saldo e status |
| `GET` | `/accounts/{id}/statement?from=&to=` | Extrato paginado (ledger) |
| `POST` | `/accounts/{id}/block` | Bloqueia conta (publica `AccountBlocked`) |

Movimentação de dinheiro **não** tem endpoint REST: ela só acontece via comandos Kafka, vindos da Saga.

## 7. Etapas de implementação

### Etapa 1: Esqueleto e cadastro de contas (Fase 1)
- Projeto Spring Boot com Web, JPA, Flyway, PostgreSQL, Validation
- Migration `V1` com `accounts`
- `POST /accounts` e `GET /accounts/{id}`
- **Pronto quando:** consigo criar e consultar uma conta; testes com Testcontainers (Postgres)

### Etapa 2: Primeiro producer (Fase 1)
- Configurar `spring-kafka`, `KafkaTemplate`, `acks=all`, `enable.idempotence=true`
- Publicar `AccountOpened` em `account.events.v1` com **chave = accountId**
- Visualizar a mensagem no Kafka UI
- **Conceito:** producer, partição, chave, offset
- **Pronto quando:** abrir conta gera mensagem na partição esperada

### Etapa 3: Consumer de comandos (Fase 1)
- `@KafkaListener` em `account.commands.v1` com `groupId=account-service`
- `ack-mode=MANUAL`, `auto.commit=false`
- Implementar débito/crédito no ledger, ainda com publicação direta
- Subir 2 instâncias e observar o rebalanceamento
- **Conceito:** consumer group, rebalance, commit de offset
- **Pronto quando:** comando de débito altera saldo e gera `DebitCompleted`

### Etapa 4: Avro + Schema Registry (Fase 2)
- Criar `.avsc` dos comandos e eventos em um módulo compartilhado (`contracts`)
- Plugin `avro-maven-plugin` para gerar classes; `KafkaAvroSerializer`/`Deserializer`
- Testar evolução: adicionar campo com default sem quebrar consumers
- **Conceito:** compatibilidade BACKWARD, subject strategy
- **Pronto quando:** tentar registrar schema incompatível falha no CI

### Etapa 5: Regras de negócio e rejeições (Fase 2)
- `DebitRejected` / `CreditRejected` como eventos (sem exceção)
- Erros transitórios (banco fora do ar) vão para retry/DLT; erros de negócio, não
- **Conceito:** diferença entre erro de negócio e erro técnico
- **Pronto quando:** saldo insuficiente gera `DebitRejected` sem cair na DLT

### Etapa 6: Idempotência do consumer (Fase 3)
- Na **mesma transação**: inserir em `processed_messages`, alterar saldo, inserir ledger e outbox
- Se `message_id` já existir, ignorar e dar ack
- Teste: enviar o mesmo comando 10 vezes → saldo muda uma vez
- **Conceito:** at-least-once + idempotência = efeito exactly-once
- **Pronto quando:** teste de duplicata passa, inclusive com consumers concorrentes

### Etapa 7: Outbox Pattern (Fase 3)
- Substituir `kafkaTemplate.send()` por insert em `outbox_events`
- `OutboxPublisher` agendado: `SELECT ... WHERE published_at IS NULL ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 100`, publica, marca `published_at`
- Teste de caos: matar o app entre o commit e o envio → evento ainda é publicado depois
- **Conceito:** dual write problem, at-least-once no publisher
- **Evolução (opcional):** trocar o polling por **Debezium (CDC)** lendo o WAL do Postgres
- **Pronto quando:** nenhum evento se perde no teste de caos

### Etapa 8: Compensação (Fase 4)
- Implementar `ReverseDebit` (idempotente, com `type=REVERSAL` no ledger)
- **Pronto quando:** transferência que falha no crédito devolve o valor ao pagador

### Etapa 9: Observabilidade e testes (Fase 6)
- Métricas: eventos publicados, tamanho do outbox pendente, latência de processamento
- Trace propagado por headers
- Testes: unitário (regras), integração (Testcontainers Kafka + Postgres), contrato Avro
- **Pronto quando:** dashboard mostra consumer lag e backlog do outbox

## 8. Estrutura de pacotes sugerida

```
com.exemplo.account
├── api/            (controllers, DTOs)
├── application/    (use cases: DebitAccount, CreditAccount, ReverseDebit)
├── domain/         (Account, LedgerEntry, regras)
├── infrastructure/
│   ├── persistence/ (repositories JPA)
│   ├── messaging/   (listeners, OutboxPublisher, mapeadores Avro)
│   └── config/
```

## 9. Perguntas de entrevista que este serviço responde

- *Como evitar débito duplicado se a mensagem for entregue duas vezes?* → `processed_messages` + constraint única no ledger, na mesma transação.
- *Como garantir que o saldo e o evento sejam consistentes?* → Outbox transacional.
- *Por que a chave é o accountId?* → Ordem por conta dentro da partição.
- *O que acontece se o consumer cair após gravar no banco e antes do commit do offset?* → A mensagem é reentregue; a idempotência absorve a duplicata.
- *Por que não usar exactly-once do Kafka aqui?* → Ele não cobre efeitos colaterais no banco de dados.
