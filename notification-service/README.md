# notification-service

Avisa o cliente (e-mail, SMS, push) sobre o que aconteceu: transferência concluída, estornada, conta bloqueada, alerta de fraude.

## 1. Por que este serviço existe

Notificação é o caso de uso ideal para aprender **tratamento de falhas em consumers**: depende de sistemas externos instáveis (provedor de e-mail/SMS), nunca deve bloquear o fluxo financeiro e **não pode enviar duas vezes** a mesma mensagem. Se ele cair, o dinheiro continua andando; as notificações simplesmente chegam depois.

## 2. Responsabilidades

- Consumir eventos de vários tópicos e decidir quais geram notificação
- Montar a mensagem por template e canal
- Enviar com **retry e backoff**, sem travar a partição
- Mandar mensagens irrecuperáveis para a **DLT** e permitir reprocessá-las
- Garantir **no máximo um envio por evento**
- Manter uma **réplica local de contatos** dos clientes

## 3. Decisões técnicas e por quê

| Decisão | Alternativa descartada | Motivo |
|---|---|---|
| **Consumer group próprio** (`notification-service`) | Compartilhar group com outros serviços | Cada serviço lê todo o tópico de forma independente, no próprio ritmo. É o que diferencia Kafka de uma fila tradicional |
| **`@RetryableTopic` (retry não bloqueante)** | Retry bloqueante no listener | Retry bloqueante trava a partição inteira: uma notificação com erro atrasa todas as seguintes. Com tópicos de retry, o fluxo principal segue |
| **Backoff exponencial** (ex.: 5s → 30s → 5min) | Retry imediato | Provedor externo fora do ar não melhora em 100 ms; insistir só piora |
| **Dead Letter Topic** | Descartar ou logar | Nada se perde: a mensagem fica disponível para análise e reprocessamento |
| **Exceções classificadas** (retryable vs. não retryable) | Tratar todas igual | Template inválido nunca vai funcionar na 3ª tentativa: vai direto para DLT |
| **Réplica local de contatos** (alimentada por `AccountOpened`) | REST síncrono ao account-service | Evita acoplamento temporal. Se o account-service estiver fora, as notificações continuam |
| **Dedupe por `event_id` único** | Confiar no at-least-once | Redelivery acontece; cliente não pode receber o mesmo SMS duas vezes |
| **Provider atrás de interface** (`NotificationSender`) | Chamar SDK direto | Facilita testes, troca de provedor e simulação de falhas (um *fake* que falha 30% das vezes é ótimo para demo) |

## 4. Eventos

### Consome (`groupId=notification-service`)

| Tópico | Evento | Notificação |
|---|---|---|
| `account.events.v1` | `AccountOpened` | Atualiza réplica de contatos + boas-vindas |
| `account.events.v1` | `AccountBlocked` | "Sua conta foi bloqueada" |
| `transfer.events.v1` | `TransferCompleted` | Comprovante para pagador (e aviso ao recebedor) |
| `transfer.events.v1` | `TransferFailed` / `TransferRefunded` | "Transferência não realizada / valor devolvido" |
| `fraud.alerts.v1` | `SuspiciousActivityDetected` | Alerta de segurança |

### Produz
Nada obrigatório. (Opcional: `notification.events.v1` com `NotificationSent`/`NotificationFailed` para métricas e auditoria.)

### Topologia de retry/DLT

```
transfer.events.v1
   └─ falha ─▶ transfer.events.v1-retry-0 (espera 5s)
                 └─ falha ─▶ ...-retry-1 (espera 30s)
                               └─ falha ─▶ ...-retry-2 (espera 5min)
                                             └─ falha ─▶ transfer.events.v1-dlt
```

## 5. Modelo de dados (PostgreSQL + Flyway)

```sql
-- réplica local, alimentada por AccountOpened (e futuramente AccountUpdated)
CREATE TABLE customer_contacts (
    account_id   UUID PRIMARY KEY,
    holder_name  VARCHAR(120) NOT NULL,
    email        VARCHAR(160),
    phone        VARCHAR(20),
    push_token   VARCHAR(255),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
    id           UUID PRIMARY KEY,
    event_id     UUID         NOT NULL,       -- eventId do evento de origem
    account_id   UUID         NOT NULL,
    channel      VARCHAR(10)  NOT NULL,       -- EMAIL | SMS | PUSH
    template     VARCHAR(60)  NOT NULL,       -- ex.: TRANSFER_COMPLETED
    payload      JSONB        NOT NULL,       -- variáveis do template
    status       VARCHAR(12)  NOT NULL,       -- PENDING | SENT | FAILED
    attempts     INT          NOT NULL DEFAULT 0,
    last_error   TEXT,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    sent_at      TIMESTAMPTZ,
    CONSTRAINT uq_notification_dedupe UNIQUE (event_id, channel, account_id)
);
CREATE INDEX idx_notifications_account ON notifications (account_id, created_at DESC);
CREATE INDEX idx_notifications_status ON notifications (status) WHERE status <> 'SENT';
```

**Por que a constraint `(event_id, channel, account_id)`?** Um mesmo evento pode notificar pagador e recebedor, por mais de um canal. A unicidade é por combinação, e é ela que impede o envio duplicado.

## 6. API REST (operação e consulta)

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/notifications?accountId=` | Histórico de notificações da conta |
| `POST` | `/admin/dlt/reprocess?topic=&limit=` | Reenvia mensagens da DLT para o tópico original (uso operacional) |

## 7. Etapas de implementação

### Etapa 1: Primeiro consumer (Fase 2)
- `@KafkaListener` em `transfer.events.v1`, `groupId=notification-service`, ack manual
- `NotificationSender` fake que só loga
- **Conceito:** consumer group independente; rodar 2 serviços diferentes lendo o mesmo tópico
- **Pronto quando:** `TransferCompleted` gera um log; consumer lag no Kafka UI chega a 0

### Etapa 2: Réplica de contatos (Fase 2)
- Consumir `AccountOpened` e popular `customer_contacts` (upsert)
- **Conceito:** *event-carried state transfer*: o evento leva o dado que o consumidor precisa
- **Pronto quando:** abrir conta no account-service cria o contato aqui, sem nenhuma chamada REST

### Etapa 3: Templates e persistência (Fase 2)
- Mapear evento → template/canal; gravar em `notifications` com status `PENDING`
- Dedupe pela constraint única (capturar violação e dar ack)
- **Pronto quando:** reenviar o mesmo evento 5× gera uma só notificação

### Etapa 4: Envio real/simulado (Fase 2)
- Implementar `EmailSender` (ex.: SMTP com MailHog no Docker Compose) e um `FlakySender` que falha aleatoriamente
- **Pronto quando:** e-mails aparecem no MailHog

### Etapa 5: Retry não bloqueante + DLT (Fase 2)
- `@RetryableTopic(attempts = "4", backoff = @Backoff(delay = 5000, multiplier = 6), dltStrategy = ..., exclude = {InvalidTemplateException.class})`
- `@DltHandler` que registra a falha em `notifications` (`FAILED`) e dispara alerta/métrica
- **Conceito:** blocking vs. non-blocking retry, DLT, classificação de erros
- **Pronto quando:** com o `FlakySender` ativo, todas as notificações chegam eventualmente e as irrecuperáveis aparecem na DLT

### Etapa 6: Reprocessamento da DLT (Fase 2/6)
- Endpoint admin que lê a DLT e republica no tópico original após correção
- **Conceito:** a DLT não é lixeira, é fila de trabalho para a operação
- **Pronto quando:** corrijo um template quebrado e reprocesso as mensagens sem perdê-las

### Etapa 7: Rebalanceamento e escala (Fase 2/6)
- Subir 3 instâncias e matar uma durante o consumo; observar rebalance e que nenhuma notificação é duplicada
- Experimentar `max.poll.interval.ms`, `max.poll.records` e *static membership* (`group.instance.id`)
- **Pronto quando:** consigo explicar por que o número de consumers úteis é limitado pelo número de partições

### Etapa 8: Métricas e testes (Fase 6)
- Métricas: enviadas/falhas por canal, tamanho da DLT, consumer lag
- Testes: `EmbeddedKafka` ou Testcontainers com cenários de falha, duplicata e DLT
- **Pronto quando:** alerta no Grafana dispara se a DLT crescer

## 8. Estrutura de pacotes sugerida

```
com.exemplo.notification
├── api/
├── application/    (HandleTransferEvent, HandleFraudAlert, TemplateResolver)
├── domain/         (Notification, Channel, Template)
└── infrastructure/
    ├── messaging/  (listeners, config de retry/DLT)
    ├── sender/     (EmailSender, SmsSender, FlakySender)
    └── persistence/
```

## 9. Perguntas de entrevista que este serviço responde

- *O que é um consumer group e o que acontece quando um consumer entra ou sai?* → Rebalanceamento de partições.
- *Por que retry bloqueante é perigoso?* → Trava a partição e atrasa mensagens saudáveis.
- *Para que serve a DLT e como você trata o que cai nela?* → Isolar mensagens "venenosas", alertar e reprocessar após correção.
- *Como evitar enviar o mesmo SMS duas vezes?* → Constraint única por `eventId` + at-least-once.
- *Como lidar com dados de outro serviço sem chamá-lo?* → Réplica local via eventos (*event-carried state transfer*).
- *Quantos consumers posso ter num grupo?* → Úteis, no máximo o número de partições.
