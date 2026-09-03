# Changelog

All notable implementation changes to the Payment Service POC are documented here.

The README describes the **current architecture and rationale**. This file documents **how the implementation evolved**, including historical architecture and relevant code snippets.

---

# [0.3.0] — September 2, 2026 — 2:00 PM

## Transactional Outbox Pattern

### Why This Change?

The previous implementation performed database persistence and Kafka publication as separate operations. This created a potential dual-write consistency problem.

```text
PostgreSQL ✓
     │
     └── Payment persisted

Kafka ✗
     │
     └── Event publication failed
```

The Transactional Outbox Pattern was introduced so that the payment and the **intent to publish the event** are persisted in the same PostgreSQL transaction.

### Architecture Before

```text
PaymentService
    │
    ├── Save Payment
    ├── Save Idempotency
    └── Publish Kafka Event
```

### Architecture After

```text
PaymentService
    │
    │ @Transactional
    ▼
PostgreSQL
    ├── Payment
    ├── Idempotency
    └── Outbox Event
             │
             ▼
      Outbox Publisher
             │
             ▼
           Kafka
```

### Added

- `OutboxEvent`
- `OutboxEventRepository`
- `OutboxPublisher`
- Scheduled outbox polling
- Outbox event serialization/deserialization

### PaymentService Change

The direct Kafka publication was removed from the payment transaction workflow.

```java
@Transactional
public String createPayment(
        String idempotencyKey,
        Payment payment) {

    if (idempotencyService.exists(idempotencyKey)) {
        return "Payment already submitted: "
                + idempotencyService.getPaymentId(idempotencyKey);
    }

    PaymentCreatedEvent event =
            new PaymentCreatedEvent(
                    payment.getPaymentId(),
                    payment.getCustomerId(),
                    payment.getAmount(),
                    payment.getCurrency()
            );

    paymentRepository.save(payment);

    idempotencyService.save(
            idempotencyKey,
            payment.getPaymentId()
    );

    String payload =
            objectMapper.writeValueAsString(event);

    OutboxEvent outboxEvent =
            new OutboxEvent(
                    payment.getPaymentId(),
                    "PaymentCreatedEvent",
                    payload
            );

    outboxEventRepository.save(outboxEvent);

    return "Payment submitted: "
            + payment.getPaymentId();
}
```

The important change is that this is no longer executed directly by `PaymentService`:

```java
paymentProducer.publish(event);
```

Instead:

```text
PaymentService
      │
      ▼
OutboxEvent
      │
      ▼
OutboxPublisher
      │
      ▼
Kafka
```

### Outbox Publisher

```java
@Scheduled(fixedDelay = 5000)
public void publishEvents() {

    List<OutboxEvent> events =
            outboxEventRepository
                    .findTop100ByStatusOrderByCreatedAtAsc("NEW");

    for (OutboxEvent outboxEvent : events) {

        try {

            PaymentCreatedEvent event =
                    objectMapper.readValue(
                            outboxEvent.getPayload(),
                            PaymentCreatedEvent.class
                    );

            paymentProducer.publish(event);

            outboxEvent.markPublished();

            outboxEventRepository.save(outboxEvent);

        } catch (Exception e) {

            System.err.println(
                    "Failed to publish outbox event "
                            + outboxEvent.getId()
            );
        }
    }
}
```

### Result

The database now stores:

```text
Payment
Idempotency
Outbox Event
```

as part of the payment transaction.

The publisher independently handles:

```text
Outbox Event → Kafka
```

### Remaining Reliability Consideration

The publisher can successfully send to Kafka and then fail before marking the outbox record as published. The event can therefore be published more than once.

This means downstream consumers must be idempotent.

---

# [0.2.0] — September 2, 2026 — 10:00 AM

## Service Layer + `@Transactional` + PostgreSQL

### Why This Change?

The initial implementation placed the payment workflow directly inside the controller.

The Service Layer was introduced to separate:

```text
HTTP handling
     from
Business workflow
```

It also established a natural boundary for transaction management.

### Architecture Before

```text
PaymentController
    │
    ├── Idempotency
    ├── Create Event
    └── Publish Kafka
```

### Architecture After

```text
PaymentController
        │
        ▼
PaymentService
        │
        │ @Transactional
        ▼
PostgreSQL
        │
        ▼
      Kafka
```

### Added

- `PaymentService`
- `PaymentRepository`
- PostgreSQL
- Spring Data JPA
- Hibernate
- `@Transactional`
- PostgreSQL Docker container
- PostgreSQL application configuration

### Controller Implementation

The controller was reduced to HTTP handling and delegation:

```java
@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(
            PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<String> createPayment(
            @RequestHeader("Idempotency-Key")
            String idempotencyKey,
            @RequestBody Payment payment) {

        String result =
                paymentService.createPayment(
                        idempotencyKey,
                        payment
                );

        return ResponseEntity.ok(result);
    }
}
```

### Service Implementation

The payment workflow moved into the service:

```java
@Service
public class PaymentService {

    private final PaymentProducer paymentProducer;
    private final IdempotencyService idempotencyService;
    private final PaymentRepository paymentRepository;

    public PaymentService(
            PaymentProducer paymentProducer,
            IdempotencyService idempotencyService,
            PaymentRepository paymentRepository) {

        this.paymentProducer = paymentProducer;
        this.idempotencyService = idempotencyService;
        this.paymentRepository = paymentRepository;
    }

    @Transactional
    public String createPayment(
            String idempotencyKey,
            Payment payment) {

        if (idempotencyService.exists(idempotencyKey)) {
            return "Payment already submitted: "
                    + idempotencyService.getPaymentId(idempotencyKey);
        }

        PaymentCreatedEvent event =
                new PaymentCreatedEvent(
                        payment.getPaymentId(),
                        payment.getCustomerId(),
                        payment.getAmount(),
                        payment.getCurrency()
                );

        paymentRepository.save(payment);

        idempotencyService.save(
                idempotencyKey,
                payment.getPaymentId()
        );

        paymentProducer.publish(event);

        return "Payment submitted: "
                + payment.getPaymentId();
    }
}
```

### Why `@Transactional`?

The intention was to group participating database operations into one logical unit of work:

```text
Start Transaction
      │
      ├── Save Payment
      ├── Save Idempotency
      │
      ▼
    COMMIT
```

If a database operation failed before commit:

```text
Save Payment          ✓
Save Idempotency      ✗
                       │
                       ▼
                   ROLLBACK
```

### Important Limitation Discovered

`@Transactional` protects the database transaction, but it does not automatically make PostgreSQL and Kafka one atomic transaction.

This became the motivation for the Transactional Outbox implementation in `0.3.0`.

---

# [0.1.0] — August 19, 2026 — 4:00 PM

## Initial Kafka + Idempotency POC

### Purpose

Created the initial POC to demonstrate:

- Spring Boot REST API
- Kafka producer
- Kafka consumer
- Event-driven processing
- Basic request idempotency
- In-memory implementation

The initial implementation intentionally avoided database persistence and a Service Layer so that the basic Kafka and idempotency concepts could be demonstrated first.

### Initial Architecture

```text
Client
   │
   ▼
PaymentController
   │
   ├── IdempotencyService
   │
   └── PaymentProducer
          │
          ▼
        Kafka
          │
          ▼
    PaymentConsumer
```

### Initial Payment Workflow

The payment workflow was implemented directly in the controller:

```text
Receive Request
      │
      ▼
Check Idempotency
      │
      ├── Duplicate → Return existing payment
      │
      └── New
           │
           ▼
      Create Event
           │
           ▼
      Publish Kafka
```

### Initial Idempotency Implementation

The POC used an in-memory `ConcurrentHashMap`:

```java
private final Map<String, String> processedKeys =
        new ConcurrentHashMap<>();
```

The mapping was:

```text
Idempotency Key → Payment ID
```

### Initial Kafka Producer

The producer published the payment-created event using Spring Kafka:

```java
kafkaTemplate.send(
        "payment.created",
        event.paymentId(),
        event
);
```

### Initial Consumer

The consumer listened to the payment-created topic:

```java
@KafkaListener(
        topics = "payment.created",
        groupId = "payment-service"
)
public void consume(PaymentCreatedEvent event) {

    System.out.println(
            "Processing payment: "
                    + event.paymentId()
    );
}
```

### Limitations at This Stage

- No Service Layer
- No PostgreSQL persistence
- Idempotency stored only in memory
- Direct Kafka publication
- No Transactional Outbox
- No production-grade retry/DLT implementation

---

# Implementation Evolution

```text
Aug 19 — 4:00 PM
Initial Kafka + Idempotency POC
        │
        ▼
Sep 2 — 10:00 AM
Service Layer + @Transactional + PostgreSQL
        │
        ▼
Sep 2 — 2:00 PM
Transactional Outbox Pattern
```

| Version | Date | Implementation |
|---|---|---|
| `0.1.0` | Aug 19, 2026 — 4:00 PM | Initial Kafka + Idempotency POC |
| `0.2.0` | Sep 2, 2026 — 10:00 AM | Service Layer + `@Transactional` + PostgreSQL |
| `0.3.0` | Sep 2, 2026 — 2:00 PM | Transactional Outbox Pattern |

---

# Next Planned Implementations

1. **Persistent Idempotency**
   - Move from `ConcurrentHashMap` to PostgreSQL or Redis.
   - Add expiration and retention.
   - Protect against concurrent duplicate requests.

2. **Consumer Idempotency**
   - Ensure duplicate Kafka delivery does not produce duplicate business effects.

3. **Retry and DLT**
   - Add transient-error retries.
   - Add retry backoff.
   - Add Dead Letter Topic handling.

4. **Outbox Improvements**
   - Add retry count.
   - Add last-attempt timestamp.
   - Add error details.
   - Add better status management.
   - Improve multi-publisher coordination.

5. **Observability**
   - Metrics
   - Structured logs
   - Correlation IDs
   - Distributed tracing
   - Kafka consumer lag monitoring
