# ThessaCodes | Java Spring Boot Demo Payment Service

A Proof of Concept (POC) demonstrating an event-driven payment service built with **Java 17**, **Spring Boot**, **Apache Kafka**, and **PostgreSQL**.

The objective is not to implement a production payment gateway, but to demonstrate backend engineering concepts commonly used in payment-oriented systems:

- REST APIs
- Service Layer architecture
- Transaction management with `@Transactional`
- PostgreSQL persistence with Spring Data JPA
- Kafka producers and consumers
- Request idempotency
- Transactional Outbox Pattern
- Asynchronous event processing
- Containerized local infrastructure

**Repository:** https://github.com/i-am-thessa/thessacodes-java-payment-service.git

---

# Changelog

Detailed implementation history and implementation-specific code snippets are maintained in:

**[CHANGELOG.md](CHANGELOG.md)**

| Version | Milestone |
|---|---|
| `0.1.0` | Initial Kafka + Idempotency POC |
| `0.2.0` | Service Layer + `@Transactional` + PostgreSQL |
| `0.3.0` | Transactional Outbox Pattern |

---

# Current High-Level Architecture

```text
                                      Client
                                        │
                                        │ POST /payments
                                        │ Idempotency-Key
                                        ▼
                              ┌─────────────────────┐
                              │  PaymentController  │
                              └──────────┬──────────┘
                                         │
                                         ▼
                              ┌─────────────────────┐
                              │   PaymentService    │
                              │    @Transactional   │
                              └──────────┬──────────┘
                                         │
                                         ▼
                              ┌─────────────────────┐
                              │     PostgreSQL      │
                              │                     │
                              │  Payment            │
                              │  Idempotency*       │
                              │  Outbox Event       │
                              └──────────┬──────────┘
                                         │
                                         ▼
                              ┌─────────────────────┐
                              │  Outbox Publisher   │
                              └──────────┬──────────┘
                                         │
                                         ▼
                              ┌─────────────────────┐
                              │       Kafka         │
                              │  payment.created    │
                              └──────────┬──────────┘
                                         │
                                         ▼
                              ┌─────────────────────┐
                              │  PaymentConsumer    │
                              └──────────┬──────────┘
                                         │
                                         ▼
                                Payment Processing
```

> **Current POC note:** Idempotency is still implemented in memory in the current codebase. The architecture is intentionally evolving toward persistent idempotency.

---

# Architecture Overview

The payment request follows these stages:

1. Client submits a payment through the REST API.
2. `PaymentController` receives the request and delegates to `PaymentService`.
3. `PaymentService` coordinates the payment business workflow.
4. Payment persistence and the outbox event are handled within the PostgreSQL transaction.
5. The transaction commits.
6. `OutboxPublisher` retrieves pending outbox events.
7. The event is published to Kafka.
8. `PaymentConsumer` consumes the `PaymentCreatedEvent` asynchronously.

This separates **request ingestion**, **business orchestration**, **persistence**, **event publication**, and **event consumption**.

---

# Why the Service Layer?

The Service Layer separates **HTTP concerns** from **business logic**.

The controller handles request/response concerns, while the service coordinates:

- Idempotency validation
- Payment creation
- Persistence
- Event creation
- Transaction management

The Service Layer also provides a natural boundary for `@Transactional`.

```text
Controller
    │
    │ HTTP concerns
    ▼
Service
    │
    │ Business workflow
    ▼
Repository / Outbox / Messaging
```

This makes the business workflow easier to test, evolve, and reuse without coupling it to the HTTP layer.

---

# Why `@Transactional`?

`@Transactional` establishes the **database transaction boundary** for the payment operation.

The participating database operations are treated as one logical unit of work:

```text
Start Transaction
      │
      ├── Save Payment
      ├── Save Idempotency
      ├── Save Outbox Event
      │
      ▼
    COMMIT
```

If a participating database operation fails before commit, the database changes covered by the transaction can be rolled back together:

```text
Save Payment          ✓
Save Idempotency      ✓
Save Outbox Event     ✗
                       │
                       ▼
                   ROLLBACK
```

This prevents the database from being left in a partially completed state.

> **Important:** `@Transactional` does not automatically make PostgreSQL and Kafka one atomic transaction. It also does not automatically roll back an independent cache update.

---

# Why Transactional Outbox?

A direct database + Kafka workflow creates a potential **dual-write problem**:

```text
PaymentService
    │
    ├── Save Payment ─────► PostgreSQL ✓
    │
    └── Publish Event ────► Kafka ✗
```

The Transactional Outbox Pattern changes this to:

```text
PaymentService
      │
      │ @Transactional
      ▼
 PostgreSQL
 ┌────────────────────┐
 │ Payment             │
 │ Idempotency         │
 │ Outbox Event        │
 └──────────┬─────────┘
            │
          COMMIT
            │
            ▼
     Outbox Publisher
            │
            ▼
          Kafka
```

The payment and the intent to publish its event are persisted in the same database transaction.

If Kafka is temporarily unavailable, the outbox record remains available for a later publishing attempt.

> The Outbox Pattern improves database-to-message reliability, but it does not automatically provide exactly-once business processing. Consumers should remain idempotent.

---

# Event-Driven Architecture

Kafka decouples payment submission from downstream processing.

```text
Payment Service
      │
      │ PaymentCreatedEvent
      ▼
    Kafka
      │
      ▼
Payment Consumer
      │
      ▼
Payment Processing
```

The producer does not need to synchronously wait for downstream processing.

The architecture can later support multiple independent consumers:

```text
                         ┌── Payment Processing
                         │
Payment Service ──► Kafka ├── Notification
                         │
                         ├── Audit
                         │
                         └── Future Consumers
```

Different consumer groups can independently process the same event stream.

---

# Architecture Decisions

| Decision | Rationale |
|---|---|
| REST API | Provides a simple synchronous interface for payment submission. |
| Service Layer | Separates HTTP concerns from business logic and provides a transaction boundary. |
| PostgreSQL + JPA | Provides persistent relational storage for payment information. |
| `@Transactional` | Groups participating database operations into one transaction and enables rollback when the transaction fails. |
| Transactional Outbox | Addresses the database-to-Kafka dual-write consistency problem. |
| Kafka | Decouples payment submission from downstream asynchronous processing. |
| `PaymentCreatedEvent` | Defines an explicit event contract between the producer and consumers. |
| Idempotency Key | Prevents duplicate processing of the same client request. |
| Constructor Injection | Makes dependencies explicit and improves testability. |
| Docker Compose | Provides repeatable local Kafka and PostgreSQL infrastructure. |
| JSON Serialization | Makes event payloads easy to inspect and integrate with other services. |

---

# Technologies

### Backend

- Java 17
- Spring Boot 4.1.0
- Spring Web MVC
- Spring Validation
- Spring Actuator
- Spring Data JPA
- Hibernate

### Messaging

- Apache Kafka
- Spring Kafka
- Kafka Producer
- Kafka Consumer
- JSON event serialization/deserialization

### Database

- PostgreSQL
- Spring Data JPA
- Hibernate

### Development & Infrastructure

- Maven
- Docker
- Docker Compose
- Lombok
- Jackson
- Spring Boot DevTools

### Testing

- JUnit / Spring Boot Test
- Spring Kafka Test
- Spring Web MVC Test
- Spring Validation Test
- Spring Actuator Test

---

# Learning Objectives

This project demonstrates how to:

- Build a RESTful payment API using Spring Boot.
- Separate HTTP handling from business logic using a Service Layer.
- Define database transaction boundaries using `@Transactional`.
- Persist payment data using Spring Data JPA and PostgreSQL.
- Implement an event-driven architecture using Apache Kafka.
- Publish payment events asynchronously.
- Consume Kafka events using `@KafkaListener`.
- Implement request idempotency using an `Idempotency-Key`.
- Address the database-to-Kafka dual-write problem using the Transactional Outbox Pattern.
- Run Kafka and PostgreSQL locally using Docker Compose.

---

# Project Structure

```text
src/main/java/com/thessacodes/java/demo_payment_service/
├── controller/
│   └── PaymentController.java
├── model/
│   ├── Payment.java
│   ├── PaymentCreatedEvent.java
│   ├── PaymentStatus.java
│   └── OutboxEvent.java
├── repository/
│   ├── PaymentRepository.java
│   └── OutboxEventRepository.java
└── service/
    ├── PaymentService.java
    ├── IdempotencyService.java
    ├── OutboxPublisher.java
    ├── producer/
    │   └── PaymentProducer.java
    └── consumer/
        └── PaymentConsumer.java
```

### Package Responsibilities

| Component | Responsibility |
|---|---|
| `PaymentController` | Handles HTTP requests and delegates to `PaymentService`. |
| `PaymentService` | Coordinates payment business logic and transaction boundaries. |
| `PaymentRepository` | Provides payment persistence through Spring Data JPA. |
| `OutboxEventRepository` | Persists and retrieves outbox events. |
| `OutboxPublisher` | Publishes pending outbox events to Kafka. |
| `PaymentProducer` | Publishes Kafka payment events. |
| `PaymentConsumer` | Consumes payment events. |
| `IdempotencyService` | Provides the current in-memory idempotency implementation. |

---

# API

## Create Payment

```text
POST /payments
```

Required header:

```text
Idempotency-Key: <unique-key>
```

Example request:

```json
{
  "paymentId": "PAY-10001",
  "customerId": "CUS-10001",
  "amount": 1500.00,
  "currency": "PHP"
}
```

The API returns a payment submission response while downstream processing occurs asynchronously.

---

# Event Flow

```text
Client
  │
  ▼
PaymentController
  │
  ▼
PaymentService
  │
  │ @Transactional
  ▼
PostgreSQL
  ├── Payment
  ├── Idempotency
  └── Outbox Event
           │
           │ commit
           ▼
    OutboxPublisher
           │
           ▼
         Kafka
           │
           ▼
    PaymentConsumer
           │
           ▼
    Payment Processing
```

---

# Local Docker Setup

## Prerequisites

Install:

- Java 17 or later
- Maven or Maven Wrapper
- Docker Desktop
- Git

Verify:

```bash
java -version
mvn -version
docker --version
docker compose version
```

## Start Kafka and PostgreSQL

From the project root:

```bash
docker compose up -d
```

Check:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f kafka
docker compose logs -f postgres
```

Stop:

```bash
docker compose down
```

Remove containers and volumes:

```bash
docker compose down -v
```

> `docker compose down -v` removes Docker volumes and therefore deletes PostgreSQL data stored in those volumes.

---

# Why PostgreSQL Uses `5433:5432`

The Docker Compose configuration maps PostgreSQL as:

```yaml
ports:
  - "5433:5432"
```

This means:

```text
Mac Host                         Docker Container
────────                         ─────────────────
localhost:5433  ─────────────►  PostgreSQL:5432
```

- `5433` is the port exposed on the host/Mac.
- `5432` is PostgreSQL's port inside the Docker container.

A native PostgreSQL installation on the Mac was already using host port `5432`. Docker therefore uses `5433` to avoid a host-port conflict.

```text
Native PostgreSQL
localhost:5432

Docker PostgreSQL
localhost:5433
      │
      ▼
Container PostgreSQL:5432
```

Because Spring Boot currently runs directly on the Mac/IDE, it connects through:

```text
localhost:5433
```

If Spring Boot were also running inside Docker Compose, it could connect through:

```text
postgres:5432
```

---

# PostgreSQL / pgAdmin

Spring Boot:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5433/paymentdb
spring.datasource.username=postgres
spring.datasource.password=postgres
```

pgAdmin:

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5433` |
| Database | `paymentdb` |
| Username | `postgres` |
| Password | `postgres` |

Verify:

```bash
psql -h localhost -p 5433 -U postgres -d paymentdb
```

---

# Kafka Configuration

Kafka is exposed to the host at:

```text
localhost:9092
```

Spring Boot:

```properties
spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.group-id=payment-service
spring.kafka.consumer.auto-offset-reset=earliest
```

Payment events are published to:

```text
payment.created
```

---

# Running the Application

Start infrastructure:

```bash
docker compose up -d
```

Run:

```bash
./mvnw spring-boot:run
```

Or:

```bash
mvn spring-boot:run
```

Application:

```text
http://localhost:8080
```

---

# Testing

Example request:

```bash
curl -X POST http://localhost:8080/payments   -H "Content-Type: application/json"   -H "Idempotency-Key: payment-request-10001"   -d '{
    "paymentId": "PAY-10001",
    "customerId": "CUS-10001",
    "amount": 1500.00,
    "currency": "PHP"
  }'
```

Use the same `Idempotency-Key` to demonstrate duplicate-request protection.

---

# Current Limitations

### Idempotency

- Idempotency is currently stored in memory.
- Idempotency does not survive application restart.
- Persistent/shared idempotency storage is a future enhancement.

### Outbox

- Outbox publishing currently uses scheduled polling.
- Kafka delivery may be repeated if the publisher fails after sending but before marking the outbox event as published.
- Consumers therefore need idempotent processing.

### Kafka Reliability

- Retry strategy is not yet implemented.
- Dead-letter topic handling is not yet implemented.

### Security

- Authentication and authorization are not implemented.
- API rate limiting is not implemented.
- Production secrets management and TLS are not implemented.

### Payment Processing

The consumer currently represents downstream payment processing rather than integrating with a real payment gateway.

---

# Future Enhancements

### Payment Processing

- Payment status lifecycle
- Authorization
- Capture
- Cancellation/refund
- Failure handling
- External payment gateway integration

### Idempotency

- Persistent idempotency records
- Redis-backed idempotency
- Expiration and retention
- Atomic duplicate-request handling
- Concurrent request protection

### Kafka Reliability

- Retry topics
- Dead-letter topics
- Consumer error handlers
- Consumer retry policies
- Kafka transactions
- Schema management
- Consumer idempotency

### Observability

- Structured logging
- Correlation IDs
- Micrometer metrics
- Prometheus
- Grafana
- Distributed tracing
- OpenTelemetry
- Datadog instrumentation

### DevOps

- Containerized application
- CI/CD
- Multi-environment configuration
- Infrastructure as Code
- Kubernetes deployment
- Cloud Kafka deployment

---

# Project Documentation

| Document | Description |
|---|---|
| `README.md` | Current architecture, setup, usage, and architectural rationale. |
| `CHANGELOG.md` | Version history, implementation evolution, and implementation-specific code snippets. |

---

# Author

**ThessaCodes [Java]**

**Project:** Demo Payment Service  
**Repository:** https://github.com/i-am-thessa/thessacodes-java-payment-service.git  
**Created:** August 2026

---

> This project is a learning and portfolio POC focused on demonstrating Java Spring Boot, Kafka, event-driven architecture, transactions, idempotency, and payment-system design concepts. It is not intended to process real financial transactions.
