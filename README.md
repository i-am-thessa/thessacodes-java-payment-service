# ThessaCodes | Java Spring Boot Demo Payment Service

A Proof of Concept (POC) demonstrating an event-driven payment service built with **Java**, **Spring Boot**, and **Apache Kafka**.

The objective of this project is not to implement a production payment gateway, but to demonstrate important backend engineering concepts used in payment-oriented systems, including **REST APIs**, **event-driven architecture**, **Kafka producers and consumers**, **idempotency**, **JSON event serialization**, and **containerized local infrastructure**.

---

# High-Level Architecture

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
                           ┌─────────────┴─────────────┐
                           │                           │
                           ▼                           ▼
                 IdempotencyService            PaymentProducer
                           │                           │
                           │                           │ publish
                           │                           ▼
                           │                  ┌─────────────────┐
                           │                  │ Kafka Topic     │
                           │                  │ payment.created │
                           │                  └────────┬────────┘
                           │                           │
                           │                           │ consume
                           │                           ▼
                           │                  ┌─────────────────┐
                           │                  │ PaymentConsumer │
                           │                  └────────┬────────┘
                           │                           │
                           │                           ▼
                           │                    Payment Processing
                           │
                           ▼
                    In-Memory Idempotency
                         Store
```

The service exposes a REST endpoint for submitting payments. Instead of processing the payment synchronously inside the HTTP request, it publishes a `PaymentCreatedEvent` to the `payment.created` Kafka topic.

A Kafka consumer listens for the event and represents the downstream payment-processing component.

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
- Implement an event-driven architecture using Apache Kafka.
- Publish domain events asynchronously from a REST controller.
- Consume Kafka events using `@KafkaListener`.
- Serialize and deserialize payment events as JSON.
- Implement request idempotency using an `Idempotency-Key`.
- Persist payments using Spring Data JPA and PostgreSQL.
- Define a transaction boundary using Spring's `@Transactional`.
- Use constructor-based dependency injection.
- Separate API handling, business logic, persistence, event production, event consumption, and idempotency concerns.
- Run Kafka and PostgreSQL locally using Docker Compose.
- Configure Kafka producer and consumer behavior through Spring Boot properties.

---

# Development Walkthrough

The project was intentionally kept small so that each backend concept can be observed independently before introducing additional production concerns.

| Phase | Description |
|--------|-------------|
| ✅ Spring Boot Setup | Created a Java 17 Spring Boot application using Maven. |
| ✅ REST API | Added a `POST /payments` endpoint for payment submission. |
| ✅ Payment Model | Defined a `Payment` record containing payment ID, customer ID, amount, and currency. |
| ✅ Kafka Event | Defined a `PaymentCreatedEvent` representing the event published to Kafka. |
| ✅ Kafka Producer | Implemented `PaymentProducer` using Spring's `KafkaTemplate`. |
| ✅ Kafka Consumer | Implemented `PaymentConsumer` using `@KafkaListener`. |
| ✅ Idempotency | Added an in-memory idempotency service using `ConcurrentHashMap`. |
| ✅ Docker Kafka | Added Docker Compose configuration for a local Apache Kafka broker. |
| ✅ JSON Messaging | Configured Spring Kafka to serialize and deserialize payment events as JSON. |
| ✅ Payment Persistence | Payments are persisted using Spring Data JPA and PostgreSQL. |
| ⏳ Production Idempotency Persistence | Idempotency storage is still in-memory and should be replaced with Redis or a database. |

> **Learning Approach**
>
> The project focuses on understanding the flow of an event-driven payment request from HTTP ingestion through Kafka publication and consumption. Each component is intentionally simple so that the architectural responsibilities are clear before adding production-level infrastructure.

---

# Features

- RESTful Payment API
- Spring Boot REST Controller
- Payment Request Model
- Payment Created Event
- Apache Kafka Integration
- Kafka Producer
- Kafka Consumer
- Event-driven Payment Processing
- Idempotency-Key Support
- In-Memory Idempotency Store
- PostgreSQL Payment Persistence
- Spring Data JPA / Hibernate
- `@Transactional` Payment Service
- JSON Kafka Serialization
- Constructor-based Dependency Injection
- Docker Compose Kafka Environment
- Spring Boot Actuator
- Maven Build

---

# Architecture Decisions

| Decision | Rationale |
|----------|-----------|
| REST API | Provides a simple synchronous interface for clients to submit payments. |
| Kafka | Decouples payment submission from downstream payment processing. |
| `PaymentCreatedEvent` | Creates an explicit event contract between the producer and consumer. |
| Idempotency Key | Prevents the same client request from submitting the same payment repeatedly within the application's current runtime. |
| PostgreSQL + JPA | Persists payment records beyond application restarts. |
| `@Transactional` | Defines the database transaction boundary for payment persistence and idempotency-related database work. |
| `ConcurrentHashMap` | Provides a simple thread-safe in-memory implementation for the current idempotency POC. |
| Constructor Injection | Makes dependencies explicit and improves testability. |
| Docker Compose | Provides a repeatable local Kafka and PostgreSQL development environment. |
| JSON Serialization | Makes the Kafka event payload easy to inspect and integrate with other services. |

---

# Project Structure

```text
src/
└── main/
    ├── java/
    │   └── com/thessacodes/java/demo_payment_service/
    │       │
    │       ├── DemoPaymentServiceApplication.java
    │       │
    │       ├── controller/
    │       │   ├── Welcome.java
    │       │   └── PaymentController.java
    │       │
    │       ├── model/
    │       │   ├── Payment.java
    │       │   ├── PaymentCreatedEvent.java
    │       │   └── PaymentStatus.java
    │       │
    │       ├── repository/
    │       │   └── PaymentRepository.java
    │       │
    │       └── service/
    │           ├── IdempotencyService.java
    │           ├── PaymentService.java
    │           │
    │           ├── producer/
    │           │   └── PaymentProducer.java
    │           │
    │           └── consumer/
    │               └── PaymentConsumer.java
    │
    └── resources/
        └── application.properties

docker-compose.yml
pom.xml
```

### Package Responsibilities

| Package / Component | Responsibility |
|---|---|
| `controller` | Handles HTTP requests and delegates business operations to the service layer. |
| `model` | Contains the JPA `Payment` entity, Kafka `PaymentCreatedEvent`, and `PaymentStatus` enum. |
| `repository` | Provides Spring Data JPA persistence access for payments. |
| `service` | Contains payment business logic, transaction boundaries, and idempotency handling. |
| `service.producer` | Publishes payment events to Kafka. |
| `service.consumer` | Consumes `PaymentCreatedEvent` messages from Kafka. |

### Layered Request Flow

```text
Client
  │
  │ POST /payments
  ▼
PaymentController
  │
  │ createPayment()
  ▼
PaymentService
  │
  ├── Check Idempotency-Key
  │
  ├── Create PaymentCreatedEvent
  │
  ├── Save Payment ───────────────► PostgreSQL
  │
  ├── Save Idempotency Key
  │
  └── Publish Event ──────────────► Kafka
                                      │
                                      ▼
                               PaymentConsumer
                                      │
                                      ▼
                              Payment Processing
```

The controller is intentionally thin. Business logic is handled by `PaymentService`, while persistence and messaging are delegated to their respective components.

### Why Introduce the Service Layer?

The Service Layer was introduced primarily to establish a clear **transaction boundary** for the payment operation using Spring's `@Transactional`.

The payment creation process consists of multiple steps that should be treated as one logical database transaction:

```text
PaymentController
       │
       ▼
PaymentService.createPayment()
       │
       ├── Check Idempotency
       │
       ├── Save Payment
       │
       ├── Save Idempotency Record
       │
       └── Commit Transaction
```

By placing these operations inside a `@Transactional` service method, if an error occurs during the database operations **before the transaction is successfully completed**, Spring can roll back the database changes made within that transaction.

For example:

```text
Start Transaction
      │
      ├── Save Payment              ✓
      │
      ├── Save Idempotency Record   ✗ ERROR
      │
      ▼
Transaction Rollback
      │
      └── Payment save is rolled back
```

This prevents the database from being left in a partially completed state.

The Service Layer therefore provides a natural place to:

- Define the transaction boundary with `@Transactional`
- Coordinate multiple database operations as one business operation
- Roll back database changes when an operation fails
- Keep transaction and business logic out of the HTTP controller
- Make the business workflow easier to test and evolve

> **Important:** `@Transactional` applies to the resources participating in the configured transaction, in this case the PostgreSQL database. It does **not** automatically roll back a Kafka message that has already been published. Coordinating PostgreSQL and Kafka atomically requires an approach such as Kafka transactions or, more commonly for this type of workflow, a **Transactional Outbox** pattern.

---

# Payment Controller and Service

## `PaymentController`

`PaymentController` is intentionally kept thin. It is responsible for accepting the HTTP request and delegating the operation to `PaymentService`.

```java
@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<String> createPayment(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody Payment payment) {

        String result = paymentService.createPayment(
                idempotencyKey,
                payment
        );

        return ResponseEntity.ok(result);
    }
}
```

This keeps HTTP concerns out of the business/service layer.

## `PaymentService`

The payment workflow has been moved into `PaymentService`.

```java
@Service
public class PaymentService {

    private final PaymentProducer paymentProducer;
    private final IdempotencyService idempotencyService;
    private final PaymentRepository paymentRepository;

    public PaymentService(
            PaymentProducer paymentProducer,
            IdempotencyService idempotencyService,
            PaymentRepository paymentRepo) {

        this.paymentProducer = paymentProducer;
        this.idempotencyService = idempotencyService;
        this.paymentRepository = paymentRepo;
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

        // Persist payment
        paymentRepository.save(payment);

        // Save idempotency record
        idempotencyService.save(
                idempotencyKey,
                payment.getPaymentId()
        );

        // Publish event
        paymentProducer.publish(event);

        return "Payment submitted: " + payment.getPaymentId();
    }
}
```

### Why `@Transactional`?

`@Transactional` defines the database transaction boundary for the `createPayment()` operation.

The main purpose is to ensure that the database changes made during the operation are treated as one logical unit of work. If an exception occurs before the transaction is successfully committed, Spring rolls back the database changes covered by the transaction rather than leaving the database partially updated.

The intended database flow is:

```text
createPayment()
      │
      ▼
Check Idempotency
      │
      ▼
Save Payment
      │
      ▼
Save Idempotency Record
      │
      ▼
Commit Database Transaction
```

If a database operation fails and the exception causes the transaction to roll back, the payment persistence changes are not committed.

**Important:** Spring's `@Transactional` does not automatically make PostgreSQL and Kafka one atomic transaction. Kafka publication is still a separate system operation in this implementation. The producer currently calls `KafkaTemplate.send()`, which is asynchronous.

For a production-grade payment workflow, a **Transactional Outbox** pattern would be a natural next step:

```text
                PostgreSQL Transaction
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
        Payment Table       Outbox Table
                                 │
                                 │
                                 ▼
                         Outbox Publisher
                                 │
                                 ▼
                               Kafka
```

This avoids relying on a single local database transaction to atomically coordinate a database write and an external Kafka publish.

---

# Payment Request

The API accepts a payment payload with the following structure:

```json
{
  "paymentId": "PAY-10001",
  "customerId": "CUS-10001",
  "amount": 1500.00,
  "currency": "PHP"
}
```

The client must also provide an `Idempotency-Key` header:

```text
Idempotency-Key: payment-request-10001
```

---

# API Endpoints

## Welcome

```http
GET /
```

Returns:

```text
Welcome to Demo Payment Service with Kafka Implementation!
```

---

## Create Payment

```http
POST /payments
```

### Headers

```text
Content-Type: application/json
Idempotency-Key: payment-request-10001
```

### Request

```json
{
  "paymentId": "PAY-10001",
  "customerId": "CUS-10001",
  "amount": 1500.00,
  "currency": "PHP"
}
```

### First Submission

```text
Payment submitted: PAY-10001
```

The service saves the idempotency key and publishes a `PaymentCreatedEvent` to:

```text
payment.created
```

### Duplicate Submission

If the same `Idempotency-Key` is submitted again:

```text
Payment already submitted: PAY-10001
```

The event is not published again by the controller.

---

# Event Flow

The payment submission follows this sequence:

```text
1. Client
      │
      │ POST /payments
      ▼
2. PaymentController
      │
      │ delegates request
      ▼
3. PaymentService
      │
      ├── Check Idempotency-Key
      │
      ├── Create PaymentCreatedEvent
      │
      ├── Save Payment ───────► PostgreSQL
      │
      ├── Save Idempotency Key
      │
      └── Publish Event
                  │
                  ▼
             4. Kafka
                  │
                  │ payment.created
                  ▼
             5. PaymentConsumer
                  │
                  ▼
             6. Payment Processing
```

This demonstrates the basic separation between **request ingestion** and **asynchronous event processing**.

---

# Kafka Configuration

Kafka is configured in `application.properties`:

```properties
spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.group-id=payment-service
spring.kafka.consumer.auto-offset-reset=earliest
```

The producer uses:

```text
StringSerializer
JsonSerializer
```

The consumer uses:

```text
StringDeserializer
JsonDeserializer
```

The trusted package is restricted to the application's event model package:

```properties
spring.kafka.consumer.properties.spring.json.trusted.packages=com.thessacodes.java.demo_payment_service.model
```

The default event type is:

```text
com.thessacodes.java.demo_payment_service.model.PaymentCreatedEvent
```

---

# Running the Application

## Prerequisites

Install:

- Java 17 or later
- Maven
- Docker Desktop
- Docker Compose

Verify Java:

```bash
java -version
```

Verify Maven:

```bash
mvn -version
```

Verify Docker:

```bash
docker --version
```

---

## Start Kafka

From the project root:

```bash
docker compose up -d
```

This starts the Apache Kafka broker and PostgreSQL database.

Kafka:

```text
localhost:9092
```

PostgreSQL:

```text
localhost:5432
```

Check the running containers:

```bash
docker compose ps
```

---

## Run the Spring Boot Application

Using Maven:

```bash
./mvnw spring-boot:run
```

On Windows:

```bash
mvnw.cmd spring-boot:run
```

Alternatively:

```bash
mvn spring-boot:run
```

The application can then be accessed at:

```text
http://localhost:8080
```

---

# Testing the Payment API

## Submit a Payment

Using `curl`:

```bash
curl -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: payment-request-10001" \
  -d '{
    "paymentId": "PAY-10001",
    "customerId": "CUS-10001",
    "amount": 1500.00,
    "currency": "PHP"
  }'
```

Expected response:

```text
Payment submitted: PAY-10001
```

The application publishes the event to:

```text
payment.created
```

The consumer should then log information similar to:

```text
Processing payment: PAY-10001
Amount: 1500.00 PHP
```

---

## Test Idempotency

Submit the same request again using the same:

```text
Idempotency-Key: payment-request-10001
```

Expected response:

```text
Payment already submitted: PAY-10001
```

This demonstrates the basic duplicate-request protection implemented by the POC.

---

# Payment Persistence

The `Payment` model is now a JPA entity:

```java
@Entity
@Table(name = "payments")
public class Payment {

    @Id
    private String paymentId;

    private String customerId;
    private BigDecimal amount;
    private String currency;
}
```

`PaymentRepository` provides the Spring Data JPA repository used by `PaymentService`:

```java
@Repository
public interface PaymentRepository
        extends JpaRepository<Payment, UUID> {
}
```

The application uses PostgreSQL as the relational database.

The current payment flow therefore persists the payment before publishing the corresponding Kafka event.

> **Implementation note:** The entity's `paymentId` is currently declared as `String`, while the repository in the supplied project declares `UUID` as its repository ID type. These types should be aligned before treating the repository as production-ready.

---

# Idempotency Implementation

The current implementation stores processed keys in memory:

```java
private final Map<String, String> processedKeys =
        new ConcurrentHashMap<>();
```

The service stores:

```text
Idempotency Key → Payment ID
```

Example:

```text
payment-request-10001 → PAY-10001
```

This is intentionally a simple POC implementation.

> **Important:** The current idempotency store is not persistent. Restarting the application clears the stored keys.

For a production implementation, the idempotency record should be persisted in a shared datastore such as **Redis** or a database, with appropriate expiration, atomicity, and concurrency handling.

---

# Kafka Topic

The producer publishes payment events to:

```text
payment.created
```

The payment ID is used as the Kafka message key:

```java
kafkaTemplate.send(
    "payment.created",
    event.paymentId(),
    event
);
```

This provides a natural key for payment-related event partitioning if the Kafka topic is later configured with multiple partitions.

---

# What This POC Demonstrates

### REST

- Spring Boot REST controllers
- HTTP headers
- JSON request bodies
- HTTP responses

### Event-Driven Architecture

- Asynchronous event publication
- Producer/consumer separation
- Kafka topics
- Event contracts

### Payment-System Concepts

- Idempotency
- Payment identifiers
- Customer identifiers
- Monetary values using `BigDecimal`
- Event-based processing

### Spring Boot

- Dependency injection
- `@RestController`
- `@Service`
- `@KafkaListener`
- Spring Boot configuration

### Local Infrastructure

- Docker Compose
- Local Apache Kafka
- Maven-based application startup

---

# Current Limitations

This is a learning-focused POC and does **not** yet implement several production payment-service requirements.

### Persistence

- Payments are persisted in PostgreSQL through Spring Data JPA.
- Idempotency is still stored only in memory.
- Payment status is currently represented by `PaymentStatus` but is not yet persisted as part of the payment lifecycle.

### Reliability

- No retry strategy is implemented.
- No dead-letter topic is configured.
- No explicit Kafka error-handling strategy is implemented.
- `@Transactional` protects the database transaction but does not make the PostgreSQL write and Kafka publication atomic.
- No transactional outbox pattern is implemented.

### Security

- No authentication.
- No authorization.
- No API rate limiting.
- No request signature verification.
- No encryption/key-management implementation.

### Payment Processing

The Kafka consumer currently logs the payment rather than integrating with a real payment processor.

---

# Future Enhancements

### Payment Processing

- Payment status lifecycle
- Payment persistence
- Payment authorization
- Payment capture
- Payment cancellation/refund
- Payment failure handling
- External payment gateway integration

### Idempotency

- Redis-backed idempotency store
- Database-backed idempotency records
- Idempotency-key expiration
- Atomic duplicate-request handling
- Concurrent request protection
- Idempotency Client State Management
- Idempotency Keys Clean Up and Retention Policies
- Idempotency Retry Scenarios

### Kafka Reliability

- Retry topics
- Dead-letter topics
- Error handlers
- Consumer retry policies
- Kafka transactions
- Message delivery guarantees
- Schema management

### Security

- Spring Security
- JWT Authentication
- Role-Based Access Control (RBAC)
- API authentication
- Request signing
- Secrets management
- TLS configuration

### Quality

- Unit tests
- Cucumber , Karate Tests
- Controller tests
- Kafka integration tests
- Testcontainers
- Mockito
- JaCoCo code coverage
- Checkstyle / Spotless
- Centralized exception handling
- Performance Tests

### Observability

- Structured logging
- Correlation IDs
- Micrometer metrics
- Spring Boot Actuator
- Prometheus
- Grafana
- Distributed tracing
- OpenTelemetry
- Datadog Instrumentation

### DevOps

- Dockerized application
- Agnostic CI/CD using MakeFile or TaskFile
- CI/CD using GitHub Actions, Gitlab CI/CD, AzureDevOps
- Multi-environment configuration
- Infrastructure as Code
- Kubernetes deployment
- Cloud Kafka deployment

---
# Future High-Level Architecture

The current POC demonstrates a single payment consumer. The intended future architecture demonstrates the main advantage of an event-driven platform such as **Apache Kafka**: a single payment event can be consumed independently by multiple downstream services.

```text
                                      Client
                                        │
                                        │ POST /payments
                                        ▼
                              ┌─────────────────────┐
                              │  Payment Service    │
                              │   Spring Boot       │
                              └──────────┬──────────┘
                                         │
                                         │ PaymentCreatedEvent
                                         ▼
                              ┌─────────────────────┐
                              │       Kafka         │
                              │                     │
                              │ payment.created     │
                              └──────────┬──────────┘
                                         │
                         ┌───────────────┼────────────────┐
                         │               │                │
                         ▼               ▼                ▼
                ┌────────────────┐ ┌──────────────┐ ┌──────────────┐
                │ Payment        │ │ Notification │ │ Audit        │
                │ Processing     │ │ Service      │ │ Service      │
                │ Service        │ │              │ │              │
                └───────┬────────┘ └──────┬───────┘ └──────┬───────┘
                        │                 │                │
                        ▼                 ▼                ▼
                  Payment DB       Email / SMS /      Audit Log /
                                   Push Notification  Audit DB
```

### Why Kafka?

The key advantage is **decoupling**.

The Payment Service does not need to know which systems need to react to a successful payment. It publishes a `PaymentCreatedEvent` once, and multiple independent consumers can subscribe to the event.

For example:

* **Payment Processing Service** — performs downstream payment processing.
* **Notification Service** — sends email, SMS, or push notifications.
* **Audit Service** — records the payment event for auditing and compliance.
* **Future services** — additional consumers can be introduced without modifying the Payment Service.

```text
                    PaymentCreatedEvent
                           │
                           ▼
                    ┌─────────────┐
                    │    Kafka    │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
          Consumer A   Consumer B   Consumer C
          Payment      Notification Audit
          Processing   Service      Service
```

This means the producer and consumers are **loosely coupled**. Adding a new consumer does not require the Payment Service to implement new integrations or call additional APIs.

### Consumer Groups

Kafka consumer groups can also be used to control how events are distributed.

For example:

```text
payment-processing-group
        │
        └── Payment Processing Service

notification-group
        │
        └── Notification Service

audit-group
        │
        └── Audit Service
```

Each consumer group receives its own copy of the event.

Therefore, a single:

```text
PaymentCreatedEvent
```

can be processed independently by:

```text
Payment Processing
        +
Notification
        +
Audit
```

This is one of the key architectural advantages this POC is intended to demonstrate as it evolves from a simple Kafka producer/consumer example into a more realistic event-driven payment platform.

### Future Evolution

The current implementation starts with:

```text
Payment Service
      │
      ▼
    Kafka
      │
      ▼
Payment Consumer
```

The planned architecture evolves toward:

```text
                         ┌── Payment Processing
                         │
Payment Service ──► Kafka ├── Notification
                         │
                         ├── Audit
                         │
                         └── Future Consumers
```

This allows new business capabilities to be added as independent services while keeping the core Payment Service relatively unchanged.

---

# Project Documentation

Additional documentation can be added under the `docs/` directory as the project evolves.

Recommended documentation:

| Document | Description |
|----------|-------------|
| `API.md` | REST API endpoints, request/response examples, and API behavior. |
| `ARCHITECTURE.md` | Event-driven architecture and component responsibilities. |
| `KAFKA.md` | Kafka topics, producers, consumers, partitions, and delivery semantics. |
| `IDEMPOTENCY.md` | Idempotency design and production implementation considerations. |
| `DEVELOPMENT.md` | Local development setup and development workflow. |
| `DEPLOYMENT.md` | Deployment and infrastructure instructions. |
| `ROADMAP.md` | Planned improvements and future enhancements. |
| `CHANGELOG.md` | Version history and notable project changes. |

---

# Author

**ThessaCodes [Java]**

**Project:** Demo Payment Service
**Repository:** https://github.com/i-am-thessa/thessacodes-java-payment-service.git  
**Created:** August 2026

---

> This project is a learning and portfolio POC focused on demonstrating Java Spring Boot, Kafka, event-driven architecture, and payment-system design concepts. It is not intended to process real financial transactions.
