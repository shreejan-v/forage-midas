# Midas Core: event-driven transaction processing (Spring Boot + Kafka)

A small transaction-processing service built on the starter code of the **J.P. Morgan Chase Advanced Software Engineering job simulation on Forage**. Transactions arrive on a Kafka topic, are validated against user balances stored in an H2 database, earn a bonus from an external incentive REST API, and the resulting balances are exposed through a REST endpoint.

> This is a learning project from a public virtual job simulation. It is not JPMC production code and is not affiliated with or endorsed by JPMorgan Chase.

## Contents
- [Background](#background)
- [What it does](#what-it-does)
- [Forage tasks covered](#forage-tasks-covered)
- [Components](#components)
- [Data model](#data-model)
- [Message and API reference](#message-and-api-reference)
- [Walk-through of one transaction](#walk-through-of-one-transaction)
- [Configuration](#configuration)
- [Design decisions](#design-decisions)
- [Tests](#tests)
- [Project structure](#project-structure)
- [Running locally](#running-locally)
- [Troubleshooting](#troubleshooting)
- [Limitations](#limitations)
- [Possible improvements](#possible-improvements)
- [What I learned](#what-i-learned)
- [Tech stack](#tech-stack)

## Background
Forage job simulations give a starter repository and a sequence of tasks modelled on real engineering work. For Midas, the starter contains the Spring Boot skeleton, the `UserRecord` entity, the `Transaction` and `Balance` message classes, test data files and verifier tests. Everything that consumes, validates, records and exposes transactions was left to implement, and is the work in this repository.

## What it does
```
Kafka topic "trader-updates"
   |  JSON Transaction {senderId, recipientId, amount}
   v
TransactionListener (@KafkaListener)
   v
TransactionProcessor (@Transactional)
   |-- reject if sender/recipient unknown, self-transfer, amount <= 0, or insufficient funds
   |-- ask IncentiveClient for a bonus (POST /incentive; falls back to 0 if the API is down)
   |-- debit sender by amount, credit recipient by amount + incentive
   |-- write a TransactionRecord (audit trail)
   v
H2 (UserRecord, TransactionRecord)
   ^
BalanceController  GET /balance?userId={id}  ->  {"amount": ...}   (unknown user -> 0)
```

## Forage tasks covered
| Task | What it asks | Where it lives |
|---|---|---|
| 1 | Set up the project and boot the app | `MidasCoreApplication`, `pom.xml` |
| 2 | Consume transactions from Kafka | `component/TransactionListener`, Kafka config in `application.yml` |
| 3 | Validate transactions and persist balances | `service/TransactionProcessor`, `entity/TransactionRecord` |
| 4 | Call the incentive API and credit the bonus | `component/IncentiveClient`, `foundation/Incentive` |
| 5 | Expose balances over REST | `controller/BalanceController` (port 33400) |

## Components
| Class | Package | Responsibility |
|---|---|---|
| `TransactionListener` | `component` | `@KafkaListener` on `${general.kafka-topic}`; deserialises JSON into `Transaction` and passes it to the processor |
| `TransactionProcessor` | `service` | business rules: validation, incentive lookup, debit and credit, audit record, all inside one `@Transactional` method |
| `IncentiveClient` | `component` | `RestTemplate` POST to the incentive API, 2 s connect and read timeouts, returns 0 on any client error |
| `BalanceController` | `controller` | `GET /balance?userId=` returning a `Balance` JSON body |
| `UserRepository`, `TransactionRepository` | `repository` | Spring Data repositories over H2 |
| `UserRecord`, `TransactionRecord` | `entity` | JPA entities (users and applied transactions) |
| `Transaction`, `Balance`, `Incentive` | `foundation` | message and response objects shared with Kafka and the REST APIs |

## Data model
**USER_RECORD** (from the starter)

| Column | Type | Notes |
|---|---|---|
| `id` | bigint | generated; the Forage user file loads users as IDs 1 to 11 in file order |
| `name` | varchar | not null |
| `balance` | real | not null; `float` in Java |

**TRANSACTION_RECORD** (added)

| Column | Type | Notes |
|---|---|---|
| `id` | bigint | generated |
| `sender_id` | bigint | foreign key to `USER_RECORD` |
| `recipient_id` | bigint | foreign key to `USER_RECORD` |
| `amount` | real | amount debited from the sender |
| `incentive` | real | bonus credited to the recipient on top of `amount` |

Only applied transactions are recorded; rejected ones are logged but not stored.

## Message and API reference
**Kafka message** on topic `trader-updates` (JSON, no type headers):
```json
{"senderId": 1, "recipientId": 2, "amount": 90.60}
```

**Incentive API** (external, called by Midas):
```
POST http://localhost:8080/incentive
Content-Type: application/json
{"senderId": 1, "recipientId": 2, "amount": 90.60}

200 OK
{"amount": <incentive>}
```

**Balance API** (served by Midas):
```
GET http://localhost:33400/balance?userId=1

200 OK
{"amount": 1326.98}
```
The example value is user 1 after the Task 5 transaction file with a zero incentive, as checked in `KafkaToBalanceIntegrationTest`. Unknown user IDs return `{"amount": 0.0}` rather than 404, because the Forage verifier queries IDs 0 to 12 and expects a value for each.

## Walk-through of one transaction
This mirrors `TransactionProcessorTest.incentiveIsCreditedToRecipientOnly`:
1. Users: alice has 100.00, bob has 50.00.
2. A message `{"senderId": alice, "recipientId": bob, "amount": 30}` arrives on the topic.
3. Both users exist, they are different, 30 > 0 and alice can cover it, so the transaction is valid.
4. The incentive API returns 5.
5. alice becomes 70.00 (debited 30 only); bob becomes 85.00 (30 + 5 incentive).
6. One `TransactionRecord` (amount 30, incentive 5) is written in the same database transaction.

If bob instead tried to send 50.01, step 3 fails: nothing is debited, nothing is recorded, and the incentive API is never called.

## Configuration
| Property | Default | Meaning |
|---|---|---|
| `general.kafka-topic` | `trader-updates` | topic the listener consumes |
| `general.incentive-api-url` | `http://localhost:8080/incentive` | incentive service endpoint |
| `server.port` | `33400` | port for the balance API (the Forage verifier expects 33400) |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Kafka broker |
| `spring.kafka.consumer.group-id` | `midas-core` | consumer group |
| `spring.kafka.consumer.auto-offset-reset` | `earliest` | read messages published before the listener started |
| `spring.datasource.url` | `jdbc:h2:mem:midas` | in-memory database |

Any of these can be overridden with environment variables through Spring relaxed binding, for example `SERVER_PORT=8081` or `GENERAL_INCENTIVE_API_URL=http://host:8080/incentive`.

## Design decisions
- **One transaction, one database transaction.** Validation, both balance updates and the audit record happen inside a single `@Transactional` method, so a failure cannot debit the sender without crediting the recipient.
- **Validation before the incentive call.** The incentive API is only called for transactions that will actually be applied.
- **Fail-safe external call.** The incentive client has 2-second timeouts and returns a zero incentive if the API errors, so an outage costs at most about 2 seconds per message and does not block the transfer itself. Whether a real bank would rather retry or park the message is a product decision; this is documented as a limitation.
- **Configuration over code.** Topic, broker, incentive URL and port are in `src/main/resources/application.yml` and can be overridden with environment variables (for example `GENERAL_INCENTIVE_API_URL`).
- **Unknown users read as 0.** `GET /balance` returns 0 for users that do not exist, matching the Forage verifier.

## Tests
The original Forage `Task*Tests` classes are interactive verifiers: several loop forever waiting for a debugger. They are kept unchanged but excluded from the default build, and replaced by assertive tests:

| Test | What it checks |
|---|---|
| `TransactionProcessorTest` | valid transfer, incentive credited to the recipient only, spending the exact balance, insufficient funds, unknown users, self-transfer, zero and negative amounts (rejections leave balances and the audit table untouched and never call the incentive API) |
| `IncentiveClientTest` | the client POSTs the transaction JSON and parses the incentive; returns 0 when the API fails (mock HTTP server, no network) |
| `KafkaToBalanceIntegrationTest` | end to end through an embedded Kafka broker: loads the Forage user file, publishes the Task 5 transaction file, and checks `GET /balance` for user IDs 0 to 12 against balances computed independently by a Python reference script |

Result of the last run: **9 tests, 0 failures** (JDK 26 on Windows). On JDK 26, Mockito's ByteBuddy needs an experimental flag, as shown below; on Java 17 or 21 it should not be needed, but that was not tested here.

```bash
./mvnw test                                                          # assertive test suite
./mvnw test "-DargLine=-Dnet.bytebuddy.experimental=true -XX:+EnableDynamicAgentLoading"   # on JDK 26
./mvnw test -Dtest=TaskFiveTests -Dsurefire.failIfNoSpecifiedTests=false   # an original Forage verifier
```

## Project structure
```
src/main/java/com/jpmc/midascore/
  MidasCoreApplication.java
  component/   TransactionListener, IncentiveClient, DatabaseConduit
  controller/  BalanceController
  entity/      UserRecord, TransactionRecord
  foundation/  Transaction, Balance, Incentive
  repository/  UserRepository, TransactionRepository
  service/     TransactionProcessor
src/main/resources/application.yml
src/test/java/com/jpmc/midascore/
  integration/ TransactionProcessorTest, IncentiveClientTest, KafkaToBalanceIntegrationTest
  Task*Tests, KafkaProducer, UserPopulator, BalanceQuerier, FileLoader   (Forage-provided)
src/test/resources/test_data/            Forage user and transaction files
services/transaction-incentive-api.jar   Forage-provided incentive service
```

## Running locally
Requires Java 17+ and a Kafka broker on `localhost:9092`.
```bash
java -jar services/transaction-incentive-api.jar   # Forage-provided incentive API on port 8080
./mvnw spring-boot:run                              # Midas on port 33400
curl "http://localhost:33400/balance?userId=1"
```

## Troubleshooting
- **`release version 17 not supported`**: Maven found a Java runtime without a compiler (a JRE). Install a JDK 17+ and point `JAVA_HOME` at it.
- **Mockito or ByteBuddy errors on very new JDKs** (for example JDK 26): add `-DargLine="-Dnet.bytebuddy.experimental=true -XX:+EnableDynamicAgentLoading"`.
- **A Forage `Task*Tests` run never finishes**: by design; those verifiers wait for you to inspect values in a debugger. Stop them manually.
- **Port 9092 already in use** when running a Forage verifier: those tests bind the embedded broker to 9092, so stop any local Kafka first. The assertive tests use a random port and are not affected.

## Limitations
- **Money is stored as `float`** because the Forage starter (entities and test helpers) uses `float`. This loses precision: one test user's starting balance of 779421.33 is stored as 779421.31. A production system should use `BigDecimal` or integer minor units; changing it here would break the provided Forage helpers.
- The end-to-end test mocks the incentive API to return 0, so it verifies routing, validation and persistence, not the incentive amounts of the real Forage service.
- Messages are processed one at a time with no idempotency key, so a redelivered Kafka message would be applied twice.
- H2 is in memory; balances are lost on restart.

## Possible improvements
- Store money as `BigDecimal` (or integer paise/cents) end to end.
- Idempotent processing with a transaction ID and a unique constraint, so redelivered messages are skipped.
- A dead-letter topic for rejected or failed messages instead of only logging them.
- Retry with backoff for the incentive API, and a metric for how often the fallback is used.
- PostgreSQL via Docker Compose and Testcontainers instead of in-memory H2.
- A `GET /transactions?userId=` endpoint over the audit table.

## What I learned
- Wiring Spring Kafka with JSON (de)serialisation and consumer groups.
- Keeping multi-row updates consistent with `@Transactional`.
- Testing event-driven code end to end with an embedded broker and Awaitility instead of fixed sleeps.
- Isolating an external HTTP dependency with a mock server, and choosing a fallback when it fails.
- Why `float` is the wrong type for money, seen as a real precision loss in the test data.

## Tech stack
Java 17, Spring Boot 3.2, Spring Kafka, Spring Data JPA, H2, JUnit 5, Mockito, AssertJ, Awaitility, embedded Kafka.
