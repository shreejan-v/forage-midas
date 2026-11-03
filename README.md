# Midas Core: event-driven transaction processing (Spring Boot + Kafka)

A small transaction-processing service built on the starter code of the **J.P. Morgan Chase Advanced Software Engineering job simulation on Forage**. Transactions arrive on a Kafka topic, are validated against user balances stored in an H2 database, earn a bonus from an external incentive REST API, and the resulting balances are exposed through a REST endpoint.

> This is a learning project from a public virtual job simulation. It is not JPMC production code and is not affiliated with or endorsed by JPMorgan Chase.

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

## Running locally
Requires Java 17+ and a Kafka broker on `localhost:9092`.
```bash
java -jar services/transaction-incentive-api.jar   # Forage-provided incentive API on port 8080
./mvnw spring-boot:run                              # Midas on port 33400
curl "http://localhost:33400/balance?userId=1"
```

## Limitations
- **Money is stored as `float`** because the Forage starter (entities and test helpers) uses `float`. This loses precision: one test user's starting balance of 779421.33 is stored as 779421.31. A production system should use `BigDecimal` or integer minor units; changing it here would break the provided Forage helpers.
- The end-to-end test mocks the incentive API to return 0, so it verifies routing, validation and persistence, not the incentive amounts of the real Forage service.
- Messages are processed one at a time with no idempotency key, so a redelivered Kafka message would be applied twice.
- H2 is in memory; balances are lost on restart.

## Tech stack
Java 17, Spring Boot 3.2, Spring Kafka, Spring Data JPA, H2, JUnit 5, Mockito, AssertJ, Awaitility, embedded Kafka.
