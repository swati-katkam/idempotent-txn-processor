# Idempotent Background Transaction Processing Service

A background job that processes transaction requests which may arrive **out of order**, **duplicated**
or **retried after partial failure**, without ever producing a duplicate ledger effect or an
inconsistent transaction state.

Built for the *FDE L2 Practical Assessment*. Plain Java 17, no frameworks, one test dependency
(JUnit 5). The design write-up is in **[docs/DESIGN.md](docs/DESIGN.md)**.

| Requirement | Where |
|---|---|
| L2-FR-01 replaceable input source | `ingest/TransactionSource` (CSV file adapter and in-memory queue adapter) |
| L2-FR-02 validation, malformed records isolated | `validation/TransactionValidator`, dead-letter `store/RejectionStore` |
| L2-FR-03/04 state and status lifecycle | `domain/TransactionStatus` (enforced state machine), `domain/TransactionRecord` (audit trail) |
| L2-FR-05/06 process once, detect late duplicates | `store/TransactionStateStore#claim` (requestId and transactionId plus payload fingerprint) |
| L2-FR-07 out-of-order strategy | `ordering/SequenceOrderingBuffer` (sequence-based, option A) |
| L2-FR-08/09 retryable failures, no duplicate effects | `retry/*`, idempotent `posting/PostingGateway`, `simulation/FaultInjectingPostingGateway` |
| L2-FR-10 business logic separate from input | `processing/TransactionProcessor` knows nothing about CSV or queues |
| L2-FR-11 summary with counts by status | `observability/ProcessingReport` (console and JSON) |
| L2-FR-12 tests | 61 tests in `src/test/java` |

---

## 1. Prerequisites

| Tool | Version | Check |
|---|---|---|
| JDK | **17 or newer** (tested with Temurin 21) | `java -version` |
| Maven | 3.8+ (tested with 3.9.9) | `mvn -v` |

No database, broker, network access or secrets are needed at runtime. Maven downloads JUnit
on the first build.

Windows quick install (pick one): `winget install EclipseAdoptium.Temurin.21.JDK` and
`winget install Apache.Maven`, or download the zips from https://adoptium.net and
https://maven.apache.org/download.cgi and add their `bin` folders to `PATH`. Set `JAVA_HOME` to the JDK folder.

## 2. Build and test

```bash
mvn clean verify          # compiles, runs all 61 tests, builds target/txn-processor.jar
```

Test reports land in `target/surefire-reports/`.

## 3. Run

```bash
# L2 suggested data set (section 9.8). TXN-2005 is configured to fail once, transiently.
java -jar target/txn-processor.jar

# Wider edge cases: business-rule failure, retry exhaustion, lost ack after commit,
# reversal, idempotency conflict, gap in sequence, malformed rows
java -jar target/txn-processor.jar samples/transactions-edge-cases.csv \
     --fault TXN-3003:5:BEFORE_COMMIT --fault TXN-3004:1:AFTER_COMMIT \
     --report out/edge-report.json

# Help / options
java -jar target/txn-processor.jar --help
```

* **stdout**: the processing summary. **stderr**: structured `event=... key=value` logs.
* A JSON report is written to `out/processing-report.json` (or the `--report` path).
* **Exit code** `0` = everything resolved, `2` = finished but FAILED/PENDING items need attention,
  `1` = usage or startup error. A scheduler (cron, Control-M, Airflow) can alert on the exit code.
* Any setting in [application.properties](src/main/resources/application.properties) can be overridden
  with `-Dtxn.<key>=<value>`, e.g. `java -Dtxn.retry.maxAttempts=5 -Dtxn.ordering.scope=ACCOUNT -jar ...`.
  Log level: `-Dtxn.log.level=FINE`.

### Input format

CSV with a header row; columns are matched by name, `#` lines are comments.

```
transactionId,requestId,sequenceNumber,accountId,transactionType,amount,currency,eventTime,originalTransactionId
TXN-2001,REQ-1,1,ACC-201,CREDIT,100,USD,2026-11-01T09:00:01Z,
```

`eventTime` and `originalTransactionId` are optional (`originalTransactionId` is required for `REVERSAL`).

## 4. Sample input and output

Committed in [samples/](samples/) and [samples/output/](samples/output/):

| Input | Summary (stdout) | Event log (stderr) | JSON report |
|---|---|---|---|
| [transactions.csv](samples/transactions.csv) | [run-suggested.stdout.txt](samples/output/run-suggested.stdout.txt) | [run-suggested.log](samples/output/run-suggested.log) | [run-suggested.report.json](samples/output/run-suggested.report.json) |
| [transactions-edge-cases.csv](samples/transactions-edge-cases.csv) | [run-edge-cases.stdout.txt](samples/output/run-edge-cases.stdout.txt) | [run-edge-cases.log](samples/output/run-edge-cases.log) | [run-edge-cases.report.json](samples/output/run-edge-cases.report.json) |

Result for the suggested data set:

| # | Request | Outcome | Why |
|---|---|---|---|
| 1 | TXN-2002 / REQ-2 seq 2 | PENDING, then **PROCESSED** | arrived before seq 1, so it was parked and released when seq 1 completed |
| 2 | TXN-2001 / REQ-1 seq 1 | **PROCESSED** | |
| 3 | TXN-2002 / REQ-2-DUP | **DUPLICATE** | same transactionId with an identical payload; no second posting |
| 4 | TXN-2003 / REQ-3 seq 3 | **PROCESSED** | |
| 5 | TXN-2004 / REQ-4 amount -10 | **REJECTED** (`amount:MUST_BE_POSITIVE`) | dead-lettered; its sequence slot is released so seq 5 is not blocked |
| 6 | TXN-2005 / REQ-5 seq 5 | RETRY_PENDING, then **PROCESSED** (2 attempts) | simulated transient failure, retried after 100 ms of backoff |

Final balances: ACC-201 = 85 (100 - 40 + 25), ACC-203 = 500. Each transaction is posted to the ledger exactly once.

## 5. Tests

| Test class | What it proves |
|---|---|
| `SuggestedDatasetTest` | The 9.8 data set end to end: statuses, lifecycle, balances, summary counts, masking |
| `IdempotencyTest` | Same request x3, new requestId for same txn, payload conflict, requestId reuse, duplicate while pending, full batch replay |
| `OrderingTest` | Reverse arrival, debit before its funding credit, gaps reported, late fill releases backlog, sequence conflicts, poison record doesn't block, per-account scope |
| `RetryTest` | Retry then success, exhaustion leads to FAILED with no effect, lost ack after commit is not double-posted, head-of-line wait during retry, operator replay, backoff curve |
| `ValidationTest` | 18 invalid field cases, all errors reported together, batch isolation, no value echo in reasons |
| `BusinessRulesTest` | Insufficient funds, reversal rules, no partial state on rule failure |
| `StateAndConcurrencyTest` | Illegal transitions refused, audit trail, 16 threads claiming one transaction give exactly one winner |
| `CsvSourceTest` | Header mapping, malformed rows, fail-fast on missing columns, both sample files end to end |

## 6. Project layout

```
src/main/java/com/fde/txnprocessor/
  App.java                         CLI: runs the worker on a background thread, prints and writes the report
  TransactionProcessingModule.java composition root (wires adapters to core; the only place to swap infra)
  config/        ProcessorConfig   properties + -Dtxn.* overrides
  domain/        request/record/status model + lifecycle state machine
  validation/    TransactionValidator
  store/         TransactionStateStore (idempotency + state) and RejectionStore (dead letter) ports + in-memory adapters
  ordering/      SequenceOrderingBuffer + OrderingPolicy
  posting/       PostingGateway port, PostingRules (pure business rules), InMemoryLedger
  retry/         RetryPolicy (bounded exponential backoff), RetryScheduler
  processing/    TransactionProcessor (core), TransactionWorker (background loop)
  ingest/        TransactionSource port, CSV and in-memory queue adapters
  observability/ structured EventLog, masking, metrics, ProcessingReport
  simulation/    FaultInjectingPostingGateway (BEFORE_COMMIT / AFTER_COMMIT transient faults)
docs/DESIGN.md   design note
samples/         input files and captured output
```
