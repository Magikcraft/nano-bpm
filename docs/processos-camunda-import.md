# ProcessOS — Camunda 8 export → Nano trace import

> Status: **implemented**. Module `processos/src/camunda_import.rs`; CLI
> `processos import-camunda`. This document is the reference for the importer:
> what it does, the record→trace mapping, the accepted input formats, the
> fidelity tiers and their limits, and the path from this offline transformer to
> an in-engine Zeebe exporter.

## Why

A consultant working a customer engagement frequently has the customer's
**Camunda 8 / Zeebe** execution history but no Nano engine to point ProcessOS
at. ProcessOS already reads *captured* history through a
[`DatasetSource`](../processos/src/dataset.rs) — a folder of per-instance trace
JSON, the same shape the Nano gateway serves at `GET /console/api/traces/{key}`.
The importer makes a Camunda export speak that shape, so the customer's existing
C8 data loads straight into a workspace dataset and every ProcessOS read-path
(Insights, the cockpit, `query_traces`, replay/`simulate`) works unchanged —
**no Nano engine, and no Java**.

## The core idea: the same fold, run offline

A **Nano trace is a projection, not a log.** The Nano gateway's
[`TraceStore`](../server/src/console/trace.rs) folds the engine's *ordered event
stream* (`ProcessInstanceCreated`, `ElementActivating/Activated/Completed`,
`JobCreated/Activated/Completed/Failed`, `IncidentRaised/Resolved`,
`VariablesUpdated`, …) into per-instance, per-element traces.

A Camunda broker streams an almost identical artefact: an ordered log of
`Record<?>` documents (position, key, timestamp, valueType, intent, value),
which the Elasticsearch / Opensearch / debug-log exporters persist as JSON. The
importer runs **the same fold over Camunda's records** instead of Nano's events.
The two vocabularies line up nearly 1:1:

| Camunda record — `valueType` / intent                     | Nano trace contribution                              |
|-----------------------------------------------------------|------------------------------------------------------|
| `PROCESS_INSTANCE` ELEMENT_ACTIVATING/ACTIVATED (process) | instance `startedAt`                                 |
| `PROCESS_INSTANCE` ELEMENT_COMPLETED (process)            | `outcome = completed`, `durationMs`                  |
| `PROCESS_INSTANCE` ELEMENT_TERMINATED (process)           | `outcome = terminated`, `durationMs`                 |
| `PROCESS_INSTANCE` ELEMENT_* (flow element)               | `elements[].elementId` + `durationMs`                |
| `JOB` CREATED                                             | job queue start                                      |
| `JOB_BATCH` ACTIVATED                                     | job activation instant → queue/service split         |
| `JOB` COMPLETED                                           | job end (`serviceMs`); Tier-2 `jobCompleted` stimulus|
| `JOB` FAILED / ERROR_THROWN                               | `elements[].job.failures`                            |
| `INCIDENT` CREATED                                        | `incidents[]` + `elements[].incidents`               |
| `PROCESS_INSTANCE_CREATION` CREATED                       | Tier-1 `creationVariables`                           |

Because both sides are an ordered fold, the importer is also the architectural
template for an **in-engine exporter** (see *Path to a live exporter* below).

## Output

The importer writes a single `traces.json` array under the output directory —
byte-compatible with [`contracts::InstanceTrace`](../processos/src/contracts.rs)
(camelCase), exactly what `DatasetSource::open` reads. Drop that folder into a
workspace process binding (`{"displayName": …, "dataset": "<abs out-dir>"}`) and
ProcessOS treats it like any other captured dataset.

Each trace carries: `instanceKey, processId, version, outcome, startedAt,
durationMs, elements[{elementId, durationMs, incidents, job{type, queueMs,
serviceMs, failures}}], incidents[{elementId, kind, reason}]`, plus the optional
`creationVariables` and `stimuli` capture tiers.

## Fidelity tiers

The importer mirrors Nano's own capture tiers, emitting as much as the export
allows:

- **Tier-0 — always.** Instance lifecycle, per-element durations, jobs
  (type, failures, and queue/service timing), and incidents. Sufficient for
  Insights, domain inference, and `query_traces` analysis.
- **Tier-1 — `creationVariables`.** The instance's creation inputs, taken from
  the `PROCESS_INSTANCE_CREATION` record's `variables`. Present only when that
  record is in the export.
- **Tier-2 — `stimuli`.** An ordered `jobCompleted` log (job *type* + the
  completion variables), so a trace is **recorded-input replayable** by the
  Alternate Reality Engine (`simulate` / `compare_variants`). Enabled by default;
  pass `--no-tier2` to skip it.

### Known limits (read before trusting replay)

- **Queue/service split needs `JOB_BATCH ACTIVATED` records.** With them, a job's
  wait is split into `queueMs` (created→activated) and `serviceMs`
  (activated→completed). Without them, the whole `created→completed` wait is
  reported as `serviceMs` and `queueMs` is omitted. Make sure job-batch records
  are in the export if the queue tail matters.
- **Tier-2 captures job/worker outputs only.** Other external inputs — message
  correlation, timers, user-task completions — are **not** yet folded into the
  stimulus log. A model that consumes them is therefore only *partially*
  replayable. (`stimuliTruncated` is reserved to flag this; today the importer
  captures `jobCompleted` and leaves the flag `false`.)
- **Replay determinism is a separate gate.** Replaying a Camunda-captured
  stimulus log inside the Nano harness requires the customer's BPMN to be
  deployable on Nano and within its supported element subset. Where it is, you get
  full hypothesis testing; where the model uses C8-only constructs, you are
  limited to Tier-0/Tier-1 analysis.
- **Only `EVENT` records fold.** `COMMAND` / `COMMAND_REJECTION` records are
  intent, not settled outcome, and are ignored.

## Accepted input formats

`load_records` is tolerant of the common Camunda dump layouts (a single file or a
directory of `*.json` / `*.ndjson` / `*.jsonl` / `*.log`):

- **NDJSON** — one record JSON object per line (debug-log / file exports).
- **JSON array** — `[ {record}, … ]`.
- **Elasticsearch / Opensearch search response** — `{ "hits": { "hits": [ {
  "_source": {record} } ] } }`, and bare `{ "_source": {record} }` lines, so a raw
  `_search` / scroll dump works without pre-processing.

Records are folded in `(timestamp, position)` order, so a `JOB CREATED` is always
processed before its `JOB_BATCH ACTIVATED` / `JOB COMPLETED` regardless of how the
input file is ordered.

## CLI

```bash
# input may be a file or a directory of any accepted format
processos import-camunda <records.json|dir> <out-dir> [--no-tier2]
```

Example:

```bash
processos import-camunda ./zeebe-records.ndjson /tmp/c8-dataset
# -> writes /tmp/c8-dataset/traces.json and prints a fold summary:
# {
#   "recordsRead": 9, "traces": 1,
#   "completed": 1, "terminated": 0, "active": 0,
#   "withIncidents": 0, "withCreationVariables": 1, "withStimuli": 1,
#   "processes": ["north-wind-loan"], "outDir": "/tmp/c8-dataset"
# }

# then bind it as a workspace dataset, or sanity-check with the corpus inference:
processos infer /tmp/c8-dataset 500
```

## Getting the records out of Camunda

Any exporter that emits the record JSON works:

- **Elasticsearch / Opensearch exporter** (the default in most C8 installs):
  dump the `zeebe-record-*` indices (`_search` / scroll, or an export tool) to a
  file/dir and point the importer at it. The `_source` envelope is unwrapped
  automatically.
- **Debug-log exporter** (`io.camunda.zeebe.broker.exporter.debug.DebugLogExporter`):
  it logs `record.toJson()` per record — capture those lines as NDJSON.
- Any custom exporter that writes `Record#toJson()`.

You do **not** need every value type — the importer reads only
`PROCESS_INSTANCE`, `PROCESS_INSTANCE_CREATION`, `JOB`, `JOB_BATCH`, and
`INCIDENT` and ignores the rest, so a filtered export is fine (but include
`JOB_BATCH` for the queue/service split and `PROCESS_INSTANCE_CREATION` for
Tier-1).

## Path to a live exporter

The fold is deliberately factored as a pure library:
`transform(records, tier2) -> Vec<TraceOut>` and `load_records(path)`, with
`import(...)` as the thin file-I/O wrapper. The Zeebe exporter SPI
(`io.camunda.zeebe.exporter.api.Exporter`) is a JVM contract loaded into the
broker, so the *plugin* must be JVM — but the *fold* need not be Java. The same
Rust `transform` can back, in increasing coupling:

1. **Offline ETL (this importer).** Pure Rust, zero Java; run over an existing
   ES/OS export. Lowest risk, best first step.
2. **Out-of-process sidecar.** A ~100-line Java `Exporter` shim forwards each
   `record.toJson()` over IPC to a Rust sidecar running this fold. Crash-isolated
   from the broker; must honour Zeebe's at-least-once contract (advance
   `Controller.updateLastExportedRecordPosition` only after durable accept; dedup
   by record `position`).
3. **In-process FFI.** The fold compiled to a `cdylib` and called from the Java
   shim via JNI (Project Panama/FFM is only *preview* on Camunda's JDK 21).
   Highest throughput, highest blast radius — a native panic crashes the broker,
   so guard the FFI edge with `catch_unwind`.

Start at (1); graduate to (2) when live capture is needed; reserve (3) for proven
throughput demands.

## Tests

`processos/src/camunda_import.rs` carries unit tests covering the happy-path fold
(instance + element + job queue/service split + Tier-1 + Tier-2), the
no-`JOB_BATCH` fallback, incident capture, terminated outcome, the `EVENT`-only
filter, and the NDJSON / array / Elasticsearch-envelope loaders. Run them with
`cargo test --bin processos camunda_import`.
