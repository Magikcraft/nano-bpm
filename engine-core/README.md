# nanobpmn-engine-core

A minimal, **embeddable** BPMN execution engine.

It follows the **Camunda 8 (Zeebe)** execution architecture — a deterministic
`command → event → applier` state machine driven by a single sequential writer —
with all of the distributed-systems machinery deliberately removed. There is no
Raft, no partitioning, no exporters, no gRPC and no RocksDB. What's left is the
part that makes a process engine correct, testable and portable.

## Why the Camunda 8 model (not the Camunda 7 PVM)?

Two execution architectures were considered:

| | Camunda 7 PVM (`camunda-bpm-platform`) | Camunda 8 / Zeebe |
| --- | --- | --- |
| Execution | Token walks the graph via `ActivityBehavior` + atomic operations | Element-lifecycle state machine driven by a stream of commands/events |
| State mutation | Mutable entities, in-place + dirty-check flush | `command → event → applier`; events are the source of truth |
| Persistence | Relational DB via MyBatis (ORM, schema, migrations) | Append-only log + key-value store; **replay to recover** |
| Concurrency | Multi-threaded, guarded by **optimistic locking** + retries | **Single writer** — no locks, no races, no retries |
| Async work | Background `JobExecutor` polling locked job rows | Just the next record to process |

For a *nano*, embeddable engine the Camunda 8 model wins decisively:

1. **It removes the hardest parts.** Single-writer sequential processing makes
   optimistic locking, transaction managers and a job-acquisition protocol
   unnecessary — an entire class of concurrency bugs cannot occur.
2. **`command → event → applier` is trivially testable and deterministic.** Feed
   commands, assert the event stream; replay the events to reconstruct state
   (see the `should_be_deterministic_and_replayable` test).
3. **Persistence is optional and pluggable.** The engine holds state in memory
   and emits a replayable event log; back it with a WAL, `redb`, `sled` or
   SQLite — or nothing at all.
4. **It matches the rest of nanobpmn.** The generated REST layer *is* the
   Camunda 8 v2 API, so an engine speaking C8 semantics wires straight behind it.

Camunda 7 remains a useful **per-element semantic reference** (its BPMN behaviour
catalogue is mature and readable); it is the *execution architecture* we don't
copy.

## Why it runs on phones (and in the browser)

`engine-core` has **zero dependencies and uses only `std`**. The same crate
compiles for:

- native servers (`x86_64` / `aarch64`),
- **iOS** (`aarch64-apple-ios`) and **Android** (`aarch64-linux-android`),
  embedded in-process and called from Swift/Kotlin — generate the bindings with
  [UniFFI](https://mozilla.github.io/uniffi-rs/),
- **`wasm32`** (verified: `make engine-wasm`), for a mobile web view or a
  JS/React Native layer.

This is only possible because the engine is a single owned state machine with no
threads, no locks, no networking and no database. On a phone you embed
`engine-core` directly; you do **not** run the HTTP `server/` crate there. Keep
FFI surfaces coarse (submit a command, drain the events) rather than chatty, and
build with `opt-level = "z"` + LTO to keep the binary small.

## The execution model

Every BPMN element instance walks the same lifecycle, mirroring Zeebe:

```text
ACTIVATING -> ACTIVATED -> COMPLETING -> COMPLETED --(take outgoing flow)--> ACTIVATING(next)
```

- **Pass-through elements** (start/end events) traverse it in one burst.
- A **service task** rests in `ACTIVATED` after creating a job, and only advances
  to `COMPLETING` when a `CompleteJob` command arrives. This is how asynchronous
  work is modelled with no background thread.
- A process **instance completes** when its last token is consumed (its set of
  active element instances becomes empty).

### Pieces

| File | Responsibility |
| --- | --- |
| `model.rs` | `ProcessDefinition` / `Element` / `ElementKind` and the `ProcessBuilder`. |
| `command.rs` | `Command` — the only way to drive the engine. |
| `event.rs` | `Event` — immutable facts; a replayable log. |
| `state.rs` | `State` and `apply()` — the **sole** mutator of state. |
| `engine.rs` | `Engine::apply_command` — the single-writer loop and the processor. |

> **Scope.** This is a POC: the model supports start events, service tasks and
> end events with sequence flows. Gateways, intermediate events, timers,
> sub-processes and BPMN 2.0 XML parsing are intended extension points — new
> element kinds plug into `process_step` without touching the architecture.

## Usage

```rust
use nanobpmn_engine_core::{Command, Engine, ProcessBuilder};

let mut engine = Engine::new();

let process = ProcessBuilder::new("order")
    .start_event("start")
    .service_task("charge", "payment")
    .end_event("end")
    .connect("start", "charge")
    .connect("charge", "end")
    .build()
    .unwrap();

engine.apply_command(Command::DeployProcess(process)).unwrap();
let events = engine
    .apply_command(Command::CreateInstance { process_id: "order".into() })
    .unwrap();

let instance_key = events.iter().find_map(|e| e.instance_key()).unwrap();
assert!(!engine.is_completed(instance_key)); // parked on the service task

let job_key = engine.pending_jobs()[0].key;
engine.apply_command(Command::CompleteJob { job_key }).unwrap();
assert!(engine.is_completed(instance_key)); // token resumed, instance done
```

## Build & test

```bash
cargo test                              # unit + integration + doctests
cargo build --target wasm32-unknown-unknown
```
