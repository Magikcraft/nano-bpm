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
   SQLite — or nothing at all. `Engine::replay(events)` reconstructs state (and
   the key generator) from a recorded log; enable the off-by-default `serde`
   feature to (de)serialize events for an on-disk journal. The server ships one
   (set `NANOBPMN_JOURNAL`); see the repo `README.md`.
4. **It matches the rest of nanobpmn.** The generated REST layer *is* the
   Camunda 8 v2 API, so an engine speaking C8 semantics wires straight behind it.

Camunda 7 remains a useful **per-element semantic reference** (its BPMN behaviour
catalogue is mature and readable); it is the *execution architecture* we don't
copy.

## Why it runs on phones (and in the browser)

`engine-core` has **zero dependencies and uses only `std`** by default (the
optional `serde` feature adds `serde` only when a host opts into event
(de)serialization). The same crate compiles for:

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
- **Job activation** mirrors Camunda 8: a worker activates available jobs of a
  type (`ActivateJobs`), locking each until `now + timeout`. A job must be
  activated before it can be completed. Locks expire — either lazily on the next
  activation or via an explicit `ExpireJobs` tick — making the job activatable
  again. Completion is by key alone (no worker check), so a slow worker whose
  lock expired and whose job was re-activated by another worker can still
  complete it; the first completion wins. The engine is **clock-free**: the
  caller supplies `now` (a logical instant) on `ActivateJobs`/`ExpireJobs`.
- **Job failure** (`FailJob`) sets a job's remaining retries. With retries left
  the job returns to the activatable pool; with none it parks (`JobState::Failed`)
  and an **incident** is raised on the instance — the same incident mechanism an
  exclusive gateway uses when no flow matches. Jobs start with a default retry
  count and, like completion, failing requires prior activation.
- **Business errors** (`ThrowJobError`) let a worker raise a named error from a
  job. If the job's service task has an **error boundary event** with a matching
  `errorCode`, the task is interrupted and the boundary's outgoing flow runs the
  error-handling path; an unmatched error raises an incident instead. The job is
  consumed either way (`JobState::Errored`) and, like the other transitions,
  throwing requires prior activation.
- **Incidents** are first-class records with a key (`State::incidents`) and a
  `created_at` timestamp, raised when a token cannot proceed: a job exhausted its
  retries, an exclusive gateway matched no flow, or a thrown error went uncaught
  (`IncidentKind`). A job-incident links back to its job. Recovery mirrors
  Camunda: `ResolveIncident` **retries the failed work** rather than merely
  clearing the record. A job-incident returns the parked job to the activatable
  pool (after `UpdateJobRetries` restores its retries; resolution is rejected
  while it still has none). A gateway incident re-evaluates the gateway against
  the current variables. An uncaught-error incident re-creates the service-task
  job. If the retry fails again, a fresh incident is raised by the same code
  paths that raised the original.
- **Incident lifecycle** is auditable: incidents carry an `IncidentState`
  (`Active`/`Resolved`). Resolving transitions the record to `Resolved` —
  stamping `resolved_at` (from the host clock) and any `operation_reference` —
  and **retains it** rather than deleting it, so the read APIs expose a full
  history. Only `Active` incidents can be resolved (re-resolving a `Resolved`
  one is rejected). `Engine::incidents()` returns the whole history;
  `Engine::active_incidents()` filters to open ones, and the per-instance active
  index (`ProcessInstance::incidents`) drives `hasIncident`.
- **Timer intermediate catch events** park a token mid-flow until a deadline.
  Reaching one arms a `Timer` (`due_at = now + duration`) and rests in
  `ACTIVATED`; a host-driven `TriggerTimers { now }` tick fires every due timer,
  releasing its token along the event's outgoing flow. Like job-lock expiry the
  engine stays **clock-free** — the host supplies `now` on the tick. Timers are
  durable (`TimerCreated`/`TimerTriggered` are journaled), so a parked timer
  survives a restart and fires on the next due tick; a fired timer is retained so
  it never re-fires.
- **Interrupting timer boundary events** attach a deadline to a service task. When
  the task activates, its boundary timer is armed (`due_at = now + duration`); if
  the timer fires before the job is done, the engine **cancels the job**
  (`JobState::Canceled`, terminal — no longer activatable/completable), interrupts
  the activity, and routes the token out the boundary's outgoing flow. If the job
  instead completes (or the task is interrupted another way, e.g. an error
  boundary) first, the armed boundary timer is **disarmed** (`TimerState::Canceled`)
  so it never fires. `JobCanceled`/`TimerCanceled` are journaled, so both outcomes
  survive a restart.
- **Message intermediate catch events** park a token mid-flow until a matching
  message is correlated. Reaching one opens a `MessageSubscription` keyed by the
  message name and a **correlation value** — the stringified value of the named
  instance variable captured at open time. A host-driven `CorrelateMessage
  { message_name, correlation_key, variables }` (the engine's `correlate_message`
  helper, or the REST `publishMessage`/`correlateMessage` endpoints) releases the
  token of every matching open subscription along the event's outgoing flow,
  merging the message's variables into the instance first. Like timers the engine
  stays **clock-free** and the message flow is durable
  (`MessagePublished`/`MessageSubscriptionCreated`/`MessageCorrelated` are
  journaled), so a parked subscription survives a restart and a settled one is
  retained so it never re-correlates. Messages are **not buffered** — with no
  matching open subscription the message is simply dropped (no TTL/dedup).
- **Interrupting message boundary events** attach a subscription to a service
  task. When the task activates a subscription is opened; if a matching message is
  correlated before the job is done, the engine **cancels the job**, interrupts
  the activity, and routes the token out the boundary's outgoing flow. If the job
  instead completes (or the task is interrupted another way) first, the open
  subscription is **cancelled** (`MessageSubscriptionState::Canceled`) so it never
  correlates. Both outcomes are journaled and survive a restart.
- **Variables** can be merged into a scope with `SetVariables` (the scope key may
  be a process instance or any active element instance — nano keeps a single
  instance-level scope). Typically used to correct the data behind a gateway
  incident before resolving it, so the re-evaluation on resolution succeeds.
- The engine reads **no wall clock**: the host supplies `now` to
  `apply_command_at`, and timestamped events (e.g. a raised incident) carry it,
  so replay reconstructs identical timestamps.
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

> **Scope.** This is a POC. The model supports start/end events, service tasks,
> **error boundary events** (a worker `throwError` caught by a matching boundary,
> interrupting the task and routing to its error-handling path),
> **interrupting timer boundary events** (a deadline on a service task that, when
> it fires first, cancels the job and routes the token out the boundary),
> **exclusive (XOR) gateways** (condition-based routing with a default flow,
> raising an incident when nothing matches), **parallel (AND) gateways**
> (split takes all branches; join synchronises them), **timer intermediate
> catch events** (a token parks until its `timeDuration` elapses, fired by a
> host clock tick) and **message events** — **message intermediate catch events**
> (a token parks until a matching message is correlated) and **interrupting
> message boundary events** (a message that, correlated first, cancels the job and
> routes the token out the boundary). Instances carry simple
> variables (`Bool`/`Int`/`Str`) used by gateway conditions and message
> correlation. Processes can be
> built programmatically with [`ProcessBuilder`] or parsed from BPMN 2.0 XML for
> that same subset (including `boundaryEvent`/`errorEventDefinition`,
> `intermediateCatchEvent`/`timerEventDefinition` and
> `messageEventDefinition`/`zeebe:subscription`) via the
> [`bpmn`] module (`bpmn::parse_bpmn`), a tiny dependency-free scanner.
> Deployments assign a per-id **version** and a unique process-definition key.
> Sub-processes and non-interrupting events are intended extension points —
> new element kinds plug into `process_step` without touching the architecture.

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

// A worker activates the job (locking it for 30s) before completing it.
let jobs = engine.activate_jobs("payment", "worker-1", 10, 30_000, 0);
engine.apply_command(Command::complete_job(jobs[0].key)).unwrap();
assert!(engine.is_completed(instance_key)); // token resumed, instance done
```

## Build & test

```bash
cargo test                              # unit + integration + doctests
cargo build --target wasm32-unknown-unknown
```
