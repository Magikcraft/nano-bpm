# Camunda compatibility — the supported subset

Status: **audit only.** This is an honest snapshot of what Nano executes and
serves today versus Camunda 8 / Zeebe, so that a Camunda user can answer the one
question that decides everything: *"will my app run against this?"* The answer is
"yes, for a well-defined subset — and here is exactly where the edges are."

Audit taken against Nano **v0.0.11** (`server/Cargo.toml`). The REST surface is
generated from the Camunda 8 **Orchestration Cluster API v2** (`spec/rest-api.yaml`,
`info.version: "0.1"`, base path `/v2`). A running node also serves the exact spec
it implements from its **offline Swagger UI at `/swagger`**
(`server/src/console/mod.rs`); that UI is the live source of truth for one binary,
and this document is the narrative around it.

A compatibility document that overclaims is worse than none: it turns a solvable
"not supported yet" into a broken promise discovered at 2am. So every row below is
traceable to code, a spec file, or an ADR — and anything not verified from the
source is marked **unverified** rather than guessed.

## Status vocabulary

| Status | Meaning |
|---|---|
| **executed** | The engine has real runtime semantics; a token advances through it. |
| **served** | (REST) The operation is wired to the embedded engine and returns real results. |
| **partial** | Supported with a documented deviation or a subset of sub-behaviours. |
| **parsed-not-executed** | The XML parser accepts the element, but the runtime ignores it or treats it as a no-op pass-through. **This is the distinction that bites hardest** — the model deploys clean and then silently does not do what it says. |
| **stubbed (501)** | (REST) The route exists but returns `501 Not Implemented`. |
| **unsupported** | Not handled: no parser arm, no runtime dispatch. |
| **planned** | A committed compatibility target that is not built yet (linked to an ADR/issue). |
| **unverified** | Could not be confirmed from the source at audit time. |

---

## 1. BPMN element coverage

Grounded in `engine-core` (the parser is `engine-core/src/bpmn.rs`; execution is
`engine-core/src/engine/`). The parser's element arms are the enumerable set of
what Nano *recognizes*; everything not listed there is dropped on parse.

### Tasks

| Element | Status | Evidence |
|---|---|---|
| `serviceTask` | **executed** | `bpmn.rs:269`; job created at `engine/mod.rs:3228` |
| `businessRuleTask` (DMN) | **executed** | `bpmn.rs:289`; in-engine DMN eval at `engine/mod.rs:4695` (via `zeebe:calledDecision`, `bpmn.rs:301`) |
| `scriptTask` | **executed** | `bpmn.rs:289`; inline FEEL, no job, at `engine/mod.rs:4650` |
| `userTask` | **executed** | `bpmn.rs:310`; user-task record + listeners at `engine/mod.rs:3273` |
| `receiveTask` | **parsed-not-executed** | Parsed as an inert throw/pass-through, `bpmn.rs:540`. Use a message intermediate **catch** event to wait for a message instead. |
| `manualTask` | **unsupported** | No parser arm in `bpmn.rs`. |
| `sendTask` | **unsupported** | No parser arm in `bpmn.rs`. |

### Gateways

| Element | Status | Evidence |
|---|---|---|
| `exclusiveGateway` | **executed** | `bpmn.rs:255`; routing at `engine/mod.rs:5000`. `default` flow honoured (`bpmn.rs:259`). |
| `parallelGateway` | **executed** | `bpmn.rs:263`; fork/join at `engine/mod.rs:3066`. |
| `eventBasedGateway` | **executed** | `bpmn.rs:266`; deferred choice at `engine/mod.rs:5047`. |
| `inclusiveGateway` | **unsupported** | No parser arm in `bpmn.rs`. |
| `complexGateway` | **unsupported** | No parser arm in `bpmn.rs`. |

### Events

| Element | Status | Evidence |
|---|---|---|
| `startEvent` (none / message / timer) | **executed** | `bpmn.rs:246`; `engine/mod.rs:610` |
| `endEvent` | **executed** | `bpmn.rs:252`; `engine/mod.rs:5019` |
| `intermediateCatchEvent` (timer / message / signal / conditional) | **executed** | `bpmn.rs:527`; `engine/mod.rs:3386` |
| `intermediateThrowEvent` | **parsed-not-executed** | Inert pass-through, `bpmn.rs:533`. A **message-throw** event does **not** publish a message; publish via the REST `publishMessage`/`correlateMessage` API or a service-task worker instead. |
| `boundaryEvent` — error | **executed** | `bpmn.rs:412`; `engine/boundary.rs:6` |
| `boundaryEvent` — timer | **executed** (interrupting + non-interrupting) | `bpmn.rs:388`; `engine/boundary.rs:462` |
| `boundaryEvent` — message | **executed** (interrupting + non-interrupting) | `engine/boundary.rs:521` |
| `boundaryEvent` — signal | **executed** (interrupting + non-interrupting) | `engine/boundary.rs:588` |
| `boundaryEvent` — conditional | **executed** (interrupting + non-interrupting) | `engine/boundary.rs:616` |
| `terminateEndEvent` | **executed** | An `endEvent` carrying a `terminateEventDefinition` kills every other active token in its enclosing scope (parallel-split siblings, pending timers, open jobs/subscriptions) and completes that scope. A top-level terminate end terminates the whole instance; a sub-process-scoped one ends only that sub-process scope and the parent continues on the sub-process's outgoing flow. `bpmn.rs` (`terminateEventDefinition` arm); `model.rs` (`ElementKind::TerminateEndEvent`); `engine/mod.rs` (`complete_terminate_end`). |
| `escalation` events | **unsupported** | No parser arm / dispatch. |
| `compensation` events | **unsupported** | No parser arm / dispatch. |

Event definitions that are wired: `timerEventDefinition` (`timeDuration` /
`timeCycle` / `timeDate`, `bpmn.rs:547`), `messageEventDefinition` (`bpmn.rs:418`),
`signalEventDefinition` (`bpmn.rs:432`), `conditionalEventDefinition`
(`bpmn.rs:567`). `errorEventDefinition` is handled on boundary events.

### Sub-processes and markers

| Element | Status | Evidence |
|---|---|---|
| `subProcess` (embedded) | **executed** | `bpmn.rs:345`; scope + inner start at `engine/mod.rs:3491` |
| `multiInstanceLoopCharacteristics` (sequential + parallel) | **executed** | `bpmn.rs:574`; `model.rs:305`. `completionCondition` honoured (`bpmn.rs:612`). |
| `callActivity` | **parsed-not-executed (gateway)** | Parsed as `NodeKind::Call` (`bpmn.rs:319`) with the callee id captured (`calledElement` / `zeebe:calledElement`), but the gateway server's deploy path does **not** inline-expand or natively execute it. Inline expansion (`ProcessDefinition::inline_call_activities`, `model.rs:988`) exists **only** in the `processos` replay harness, not the live engine. A token reaching a call activity in a deployed model does not start the callee. |
| `adHocSubProcess` | **partial** | The container is a runtime-managed, job-bearing activity (`bpmn.rs:361`; `engine/mod.rs:1020`), but inner elements are catalogued/pruned rather than executed by token flow. This is the ADR 0022 §E.1 / ADR 0023 execution-parity gap; it collapses today to a single job rather than the full Camunda ad-hoc activation model. See [ADR 0023](adr/0023-adhoc-subprocess-execution-parity.md). |
| `eventSubProcess` | **parsed-not-executed** | Parsed as an embedded `subProcess`, but the `triggeredByEvent` event-sub-process semantics (event-triggered start, interrupting/non-interrupting scope handling) are not special-cased in `bpmn.rs` / `engine/mod.rs`. |
| `transaction` sub-process | **unsupported** | No parser arm in `bpmn.rs`. |

### Flow

| Element | Status | Evidence |
|---|---|---|
| `sequenceFlow` (with FEEL condition + default) | **executed** | `bpmn.rs:521`; `model.rs:139`; `engine/mod.rs:5032` |

---

## 2. DMN / FEEL

Nano ships a **real recursive-descent FEEL engine** (`engine-core/src/feel/`)
driving a DMN decision-table evaluator (`engine-core/src/dmn/eval.rs`) — not a
string approximation. Coverage is near-complete for the constructs real DMN tables
and literal expressions use (~110 builtins, including the full Allen interval
algebra). Business-rule tasks bound via `zeebe:calledDecision` evaluate **in-engine,
no job** (`bpmn.rs:301`, `engine/mod.rs:4695`).

The full construct-by-construct breakdown is not duplicated here — see the
dedicated **[DMN / FEEL parity audit](dmn-feel-parity-audit.md)**, which lists every
supported unary test, expression form, and builtin with citations, and the small
remaining gaps.

When a native DMN engine milestone is signed, it is dedicated to Sebastian Menski
(creator of Camunda's `engine-dmn`), per the engineer-signature naming discipline
(`bpmn.rs:286`).

---

## 3. Agentic / AI-Agent elements

Camunda is moving the AI Agent **into the engine** via the `AgentInstance` record
family (an engine-hosted reasoning loop whose tools are modeled inner elements of an
`adHocSubProcess`).

**Today's answer: engine-native `AgentInstance` is not implemented** — there is no
`AgentInstance` symbol in `engine-core`. A Camunda user who brings a diagram with an
AI-Agent ad-hoc sub-process will find the ad-hoc container handled only at the
**partial** level above (collapsed to a job), and the agentic loop **not** run by the
engine.

This is a deliberate, documented position, not an oversight. [ADR 0046](adr/0046-agent-as-worker-vs-agent-in-the-node.md)
distinguishes two topologies:

- **Agent-as-worker** (Nano-native, differentiated): the agent is an external job
  worker; the engine routes to and awaits it. This is what `c8ctl nano hire`/`work`
  produces today and is Nano's leading model for open-ended, long-running agents.
- **Agent-in-the-node** (Camunda `AgentInstance`): the engine hosts the loop. Nano
  treats this as a **planned compatibility target** — a drop-in obligation — **not**
  the authoring model, and it is **not built yet**.

So: **agent-as-worker = supported today; engine-native `AgentInstance` = planned
(compat), unbuilt.**

---

## 4. REST v2 API surface

The whole Orchestration Cluster v2 surface is **routable**; operations not wired to
the engine return `501 Not Implemented` through a shared handler
(`server/src/main.rs:14168`). The authoritative, **checked-in** registry of served
operations is the `OVERRIDES` map in the stub generator
(`scripts/gen-stub-server.py:35-74`): at build time it generates
`server/src/stub_impls.rs` (git-ignored), where each REST trait method either
delegates to a real `*_impl` handler (served) or returns `Err(())` → 501. The
served set below is the `OVERRIDES` map; the cited line numbers are the checked-in
`*_impl` handlers in `server/src/main.rs`.

### Served (wired to `engine-core`)

| Group | Operations served | Notes |
|---|---|---|
| **Deployment** | `createDeployment` (deploy BPMN/DMN resources) | Idempotent redeploy (byte-identical → version reuse), mirroring Zeebe. `main.rs:8290` (`OVERRIDES` module `resource`). |
| **Process instances** | `createProcessInstance` (incl. `awaitCompletion` create-with-result), `cancelProcessInstance`, `getProcessInstance`, `searchProcessInstances` | `main.rs:3622`, `3684` (awaitCompletion + `fetchVariables`). **Deviation:** on `awaitCompletion` timeout Nano returns `200` + `processCompleted:false` (poll), not Camunda's `504`. |
| **Jobs** | `activateJobs` (long-poll), `completeJob`, `failJob`, `throwError`, `updateJob`, `searchJobs` | `main.rs:1111`+. |
| **Messages** | `publishMessage`, `correlateMessage` | `main.rs:1331`, `6687`. |
| **Incidents** | `getIncident`, `resolveIncident`, `searchIncidents` | `main.rs:1045`+. |
| **Decisions** | `evaluateDecision`, get definition + **XML**, get requirements + **XML**, get instance, delete instance, search (definitions / requirements / instances) | `main.rs:6996`+; DMN evaluated in-engine. |
| **Process definitions** | get **XML**, `searchProcessDefinitions` | `main.rs` `get_process_definition_xml_impl` / `search_process_definitions_impl`. |
| **Element instances** | `getElementInstance`, `searchElementInstances` (incl. `$or`), `searchElementInstanceIncidents`, `searchElementInstanceWaitStates`, set/create variables, `getVariable`, `searchVariables` | `main.rs` `get_element_instance_impl` / `search_element_instances_impl` / `search_element_instance_incidents_impl` / `search_element_instance_wait_states_impl`. Element instances are projected into the read model from the engine's per-element lifecycle events (`ElementActivating`/`ElementActivated`/`ElementCompleted`; process-scope `ProcessInstanceTerminated` terminates still-active elements). Wait states are derived from the jobs read model (`JOB`, an activatable job) and a dedicated message-subscription read model (`MESSAGE`, an open message catch). `searchElementInstanceIncidents` returns incidents of the element instance and its scope-subtree descendants (transitive `scope_key` closure). `rootProcessInstanceKey` equals `processInstanceKey`: Nano expands call activities inline at deploy time (`inline_call_activities`) and never spawns a separate child process instance, so every element instance is self-rooted (matching Camunda's `root == self` for a top-level instance). **Deviations:** `startDate`/`endDate` are stamped at projection time (the lifecycle events carry no engine-authored timestamp, so a full read-model rebuild re-dates them); the `type = UNKNOWN` fallback is defensive only (unreachable at runtime — every runtime element id is in the deployed model); `startDate`/`endDate` filters are not yet honoured. |
| **User tasks** | get, search, `assign`, `unassign`, `complete`, `update` | `main.rs:7523`+. |
| **Cluster** | `getTopology` | `get_topology_impl` (`OVERRIDES` module `cluster`). |

### Stubbed (501) or absent

| Group | Operation(s) | Status |
|---|---|---|
| Process instances | `migrate`, `modify`, batch operations | **stubbed (501)** (not in `OVERRIDES`) |
| Signals | `broadcastSignal` | **stubbed (501)** — note the *engine* handles signal catch/boundary events; only the REST **broadcast** endpoint is unwired. |
| Process definitions | get definition (non-XML), start form, all statistics | **stubbed (501)** |
| Jobs | all statistics endpoints | **stubbed (501)** |
| User tasks | search user-task variables | **stubbed (501)** |
| Messages | message-subscription search | **stubbed (501)** |
| Resources | `getResource`, `getResourceContent`, `getResourceContentBinary`, `searchResources`, `deleteResource` (`spec/deployments.yaml`) | **stubbed (501)** — only `createDeployment` in this spec area is served. |
| Authentication | `me` | **stubbed (501)** |
| Identity — Authorizations, Roles, Groups, Tenants, Users | all | **stubbed (501)** (not in `OVERRIDES`) — see §6. |

---

## 5. Operational differences that change behaviour

These are not missing features — they are places where a **supported** operation
behaves differently enough that a client written against Camunda must know.

### `503 RESOURCE_EXHAUSTED` at the ceiling

A client that has never seen a `503` from Camunda **will** meet one here.
`createProcessInstance` sheds with `503 RESOURCE_EXHAUSTED` when the engine
saturates (`server/src/backpressure.rs`, `main.rs:3654`). The SDK reads this as a
backpressure signal and answers with a retry backoff; a hand-rolled client must do
the same. This is intrinsic to Nano's self-optimizing design, not a fault.

### SLA mode — the one behavioural knob

`NANOBPMN_SLA_MODE` chooses what happens at the capacity ceiling
(`server/src/backpressure.rs`, [ADR 0013](adr/0013-sla-modes-and-varstore-wal-bounding.md)):

- **`latency`** (default) — shed admission (`503`) to keep accepted work fast.
- **`admission`** — keep starting instances; latency and backlog grow (bounded only
  by memory-safety rails).

It is switchable at runtime, cluster-wide. [ADR 0021](adr/0021-process-sla-as-a-first-class-abstraction.md)
tracks unifying this with `zeebe:priorityDefinition` job priority into a declared
per-process SLA.

### Durability tiers

Two orthogonal, explicit durability choices ([ADR 0003](adr/0003-write-path-durability-tiers.md)):
`NANOBPMN_DURABILITY=sync|async` (local journal fsync) and
`NANOBPMN_REPLICATION=quorum|leader-durable` (the Kafka `acks=all` vs `acks=1`
analogue for the command log). Default is fully durable (quorum + sync). An
acknowledgement's meaning — and what a node crash costs — is set by these; the
[README "Durability and recovery"](../README.md#durability-and-recovery) and
[USERGUIDE "Tune durability and performance"](../USERGUIDE.md#tune-durability-and-performance)
sections cover the trade in depth.

### Consistency

The search/read endpoints are served from a read model that is **eventually
consistent** with the write path; a create followed immediately by a search may not
yet reflect the new instance. See [README "Read model and consistency"](../README.md#read-model-and-consistency).

---

## 6. What is deliberately absent

Nano is a **headless engine plus a RAD IDE**, not a re-implementation of the whole
Camunda 8 platform. The following are intentionally not present:

- **Operate, Tasklist, Optimize.** There is no drop-in equivalent. Nano's position
  ([ADR 0022](adr/0022-nano-rad-application.md)) is that the *binding* — an App that
  ties triggers, processes, decisions, forms, and data into one artifact — is the
  product, in place of "build a bespoke frontend, or run Operate + Tasklist." The web
  console provides model authoring, an explorer, traces, and a worker view for
  development, not a production Operate/Tasklist substitute.
- **Elasticsearch / an exporter data lake.** Nano keeps a built-in read model; there
  is no Elasticsearch dependency and no Camunda exporter protocol.
- **Identity, authorization, multi-tenancy.** The C8 identity-management endpoints
  (Authorizations, Roles, Groups, Tenants, Users) and `authentication/me` are
  **stubbed (501)** (absent from the `OVERRIDES` map in
  `scripts/gen-stub-server.py`). There is no engine-level tenancy
  model; treat Nano as single-tenant.
- **Zeebe gRPC.** Nano serves the **v2 REST** API (and a Falcon WebSocket command
  stream), not the legacy Zeebe gRPC gateway protocol.

---

## How to read a gap

Where a gap has an issue or ADR, it is linked above. If you hit an element that
deploys but does not behave — that is almost certainly a **parsed-not-executed** row
in §1, and the honest fix is to model around it (e.g. a message **catch** event or a
service-task worker in place of a message **throw**). If you hit a `501`, the
operation is genuinely unwired, not misconfigured.

Found drift between this audit and reality, or between it and `README.md` /
`USERGUIDE.md`? That is exactly the kind of staleness [#388](https://github.com/Magikcraft/nano-bpm/issues/388)
tracks — please file it.
