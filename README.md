# nanobpmn

A self-contained Rust code-generation project for the Camunda 8 Orchestration
Cluster REST API. It bundles a copy of the OpenAPI specification and generates a
Rust REST layer (models + `axum` router + service traits) from it, plus a
runnable stub server.

No backend services are wired into *most* of the REST layer: operations respond
with `501 Not Implemented`. A few operations are now backed by the embedded
**`engine-core`** BPMN engine as a proof of the REST → engine path:

- `POST /v2/deployments` (`createDeployment`) parses the uploaded BPMN 2.0 XML
  resources and deploys them, assigning each process a key and a per-id version.
- `POST /v2/process-instances` (`createProcessInstance`, by `processDefinitionId`)
  starts a real instance and returns its engine-assigned key.
- `POST /v2/jobs/activation` (`activateJobs`) activates available jobs of a type,
  locking each to the worker until `now + timeout`; supports Camunda-style
  long-polling via `requestTimeout` (a waiting request wakes as soon as a job
  becomes available).
- `POST /v2/jobs/{jobKey}/completion` (`completeJob`) completes a job and resumes
  the token, merging any returned variables into the instance (so worker output
  drives downstream gateway routing). A job must have been activated first;
  completing an un-activated job returns `409`. Completion is by key alone (no
  worker check), so a slow worker whose lock expired can still complete the job —
  first completion wins.
- `POST /v2/jobs/{jobKey}/failure` (`failJob`) sets a job's remaining retries.
  With retries left the job returns to the activatable pool; with none an
  incident is raised and the job parks (no longer activatable or completable).
- `POST /v2/jobs/{jobKey}/error` (`throwError`) raises a business error from a
  job. If the job's service task has an error boundary event with a matching
  `errorCode`, the task is interrupted and the boundary's outgoing path runs
  (e.g. a refund/compensation flow); otherwise an incident is raised. The job is
  consumed either way. Returns `404` for an unknown or un-activated job and
  `409` if the job is no longer active.
- `PATCH /v2/jobs/{jobKey}` (`updateJob`) applies the changeset's `retries` to a
  job — used to recover a job parked on a no-retries incident (timeout updates
  are not modelled).
- `POST /v2/incidents/{incidentKey}/resolution` (`resolveIncident`) resolves an
  incident by **retrying the failed work**, not just clearing the record. A
  job-incident returns the parked job (which must have retries again) to the
  activatable pool — recovery loop: `failJob`(0) → `updateJob`(retries) →
  `resolveIncident` → re-activate → `completeJob`. A gateway incident
  re-evaluates the gateway; an uncaught-error incident re-creates the
  service-task job. If the retry fails again, a fresh incident is raised.
  Incidents now carry a real `creationTime` (the server feeds the engine its
  clock at command time). Resolving accepts an optional `operationReference`
  (recorded on the retained record for audit) and returns `409` if the incident
  is already resolved.
- `PUT /v2/element-instances/{elementInstanceKey}/variables`
  (`createElementInstanceVariables`) merges variables into a scope (the key may
  be a process instance or an active element instance). Use it to correct the
  data behind a gateway/condition incident, then `resolveIncident` re-evaluates
  and the token proceeds. `local` is accepted but has no effect (single scope).

Read endpoints make engine state observable:

- `GET /v2/process-instances/{processInstanceKey}` (`getProcessInstance`) —
  reports state (`ACTIVE`/`COMPLETED`) and `hasIncident`.
- `GET /v2/incidents/{incidentKey}` (`getIncident`) and
  `POST /v2/incidents/search` (`searchIncidents`) — expose incidents (active
  **and** resolved, since resolved records are retained as an audit trail; each
  reports its `state`, `ACTIVE` or `RESOLVED`), including the `incidentKey`
  needed to resolve them. `errorType` reflects the cause (`JOB_NO_RETRIES`,
  `CONDITION_ERROR`, `UNHANDLED_ERROR_EVENT`).
- `POST /v2/process-instances/search` (`searchProcessInstances`) and
  `POST /v2/jobs/search` (`searchJobs`) round out the read surface.

All three search endpoints implement the full v2 query contract:

- **Filters** use the advanced operator algebra — `$eq`, `$neq`, `$exists`,
  `$in`, `$notIn`, and `$like` (with `*`/`?` wildcards) — in addition to plain
  scalar equality. Examples: `state: {$in: ["FAILED", "ERROR_THROWN"]}`,
  `type: {$like: "pay*"}`, `processInstanceKey: {$exists: true}`.
- **Sort** accepts multiple `{field, order}` clauses applied in order, with the
  entity key as a deterministic final tiebreak.
- **Pagination** supports all four request shapes: `{limit}`, offset
  `{from, limit}`, forward cursor `{after, limit}`, and backward cursor
  `{before, limit}`. Responses carry `totalItems` plus `startCursor`/`endCursor`
  (opaque, padding-free base64 of the entity key) for stable cursor walks.

Timestamps are reported as the Unix epoch since the engine is clock-free.

A demo process (`processDefinitionId: "demo"`, a single service task) is
pre-deployed at server startup, but you can also deploy your own `.bpmn` files
through the deployment endpoint. Engine and parse errors map to real status
codes (`400`/`404`/`409`); everything else is still `501`.

### Durability (event-log replay)

The engine is in-memory but event-sourced: every command returns the complete,
ordered list of events it produced, and replaying those events over a fresh
state reconstructs it exactly. The server turns that into crash durability with
an **append-only journal**. Set `NANOBPMN_JOURNAL` to a file path and every
durable command's events are appended (newline-delimited JSON) and flushed
before the response returns; on startup the log is replayed through
`Engine::replay`, which also advances the key generator past every key the log
assigned so post-recovery commands never collide with replayed ones. Without the
env var the server runs purely in memory (ephemeral).

```bash
NANOBPMN_JOURNAL=./nanobpmn.journal PORT=8099 cargo run
```

Job **activation locks are intentionally not journaled** — they are volatile
lease state. A restart forfeits every lock, returning uncompleted jobs to the
activatable pool, so a worker simply re-activates after recovery. Everything
durable (deployments, instances, element progress, jobs, incidents, variables,
completion, **armed timers**) survives. The `serde` (de)serialization lives
behind an off-by-default `serde` feature on `engine-core`, so the engine stays
dependency-free for mobile/wasm embedders that don't need persistence.

### Background tick (timers and lock expiry)

The engine reads no wall clock; the host drives time in. The server runs a
single background task (every 500 ms) that feeds `now` into the engine via two
ticks: `TriggerTimers` fires every due **timer intermediate catch event** and
**interrupting timer boundary event** (durable — journaled), and `ExpireJobs`
releases activation locks past their deadline (volatile — not journaled). When a timer fires it may unblock
downstream work, so the tick wakes any long-polling `activateJobs`. A timer
parked before a restart is recovered by replay and fired by the first due tick
afterwards.

## Two crates

nanobpmn is deliberately split so the execution engine stays embeddable
(including on mobile via FFI and in the browser via wasm) while the REST layer
remains a server-only concern:

| Crate | What it is | Runs where |
| --- | --- | --- |
| **`engine-core/`** | The BPMN engine: a deterministic single-writer `command → event → applier` state machine. **Zero dependencies, `std`-only.** | Server, iOS/Android (FFI, e.g. UniFFI), `wasm32` |
| **`server/`** + `generated/` | The Camunda 8 v2 REST API generated from `spec/`, with a stub server. | Server only |

You would **not** run the HTTP server on a phone; there you embed `engine-core`
directly and call it through generated bindings. See
[`engine-core/README.md`](engine-core/README.md) for the architecture and the
rationale for following the Camunda 8 (Zeebe) model rather than the Camunda 7 PVM.

## Approach

The REST layer is generated with [OpenAPI Generator](https://openapi-generator.tech)
using its [`rust-axum`](https://openapi-generator.tech/docs/generators/rust-axum)
server generator (run via Docker, version-pinned). It produces a self-contained
library crate (`camunda-gateway-rest`) with:

- **`src/models.rs`** — serde structs for every schema in the spec.
- **`src/apis/`** — one trait per API tag, with one async method per operation.
- **`src/server/mod.rs`** — an `axum` router (`server::new(api_impl)`) that
  extracts requests, dispatches to the trait implementation, and serializes
  responses.

## Layout

```
nanobpmn/
├── Makefile                       # generate / build / run / fmt / clippy / clean
├── openapi-generator-config.yaml  # generator configuration
├── spec/                          # bundled OpenAPI spec (source of truth)
│   ├── rest-api.yaml              # entrypoint ($refs the sibling files)
│   └── *.yaml
├── scripts/
│   ├── generate.sh                # end-to-end generation pipeline
│   ├── preprocess-spec.py         # sanitizes a temp copy of the spec
│   ├── postprocess-generated.py   # patches known rust-axum generator bugs
│   └── gen-stub-server.py         # generates the server's stub trait impls
├── server/                        # runnable stub server (binary crate)
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs                # ServerImpl, auth/error glue, bootstrap
│       └── stub_impls.rs          # generated trait impls (git-ignored)
├── build/                         # temp sanitized spec (git-ignored)
└── generated/                     # generated library crate (git-ignored)
```

The engine-core crate sits alongside these:

```
nanobpmn/
└── engine-core/                   # embeddable BPMN engine (zero-dep, std-only)
    ├── Cargo.toml
    ├── src/
    │   ├── lib.rs                 # crate docs + public API
    │   ├── model.rs               # ProcessDefinition / Element + ProcessBuilder
    │   ├── command.rs             # Command enum (engine inputs)
    │   ├── event.rs               # Event enum (engine facts)
    │   ├── state.rs               # State + apply() — the sole mutator
    │   └── engine.rs              # single-writer loop + processor
    └── tests/public_api.rs
```

The `generated/` crate, `build/`, and `server/src/stub_impls.rs` are **build
artifacts** and are git-ignored. Regenerate them on demand with `make generate`.
The `spec/` tree is the committed source of truth.

## Requirements

- Docker (to run the pinned `openapi-generator-cli` image)
- A Rust toolchain (`cargo`, `rustfmt`)
- Python 3 with PyYAML (for the spec pre-processing step)

## Usage

```bash
# Generate the Rust REST layer + server stubs from spec/
make generate

# Generate if needed, then compile both crates
make build

# Run the stub server (defaults to port 8080; override with PORT)
make run
PORT=18080 make run

# Lint / format
make clippy
make fmt

# Remove all generated artifacts
make clean
```

## Engine (`engine-core`)

The embeddable BPMN engine builds and tests with plain `cargo` — no Docker, no
code generation:

```bash
make engine-test    # unit + integration + doc tests
make engine-build   # debug build
make engine-wasm    # prove it compiles for wasm32 (needs the wasm32 target)
```

See [`engine-core/README.md`](engine-core/README.md) for the architecture.

## Stub server

The `server/` crate wires the generated REST layer into a runnable `axum` server.
Most operations return `Err(())`, which the server's `ErrorHandler` maps to a
`501 Not Implemented` response, so the whole API surface is routable end to end.
A few operations are backed by the embedded `engine-core` engine (see above):

```console
$ PORT=18080 make run
... listening on http://0.0.0.0:18080/v2

# Engine-backed: deploy a BPMN file -> process is parsed and versioned
$ curl -s -X POST localhost:18080/v2/deployments \
    -H 'Authorization: Bearer x' -F 'resources=@order.bpmn'
{"deploymentKey":"3","tenantId":"<default>","deployments":[{"processDefinition":
  {"processDefinitionId":"shipping","processDefinitionVersion":1,
   "resourceName":"order.bpmn","processDefinitionKey":"4",...},...}]}

# Engine-backed: start an instance of the just-deployed process
$ curl -s -X POST localhost:18080/v2/process-instances \
    -H 'Authorization: Bearer x' -H 'Content-Type: application/json' \
    -d '{"processDefinitionId":"shipping"}'
{"processDefinitionId":"shipping",...,"processInstanceKey":"5",...}

# Engine-backed: activate the job parked on the service task -> locked to "w1"
$ curl -s -X POST localhost:18080/v2/jobs/activation \
    -H 'Authorization: Bearer x' -H 'Content-Type: application/json' \
    -d '{"type":"ship","worker":"w1","timeout":60000,"maxJobsToActivate":10}'
{"jobs":[{"type":"ship",...,"jobKey":"8","deadline":...,...}]}

# Engine-backed: complete the activated job -> 204, token resumes, instance ends
$ curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:18080/v2/jobs/8/completion \
    -H 'Authorization: Bearer x' -H 'Content-Type: application/json' -d '{}'
204

# Still a stub:
$ curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:18080/v2/process-instances/search \
    -H 'Authorization: Bearer x' -H 'Content-Type: application/json' -d '{}'
501
```

The `ServerImpl` type (which owns the embedded engine), authentication, error
glue, and the engine-backed handlers live in `server/src/main.rs` (committed).
The per-tag trait impls are generated into `server/src/stub_impls.rs` by
`gen-stub-server.py`, which routes the wired operations to the handlers via its
`OVERRIDES` table and stubs everything else.

## Generation pipeline

`scripts/generate.sh` runs four stages:

1. **Preprocess** (`preprocess-spec.py`) — the bundled spec in `spec/` is never
   edited. It is copied into `build/spec/`, where a few constructs that the beta
   `rust-axum` generator cannot handle are sanitized (currently: schema-less
   request bodies such as `content: { application/json: {} }`). Files that need
   no change are copied verbatim to keep the transform minimal.
2. **Generate** — `openapi-generator-cli` (Docker, version-pinned) emits the
   crate into `generated/`.
3. **Post-process** (`postprocess-generated.py`) — deterministically patches
   known `rust-axum` code-generation bugs so the crate compiles and behaves
   correctly (an invalid `oneOf` date-time enum variant, discriminator helpers
   for optional `type` fields, and `#[serde(deny_unknown_fields)]` on the four
   pagination structs so the untagged `SearchQueryPageRequest` can disambiguate
   limit/offset/forward-cursor/backward-cursor requests instead of always
   collapsing to limit pagination).
4. **Stub impls** (`gen-stub-server.py`) — parses the generated trait
   definitions and emits `server/src/stub_impls.rs`.

## Updating the spec

`spec/` is a copy of the Camunda v2 OpenAPI spec
(`zeebe/gateway-protocol/src/main/proto/v2` in `camunda/camunda`). To refresh it,
replace the files under `spec/` and run `make generate`.
