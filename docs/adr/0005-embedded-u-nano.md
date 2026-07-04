# ADR 0005 — Bernd (the embedded Nano engine)

Status: **Revised — 2026-07-05.** Original: Proposed 2026-06-28, Deno-only.
This revision generalizes the embedded engine to a multi-language artifact (Deno, Java/GraalVM, and any future host that can load a wasm cdylib) and establishes the transactional-semantics + developer-experience positions that were left open in Rev 1.
Relates to: `docs/sdk-nano-decorator-design.md` (the runtime decorator), `docs/falcon-design.md`,
`engine-core/src/ffi.rs` (C-ABI surface), `engine-wasm/src/lib.rs` (μ-nano / `TestEngine`),
`clients/node-stream/` (`@nanobpmn/sdk` streaming layer), `server/src/console/worker_sdk.ts`
(Deno `defineWorker`), `processos/src/supervisor.rs` (spawn-an-engine model), `dist/engine-wasm-ffi/`
(the universal wasm artifact published from `make engine-wasm-ffi-dist`),
jwulf/nano-bpm#10 (Maven + npm packaging), jwulf/camunda-client-java-falcon#1 (`NanoTransport.embedded()`).

## Signature — why "Bernd"

The embedded engine feature is named **Bernd**, after Bernd Ruecker (co-founder of Camunda, long-time evangelist of the Saga / compensation pattern, and the person whose talks and books established the intellectual framing that "you don't need shared transactions; you need workflows that reason about failure honestly").

Nano's embedded engine sits directly on that inheritance. It could have offered C7-style shared-transaction semantics — many developers would have asked for it — but chose not to, because the pattern Bernd taught turns out to work at every scale, and building the illusion again would have been an act of forgetting. So we sign the work.

Naming conventions that follow:

- Java runtime library: `io.github.jwulf:nano-bernd` (Maven Central)
- Java runtime class: `Bernd` (`Bernd.builder().clock(...).build()`)
- npm cross-runtime package: `@nanobpm/nano-bernd` (a thin ergonomic wrapper) and `@nanobpm/nano-engine-ffi` (the raw wasm + host, unopinionated)
- Docs / marketing surface: "Bernd — Nano's embedded engine"
- ADR references: `Bernd` (capitalized proper noun) for the feature, `μ-nano` for the wasm blob when the engine-vs-wrapper distinction matters
- Not: `bernd()` (lowercase in the transport API) — the transport is `NanoTransport.embedded(Bernd)` because *the feature is Bernd, the transport is embedded*. Bernd is a runtime that the SDK's embedded transport binds to.

## Context

We are unifying the modeler and worker IDE into a single browser-based RAD environment
oriented around **Nano applications**: a user authors models, DMN, forms and worker/application
code, tests them, deploys, and can have Deno cross-compile the application into a native binary.

The end-state we want to enable is a **single application source** that can be:

1. **Lift-and-shifted on disk** and run against a stock **Camunda 8** engine;
2. The same source run against a **remote Nano** engine (auto-upgrading to the Falcon protocol);
3. Cross-compiled into a **self-contained native binary** that runs against Camunda or remote Nano;
4. With one button — **"Embed Bernd"** — cross-compiled into a self-contained native binary with
   **Bernd embedded**: a single-node engine used *only by the application itself*, needing no
   external Nano server.

Since Rev 1 the *host* set has broadened: Bernd is not Deno-only. The **same** wasm cdylib (`engine-core --features ffi`, packaged as `dist/engine-wasm-ffi/nano_engine.wasm` by `make engine-wasm-ffi-dist`) is loaded by:

- **Deno / Node / browser** — via `@nanobpm/nano-engine-ffi` (plain `WebAssembly.instantiate`, no wasm-bindgen).
- **JVM / GraalVM Native Image** — via `io.github.jwulf:nano-bernd` (Chicory, pure Java, AOT-compiles into a static native binary with the wasm baked in).
- **Future**: Python (wasmtime-py), Go (wazero), .NET (Wasmtime.NET) — same wasm, same C-ABI.

The wasm carries **zero imports** and a small `nbpmn_*` C-ABI (see `engine-core/src/ffi.rs`), so every host implements the same coarse surface without host-language-specific glue.

The driving question this ADR answers:

> **Can we deliver all four modes across all these host languages without maintaining a separate SDK per host, and without silently changing semantics between remote and embedded — letting users write against the SDK surface and choose the deployment mode by configuration?**

### What already exists (the load-bearing facts)

- **`engine-core`** is `std`-only, zero-dependency, single-writer, event-sourced and *replayable*,
  and explicitly compiles unchanged for servers, mobile (FFI) and `wasm32`. Commands take an
  injected clock (`apply_command_at(cmd, now)`) — the engine never reads a wall clock. Persistence
  is a *caller* concern ("append the event log anywhere … and replay to recover")
  (`engine-core/src/lib.rs`).
- **`engine-core/src/ffi.rs`** already exposes a coarse C-ABI (`ffi` feature): `engine_new`,
  `deploy_bpmn`, `create_instance`, `correlate_message`, `trigger_timers`, `is_completed`, … —
  the seed of an embeddable runtime, not just a simulator.
- **`engine-wasm` (μ-nano)** is a `wasm-bindgen` wrapper (~568 KB release) exposing `TestEngine`.
  Today it is a **modeler simulator** (virtual clock, no persistence, no dispatch loop), but it is
  already `wasm32`-clean and Deno can load it directly via `WebAssembly.instantiate`.
- **The FFI wasm is now a first-class distributable.** `make engine-wasm-ffi-dist` emits
  `dist/engine-wasm-ffi/nano_engine.wasm` (wasm-opt -Oz, ~780 KiB) + a `manifest.json` (ABI version,
  engine version, sha256, exports). This is the artifact `nano-bernd` (Java) and `@nanobpm/nano-engine-ffi`
  (JS) both consume. **The single wasm binary is the single source of engine truth across every host.**
- **The SDK story is already a single surface.** `docs/sdk-nano-decorator-design.md` establishes
  that users write against the generated `@camunda8/orchestration-cluster-api` SDKs, and
  Nano-awareness lives **entirely in the hand-written runtime seam** (survives regeneration, no
  generator fork). There are exactly **two seams**: a *transport* seam (detect via `/v2/topology`
  `nano` advertisement, then connect/migrate) and an *operation* seam (only `createProcessInstance`
  and the job worker's `activateJobs`/complete/fail upgrade). When connected to Camunda the client
  is **byte-identical** to today; when connected to Nano it upgrades to the Falcon protocol.
- **`@nanobpmn/sdk`** (`clients/node-stream/`) is explicitly "layered on top of
  `@camunda8/orchestration-cluster-api`" and already implements the Falcon transport,
  detection (`detectNanobpm` probes the `/falcon` upgrade for a `welcome` frame), and a
  streaming job worker that *falls back to Camunda REST*.
- **The worker authoring surface is already transport-agnostic.** `server/src/console/worker_sdk.ts`
  gives users `defineWorker({ type, handle })` over Deno's native WebSocket; the handler never names
  a transport.
- **Today's "embed" is spawn-a-process.** `processos/src/supervisor.rs` starts the Nano gateway
  binary as a child and talks to it over REST — the exact indirection the embedded mode removes.

## Decision

**Yes — one SDK surface, deployment mode chosen at build/embed time, no separate SDK.**

The mechanism is the *decorator's existing two seams*, extended with a **third transport backend**:
an **in-process embedded μ-nano** reached over a **loopback Falcon**. The embedded engine
**implements the Falcon protocol** (and the REST subset the app uses) over an in-memory /
localhost channel, so the *same* decorator code path that lights up for a remote Nano also lights up
for the embedded engine. The user's application source is identical across all four modes; only the
**transport binding** changes, and that binding is selected by the build.

### Why this needs no separate SDK

The decorator design already reduced "talk to Nano vs Camunda" to two polymorphic operations behind
one transport chokepoint. Embedded mode adds a transport implementation, not an API:

| Mode | Transport binding | Selected | Engine |
| --- | --- | --- | --- |
| Camunda | REST (stock SDK) | runtime: `/v2/topology` has no `nano` | external Camunda |
| Nano (remote) | Falcon WS | runtime: `/v2/topology` advertises `nano` | external Nano gateway |
| **Embedded** | **Falcon over loopback** | **compile time: "Embed Bernd" build** | **in-process μ-nano.wasm** |

The single switch the decorator already keys on is the `/v2/topology` `nano` advertisement. The
embedded host **answers `/v2/topology` with the same `nano` field** (engine `nanobpmn`, a loopback
`falconPath`), so detection is *reused verbatim* — the client cannot tell embedded from
remote Nano except by the address it was told to bind to. This is the crux: **embedded μ-nano is
"a Nano gateway that happens to live in the same process,"** not a new client API.

### Shape of the "Embed Bernd" binary

```
┌──────────────────  deno compile --target …  → single native binary  ──────────────────┐
│  Deno runtime                                                                          │
│   ├─ μ-nano.wasm   (engine-core single-writer state machine, embedded asset)           │
│   ├─ Embedded host (TS)                                                                 │
│   │    • owns wall-clock time → feeds Date.now() into apply_command_at                  │
│   │    • owns persistence: appends the event log to disk/SQLite (Deno FS), replays      │
│   │      on boot (engine-core is event-sourced for exactly this)                        │
│   │    • runs the timer tick + job-dispatch loop                                        │
│   │    • serves the Falcon protocol + REST subset on a loopback transport       │
│   │    • answers /v2/topology with the `nano` advertisement                             │
│   ├─ @camunda8/orchestration-cluster-api + nano runtime decorator (UNCHANGED)           │
│   └─ App: process models + defineWorker(...) handlers (UNCHANGED source)                │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

The application's worker handlers and `createProcessInstance` calls go through the decorator's
operation seam exactly as in remote mode; the seam routes them to the loopback Falcon; the
host bridges loopback frames to the μ-nano FFI. For the app's own traffic this can collapse to
**in-process function calls** (no socket, no serialization) while still optionally exposing a real
localhost endpoint for external tooling / the cockpit.

### Cross-compilation falls out for free

`deno compile --target` cross-compiles win/mac/linux × x64/arm64 **from one machine**, and
μ-nano.wasm is platform-neutral, so the embedded engine ships identically everywhere. This is
strictly easier than embedding a native Rust binary or FFI'ing a per-platform `cdylib`
(which needs `--allow-ffi`, runtime extraction, and the painful Rust cross-compile story). WASM is
**one artifact that runs anywhere Deno runs** — directly serving the "Deno cross-compiles native
binaries" goal.

For **JVM hosts**, `nano-bernd` (Chicory) loads the *same* wasm inside a plain JVM; for **GraalVM Native Image**, the wasm is registered as a resource, Chicory's AOT-compiled interpreter is reachable-from-main, and `native-image` produces a single static binary with no JVM required. So the "one wasm blob, every host" property extends the "Embed Bernd" story to Java microservices without duplicating engine code.

## The embedded transactional model (position)

Because Bernd runs in-process, the temptation is to expose C7-style ambient-transaction semantics: `implementation="java:MyBean"`, delegate methods executed inside the same JPA/JDBC transaction as the engine's state mutation, "if my business code throws, everything rolls back." This ADR **rejects that pattern**. The reasoning:

1. **It only works embedded.** The same source running against remote Nano or Camunda cannot participate in the engine's transaction — the engine is over the network. Shipping shared-tx semantics silently changes what "the same application source in every mode" *means*: correctness depends on deployment topology, which is exactly the property the SDK-unification story sells against.
2. **It re-imports the C7→C8 pain.** The migration cost of Camunda 7 → Camunda 8 was overwhelmingly the loss of ambient-tx semantics; developers who leaned on `JavaDelegate` had to redesign around Sagas and compensation. Reintroducing shared-tx in Bernd would let developers write code that *cannot be lifted-and-shifted*, undoing the C8 lesson we're building on.
3. **`completeJob(jobKey)` is already idempotent at the engine layer.** `jobKey` IS the idempotency key: an activated job can be completed exactly once. Business-logic idempotency belongs in the business domain using natural keys (order id, payment id, …) — the engine cannot help with that regardless of where it runs.
4. **Outbox is defeated by lock-timeout reactivation.** A worker that writes to its DB inside a "business + outbox row" transaction and then completes the job over the SDK still races the engine: if the complete is delayed and the job lock expires, the engine reactivates the same job and hands it to another worker; the outbox row is now for a job that no longer exists. Outbox is a useful pattern, but not one Bernd should elevate to a first-class primitive — it is defensive plumbing for a specific class of side-effects, not a general answer.

The position, in one line: **Bernd is a transport for Nano, not a semantics change to Nano.** Same worker code, function-call transport instead of network — nothing more.

Implications for the API surface:

- Worker handlers run *outside* the engine's write path. Even in-process, the engine applies the completion command atomically after the handler returns; the handler is a normal function call, not an interceptor inside a shared transaction.
- There is no `implementation="java:BeanName"` element in Nano BPMN. Service tasks are always jobs. This is a hard non-goal.
- There is no `implementation="expression"` short-circuit that evaluates a script inside the engine. Same reason.
- There is no ambient `EntityManager` / `Connection` / `TransactionSynchronization` propagated into worker handlers by Bernd. Users who want their worker + DB update in one tx should own that transaction inside the handler and rely on idempotency to survive re-delivery — the same discipline they'd use against remote Nano.

## Developer experience: the ergonomics that matter

Rejecting shared-tx does not mean rejecting ergonomics. In-process affords a **much shorter feedback loop, deterministic testing, and complete state visibility** — that is where Bernd's DX story lives. The following ergonomics are in scope for the SDK/runtime layer (they preserve the "same code remote or embedded" invariant because remote SDKs already have equivalents, or the ergonomic is a *local addition* that no-ops on remote):

1. **Auto complete/fail wrapping in the worker framework.** A handler that returns normally completes the job; a handler that throws fails it with a structured error and the appropriate retry classification. Users only write business logic; the SDK owns the acknowledgement. Same on remote and embedded.
2. **`createInstance(...).awaitCompletion(Duration)`**. First-class API that returns instance outputs when the instance finishes (embedded: direct in-process await; remote: the Falcon `awaitInstance` frame). Same signature, same code.
3. **`runWorkflow(bpmnPath, vars)` scripting shortcut.** For scripts, tests and CLIs: deploy → start → await result → return, with resource cleanup. Sugar over (2) and `deployResourcesFromFiles`.
4. **Structured worker exceptions.** `BpmnError(code, message)` triggers BPMN error boundary events; `Unrecoverable` fails the job with zero retries; other exceptions retry with the default policy. Handlers become declarative about failure semantics without ever calling `job.fail(...)`.
5. **Deterministic test mode (Bernd-only, but semantically portable).** `Bernd.builder().testClock(Clock.fixed(...)).manualTick().build()` produces a Bernd whose timer/dispatch loop only runs on `.step()`. Combined with in-memory persistence, this gives synchronous, replayable, breakpoint-friendly BPMN tests. Remote-Nano equivalent: the modeler simulator (already exists in `engine-wasm`) covers the design-time case; the runtime test mode is a Bernd affordance.
6. **Engine state query API (Bernd-only).** `bernd.state.instances[key]`, `bernd.state.jobs(pending=true)`, `bernd.state.incidents()`, `bernd.state.timers()`. Direct reads of the single-writer snapshot — no REST round-trip, no eventual consistency, no lag against the engine's actual state. This is Bernd's answer to "the read model" for local dashboards, tests, and Cockpit-in-a-box.
7. **Job lifecycle tracer (dev-mode Bernd).** Every state transition emitted as a structured log with instance + element + timing; opt-in via `.trace(true)` or an env var. Turns "why did my process hang" into a scan-the-trace exercise instead of a code-reading exercise.
8. **App-level completion queue (opt-in).** A bounded queue between the dispatch loop and the worker handlers, with metrics (depth, wait time, drops). This is **not** a durability boundary — a job with an outstanding lock will be reactivated if not completed in time regardless of queue state — but it is a valuable backpressure and observability primitive. Users who want it, get it; users who don't, don't pay for it.

The non-ergonomics Bernd deliberately does not ship:

- **No `Idempotency-Key` request header parameter.** `jobKey` already fills that role at the engine layer.
- **No outbox helper as a first-class API.** See "position" §4. If users want the pattern they can build it, but the SDK does not sanction it — because it does not survive lock-timeout reactivation, and sanctioning it would mislead.
- **No `JavaDelegate` / `implementation="java:..."` shortcut.** See "position" §2.
- **No shared JDBC/JPA transaction.** See "position" §1.
- **No XA / 2PC.** Same reason, and 2PC has its own well-known failure modes.

## The distributed monolith: front-loaded mental model

Bernd users need to internalize one truth on day one, not on day thirty when the first race hits: **an application with an embedded engine is a distributed system, whether the engine lives in the same process or not.** The worker handler is *decoupled* from the engine's commit — the engine may reactivate the same job on a lock timeout while the handler is still running, and the handler will race the second activation to completion. This is exactly the property that makes Bernd portable to remote Nano; it is also exactly the property that surprises developers coming from C7's ambient-transaction world.

The SDK, docs, IDE and template README all lead with this. Not in an FAQ. Not in an "advanced" chapter. First page. The distributed-monolith framing is not a warning; it is the design.

## Multi-language embedded (the shape today)

| Host | Package | Wasm loader | Cross-compile story |
| --- | --- | --- | --- |
| Deno / Node / browser | `@nanobpm/nano-engine-ffi` (raw), `@nanobpm/nano-bernd` (ergonomic) | `WebAssembly.instantiate` | `deno compile --target …` |
| JVM | `io.github.jwulf:nano-bernd` | Chicory (pure Java) | plain `java -jar` fat-jar |
| GraalVM Native Image | `io.github.jwulf:nano-bernd` | Chicory (AOT-compiled by GraalVM) | `native-image` → single static binary, wasm embedded as a resource |
| Future (Python) | `nano_bernd` | wasmtime-py | PyInstaller / native binary |
| Future (Go) | `github.com/jwulf/nano-bernd-go` | wazero (pure Go) | `go build` static binary |

Each host implements the same **operation-seam contract** (§B.2/B.3): give the SDK an `EmbeddedEndpoint` that answers REST + Falcon (or direct calls, for the in-process realization). Every host uses the same `nbpmn_*` C-ABI, so adding a new host is bounded work: implement the endpoint, wire the transport switch, done. The engine itself is never re-implemented.

## Consequences

### What this buys us

- **One SDK to maintain**: `@camunda8/orchestration-cluster-api` + the already-designed nano runtime
  decorator. No `EmbeddedNanoClient`, no parallel worker class. The "Embed Bernd" button is a *build
  configuration*, not a code rewrite.
- **True lift-and-shift**: identical application source across Camunda, remote Nano and embedded;
  the mode is a binding (env var / build flag), proven by the decorator's structural idempotency
  ("absent the `nano` field, every code path is the existing REST path").
- **No external dependency for the embedded app**: removes the spawn-a-gateway indirection
  (`supervisor.rs`) for self-contained deployments.

### Honest limits / gaps (must be designed, not assumed)

1. **μ-nano is a simulator today, not a runtime.** To back an embedded app it needs (mostly in the
   TS host, keeping `engine-core` untouched per its design rule): real wall-clock timers, a
   job-dispatch loop, and **durable persistence + replay-on-boot** (WASM has no disk; the host owns
   the journal via Deno FS/SQLite). This persistence layer is the single biggest piece of new work.
2. **"The entire surface" is bounded by what embedded μ-nano implements.** The full Camunda gateway
   spec we ship has ~158 REST paths (191 generated handlers in `server/src/stub_impls.rs`); the
   Falcon covers the hot path (create + job lifecycle), but the rest (deployment, message
   correlation, user-task completion, incident resolution, and especially the **query/search read
   API**) needs in-process handlers over the engine's snapshot. Embedded mode must implement the
   subset Nano supports and return a clear *"unsupported in embedded mode"* error for the remainder
   — and the RAD environment should surface that capability matrix (Appendix A) to the author at
   "Embed" time.
3. **The query API needs an in-memory read model.** Camunda's search API is backed by a read store;
   μ-nano's snapshot already exposes instances/jobs/incidents/timers, but mapping it to the Camunda
   query surface is real work and is what most limits "embed any app unchanged."
4. **At-least-once / recovery semantics.** Replay-to-recover must be idempotent w.r.t.
   side-effecting workers (crash-restart re-runs un-acked jobs) — the same contract any Zeebe-style
   engine has; document it.
5. **No clustering, single node by definition.** Embedded mode is RF=1, no Raft/partitions — which
   is exactly the intent ("a single-node nano used only by the application itself"), but it means the
   cluster-only behaviours of the full gateway are out of scope for this artifact.

### Non-goals

- No generator fork and no client-API spec change (consistent with the decorator design).
- Not a distributed/clustered embedded engine.
- Not a replacement for the full gateway — embedded mode is the single-tenant, single-node deployment
  target of the *same* application.
- **No shared JVM / JPA / JDBC transaction** between the worker handler and the engine's commit (see "position" §1).
- **No `JavaDelegate` / `implementation="java:..."` / `implementation="expression"`** BPMN elements. Service tasks are always jobs, embedded or not (see "position" §2).
- **No `Idempotency-Key` request header parameter** on `createProcessInstance` or job APIs. `jobKey` is the idempotency key at the engine layer; business idempotency belongs in the business domain (see "position" §3).
- **No first-class outbox helper.** The pattern does not survive lock-timeout reactivation and sanctioning it would mislead (see "position" §4).
- **No XA / 2PC.**
- Not a *silent* semantics change between embedded and remote: any ergonomic that only exists embedded (state query, tracer, manual tick) must be scoped so that removing it and switching to remote still leaves the application correct.

## Options considered

- **A. `deno compile` a Rust binary / FFI a native `cdylib`.** Deno can't link Rust; FFI needs a
  per-platform dylib, `--allow-ffi`, runtime extraction, and reintroduces Rust cross-compilation.
  Rejected in favour of WASM's single platform-neutral artifact.
- **B. Embedded μ-nano.wasm + loopback Falcon (this ADR).** One artifact, trivial
  cross-compile, and — critically — **reuses the decorator's existing transport/operation seams** so
  no new SDK is required.
- **C. A separate `@nano/embedded` SDK.** Rejected: duplicates the worker/create surface, doubles the
  test matrix, and breaks lift-and-shift (the app would import different packages per mode).

## Rollout sketch (reference-first, mirrors the decorator rollout)

- **E0 — Loopback spike**: load existing `engine-wasm` in Deno, inject real `Date.now()`, run a
  `defineWorker` handler in-process against it (no persistence). Proves the in-process loopback and
  the topology-advertisement reuse.
- **E1 — Durability**: host-owned event-log journal (file or SQLite) + replay-on-boot. Make-or-break.
- **E2 — Runtime API surface**: extend the wasm exports for activate/complete/fail/correlate/query +
  the timer/dispatch tick; bridge them to the Falcon + REST-subset handlers.
- **E3 — `deno compile` packaging**: embed μ-nano.wasm as an asset; verify `--target` cross-compiles;
  wire the RAD-environment "Embed Bernd" button to this build.
- **E4 — Capability matrix**: enumerate which Camunda endpoints embedded mode supports; surface
  unsupported-endpoint diagnostics to the author at embed time.

Dependency note: E0–E2 depend only on `engine-core`/`engine-wasm` (already wasm-clean) and the
shipped Falcon protocol; the client side rides the decorator design unchanged.

## Open questions

- **Read-model fidelity**: how much of the Camunda query/search API can μ-nano's snapshot back before
  diminishing returns? This bounds "embed any app unchanged."
- **In-process vs loopback-socket**: collapse the app's own traffic to direct calls (fastest) while
  still exposing a localhost socket for external tooling — confirm the decorator can bind to an
  in-process channel without a real WS (see Appendix B.0).
- **Build realization (c) at all?**: the loopback Falcon *server* is a deliberate cluster-free
  TS re-implementation of `server/src/falcon.rs` (frames reused, credits/correlation/dispatch
  re-written). Is push-based job delivery to *external* out-of-process consumers worth that code, or is
  the in-process path (a) + REST subset (b) sufficient for v1? (See Appendix B.0.)
- **Persistence format**: reuse the server journal format or a simpler host-owned log? The server's
  journal is cluster-oriented; embedded likely wants a minimal append-only event log.
- **Author-time capability checks**: should the RAD environment statically flag use of
  embedded-unsupported endpoints before the "Embed Bernd" build, rather than failing at runtime?

## Appendix A — Embedded capability matrix

Scope of "the entire surface": the Camunda OpenAPI we ship (`console/public/swagger/openapi.json`)
has **158 paths across ~37 tags**, of which **44 are `/search` (query) endpoints**. Embedded mode
does not need all of it — most admin/identity/cluster tags are meaningless for a single-tenant,
single-node embedded app. What matters is the **application-runtime** subset, scored below against
three independent layers:

- **Core** — does `engine-core` already model it? (authoritative: `engine-core/src/command.rs`,
  which defines `DeployResources`, `CreateInstance`, `CancelInstance`, the full job lifecycle
  `ActivateJobs`/`CompleteJob`/`FailJob`/`ThrowJobError`/`UpdateJobRetries`, user tasks
  `Assign`/`Unassign`/`Update`/`CompleteUserTask`, `ResolveIncident`, `SetVariables`,
  `CorrelateMessage` + message-subscription open/correlate/close, `BroadcastSignal`,
  `TriggerTimers`/`ExpireTimers`, `DispatchStartInstance`).
- **μ-nano** — is it exposed by the `engine-wasm` wrapper today? (`engine-wasm/src/lib.rs`'s
  `TestEngine` currently exposes only `deploy`, `createInstance`, `completeJob`, `failJob`,
  `advanceTime`, `snapshot`, `events` — so most engine-core capability is **present in the core but
  not yet surfaced through wasm**; adding exports is cheap, mechanical glue).
- **Transport** — in the *decorator* model, does the operation ride the Falcon protocol
  (`clients/node-stream/src/frames.ts`:
  `createInstance`/`completeJob`/`failJob`/`throwError`/`subscribe`/`awaitInstance`) or a REST call?
  **Note:** this column reflects the *remote* transport; in the default embedded realization (B.0(a))
  *both* collapse to direct in-process `EmbeddedHost` calls — the wire protocol is not used. The
  embedded host answers the REST-handler rows as in-process REST shims.

Legend: ✅ ready · 🟡 core-ready, needs wasm-export + REST/loopback wiring · 🔴 needs new
host-side work (read model / feature) · ⬛ out of scope for embedded.

| Camunda endpoint family | Core | μ-nano export | Transport | Embedded status |
| --- | --- | --- | --- | --- |
| **Deployment / Resource** (deploy BPMN/DMN/forms) | ✅ `DeployResources` | ✅ `deploy` | REST handler | 🟡 wrap `deploy` in a `/deployments` REST shim |
| **Process instance — create** | ✅ | ✅ `createInstance` | Falcon | ✅ rides the decorator hot path unchanged |
| **Process instance — create+await result** | ✅ | 🟡 | Falcon (`awaitInstance`) | 🟡 host correlates `instanceCompleted` |
| **Process instance — cancel** | ✅ `CancelInstance` | 🔴 not exposed | REST handler | 🟡 add wasm export + `/process-instances/{k}/cancellation` |
| **Job — activate/complete/fail/throw** | ✅ | ✅ (complete/fail) | Falcon | ✅ worker loop upgrades with zero app changes |
| **Job — update retries** | ✅ `UpdateJobRetries` | 🔴 | REST handler | 🟡 add export + `/jobs/{key}` |
| **User task — assign/unassign/update/complete** | ✅ | 🔴 not exposed | REST handler | 🟡 core-complete; needs wasm exports + `/user-tasks/*` shims |
| **Incident — resolve / update** | ✅ `ResolveIncident` | 🔴 | REST handler | 🟡 add export + `/incidents/{key}/resolution` |
| **Variables — set** | ✅ `SetVariables` | 🔴 | REST handler | 🟡 add export + `/.../variables` |
| **Message — correlate / publish / subscriptions** | ✅ `CorrelateMessage` + subs | 🔴 | REST handler | 🟡 add exports + `/messages/*` |
| **Signal — broadcast** | ✅ `BroadcastSignal` | 🔴 | REST handler | 🟡 add export + `/signals` |
| **Clock — pin/reset** | ✅ (host owns `now`) | 🟡 (`advanceTime`) | REST handler | 🟡 maps to the host clock injection — useful for tests |
| **FEEL / expression evaluation** | ✅ `engine-core/src/feel/*` | 🔴 | REST handler | 🟡 FEEL engine exists; surface `/expression` if needed |
| **DMN — evaluate decision** | 🔴 no DMN decision-table engine (only FEEL primitives; `decision` refs in `bpmn.rs` are business-rule-task wiring) | 🔴 | REST handler | 🔴 **needs a DMN evaluator** before embedded DMN works |
| **Query / search — process instances, jobs, incidents, element instances, user tasks, variables, message subs** | snapshot only (`engine-wasm` `Snapshot`) | 🟡 partial via `snapshot` | REST handler | 🔴 **biggest gap**: needs an in-memory read model with filter/sort/paginate to back the 44 `/search` endpoints |
| **Decision instance / requirements search, audit log search** | 🔴 | 🔴 | REST handler | 🔴 depends on DMN + read model |
| **Document API** (5) | 🔴 | — | REST handler | 🔴 needs a host-side blob store (Deno FS) if the app uses it |
| **Batch operations** (6) | 🔴 | — | REST handler | 🔴 host could loop over the read model; low priority |
| **Tenant / Role / Group / User / Authorization / Mapping rule / Global listener / Agent instance** (~70 paths) | — | — | — | ⬛ out of scope (single-tenant, no IAM in embedded) |
| **Cluster / System / License / Setup / Authentication** | — | — | — | ⬛ out of scope (single node, no auth) |

**Reading of the matrix.** The application *hot path* (create instance + job workers) is ✅ today —
it is exactly what the Falcon protocol and decorator already cover, so those apps embed unchanged.
The *write* breadth (user tasks, incidents, messages, signals, variables, cancel) is **🟡: already
modelled in `engine-core`**, blocked only on (a) mechanical `engine-wasm` exports and (b) thin
in-process REST shims — no engine research required. The two genuine 🔴 features are **DMN decision
evaluation** (no decision-table engine yet) and the **query/search read model** (snapshot ≠ a
filterable/paginated search store). Everything identity/cluster/tenant is ⬛ by design.

**Author-time contract.** The RAD environment should ship this matrix as a machine-readable
capability manifest and, at "Embed Bernd" build time, statically flag any app call into a 🔴/⬛
endpoint — failing the embed with a precise diagnostic rather than letting it fail at runtime. At
runtime, embedded-unsupported endpoints return a structured `501 EMBEDDED_UNSUPPORTED` carrying the
endpoint id, so the same error is observable in both places.

## Appendix B — Loopback transport adapter contract

The decorator (`docs/sdk-nano-decorator-design.md`) funnels everything through two seams: a transport
chokepoint and the two upgradable operations. Embedded mode supplies a **third transport
implementation** behind those seams — the *loopback adapter* — so the client code is identical to the
remote-Nano path. The adapter is the contract between the embedded host (TS, around μ-nano.wasm) and
the unchanged SDK.

### B.0 Where the Falcon protocol comes from (the crux)

**The Falcon protocol is a *gateway* concern, not an *engine* concern.** It is implemented in
`server/src/falcon.rs` (~106 KB of axum/WebSocket framing, submission/job credit coordination,
correlation **and** intra-cluster routing — forwarding deploys to the partition-0 owner, cross-partition
message correlation, leader cancellation, redirect). **`engine-core` has none of it** (no networking,
no credits, no frames). So embedding the engine does *not* give you the protocol for free — something in
the embedded binary must provide it. There are three transport realizations behind the decorator's
operation seam, and they differ precisely in *who* implements the protocol:

| Realization | Who implements the protocol | When to use |
| --- | --- | --- |
| **(a) In-process direct** | **No protocol at all.** The decorator's operation seam calls `EmbeddedHost` methods directly (function calls); `createProcessInstance`/`activateJobs`/`completeJob` are bridged straight to engine-core FFI. No frames, no credits, no socket. | The app's own traffic in the single binary — the default, fastest embedded path. |
| **(b) Loopback REST** | The host serves the **REST subset** (Appendix A) on a localhost origin; the client uses the decorator's *Camunda* (REST) transport path — no Nano upgrade, no stream. | Simplicity / external tooling that only needs REST; workers poll `activateJobs` against the in-process handler (cheap — no network). |
| **(c) Loopback Falcon** | The host runs a **minimal TS re-implementation** of the Falcon *server* (frames + credits + correlation + job push), bridging to engine-core. It is **not** shared code with `server/src/falcon.rs` — but it is bounded because *all* the cluster machinery (raft, partitions, forwarding, redirect, distributed backpressure) is absent, and it **reuses `clients/node-stream/src/frames.ts`** for encode/parse. | Out-of-process tooling/cockpit that expects the wire protocol, or when you want push-based job delivery over a real localhost socket. |

The honest consequence: **for the in-process case (a) the client never "speaks the Falcon
protocol to the embedded engine" — it speaks the *operation seam*, which the embedded transport
satisfies with direct calls.** The wire protocol only reappears in (c), and only as a deliberate,
cluster-free TS port — never by lifting `falcon.rs` (which drags axum/tokio/raft/partitions and
would defeat the "micro" goal) into wasm.

Why the protocol's *value* mostly evaporates in-process: submission credits, push-vs-long-poll latency,
and backpressure all exist to protect a *remote broker over a network*. In one process there is no
socket and no network latency, so (a) is both simpler and faster than reproducing the stream. Prefer
(a); add (c) only for external consumers.

### B.1 What the adapter must satisfy (so detection reuses verbatim)

1. **Topology advertisement.** Answer `GET /v2/topology` with the standard Camunda fields **plus**
   the `nano` block — extended with an explicit transport hint so the decorator picks the right
   realization instead of blindly trying a WebSocket:
   `{ engine: "nanobpmn", version, embedded: true, transport: "in-process" | "Falcon",
   falconPath? }`. For (a)/(b) `transport` is `in-process` and there is **no**
   `falconPath`, so the decorator must *not* attempt the `/falcon` upgrade (today's
   `detectNanobpm` keys off the WS `welcome` frame — the decorator needs this hint to use the
   in-process/REST realization without probing a socket). For (c) `transport` is `Falcon` with a
   loopback `falconPath`, and detection proceeds exactly as for remote Nano.
2. **Operation-seam fulfilment.** Realization (a) implements `createProcessInstance` /
   `activateJobs` / `completeJob` / `failJob` / `throwError` as **direct `EmbeddedHost` calls** (see
   B.3) — the decorator already abstracts these two operations, so this is the natural integration
   point and needs no framing. Realization (c) additionally serves the `ClientFrame`/`ServerFrame`
   protocol (`clients/node-stream/src/frames.ts`: client
   `subscribe`/`jobCredits`/`createInstance`/`completeJob`/`failJob`/`throwError`/`awaitInstance`/
   `heartbeat`; server `welcome`/`job`/`commandResult`/`instanceCompleted`/`submissionCredits`/
   `pressure`/`heartbeat`) over the loopback socket, reusing the frame module so `FalconClient`
   is unmodified; credit semantics may be generous (single node, no cluster to protect).
3. **REST subset.** Answer the in-scope REST endpoints from Appendix A over the same loopback origin,
   so non-stream operations (deploy, user tasks, incidents, messages, signals, query) work through the
   stock generated client paths.

### B.2 The binding (one chokepoint, two physical realizations)

```ts
// The embedded host exposes ONE origin the SDK binds to. Two realizations:
//   (a) in-process channel  — zero-copy, no socket  (app's own traffic, fastest)
//   (b) localhost ws+http    — for external tooling / the cockpit / multi-process apps
export interface EmbeddedEndpoint {
  /** Base origin the Camunda SDK is constructed with (e.g. "loopback://nano" or "http://127.0.0.1:0"). */
  readonly origin: string;
  /** REST handler: method + path + body  →  Camunda-shaped response (or 501 EMBEDDED_UNSUPPORTED). */
  fetch(req: Request): Promise<Response>;
  /** Falcon channel: a duplex of parsed frames, semantically identical to the WS. */
  openFalcon(headers?: Record<string, string>): FalconChannel;
}

export interface FalconChannel {
  send(frame: ClientFrame): void;          // createInstance / completeJob / failJob / throwError / subscribe / credits / heartbeat
  onFrame(cb: (f: ServerFrame) => void): void;  // welcome / job / commandResult / instanceCompleted / credits / pressure / heartbeat
  close(): void;
}
```

For realization (a) the SDK's `fetch` and the Falcon client are pointed at the in-process
`EmbeddedEndpoint` (no real sockets); for (b) the host binds an actual `Deno.serve` on a loopback port
and the existing `ws`/`fetch` paths are used untouched. The decorator does not know or care which.

### B.3 What the host owns around μ-nano (the engine side of the adapter)

```ts
export interface EmbeddedHost {
  // ---- engine bridge (μ-nano.wasm) ----
  apply(command: EngineCommand, nowMs: number): EngineEvent[];   // single-writer; host supplies the clock
  snapshot(): EngineSnapshot;                                    // read model source (Appendix A query gap)

  // ---- the four host responsibilities (see ADR body, "Honest limits") ----
  now(): number;                 // wall clock → injected into apply(); replaces the modeler's virtual clock
  persist(events: EngineEvent[]): Promise<void>;  // append-only journal via Deno FS/SQLite
  recover(): Promise<void>;      // replay the journal on boot (engine-core is event-sourced for this)
  tick(): void;                  // periodic: TriggerTimers(now) + ExpireJobs(now), then dispatch Created jobs
}
```

Frame/command bridging rules:
- `createInstance` frame → `apply(CreateInstance, now())`; correlate the resulting `instance_key`
  back as `commandResult`; if `awaitInstance`, hold the `corr` until the instance completes →
  `instanceCompleted`.
- `subscribe` (job type + credits) registers a consumer; the dispatch loop hands `Created` jobs of
  that type out as `job` frames, respecting credits (`ActivateJobs` under the hood with a host-chosen
  lock timeout).
- `completeJob`/`failJob`/`throwError` → the matching `CompleteJob`/`FailJob`/`ThrowJobError` command;
  ack via `commandResult`.
- Every applied batch is fed to `persist()` **before** the ack, so a crash-restart replays a
  consistent prefix (at-least-once for side-effecting workers — see ADR body).
- The dispatch loop and `tick()` are the *only* sources of wall-clock time; `engine-core` stays
  clock-free, preserving determinism and replayability.

### B.4 Invariants

- **Idempotent against Camunda / remote Nano**: the adapter changes only *what the transport is*, never
  validation, tenant defaults, retry classification or response shaping — those live outside the seam
  (decorator §5), so the same app behaves identically in all four modes.
- **One socket / one channel per client**: matches the decorator's threaded-worker assumption (the
  Falcon channel lives on the main thread; thread workers proxy actions to it).
- **Advisory control frames degrade safely**: `pressure` may be a no-op on a single node; a worker that
  ignores it still works (as on remote Nano).
- **Capability honesty**: any REST call outside Appendix A's ✅/🟡 set returns `501
  EMBEDDED_UNSUPPORTED` with the endpoint id, matching the author-time static check.

## Appendix C — Dual-role services (self-orchestrating + externally-orchestrated)

A common shape the embedded mode enables: a **microservice that orchestrates its own internal
operation with an embedded Nano engine, while being itself orchestrated as a worker/participant by a
*remote* Nano or Camunda engine.** The service has two engine relationships at once:

- **Inner / self** — its embedded μ-nano runs the service's *own* sagas (compensation, retries,
  internal fan-out). The service is both the engine host and the worker for these processes.
- **Outer / orchestrator** — a remote engine runs a larger business process; this service appears in
  it as a job worker (and/or a callable process) and must complete the jobs assigned to it.

### C.1 The rule: discriminate by **binding**, not by ambient detection

The decorator detects engine *type* (Camunda vs Nano) per `CamundaClient` instance — detection is
**per-client**, which is the right granularity (decorator design §8). A dual-role service therefore
holds **two clients**, and *which engine you are talking to is which handle you call* — there is no
ambiguous global "current engine" to disambiguate:

```ts
import { CamundaClient } from "@camunda8/orchestration-cluster-api"; // + nano runtime decorator

// INNER: my own embedded engine. Origin is supplied by the embedded host (Appendix B),
// not configured externally — it is well-known and always present in this binary.
const self = new CamundaClient({ restAddress: process.env.NANO_EMBEDDED_ORIGIN }); // e.g. "loopback://nano"

// OUTER: the engine that orchestrates ME. Address comes from deployment config; may be
// Camunda (REST) or a remote Nano (Falcon) — the decorator detects which.
const orchestrator = new CamundaClient({ restAddress: process.env.CAMUNDA_REST_ADDRESS });

// 1) Serve the OUTER orchestration's jobs — these only ever arrive on `orchestrator`.
orchestrator.createJobWorker({
  type: "fulfil-order",
  handler: async (job) => {
    // 2) Drive my OWN saga on my embedded engine — addressed explicitly via `self`.
    const run = await self.createProcessInstance({
      processDefinitionId: "fulfilment-saga",
      variables: job.variables,
      awaitCompletion: true,
    });
    return job.complete({ fulfilmentResult: run.variables.result });
  },
});

// 3) Internal-only workers bind to `self`; they never see outer jobs.
self.createJobWorker({ type: "reserve-stock", handler: reserveStock });
```

Because a job worker is bound to exactly one client, **the source of a job is implicit in which
worker fired it** — outer jobs land on `orchestrator`'s workers, inner jobs on `self`'s workers. No
job is ever ambiguous, so most code needs no explicit discriminator at all.

### C.2 When you *do* need an explicit discriminator

Two cases warrant runtime introspection rather than relying on the handle:

1. **Shared handler / library code** that may run against either engine. Key off topology, which the
   decorator already caches per client:

   ```ts
   const t = await client.getTopology();
   const role =
     !t.nano            ? "camunda"           // no nano block ⇒ stock Camunda
     : t.nano.embedded  ? "nano-embedded"     // my own in-process engine
     :                    "nano-remote";       // a remote Nano cluster
   ```

   This requires the embedded host's topology advertisement to carry an **`embedded: true`**
   discriminator inside the `nano` block (extends the §"single switch" advertisement
   `{ engine, version, falconPath }`). Absent it, embedded is indistinguishable from a remote
   single-node Nano — usually fine, but this flag is what lets a service *know it is talking to its
   own engine*.

2. **Per-job provenance**, when one handler is registered on both clients. Tag the job with its
   origin at delivery: the embedded host stamps `engine: "embedded"` (and remote Nano/Camunda are
   inferred from the client), surfaced as a read-only `job.engine` field so a shared handler can
   branch without knowing which worker invoked it.

### C.3 Why this is safe and needs no extra SDK

- **One SDK, two instances.** Both clients are the same `@camunda8/orchestration-cluster-api` + nano
  decorator; the dual role is *configuration* (two origins), not two code paths or two packages.
- **Lift-and-shift preserved.** Move the service off the embedded engine by pointing `self` at a
  remote Nano URL instead of the loopback origin — the application code is unchanged; only the
  binding moves (the ADR's central property).
- **Capability scoping is per role.** The inner engine only needs the Appendix A subset its own sagas
  use; the outer relationship uses whatever the remote engine (full Camunda or Nano) supports. A 🔴
  gap in embedded mode (e.g. DMN) constrains only the *inner* sagas, never the service's ability to
  act as a worker in the outer orchestration.

### C.4 Invariants

- **No global engine.** There is deliberately no process-wide "the engine" singleton; every call
  names its client (`self` vs `orchestrator`). A global would reintroduce exactly the ambiguity this
  pattern avoids.
- **Independent lifecycles.** The embedded engine's persistence/recovery (Appendix B.3) is private to
  the service; the outer engine's durability is the orchestrator's concern. A restart replays the
  inner journal locally and re-activates any outer jobs left un-acked on the orchestrator — the two
  recoveries do not interact.
- **Detection stays per-client.** `self` resolves to `nano-embedded`; `orchestrator` resolves to
  whatever it is pointed at — neither probe affects the other.
