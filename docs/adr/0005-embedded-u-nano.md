# ADR 0005 — Embedded μ-nano (self-contained application binaries)

Status: **Proposed — design/exploration only. No code changed by this document.**
Date: 2026-06-28.
Relates to: `docs/sdk-nano-decorator-design.md` (the runtime decorator), `docs/command-stream-design.md`,
`engine-core/src/ffi.rs` (C-ABI surface), `engine-wasm/src/lib.rs` (μ-nano / `TestEngine`),
`clients/node-stream/` (`@nanobpmn/sdk` streaming layer), `server/src/console/worker_sdk.ts`
(Deno `defineWorker`), `processos/src/supervisor.rs` (spawn-an-engine model), the unified
browser RAD-environment direction.

## Context

We are unifying the modeler and worker IDE into a single browser-based RAD environment
oriented around **Nano applications**: a user authors models, DMN, forms and worker/application
code, tests them, deploys, and can have Deno cross-compile the application into a native binary.

The end-state we want to enable is a **single application source** that can be:

1. **Lift-and-shifted on disk** and run against a stock **Camunda 8** engine;
2. The same source run against a **remote Nano** engine (auto-upgrading to the command stream);
3. Cross-compiled into a **self-contained native binary** that runs against Camunda or remote Nano;
4. With one button — **"Embed Nano"** — cross-compiled into a self-contained native binary with
   **micro-nano embedded**: a single-node engine used *only by the application itself*, needing no
   external Nano server.

The driving question this ADR answers:

> **Can we deliver all four modes without maintaining a separate SDK — letting users write
> against the entire `@camunda8/orchestration-cluster-api` surface and choose the deployment
> mode at compile time?**

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
- **The SDK story is already a single surface.** `docs/sdk-nano-decorator-design.md` establishes
  that users write against the generated `@camunda8/orchestration-cluster-api` SDKs, and
  Nano-awareness lives **entirely in the hand-written runtime seam** (survives regeneration, no
  generator fork). There are exactly **two seams**: a *transport* seam (detect via `/v2/topology`
  `nano` advertisement, then connect/migrate) and an *operation* seam (only `createProcessInstance`
  and the job worker's `activateJobs`/complete/fail upgrade). When connected to Camunda the client
  is **byte-identical** to today; when connected to Nano it upgrades to the command stream.
- **`@nanobpmn/sdk`** (`clients/node-stream/`) is explicitly "layered on top of
  `@camunda8/orchestration-cluster-api`" and already implements the command-stream transport,
  detection (`detectNanobpm` probes the `/command-stream` upgrade for a `welcome` frame), and a
  streaming job worker that *falls back to Camunda REST*.
- **The worker authoring surface is already transport-agnostic.** `server/src/console/worker_sdk.ts`
  gives users `defineWorker({ type, handle })` over Deno's native WebSocket; the handler never names
  a transport.
- **Today's "embed" is spawn-a-process.** `processos/src/supervisor.rs` starts the Nano gateway
  binary as a child and talks to it over REST — the exact indirection the embedded mode removes.

## Decision (proposed)

**Yes — one SDK surface, deployment mode chosen at build/embed time, no separate SDK.**

The mechanism is the *decorator's existing two seams*, extended with a **third transport backend**:
an **in-process embedded μ-nano** reached over a **loopback command stream**. The embedded engine
**implements the command-stream protocol** (and the REST subset the app uses) over an in-memory /
localhost channel, so the *same* decorator code path that lights up for a remote Nano also lights up
for the embedded engine. The user's application source is identical across all four modes; only the
**transport binding** changes, and that binding is selected by the build.

### Why this needs no separate SDK

The decorator design already reduced "talk to Nano vs Camunda" to two polymorphic operations behind
one transport chokepoint. Embedded mode adds a transport implementation, not an API:

| Mode | Transport binding | Selected | Engine |
| --- | --- | --- | --- |
| Camunda | REST (stock SDK) | runtime: `/v2/topology` has no `nano` | external Camunda |
| Nano (remote) | command-stream WS | runtime: `/v2/topology` advertises `nano` | external Nano gateway |
| **Embedded** | **command-stream over loopback** | **compile time: "Embed Nano" build** | **in-process μ-nano.wasm** |

The single switch the decorator already keys on is the `/v2/topology` `nano` advertisement. The
embedded host **answers `/v2/topology` with the same `nano` field** (engine `nanobpmn`, a loopback
`commandStreamPath`), so detection is *reused verbatim* — the client cannot tell embedded from
remote Nano except by the address it was told to bind to. This is the crux: **embedded μ-nano is
"a Nano gateway that happens to live in the same process,"** not a new client API.

### Shape of the "Embed Nano" binary

```
┌──────────────────  deno compile --target …  → single native binary  ──────────────────┐
│  Deno runtime                                                                          │
│   ├─ μ-nano.wasm   (engine-core single-writer state machine, embedded asset)           │
│   ├─ Embedded host (TS)                                                                 │
│   │    • owns wall-clock time → feeds Date.now() into apply_command_at                  │
│   │    • owns persistence: appends the event log to disk/SQLite (Deno FS), replays      │
│   │      on boot (engine-core is event-sourced for exactly this)                        │
│   │    • runs the timer tick + job-dispatch loop                                        │
│   │    • serves the command-stream protocol + REST subset on a loopback transport       │
│   │    • answers /v2/topology with the `nano` advertisement                             │
│   ├─ @camunda8/orchestration-cluster-api + nano runtime decorator (UNCHANGED)           │
│   └─ App: process models + defineWorker(...) handlers (UNCHANGED source)                │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

The application's worker handlers and `createProcessInstance` calls go through the decorator's
operation seam exactly as in remote mode; the seam routes them to the loopback command stream; the
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

## Consequences

### What this buys us

- **One SDK to maintain**: `@camunda8/orchestration-cluster-api` + the already-designed nano runtime
  decorator. No `EmbeddedNanoClient`, no parallel worker class. The "Embed Nano" button is a *build
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
   command stream covers the hot path (create + job lifecycle), but the rest (deployment, message
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

## Options considered

- **A. `deno compile` a Rust binary / FFI a native `cdylib`.** Deno can't link Rust; FFI needs a
  per-platform dylib, `--allow-ffi`, runtime extraction, and reintroduces Rust cross-compilation.
  Rejected in favour of WASM's single platform-neutral artifact.
- **B. Embedded μ-nano.wasm + loopback command stream (this ADR).** One artifact, trivial
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
  the timer/dispatch tick; bridge them to the command-stream + REST-subset handlers.
- **E3 — `deno compile` packaging**: embed μ-nano.wasm as an asset; verify `--target` cross-compiles;
  wire the RAD-environment "Embed Nano" button to this build.
- **E4 — Capability matrix**: enumerate which Camunda endpoints embedded mode supports; surface
  unsupported-endpoint diagnostics to the author at embed time.

Dependency note: E0–E2 depend only on `engine-core`/`engine-wasm` (already wasm-clean) and the
shipped command-stream protocol; the client side rides the decorator design unchanged.

## Open questions

- **Read-model fidelity**: how much of the Camunda query/search API can μ-nano's snapshot back before
  diminishing returns? This bounds "embed any app unchanged."
- **In-process vs loopback-socket**: collapse the app's own traffic to direct calls (fastest) while
  still exposing a localhost socket for external tooling — confirm the decorator can bind to an
  in-process channel without a real WS (see Appendix B.0).
- **Build realization (c) at all?**: the loopback command-stream *server* is a deliberate cluster-free
  TS re-implementation of `server/src/command_stream.rs` (frames reused, credits/correlation/dispatch
  re-written). Is push-based job delivery to *external* out-of-process consumers worth that code, or is
  the in-process path (a) + REST subset (b) sufficient for v1? (See Appendix B.0.)
- **Persistence format**: reuse the server journal format or a simpler host-owned log? The server's
  journal is cluster-oriented; embedded likely wants a minimal append-only event log.
- **Author-time capability checks**: should the RAD environment statically flag use of
  embedded-unsupported endpoints before the "Embed Nano" build, rather than failing at runtime?

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
- **Transport** — in the *decorator* model, does the operation ride the command stream
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
| **Process instance — create** | ✅ | ✅ `createInstance` | command stream | ✅ rides the decorator hot path unchanged |
| **Process instance — create+await result** | ✅ | 🟡 | command stream (`awaitInstance`) | 🟡 host correlates `instanceCompleted` |
| **Process instance — cancel** | ✅ `CancelInstance` | 🔴 not exposed | REST handler | 🟡 add wasm export + `/process-instances/{k}/cancellation` |
| **Job — activate/complete/fail/throw** | ✅ | ✅ (complete/fail) | command stream | ✅ worker loop upgrades with zero app changes |
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
it is exactly what the command stream and decorator already cover, so those apps embed unchanged.
The *write* breadth (user tasks, incidents, messages, signals, variables, cancel) is **🟡: already
modelled in `engine-core`**, blocked only on (a) mechanical `engine-wasm` exports and (b) thin
in-process REST shims — no engine research required. The two genuine 🔴 features are **DMN decision
evaluation** (no decision-table engine yet) and the **query/search read model** (snapshot ≠ a
filterable/paginated search store). Everything identity/cluster/tenant is ⬛ by design.

**Author-time contract.** The RAD environment should ship this matrix as a machine-readable
capability manifest and, at "Embed Nano" build time, statically flag any app call into a 🔴/⬛
endpoint — failing the embed with a precise diagnostic rather than letting it fail at runtime. At
runtime, embedded-unsupported endpoints return a structured `501 EMBEDDED_UNSUPPORTED` carrying the
endpoint id, so the same error is observable in both places.

## Appendix B — Loopback transport adapter contract

The decorator (`docs/sdk-nano-decorator-design.md`) funnels everything through two seams: a transport
chokepoint and the two upgradable operations. Embedded mode supplies a **third transport
implementation** behind those seams — the *loopback adapter* — so the client code is identical to the
remote-Nano path. The adapter is the contract between the embedded host (TS, around μ-nano.wasm) and
the unchanged SDK.

### B.0 Where the command-stream protocol comes from (the crux)

**The command-stream protocol is a *gateway* concern, not an *engine* concern.** It is implemented in
`server/src/command_stream.rs` (~106 KB of axum/WebSocket framing, submission/job credit coordination,
correlation **and** intra-cluster routing — forwarding deploys to the partition-0 owner, cross-partition
message correlation, leader cancellation, redirect). **`engine-core` has none of it** (no networking,
no credits, no frames). So embedding the engine does *not* give you the protocol for free — something in
the embedded binary must provide it. There are three transport realizations behind the decorator's
operation seam, and they differ precisely in *who* implements the protocol:

| Realization | Who implements the protocol | When to use |
| --- | --- | --- |
| **(a) In-process direct** | **No protocol at all.** The decorator's operation seam calls `EmbeddedHost` methods directly (function calls); `createProcessInstance`/`activateJobs`/`completeJob` are bridged straight to engine-core FFI. No frames, no credits, no socket. | The app's own traffic in the single binary — the default, fastest embedded path. |
| **(b) Loopback REST** | The host serves the **REST subset** (Appendix A) on a localhost origin; the client uses the decorator's *Camunda* (REST) transport path — no Nano upgrade, no stream. | Simplicity / external tooling that only needs REST; workers poll `activateJobs` against the in-process handler (cheap — no network). |
| **(c) Loopback command-stream** | The host runs a **minimal TS re-implementation** of the command-stream *server* (frames + credits + correlation + job push), bridging to engine-core. It is **not** shared code with `server/src/command_stream.rs` — but it is bounded because *all* the cluster machinery (raft, partitions, forwarding, redirect, distributed backpressure) is absent, and it **reuses `clients/node-stream/src/frames.ts`** for encode/parse. | Out-of-process tooling/cockpit that expects the wire protocol, or when you want push-based job delivery over a real localhost socket. |

The honest consequence: **for the in-process case (a) the client never "speaks the command-stream
protocol to the embedded engine" — it speaks the *operation seam*, which the embedded transport
satisfies with direct calls.** The wire protocol only reappears in (c), and only as a deliberate,
cluster-free TS port — never by lifting `command_stream.rs` (which drags axum/tokio/raft/partitions and
would defeat the "micro" goal) into wasm.

Why the protocol's *value* mostly evaporates in-process: submission credits, push-vs-long-poll latency,
and backpressure all exist to protect a *remote broker over a network*. In one process there is no
socket and no network latency, so (a) is both simpler and faster than reproducing the stream. Prefer
(a); add (c) only for external consumers.

### B.1 What the adapter must satisfy (so detection reuses verbatim)

1. **Topology advertisement.** Answer `GET /v2/topology` with the standard Camunda fields **plus**
   the `nano` block — extended with an explicit transport hint so the decorator picks the right
   realization instead of blindly trying a WebSocket:
   `{ engine: "nanobpmn", version, embedded: true, transport: "in-process" | "command-stream",
   commandStreamPath? }`. For (a)/(b) `transport` is `in-process` and there is **no**
   `commandStreamPath`, so the decorator must *not* attempt the `/command-stream` upgrade (today's
   `detectNanobpm` keys off the WS `welcome` frame — the decorator needs this hint to use the
   in-process/REST realization without probing a socket). For (c) `transport` is `command-stream` with a
   loopback `commandStreamPath`, and detection proceeds exactly as for remote Nano.
2. **Operation-seam fulfilment.** Realization (a) implements `createProcessInstance` /
   `activateJobs` / `completeJob` / `failJob` / `throwError` as **direct `EmbeddedHost` calls** (see
   B.3) — the decorator already abstracts these two operations, so this is the natural integration
   point and needs no framing. Realization (c) additionally serves the `ClientFrame`/`ServerFrame`
   protocol (`clients/node-stream/src/frames.ts`: client
   `subscribe`/`jobCredits`/`createInstance`/`completeJob`/`failJob`/`throwError`/`awaitInstance`/
   `heartbeat`; server `welcome`/`job`/`commandResult`/`instanceCompleted`/`submissionCredits`/
   `pressure`/`heartbeat`) over the loopback socket, reusing the frame module so `CommandStreamClient`
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
  /** Command-stream channel: a duplex of parsed frames, semantically identical to the WS. */
  openCommandStream(headers?: Record<string, string>): CommandStreamChannel;
}

export interface CommandStreamChannel {
  send(frame: ClientFrame): void;          // createInstance / completeJob / failJob / throwError / subscribe / credits / heartbeat
  onFrame(cb: (f: ServerFrame) => void): void;  // welcome / job / commandResult / instanceCompleted / credits / pressure / heartbeat
  close(): void;
}
```

For realization (a) the SDK's `fetch` and the command-stream client are pointed at the in-process
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
  command-stream channel lives on the main thread; thread workers proxy actions to it).
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
// Camunda (REST) or a remote Nano (command stream) — the decorator detects which.
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
   `{ engine, version, commandStreamPath }`). Absent it, embedded is indistinguishable from a remote
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
