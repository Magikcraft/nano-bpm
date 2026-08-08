# ADR 0055 — nano-sdk is the transport spine (one engine client, exposed to authors)

Status: Accepted
Date: 2026-08-01
Extends: ADR 0054 (one code-first stack), ADR 0053 (derivation is a shared library), ADR 0052 (decoupled Urban runtime)
Supersedes: the "`@nanobpm/workflow` is dependency-free / pure REST v2" invariant (ADR 0044)
Repo: nanobpm/nano-ide (`packages/workflow`, `packages/urban`, `packages/create-urban-app`)

## Context

Even after consolidating to one code-first stack (ADR 0054), the stack talks to the engine in
**four** different, hand-rolled ways:

| # | Surface | What it hand-rolls |
|---|---|---|
| 1 | `RestEngineClient` (`packages/urban/src/runtime/engine/rest.ts`) | deploy / createInstance / publishMessage / userTasks / job worker, over REST |
| 2 | `WorkflowClient` (`packages/workflow/src/client.ts`) | deploy / start / signal / job activate / complete / fail, over REST |
| 3 | Generated `worker-sdk.ts` (materialised per app, ~18KB) | job activation + completion |
| 4 | `createNanoSdkEngineClient` (`.../engine/nanosdk.ts`) | Falcon **only** for instance creation; delegates every cold path back to (1) |

This is duplicated connection logic, four times, each with its own bugs, retries, and version
skew. (4) is not even a real consolidation — it upgrades the one hot path and falls back to (1)
for everything else.

Meanwhile a single, cross-platform client already exists and is maintained on its own:
`@nanobpm/nano-sdk`. It is a **drop-in replacement for `@camunda8/orchestration-cluster-api`**
whose `createCamundaClient` returns the full upstream Orchestration Cluster client, wrapped in a
proxy that transparently upgrades the two throughput-critical paths — `createProcessInstance` and
`createJobWorker` — to the Nano **Falcon** protocol, with REST for everything else. It is
fetch-based and Node/Deno-compatible by design, and it also offers an **`embedded`** transport
(in-process host, no separate gateway).

The upstream client already exposes every verb the four surfaces above hand-roll:

`createDeployment · createProcessInstance · cancelProcessInstance ·
createJobWorker/activateJobs/completeJob/failJob · publishMessage/correlateMessage ·
broadcastSignal · completeUserTask/assignUserTask/updateUserTask · evaluateDecision (DMN)`

## Decision

**`@nanobpm/nano-sdk` is the Urban stack's single engine-transport spine.** Delete the hand-rolled
transports; every path to the engine goes through one nano-sdk client. And **expose that client to
app authors**, so Urban is not a walled garden: an author can reach any engine capability
(decisions, user tasks, signals, message correlation, raw job workers) to build novel applications
— the "Borland Delphi for process apps" goal.

Invariants:

1. **One client, hard-depended.** `@nanobpm/nano-sdk` is a normal (non-optional) dependency of
   `@nanobpm/workflow` and `@nanobpm/urban`. This **reverses** ADR 0044's deliberate
   "`@nanobpm/workflow` is dependency-free / pure REST" property. We accept that trade knowingly:
   one obvious way to talk to the engine, no duplicated connection logic, is worth more than a
   standalone-with-just-`fetch` workflow package. nano-sdk is itself cross-platform, so Node/Deno
   portability is preserved.
2. **Transport is not our code.** `@nanobpm/workflow` keeps what is genuinely its own — the
   `defineFlow` derivation, the BPMN/DI emission (ADR 0044/0047 + the diagram layout of the
   deploy path), and the `Worker`'s handler-routing / replay semantics — and delegates **all**
   engine I/O to nano-sdk. `WorkflowClient` becomes a thin convenience over a nano-sdk client;
   `Worker` runs on `createJobWorker`.
3. **The runtime seam stays, the implementation collapses.** The runtime keeps the small
   `EngineClient` interface as a **test/mock seam**, but ships exactly one implementation — a
   nano-sdk-backed adapter. `RestEngineClient`'s hand-rolled plumbing and the create-only Falcon
   split in `nanosdk.ts` are deleted.
4. **Authors get the raw client.** `@nanobpm/urban` re-exports the nano-sdk client factory and
   types, and the runtime hands each running app a pre-configured client through its
   `AppApi`/context, so handlers and workers can call any engine verb with the app's own
   connection.
5. **Derivation is untouched.** This ADR changes *how bytes reach the engine*, not *what is
   derived*. The ADR 0053 toolkit and `urban gen` are orthogonal; the diagram-interchange (DI)
   layout added to the deploy path stays — only the final `createDeployment` call changes.

## Sequencing

Each step is independently shippable and gated by the existing gateway integration tests
(`packages/workflow/test/integration.test.ts` deploys + runs a real model end-to-end):

1. **`@nanobpm/workflow` → nano-sdk.** Hard-dep nano-sdk; rewrite `WorkflowClient` (deploy/start/
   signal/getInstance) and `Worker` onto the SDK; delete the REST plumbing in `client.ts`. Keep
   `defineFlow`, the emitters, and `toDeployableBpmn`/DI. Prove parity with the integration tests.
   **Done** — `WorkflowClient` wraps `createCamundaClient` and exposes it as `.sdk`; the `Worker`
   runs one `createJobWorker` per derived job type. See the transport-selection note below.
2. **urban runtime → one `EngineClient`.** Replace `RestEngineClient` + `createNanoSdkEngineClient`
   with a single nano-sdk-backed adapter behind the `EngineClient` interface; keep a mock for unit
   tests.
3. **Expose to authors.** Re-export the client from `@nanobpm/urban`; thread a pre-built client
   into `AppApi`/context; document the escape hatch.
4. **Delete generated transport (folds into ADR 0053 Move 1).** The per-app `worker-sdk.ts`/
   `data-sdk` *connection* code is removed, not relocated — workers run on nano-sdk.

## Consequences

- Four transport surfaces collapse to one; a whole class of connection bugs and version skew
  disappears.
- Urban apps gain first-class access to **DMN decisions, user tasks, signals, and message
  correlation** for free, unlocking applications the manifest/flow surface never anticipated.
- The **`embedded`** transport becomes reachable from Urban — a local, single-process app with no
  separate gateway, which directly serves the "install it on your computer like Delphi" use case
  (prototyped separately as a follow-up).
- Package count is unchanged (still `@nanobpm/workflow`, `@nanobpm/urban`, `create-urban-app`);
  nano-sdk is one shared external dependency.
- Cost: `@nanobpm/workflow` is no longer usable with only `fetch`, and workflow/urban releases now
  track nano-sdk's API. Both are jwulf-owned, so the coupling is acceptable; pin nano-sdk with a
  caret range and cover it with a Deno smoke in CI.
- Risk: nano-sdk becomes a single point of failure for all engine I/O. Mitigated by the mock
  `EngineClient` seam (tests don't need a gateway) and by nano-sdk's own REST fallback inside the
  proxy.

### Transport selection: Falcon for creation, REST for job serving

nano-sdk upgrades two verbs to the **Falcon** protocol — `createProcessInstance` and
`createJobWorker` — and speaks REST for everything else. The two Falcon paths have different
operational needs:

- **Instance creation** is throughput-critical (it hits the single-writer engine actor), so the
  `WorkflowClient` defaults to `transport: "auto"` (Falcon on a Nano server, REST elsewhere).
- **Job serving** is resilience-critical: the ADR 0044 property is that a worker survives an engine
  SIGKILL + restart. REST long-poll delivers that (it reconnects with backoff on each activation),
  but the Falcon **push** transport does not yet recover a *mid-stream* disconnect — on an engine
  restart it emits an unhandled `falcon connect failed` reconnect rejection and gives up after one
  attempt (tracked by **jwulf/nano-sdk-js#3**). So the `Worker` defaults to `transport: "rest"`
  (override via `WorkerOptions.transport`). The workflow crash-resilience integration test is
  pinned to REST for the same reason.

Both paths run through the one nano-sdk client, so this is still a single transport spine — the
choice is only which protocol the SDK negotiates. When #3 lands, the `Worker` default can flip to
`"auto"` with no API change.
