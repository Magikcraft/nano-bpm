# Nano-Aware SDK Decorator — Design Proposal

> Status: design / proposal only. No SDK code is changed by this document.
> Companion to `docs/worker-balancing-design.md` (the server side) and the `/v2/topology`
> `nano` advertisement shipped in commit `8484633`.
>
> Goal: make the five generated Camunda SDKs in `~/workspace/orchestration-cluster-api-*`
> **Nano-aware** without forking their generators. When connected to a stock Camunda
> gateway they behave **byte-identically** to today; when connected to a nanobpmn gateway
> they (a) upgrade `createProcessInstance` and job workers to the `/command-stream`
> WebSocket, and (b) dynamically migrate between cluster nodes for load-balancing and
> failover.
>
> Grounded in a read of all five SDKs, with the JS SDK
> (`~/workspace/orchestration-cluster-api-js`) as the reference implementation.

## 1. Why a decorator, and why it's safe

Every SDK is built as **generated core + hand-written runtime wrapper**, regenerated
through a hooks pipeline that never touches the runtime layer:

| SDK | Generated | Hand-written seam | Generator |
| --- | --- | --- | --- |
| JS | `src/gen/*` | `src/runtime/*`, `src/template/*` | `@hey-api/openapi-ts` (hooks/pre → openapi-ts → hooks/post, `scripts/run-pipeline.ts`) |
| Python | `generated/` | `src/runtime/` | openapi-generator (httpx) |
| Rust | `client/` | `src/runtime/` | openapi-generator (reqwest) |
| Go | `internal/` | `pkg/camunda` | oapi-codegen |
| C# | `Sdk/Generated/` | `Sdk` + `Sdk.Generator` | custom generator |

So Nano-awareness lives **entirely in the hand-written runtime seam** — it survives
regeneration and never forks the generator. This matches the user's intuition:
*consume the SDKs as peer repositories and patch the runtime layer with a decorator.*

**Idempotency against Camunda is structural, not conditional sprinkles.** The decorator
is a strict superset: it probes topology once on connect and, absent the `nano` field,
every code path is the existing REST path. Presence of `nano` is the *single* switch.

## 2. The single switch: `/v2/topology` `nano` advertisement

The server now returns (commit `8484633`):

```jsonc
// GET /v2/topology
{ "...": "standard Camunda fields",
  "nano": { "engine": "nanobpmn", "version": "0.1.0", "commandStreamPath": "/command-stream" } }
```

- `getTopology` already exists on every generated client (JS:
  `gen/CamundaClient.ts:9772`, `/v2/topology`). It is **not** auto-called on connect today
  — adding that probe is the first decorator hook.
- `nano` **absent** ⇒ stock Camunda ⇒ pass-through (no WS, no migration). `nano`
  **present** ⇒ upgrade. The decorator caches the result and the broker directory
  (host:port + `commandStreamPath`) for migration/failover targets.

## 3. The two (and only two) insertion seams

Using JS as the reference; the same two seams exist in every SDK's runtime layer.

### 3.1 Transport seam — connection, detection, migration

The whole client funnels through one client construction:

```ts
// gen/CamundaClient.ts:1345-1362
this._client = createClient({ baseUrl: this._config.restAddress, fetch: this._fetch, ... });
installAuthInterceptor(this._client, ...);
```

Plus request/response interceptors (`gen/client/client.gen.ts:37-43`) and an existing
retry boundary (`runtime/retry.ts`, `_invokeWithRetry`). This single chokepoint hosts:

- **Detection:** probe topology on connect (§2), set `this._nano = topology.nano ?? null`.
- **Node directory:** cache `topology` brokers; refresh periodically and on every
  topology response.
- **Failover:** on a dropped connection / unreachable node, swap `baseUrl` to the next
  healthy broker and replay. **Auth survives a node swap** because the OAuth cache key is
  `oauthUrl|clientId|tokenAudience|scope` (`runtime/auth.ts:61`), independent of the node
  URL — so only the base URL changes, the token is reused.
- **Redirect:** when a `ServerFrame::Redirect` arrives on the command stream (server-side
  worker-balancing design §5.4), the same swap-baseUrl-and-reconnect path executes
  voluntarily.

### 3.2 Operation seam — only two methods upgrade

- **`createProcessInstance`** (`gen/CamundaClient.ts:4437`): today it builds an envelope,
  gates validation, then `Sdk.createProcessInstance(opts)` over REST inside
  `_invokeWithRetry`. The decorator branches at the call site: if `this._nano`, submit the
  create over the command stream (a `ClientFrame` create, correlated by `corr`, awaiting a
  `CommandResult` / `InstanceCompleted` frame) instead of the REST POST. Validation,
  tenant-default injection, and response shaping are unchanged (they wrap the transport).
- **Job workers** — the high-leverage seam. `JobWorker._poll` depends on exactly **one**
  client method: `this._client.activateJobs(body)` (`runtime/jobWorker.ts:233`), and the
  job-action methods for complete/fail. **Nothing else in the loop is transport-specific.**
  If the nano-aware client's `activateJobs` is fed by command-stream `Job` pushes (and
  `completeJob`/`failJob`/`throwError` write `ClientFrame`s to the same socket), the
  **entire** worker loop — including `ThreadedJobWorker` and the `clientProxy`
  MessagePort path — upgrades to the command stream with **zero changes to worker code**.

This is the core insight: the worker abstraction is already transport-agnostic. The
decorator changes *what `activateJobs` means*, not the polling loop.

## 4. The shared piece: a command-stream client module

The two seams are trivial wiring; the substance is one robust WS module
(`runtime/commandStream.ts` in JS, mirrored per language). It owns:

- **Lifecycle:** connect to `${baseUrl-host}${commandStreamPath}`, `Welcome`
  (submission credits + heartbeat cadence), heartbeats, reconnect/backoff.
- **Correlation:** a `corr → pending promise` map; route `CommandResult` /
  `InstanceCompleted` back to the awaiting `createProcessInstance` / `completeJob`.
- **Job delivery:** push `Job` frames into the worker's `activateJobs` consumer; respect
  job-delivery credits.
- **Submission flow control:** honor `SubmissionCredits`; this supersedes the REST 429
  loop of `BackpressureManager` (`runtime/backpressure.ts`) when on the command stream
  (the existing manager stays for the REST/Camunda path).
- **Control frames:** `Pressure` (throttle) and the new `Redirect` (migrate, §3.1).

`createProcessInstance`, the worker, and the transport seam all share this one module.

## 5. At-least-once & correctness invariants (client side)

- **Connection placement is correctness-neutral** (server design §1.1: any gateway is a
  full proxy). So failover/redirect can move a worker or producer freely; jobs un-acked on
  a dropped socket expire on their owning partition's lease and re-activate; creates are
  idempotent / forwarded. The client never has to reason about partitions.
- **A nano client talking to Camunda** never opens a WS (no `nano` field) ⇒ identical to
  today. **A worker that ignores `Redirect`** keeps working (advisory only).
- **Validation, tenant defaults, telemetry, retry classification** all live *outside* the
  transport swap, so they apply identically on REST and command-stream paths.

## 6. Rollout (reference-first, then port)

- **S1 — JS detection + command-stream upgrade (testable now):** topology probe, the
  `commandStream.ts` module, `createProcessInstance` + worker upgrade. Testable against a
  running nano gateway immediately — the server already speaks the command stream. No
  rebalancing needed yet.
- **S2 — JS failover:** broker directory + swap-baseUrl-and-reconnect on drop. Test by
  killing the connected node in a 2-node cluster.
- **S3 — JS redirect:** consume `ServerFrame::Redirect`; gated on the server-side
  worker-balancing design S2 existing. Until then, S1+S2 stand alone.
- **S4 — Port the proven shape** to python → rust → go → csharp. Same two seams, same
  shared module contract; the risk is all retired in the JS reference.

Dependency note: S1/S2 depend only on shipped server behavior (command stream +
`/v2/topology` nano flag). S3 depends on `docs/worker-balancing-design.md` S2.

## 7. Design decisions / non-goals

- **Decorator, not a parallel `NanoJobWorker`.** Make `activateJobs` /
  `createProcessInstance` polymorphic on transport so there is one worker code path and
  one test surface.
- **No generator fork, no spec change for the client API.** The upgrade rides the existing
  command-stream WS (like `Pressure`/`SubmissionCredits`), not the OpenAPI surface.
- **Do JS end-to-end before touching the other four.** The seam is identical across
  languages; proving it once de-risks the ports.
- **Out of scope here:** the per-language port details, worker process-affinity hints,
  and client-side "pin to node" — all follow once the JS reference lands.

## 8. Open questions

- **Threaded worker + WS:** the `ThreadedJobWorker` runs handlers in worker_threads and
  proxies client calls over a MessagePort (`runtime/clientProxy.ts`). The command-stream
  socket should live on the **main thread** (one socket per client), with thread workers
  continuing to proxy `completeJob`/`failJob` to it — verify the proxy already routes
  these (it forwards arbitrary methods, so likely yes).
- **Long-poll semantics:** `activateJobs` long-poll `requestTimeout` has no direct WS
  analog; on the command stream, job delivery is push + credits, so the worker's
  `pollTimeoutMs` becomes a no-op on nano (document it, keep it for the Camunda path).
- **Mixed fleets:** a client pointed at a load balancer in front of mixed engines should
  detect per-connection; detection is per-`CamundaClient` instance, which is the right
  granularity.
