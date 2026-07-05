# ADR 0016 — The Falcon protocol (unified bidirectional command stream)

Status: **Accepted — implemented.** Shipped in `server/src/falcon.rs` (the
`/falcon` WebSocket endpoint), the `@nanobpmn/sdk` client (`clients/node-stream/`),
and the intra-cluster peer uplink. Additive to the REST surface: an unmodified
Camunda 8 client keeps working unchanged.
Date: 2026-07-05.
Relates to: `docs/falcon-design.md` (the originating proposal; §1–12 job-only SSE,
§13 the unified stream, §14 the distributed-parity analysis), `docs/falcon.asyncapi.yaml`
(the wire contract, rendered by the console at `/asyncapi`), `server/src/falcon.rs`,
`clients/node-stream/src/{falconClient,frames}.ts`, and ADRs
0001 (activation fairness), 0002 (leader-local activation + lease digest),
0012 (terminal-state / exporter decoupling), 0013 (SLA modes), 0014 (create
placement). Supersedes the job-only streaming sketch in `docs/falcon-design.md` §1–12.

> **Attribution.** The Falcon protocol is named for **Falko Menge**, whose work on
> system optimisation is the genesis and inspiration for this subsystem. Nano is a
> distillation of Camunda Engineering's expertise; its subsystems are named for the
> engineers who created them. *Artists sign their work.*

## Context

Nano is a Camunda 8 REST drop-in. REST is the right *compatibility* surface — it is
stateless, cacheable, generated from the OpenAPI spec, and lets an unmodified
Camunda 8 client work unchanged — but it is the wrong *hot-path* surface for the two
highest-frequency interactions with the single-writer engine:

- **Job delivery** on REST means busy long-poll fan-out: every idle worker holds a
  request, wakes on `jobs_available`, races to activate, and most lose and re-poll.
  That is retry/thundering-herd behaviour the engine must then absorb.
- **`createProcessInstance` backpressure** on REST is a `503 RESOURCE_EXHAUSTED`
  (main.rs) that pushes flow control back onto the client as retry-with-jitter — a
  herd by another name, on a *separate* rail from job flow control.

The load-bearing facts that make a stream cheap here already existed before Falcon
(see `docs/falcon-design.md` §2): `jobs_available` is a fan-out `Notify`; `try_activate`
already leases on the one engine thread while encoding variable payloads off it;
job leases carry `worker` + `deadline` and **completion is by key alone**, so a
lease can safely expire and be reclaimed by `expire_jobs` with no special dropped-socket
handling. A streaming endpoint is the existing long-poll loop with `return` replaced
by a `yield` into a socket — the same wait/wake/lease cycle, never torn down between
jobs.

The question this ADR settles: **do we move create + the full job lifecycle onto a
single, credit-metered, bidirectional stream — and if so, without forking the
generator or breaking Camunda compatibility?**

## Decision

Add **Falcon**: one persistent, bidirectional WebSocket per client (`/falcon`) that
multiplexes two interaction patterns over a single credit-coordinated window onto the
one engine thread —

- **demand/push (jobs):** the client `Subscribe`s to a job type with a credit count;
  a single server-side dispatcher reacts to `jobs_available`, leases jobs round-robin
  across subscribers (reusing the REST activation + off-thread variable-encoding path),
  and pushes `Job` frames while credits remain.
- **request/response (writes):** `CreateInstance` / `CompleteJob` / `FailJob` /
  `ThrowError` (and the rest of the write surface) funnel to the *same* engine command
  path as the REST handlers, each answered by a `corr`-correlated `CommandResult`.

**The engine core is untouched.** Falcon is purely a new *ingress* to the existing
command/journal path and a new *consumer* of `activate_jobs`. Both ingresses share the
same command path, so a command is never double-applied, and per-connection frame order
is preserved by the journal.

**Reads stay on REST.** `search*`/`get*` are answered from the eventually-consistent
read model; deploy and admin are low-rate control ops. Keeping them on REST preserves
Camunda drop-in compatibility, statelessness, cacheability, and the generate-from-spec
workflow. Falcon carries only engine-bound *writes* + job push.

### Transport

A one-directional SSE channel is insufficient once the client must *send*
high-frequency commands, so Falcon is a genuine **WebSocket** (first-class axum
support, stays on the HTTP stack/port, trivially frames a tagged union). gRPC-bidi
was rejected to avoid a second server stack alongside the REST/axum layer. The
endpoint is therefore **not expressible in OpenAPI** and lives entirely outside the
generated rust-axum surface; the wire contract is published separately as an
**AsyncAPI** document (`docs/falcon.asyncapi.yaml`, rendered by the console at `/asyncapi`).

### Frame protocol (tagged union, `type`-tagged, camelCase)

Client→server (`ClientFrame`): the application surface is `Subscribe`, `JobCredits`,
`CreateInstance`, `CompleteJob`, `FailJob`, `ThrowError`, `AwaitInstance`,
`PublishMessage`, `CancelInstance`, `UpdateJobRetries`, `ResolveIncident`,
`SetVariables`, `ActivateJobs`, plus `Heartbeat`. A set of **intra-cluster-only**
frames (`Deploy`, `InstallDeployment`, `RouteSubscription`, `ForwardCreate`,
`ForwardUserTask`, `GetByKey`, `Raft`, `LeaseDigest`, `Promote`, `SetSlaMode`,
`PressureReport`) reuse the same wire type so a node can act as a Falcon *client* to
its peers — the peer uplink speaks the exact protocol it serves (`ClientFrame` derives
both `Serialize` and `Deserialize` for this reason).

Server→client (`ServerFrame`): `Welcome` (initial submission window + heartbeat
cadence, sent once on connect), `Job` (a pushed activated job, carried as
pre-serialized `RawValue` so the hot path serializes each job exactly once),
`CommandResult { corr, status, body }`, `InstanceCompleted { corr, … }`,
`SubmissionCredits { n }`, `Pressure { level, retryAfterMs? }`, `Heartbeat`.

### Two credit lanes

| lane | granted by | gates | purpose |
|------|-----------|-------|---------|
| **Job-delivery credits** | worker → server | the server *pushing* jobs | worker-side flow control |
| **Submission credits** | server → client | the client *sending* engine-bound writes | unified create-side backpressure |

- A client must hold a **submission credit** to send a `CreateInstance`. The server
  replenishes them from engine `processing` headroom via the existing adaptive
  backpressure controller. Under saturation it **withholds** credits → the client
  stalls intake → **no 503, no client retry, no herd.** This is how
  `createProcessInstance` joins the same coordinated window as activation.
- **Meter intake, not drain.** `CreateInstance` (and message publish) consume the
  scarce submission lane; `CompleteJob`/`FailJob`/`ThrowError` flow unmetered, because
  completing jobs *reduces* backlog — throttling drain would worsen overload. This
  mirrors Zeebe: backpressure applies to user commands, not job completion.

### await-completion is async and resumable

A REST create with `awaitCompletion` holds the HTTP request open until a terminal
state. On Falcon, `CreateInstance{ awaitCompletion:true }` returns an immediate
`CommandResult` (carrying the `processInstanceKey`), and a later `InstanceCompleted`
frame — routed back by `corr` when the exporter projects the terminal event — delivers
the outcome. This removes the long-held per-instance request future (strictly cheaper
under load) and, because the outcome is sourced from the **durable read model**, makes
the await **resumable across reconnect/failover**: a client that persisted the key
re-asserts `AwaitInstance{ corr, processInstanceKey }` on a new socket and is answered
immediately if already terminal, else re-registered. That durability is a parity
*advantage* over Zeebe.

### Lease safety is unchanged

A pushed job that is never completed still reclaims via lease `deadline` expiry
(`expire_jobs`) — the lease *is* the at-least-once guarantee, so a dropped socket needs
no special handling. Falcon adds no new delivery-guarantee machinery.

### Client backpressure ergonomics

Withheld submission credits mean an exhausted client `createInstance` **waits** for the
server to replenish (backpressure without retries). The `@nanobpmn/sdk` client exposes
a bounded alternative — a per-request/-client `submitTimeoutMs` that rejects with a
typed `SubmissionTimeoutError` once the window elapses — so callers can turn an
indeterminate stall into a diagnosable, back-off-able error instead of tight-looping
(which would defeat the credit window). The default remains "wait for capacity"; a
follow-up tracks bringing `submitTimeoutMs` to the other client SDKs.

## Consequences

**What this buys us**
- One credit window and one fair-share scheduler over the single engine thread for
  *all* engine-bound write traffic — the strongest form of "coordinate distributed
  workers, no thundering herd."
- Create-side backpressure with no 503 and no client retry storm; job delivery as
  server-initiated push instead of long-poll fan-out.
- Cheaper, reconnect-durable await-completion.
- **The transport doubles as the inter-node RPC:** the same frames carry raft, lease
  digests, forwarded creates/deploys, cross-partition message routing, and distributed
  pressure/placement gossip (ADRs 0001/0002/0014), so the cluster reuses one wire
  protocol end-to-end.

**Costs / honest limits**
- `/falcon` is **hand-wired**, entirely outside the generated surface. The framing
  types, socket handler, and demux/credit scheduler are hand-written and hand-tested;
  the contract is documented as AsyncAPI rather than enforced by codegen. This is a
  larger hand-written server surface than REST (the tradeoff §13.8 predicted).
- Falcon is **additive, not a replacement.** The REST `createProcessInstance`,
  `activateJobs`, `completeJob`, `failJob` endpoints remain as the Camunda-compatible
  fallback; both ingresses must stay behaviourally identical at the command path.
- A second client posture to support: SDKs must detect the `nano` advertisement and
  upgrade (see `docs/sdk-nano-decorator-design.md`), and fall back to REST when absent.

**Non-goals**
- Moving reads or low-rate control ops onto the stream.
- Any change to engine-core semantics — Falcon introduces none.

## Options considered

1. **Job-only SSE + control POST for credits** (`docs/falcon-design.md` §1–12).
   Rejected: leaves `createProcessInstance` backpressure on a separate 503 rail and
   cannot carry client→server high-frequency commands on one channel.
2. **gRPC bidi** (Zeebe-native). Rejected: a second server stack beside REST/axum for
   no compatibility gain, since REST already provides Camunda parity.
3. **WebSocket, unified create + job lifecycle (this ADR).** One socket, one credit
   model, stays on the existing HTTP stack, frames a tagged union trivially, and — as a
   bonus — becomes the intra-cluster RPC. Chosen.

## Open questions

- Submission-credit policy tuning (fixed window vs AIMD off `processing`; per-connection
  vs global fair share) — the mechanism ships; the policy continues to evolve with the
  SLA modes (ADR 0013) and placement work (ADR 0014).
- Whether to formally publish the frame contract as a versioned SDK artifact vs the
  current "AsyncAPI doc + reference `@nanobpmn/sdk`" posture.
- Bringing the client bounded-wait (`submitTimeoutMs` / typed submission-timeout) to
  the non-JS client SDKs — tracked as a follow-up issue.
