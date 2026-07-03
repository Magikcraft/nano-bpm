# Worker / Connection Balancing & Failover — Design Proposal

> Status: design / proposal only. No engine code is changed by this document.
> The client-SDK half (intelligent failover, accepting a redirect) is explicitly a
> **later** step; this doc specifies the **Nano-side** functionality first.
>
> Grounded in: `server/src/falcon.rs` (`ServerFrame::Pressure` /
> `SubmissionCredits`, the connection `Registry`, cluster job-aggregation), `server/src/memory.rs`
> (`resident_bytes`), `server/src/console/mod.rs` (`/console/api/cluster/metrics`,
> `/console/api/cluster/health`, `build_local_metrics`), `server/src/peer.rs`
> (`PeerLink` / `PeerSet`), `server/src/cluster.rs` (`Topology`, brokers), and the
> `/v2/topology` `nano` advertisement added alongside this work.

## 1. Problem

A stage-1/2/3 cluster gateway is a *full proxy*: a client connected to **any** single
node addresses the whole cluster (create placement forwards cluster-wide — UPDATE 23;
job activation aggregates from every node and routes completions back — UPDATEs 24–25).
That is a correctness win, but it has an operational cost:

- **All connection memory/CPU pressure lands on the one node clients happen to dial.**
  Each long-lived Falcon (WS) or REST long-poll connection costs
  ~0.15–0.2 MB of resident memory plus serde/frame churn on the gateway role
  (UPDATE 81 measured node-0 settling ~200 MB above its peers purely from the gateway
  role, not owned state). A fleet of thousands of workers/producers all pointed at one
  gateway concentrates that load on a single process while peers sit idle.
- **There is no failover signal.** If the node a client is connected to goes away, the
  client has no Nano-provided way to discover and move to a surviving node.

We want a **worker/connection-balancing mode**: Nano senses per-node load, the nodes
share that view, and an overloaded gateway uses the Falcon protocol to tell a subset of
its connected workers/producers to **move** to a less-loaded node — and the same
machinery gives clients the directory they need to **fail over** when a node disappears.

### 1.1 The key enabler: connection placement is decoupled from correctness

Because any gateway is already a full proxy, **where a worker or producer connects is
purely a load-distribution choice, never a correctness one**:

- A **worker** moved to another gateway still receives jobs from the *whole* cluster
  (the new gateway aggregates peer job streams exactly like the old one) and its
  completions still route to the owning partition. Moving it only relocates the
  connection's memory/CPU cost.
- A **producer** moved to another gateway still has its creates placed cluster-wide
  (forwarded to owning partitions). Moving it only relocates the connection cost.

So balancing is a free lever: we can redistribute connections without changing what any
client can do. This is what makes the feature safe to build incrementally.

## 2. What already exists (reused, not rebuilt)

| Capability | Where | Reuse |
| --- | --- | --- |
| Per-node resident memory | `memory::resident_bytes()` (jemalloc) | the primary pressure input |
| Coarse pressure frame | `ServerFrame::Pressure { level, retryAfterMs }`, edge-triggered (falcon.rs ~1419) | the precedent + transport for a finer signal |
| Submission flow control | `ServerFrame::SubmissionCredits` + backpressure AIMD | unchanged; orthogonal to *where* a client connects |
| Connection registry / counts | falcon.rs `Registry`, `nanobpm_stream_connections_active` | per-node connection-load input + the set of redirect candidates |
| Cluster metric aggregation | `/console/api/cluster/metrics` probes every peer's `/console/api/metrics` (resident bytes + connections) | the operator view; proves cross-node sensing already works |
| Cluster health probe | `/console/api/cluster/health` (per-peer reachable/latency) | liveness input for failover/target selection |
| Inter-node transport | `PeerLink` / `PeerSet` over the Falcon protocol | carries the pressure gossip + (optionally) admission checks |
| Peer directory | `Topology` brokers + `/v2/topology` (now `nano`-flagged with `falconPath`) | the failover/redirect target list clients consume |

The cluster-metrics endpoint already demonstrates that a node can read every peer's
resident memory and connection count. The new work is to turn that *observation* into a
*control loop* that emits a per-connection **move** directive.

## 3. Per-node load signal

Each node computes a normalized **pressure scalar** `P ∈ [0,1]` on a short tick (reusing
the idle-purge / metrics cadence, default ~1s), from already-collected inputs:

```
P = max(
      mem_pressure  = resident_bytes / NANOBPMN_MEM_SOFT_LIMIT_BYTES,
      conn_pressure = connections_active / NANOBPMN_MAX_CONNECTIONS_SOFT,
      flow_pressure = commit_inflight / backpressure_limit      // when set
    )
```

- `mem_pressure` is the headline the user named (SSE/WS memory). The soft limit is
  configured, not the hard OS limit, so we shed *before* the existing jemalloc
  idle-purge and OOM territory.
- `conn_pressure` captures the connection-count cost even when payloads are small.
- `flow_pressure` keeps balancing consistent with the existing submission-credit
  backpressure (a node already shedding submissions is "hot").

`P` is deliberately a single scalar so the gossip payload and the policy stay trivial.
The components are kept in the gossip record (below) for observability and future
policy refinement.

## 4. Sharing the view (cluster pressure gossip)

There is **no gossip bus today** — peer links are point-to-point request/response. Two
options:

- **(a) HTTP probe** — reuse the console `cluster/metrics` fan-out. Already implemented,
  but it is an operator-poll path (console-feature only) and is O(N) per poller.
- **(b) Piggyback gossip over the existing PeerLink** *(recommended)* — every node, on
  its pressure tick, pushes a tiny `NodePressure { nodeId, p, mem, conns, headroom,
  ts }` record to each connected peer over the Falcon protocol it is *already* holding
  (`PeerSet`). Each node keeps a `last-known pressure` map (node → record, with the tick
  `ts` for staleness). Cost: one small frame per peer per tick; no new sockets (the
  dedicated Raft lane from UPDATE 49 stays separate).

Staleness is **safe**: a stale pressure estimate can only cause a slightly sub-optimal
redirect (or a redirect that the target declines on arrival — §5.3). It can never affect
correctness because connection placement is decoupled from correctness (§1.1).

Single-node / no-peers: the gossip loop never runs and the map is empty ⇒ the pressure
tick degenerates to a local-only metric and **no redirect is ever emitted** (byte-identical
behaviour, zero overhead — same discipline as every prior cluster feature).

## 5. The rebalancing controller (coordinator-free)

Each gateway independently decides whether to shed some of **its own** connected clients.
No elected coordinator; the decision is local and uses only the gossiped view.

### 5.1 Trigger

On the pressure tick, a node considers shedding when **all** hold:

1. `P_self ≥ NANOBPMN_BALANCE_HIGH` (default 0.75) — I am hot.
2. `∃ peer: P_peer ≤ NANOBPMN_BALANCE_LOW` (default 0.50) and reachable (health probe) —
   there is somewhere meaningfully cooler to send work.
3. `P_self − P_target ≥ NANOBPMN_BALANCE_MIN_DELTA` (default 0.20) — the move is worth it
   (hysteresis; prevents thrash around the threshold).
4. Cooldown since this node's last redirect batch ≥ `NANOBPMN_BALANCE_COOLDOWN_MS`
   (default 5000).

### 5.2 Selection — *which* clients, *how many*, *where to*

- **How many:** at most `K = NANOBPMN_BALANCE_MAX_MOVES_PER_TICK` (default 16), further
  capped so the move does not push the target past `BALANCE_LOW` given its advertised
  **headroom** (§4 record). Move *gradually* and re-measure; never drain the whole fleet
  in one tick.
- **Target:** the least-loaded reachable peer, but the per-target headroom cap plus
  **per-node jitter** prevents every hot gateway stampeding the same cool node.
- **Which connections:** prefer the cheapest-to-move first. Both workers and producers are
  movable (§1.1); a worker mid-lease should ideally be redirected *after* draining its
  current credits (graceful, §5.4). Idle/low-credit connections move first. Connections
  flagged "pinned" (a future client hint) are never moved.

### 5.3 Target admission (avoid the new hot spot)

Before/at redirect, the gossiped `headroom` is the soft guard; for correctness against
stampede the **target may decline**: the redirect carries no obligation, and when the
moved client dials the target, the target — if it has since crossed `BALANCE_HIGH` — can
immediately emit its *own* redirect or simply accept (the client is no worse off than
before). Optionally the controller can do a cheap `PeerLink` `admit?(headroom)` check
before emitting, but the gossiped headroom is expected to be sufficient.

### 5.4 The directive — a new `ServerFrame::Redirect`

Add one server frame (additive; older clients ignore unknown frames — opt-in by design):

```rust
ServerFrame::Redirect {
    reason: String,        // "rebalance" | "draining" | "shutdown"
    target: RedirectTarget // { nodeId, baseUrl, falconPath }
    deadline_ms: u64,      // grace period to migrate before the node may hard-close
    scope: String,         // "connection" (this socket) — room for "all" later
}
```

`target` is built from `Topology`/`/v2/topology` broker data (host:port +
`falconPath` from the `nano` advertisement). Semantics for a cooperating client
(SDK work, later):

1. On `Redirect`, open a **new** Falcon/long-poll connection to `target`.
2. Re-subscribe (worker) / re-point creates (producer) on the new connection.
3. Drain in-flight work on the old connection, then close it within `deadline_ms`.
4. If the new connection fails, fall back to the topology directory (§6) and try the
   next node — never the node being drained.

**At-least-once is preserved** end-to-end: an un-acked job on the old connection's lease
simply expires on its owning partition and re-activates (the standard failover path);
creates are idempotent / forwarded. A worker that ignores `Redirect` keeps working — it
just doesn't relieve the pressure (acceptable; the feature is opt-in and advisory).

### 5.5 `draining` / `shutdown`

The same frame, with `reason="draining"`, lets a node being taken down for maintenance
push *all* its connections elsewhere first (graceful rolling restart) — a natural
extension once `scope="all"` is honored.

## 6. Failover directory

The same node addressing data powers client failover when a node **disappears**
(no `Redirect`, the connection just drops):

- `/v2/topology` already lists every broker's host:port and (via the `nano`
  advertisement) the `falconPath`. A nano-aware SDK caches this on connect and
  refreshes periodically; on a dropped connection it dials the next healthy broker from
  the cache.
- `/console/api/cluster/health` (or the gossiped liveness) tells the *server* which peers
  are up, so a `Redirect` is never issued toward a dead node. The client side mirrors
  this with its own reconnect-and-probe loop.

No central registry is required: the topology *is* the directory, and it is already
advertised on a stable Camunda endpoint.

## 7. Protocol & surface changes

- **Falcon**: add `ServerFrame::Redirect` (+ `RedirectTarget`) and document it in
  `docs/falcon.asyncapi.yaml` and `docs/falcon-design.md`. Add the
  per-tick `NodePressure` gossip frame to the **peer** protocol (internal; not a client
  frame).
- **console**: surface per-node pressure `P` + its components, redirects-issued /
  accepted counters, and migration counts in the Metrics cluster section (builds on the
  existing cluster aggregation; nothing new to sense).
- **`/v2/topology`**: no further change — the `nano` field added with this work already
  advertises `falconPath`, which is exactly what a redirect/failover target needs.
- **No spec/generated change** for the client API: `Redirect` rides the existing
  Falcon WS (not the OpenAPI surface), consistent with `Pressure` /
  `SubmissionCredits`.

## 8. Configuration (all opt-in; default OFF ⇒ byte-identical)

| Env | Default | Meaning |
| --- | --- | --- |
| `NANOBPMN_WORKER_BALANCING` | `off` | master switch (sensing + gossip + redirects) |
| `NANOBPMN_MEM_SOFT_LIMIT_BYTES` | unset | denominator for `mem_pressure` |
| `NANOBPMN_MAX_CONNECTIONS_SOFT` | unset | denominator for `conn_pressure` |
| `NANOBPMN_BALANCE_HIGH` | `0.75` | shed above this pressure |
| `NANOBPMN_BALANCE_LOW` | `0.50` | a peer must be at/below this to be a target |
| `NANOBPMN_BALANCE_MIN_DELTA` | `0.20` | minimum self−target improvement to act |
| `NANOBPMN_BALANCE_COOLDOWN_MS` | `5000` | per-node redirect-batch cooldown |
| `NANOBPMN_BALANCE_MAX_MOVES_PER_TICK` | `16` | cap on connections moved per tick |
| `NANOBPMN_BALANCE_GOSSIP_MS` | `1000` | pressure tick / gossip cadence |

Following repo convention (history-cap, admission, fairness, backpressure all opt-in),
the master switch defaults OFF so existing benchmarks and the single-node path are
unchanged.

## 9. Invariants

1. **Single-node / no-peers**: no gossip, no redirect, zero added hot-path cost
   (byte-identical) — gated by an empty peer set, not just the env read.
2. **Correctness independent of placement**: a `Redirect` only relocates a connection;
   what the client can do is unchanged (full-proxy property, §1.1).
3. **At-least-once preserved**: moving (or losing) a connection relies on the existing
   lease-expiry + idempotent-create paths; no new durability surface.
4. **Advisory, not mandatory**: a client that ignores `Redirect` keeps functioning;
   balancing is best-effort relief, never a hard dependency.
5. **No central coordinator**: every node decides locally from the gossiped view;
   staleness only costs an occasional sub-optimal move.

## 10. Staged rollout

- **S1 — Sense + gossip + observe (Nano-side, ship first):** pressure scalar `P`, the
  `NodePressure` peer gossip, the last-known map, and the console surface. No client
  impact; pure observability. Lets us *measure* the imbalance before acting on it
  (measure-first, as with cluster fairness Stage 1).
- **S2 — Advisory `Redirect`:** the controller (§5) emits `ServerFrame::Redirect`;
  counters in the console. Server-complete and testable in-process even before any SDK
  honors it (assert the frame is emitted to the right connections under synthetic
  pressure).
- **S3 — Client migration + failover (SDK, separate downstream effort):** decorate the
  generated Camunda SDKs to accept `Redirect`, cache the topology directory, and fail
  over on drop. This is the "intelligent client" step the user flagged as later.
- **S4 — Drain/maintenance (`scope="all"`):** graceful rolling restarts on top of S2/S3.

## 11. Open questions / risks

- **Worker affinity:** none today, but a future "process-affinity" hint (keep a worker
  near a partition it predominantly serves) could reduce cross-node aggregation hops;
  out of scope here (placement is currently correctness-neutral, so affinity is pure
  optimization).
- **Oscillation:** hysteresis (`MIN_DELTA`), cooldown, per-tick move cap, and per-target
  headroom + jitter are the anti-thrash controls; S1's measurement should validate the
  defaults before S2 ships.
- **Pinning:** clients may want to pin to a node (locality, tenancy); reserve a future
  client→server hint so the controller skips pinned connections.
- **Heterogeneous nodes:** `P` normalizes by per-node soft limits, so a bigger node with
  a higher limit naturally absorbs more before being "hot"; verify the soft limits are
  set per node, not globally.
