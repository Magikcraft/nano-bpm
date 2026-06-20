# nanobpmn

**A Rust research engine exploring high-performance BPMN execution and Camunda 8 compatibility.**

A self-contained Rust code-generation project for the Camunda 8 Orchestration
Cluster REST API. It bundles a copy of the OpenAPI specification and generates a
Rust REST layer (models + `axum` router + service traits) from it, plus a
runnable stub server.

No backend services are wired into *most* of the REST layer: operations respond
with `501 Not Implemented`. A few operations are now backed by the embedded
**`engine-core`** BPMN engine as a proof of the REST → engine path:

- `POST /v2/deployments` (`createDeployment`) parses the uploaded BPMN 2.0 XML
  resources and deploys them, assigning each process a key and a per-id version.
  Deployment is **idempotent**: redeploying a process that is byte-for-byte
  identical to the current latest version of the same id reuses that version
  (no new key, no version bump, nothing journaled), mirroring Zeebe; a changed
  model deploys as the next version.
- `POST /v2/process-instances` (`createProcessInstance`, by `processDefinitionId`
  or `processDefinitionKey`) starts a real instance and returns its
  engine-assigned key. Variables supplied on the request seed the root scope.
  With `awaitCompletion: true` the request blocks (off the engine write lock)
  until the instance reaches a terminal state or `requestTimeout` ms elapse
  (default 5s). The response carries a `processCompleted` flag: when `true`, the
  returned `variables` (optionally narrowed by `fetchVariables`) are the
  authoritative final result; when `false`, the instance is still running.
  **Deviation from Camunda:** on timeout nanobpmn returns `200` with
  `processCompleted: false` and the `processInstanceKey` (so the caller can poll)
  rather than Camunda's `504`.
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
  (e.g. a refund/compensation flow); otherwise the error propagates up any
  enclosing **embedded sub-process** to a sub-process error boundary (which
  terminates the whole sub-process scope), and if still uncaught an incident is
  raised. The job is consumed either way. Returns `404` for an unknown or
  un-activated job and `409` if the job is no longer active.
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
- `POST /v2/messages/publication` (`publishMessage`) and
  `POST /v2/messages/correlation` (`correlateMessage`) deliver a message to any
  open subscription whose name and correlation key match, releasing a **message
  intermediate catch event**'s token, interrupting an activity via an
  **interrupting message boundary event**, or spawning a parallel token via a
  **non-interrupting message boundary event** while the activity keeps running
  (such boundary events attach to a service task **or an embedded sub-process** —
  an interrupting one on a sub-process tears down its whole inner scope)
  (merging the message's variables into
  the instance first). Messages are **not buffered** (no TTL/dedup): with no
  match the message is dropped. `publishMessage` always returns `200` with the
  minted `messageKey`; `correlateMessage` returns `404` when nothing correlates
  (and otherwise `200` with the first correlated `processInstanceKey`). A
  **message start event** also correlates here: a matching `correlateMessage`
  **creates a new instance** (seeded with the message's variables) and returns its
  `processInstanceKey`.
- **Event-triggered instance creation.** Deploying a process whose start event is
  a **message start event** opens a process-level subscription (a matching
  `correlateMessage` creates an instance), and a **timer start event** arms a
  process-level timer fired by the background tick — a one-shot `timeDuration` runs
  once, a recurring `timeCycle` (`R/PT…`) creates an instance every interval. Both
  are journaled, so the subscription/schedule survives a restart.

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
- `POST /v2/variables/search` (`searchVariables`) and
  `GET /v2/variables/{variableKey}` (`getVariable`) — expose instance variables.
  nano keeps a single instance-level scope, so every variable's `scopeKey`
  equals its `processInstanceKey`. Values are reported as serialized JSON
  (a string `text` as `"text"`, numbers/booleans bare); `searchVariables`
  truncates long values unless `truncateValues=false` and flags `isTruncated`.
  Variable keys are assigned by the read model (the engine does not mint them).

All search endpoints implement the full v2 query contract:

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
ticks: `TriggerTimers` fires every due **timer intermediate catch event**,
**timer boundary event** (interrupting or non-interrupting, on a service task or
embedded sub-process; a `timeCycle` non-interrupting boundary **re-arms** for the
next interval on each fire) and **timer start
event** (durable — journaled), and `ExpireJobs`
releases activation locks past their deadline (volatile — not journaled). When a timer fires it may unblock
downstream work, so the tick wakes any long-polling `activateJobs`. A timer
parked before a restart is recovered by replay and fired by the first due tick
afterwards.

### Concurrency (read/write lock)

The server runs on a multi-threaded Tokio runtime (one worker per core), so
connection handling, HTTP parsing and JSON (de)serialization already spread
across cores. The engine itself is a **single writer**, so it sits behind a
`RwLock`: mutating operations (create instance, complete/fail job, deploy, job
activation, the background tick) take the **write** lock and are serialized,
while the read-only `search*`/`get*`/`topology` projections take the **read**
lock and therefore run **concurrently across cores**. The lock is never held
across an `.await`, and critical sections are short. A write enqueues its events
to the journal writer thread *under* the write lock (preserving command order),
then releases the lock and `.await`s durability outside it, so disk I/O no longer
serializes behind the engine lock; read-heavy load scales with the number of
cores.

### Durability (group-commit + fsync)

The journal is owned by a dedicated `nanobpmn-journal-writer` thread. Each
mutating command serializes its events and hands them to the writer over an
ordered channel, receiving a `Commit` handle. The writer batches every queued
request into a single `write_all` followed by one `sync_all` (**fsync**), then
acks each batched command — amortizing the fsync cost across concurrent writes
(**group commit**). A handler only returns its success status *after* its
`Commit` resolves, i.e. after the events are durably on disk, so a `200`/`204`
means "persisted and will survive a crash". If a journal write ever fails the
writer aborts the process rather than serve state that outran the durable log;
on restart the log replays to re-derive consistent state. Fire-and-forget
internal writes (the background timer tick, startup seeding) drop the `Commit`,
since replay re-derives their effects.

> [!NOTE]
> This uses `std::sync::RwLock`, whose fairness is platform-dependent and can
> favour readers. If sustained read saturation ever starves writers (including
> the background tick) in your deployment, switch to a writer-fair lock such as
> `parking_lot::RwLock`, or publish a periodic read snapshot so reads never
> contend with the writer at all.

### Read model (CQRS, eventual consistency)

The engine's hot state holds only what execution needs. If every completed
process instance, job and incident stayed there forever, resident memory would
grow without bound under sustained load (and never come back down). nanobpmn
follows the Camunda 8 / Operate split — a **command side** and a separate
**read side** — collapsed into a single binary:

- **Command side** — the engine + journal. It runs in-memory and is the single
  writer. Once an instance's history is durably recorded in the read model, the
  engine **evicts** the completed instance and everything it owns (jobs, timers,
  subscriptions, incidents) from hot state, so the footprint tracks *in-flight*
  work rather than all history. (Eviction is opt-in: `engine-core` keeps
  completed instances by default so embedders' audit/query code keeps working.)
- **Read side** — an embedded **SQLite read store**. A background
  `nanobpmn-exporter` thread streams the journal's event log into it (in command
  order, via the same under-the-write-lock hand-off the journal writer uses),
  projecting events into denormalized `process_definitions` / `process_instances`
  / `jobs` / `incidents` / `variables` tables. **Every `search*`/`get*` query is
  served from SQLite**, never from hot engine state — which is what makes eviction
  possible.

The read model is a pure **derived projection** of the journal, so it needs no
durability of its own: on boot the server replays any journal events the store
has not yet projected (tracked by an `exported_position` cursor; a fresh or
schema-mismatched store rebuilds from scratch), then evicts completed instances
from the recovered hot state.

Because the exporter is asynchronous, reads are **eventually consistent**: a
`search`/`get` issued in the instant after a write may briefly not observe it
(typically sub-millisecond). This mirrors Camunda 8's Operate read channel.

> [!NOTE]
> Activation locks are volatile and **not journaled**, so `activateJobs` /
> lock-expiry never reach the read store. A `searchJobs` result therefore
> reflects durable state: an activated job shows as `CREATED` without a worker
> or deadline. This is deliberate — the read model reports what survives a crash.

**Configuration.** Reads and writes share a data layout resolved from the
environment:

| Variable | Effect |
| --- | --- |
| `NANOBPMN_DATA_DIR=<dir>` | Co-locates both under one directory: `<dir>/journal.jsonl` + `<dir>/read-model.sqlite` (created if absent). |
| `NANOBPMN_JOURNAL=<file>` | Back-compat: selects the journal file; the database is `NANOBPMN_READ_DB` if set, else a sibling `read-model.sqlite`. |
| *(neither set)* | Fully in-memory: an ephemeral journal and a `:memory:` read store. Nothing is persisted. |

### Memory footprint (allocator + idle reclamation)

Load arrives in bursts: a flood of `createProcessInstance`s grows the hot-state
maps and 50 KB-class variable payloads, which are then freed as instances
complete and are evicted. Two things keep that freed memory from coming back:

- **Rust maps never shrink on removal.** Eviction removes the entries but leaves
  the `instances` / `jobs` / index maps at *peak* bucket capacity.
- **The default system allocator hoards freed pages.** macOS libmalloc (and Linux
  glibc) keep freed allocations in their own free lists rather than returning them
  to the OS, so an idle server pins its peak resident footprint long after the
  work is gone.

nanobpmn addresses both:

- It uses **jemalloc** as the global allocator (vendored, built from source — the
  binary stays self-contained). jemalloc returns unused pages to the OS on a
  **decay** schedule, driven by a background thread on Linux.
- An **idle-purge tick** closes the gap on platforms with no jemalloc background
  thread (macOS) and makes reclamation prompt everywhere: when the engine goes
  quiescent after a burst (no command exported and no create in flight for the
  quiescence window), it `shrink_to_fit`s the hot-state maps and forces jemalloc
  to purge every arena, returning the freed memory to the OS **immediately**. It
  fires once per active→idle transition, never while work is flowing, so it adds
  no steady-state cost. (In a local run, a 4 000-instance create+complete burst's
  idle tick logged `returned 156.9 MiB to the OS (214.4 -> 57.5 MiB resident)`.)

This is distinct from the **variable-spill** tier (below), which bounds the
*live* peak of a large *active* backlog by moving cold parked instances' variables
to disk; the allocator/idle-purge work bounds *idle* footprint after a backlog has
drained. Together: low idle memory, bounded live memory, full in-RAM speed for the
working set.

| Variable | Effect |
| --- | --- |
| `NANOBPMN_IDLE_PURGE_MS=<n>` | Quiescence (ms) the server must be idle before it compacts hot state and returns freed memory to the OS. Default `5000`; `0` disables the idle-purge tick. |

### Tiered hot state: variable spill and cold spill

The engine's hot state lives in RAM for speed, but two workloads make an unbounded
resident footprint a problem: a large *active* backlog (many instances parked on a
job, each carrying a 50 KB-class variable payload) and a large *dormant* backlog
(many long-lived instances parked on a timer or message, idle for minutes to days).
nanobpmn pages both classes out to disk and rehydrates them on demand, mirroring how
Camunda 8 / Zeebe back hot state with RocksDB. Both tiers reuse one SQLite store
(`<data-dir>/var-spill.sqlite`) and are **derived caches** of the durable journal — a
lost spill blob is reconstructable, so the store runs `synchronous=NORMAL`.

- **Variable spill** sheds just the *variables* of instances parked on a job once the
  resident parked-backlog exceeds a hot budget, and rehydrates them on `activateJobs`
  (the moment a worker needs them). It targets the high-throughput flood case and
  leaves the small control-state maps resident. It deliberately skips instances
  holding a timer or open message subscription, since those resume without an
  activation seam.

- **Cold spill** evicts whole *dormant* instances — control state, jobs, timers,
  subscriptions, variables — when resident RAM crosses a high-water mark, keeping only
  a slim resident **routing index** (job keys, message correlation keys, timer
  due-times) so an off-heap instance can still be found. It rehydrates an instance the
  instant an event targets it: a worker poll for its job type, a command addressing it
  by key, a correlating message, or its timer falling due. It targets the
  many-long-lived-parked-instances case and includes the timer/message-parked
  instances variable spill leaves resident. Eviction shrinks the hot-state maps and
  purges the allocator, so cold RAM is returned to the OS, not just freed internally.

Both tiers are **on by default in persistent mode** (a data directory is configured)
and off for fully in-memory runs, where spilling to an in-memory SQLite would only
double the footprint.

| Variable | Effect |
| --- | --- |
| `NANOBPMN_VAR_SPILL=<on\|off>` | Force variable spill on/off. Unset: on iff a persistent data path exists. |
| `NANOBPMN_VAR_SPILL_BUDGET=<n>` | Resident parked-instance budget before variables spill. Default `512`. |
| `NANOBPMN_COLD_SPILL=<on\|off>` | Force cold spill on/off. Unset: on iff a persistent data path exists. |
| `NANOBPMN_COLD_SPILL_MB=<n>` | High-water resident RAM (MiB) above which dormant instances are evicted to disk. Default `384`; low-water = 7/8 of high. |

## Partitions

Following Zeebe, a node may run several **partitions**, each its own
single-writer engine thread + journal. Partitioning multiplies the single-writer
throughput ceiling (each partition fsyncs and applies independently) while
keeping every command serialized within its partition.

- **Default is one partition** (`NANOBPMN_PARTITIONS=1`), which preserves the
  historical behaviour exactly: one engine thread, one `journal.jsonl`, keys
  `1, 2, 3, …`.
- Set `NANOBPMN_PARTITIONS=<n>` to run `n` partitions (0-based ids `0…n-1`).
  Keys embed their owning partition in their high bits, so a command targeting an
  existing key (complete/cancel/…) routes to exactly one partition, while a fresh
  `createProcessInstance` is balanced **round-robin** across partitions. An
  instance lives on its creating partition for life.
- **Queries, `awaitCompletion`, and the read model are global** — answered from a
  single shared projection fed by all partitions, so multi-partition is
  transparent to clients.
- Each partition gets its own journal file: `journal.partition-<p>.jsonl`.
  Deployments are journaled on partition 0 and replicated in-memory to the rest.

| Variable | Effect |
| --- | --- |
| `NANOBPMN_PARTITIONS=<n>` | Number of single-writer partitions. Default `1`. Clamped to `[1, 8192]`. Changing this requires fresh data directories (the journal layout differs). |

## Clustering, replication, and availability

A deployment is one or more **nodes** (processes). Every node is both a **broker**
(owning a subset of partitions — an engine actor + journal each) and a **gateway**
(accepts client connections for the *whole* cluster, forwarding operations it does
not own to the node that does). Partition→node placement is deterministic
(`partition_id % num_nodes`), so every node computes the same ownership map from
static config with no coordinator. Single-node (the default, `NANOBPMN_NODES`
unset) is byte-for-byte the historical behaviour.

Two independent axes:

- **Distribution** (`NANOBPMN_NODES` / `NANOBPMN_NODE_ID`): spread partitions across
  nodes for throughput. Each partition still lives on exactly one node.
- **Replication** (`NANOBPMN_RAFT=on` + `NANOBPMN_RF=<k>`): replicate each partition
  across `k` nodes as a per-partition **Raft** group, for durability and failover.

### Replication factor and quorum

With `NANOBPMN_RF=k`, each partition's replica set is `k` consecutive nodes
(`[owner, owner+1, …]`), the first being its initial leader, and writes commit
through Raft. `RF` is clamped to `[1, num_nodes]`. **A `k`-voter Raft group
commits only with a quorum of ⌊k/2⌋+1 replicas and therefore tolerates ⌊(k−1)/2⌋
failures:**

| Nodes (RF = nodes) | Quorum | Faults tolerated | On one node loss |
| --- | --- | --- | --- |
| 1 | 1 | 0 | total outage |
| **2** | **2** | **0** | **total write outage** (see warning) |
| 3 | 2 | 1 | stays up — sub-second leadership blip |
| 5 | 3 | 2 | stays up |

Raft deployments use **odd** node counts. The first size that survives a failure
is **3** (quorum 2, tolerates 1).

> ⚠️ **Two-node clusters are a trap.** A 2-node `RF=2` group has quorum 2 — *both*
> nodes are required to commit — so it tolerates **zero** faults, the *same* as a
> single node, while *doubling* the number of machines whose failure halts all
> writes. It buys **durability only** (every commit on two disks), **not
> availability**. A network partition between the two halts writes on *both* sides
> (neither has quorum). Use 3+ for fault tolerance; `RF=2`/2-node is a stepping
> stone, not a resilient deployment.

### What happens when a node is lost

nanobpmn is **CP** (consistency over availability): a partition without a quorum
refuses to commit rather than diverge — so there is **no split-brain and no data
loss**, ever. Behaviour depends on which side of the cut you are on:

- **Majority side (e.g. lose 1 of 3, `RF=3`).** The survivors hold quorum (2/3),
  **re-elect a new leader for the lost node's partitions within the election
  timeout (~sub-second)**, and keep serving. In-flight writes that were forwarded
  to the now-dead leader fail fast and retry on the new leader (see *Failover
  write path* below). Measured: throughput through a node loss holds at ~98% of
  baseline with a sub-second p99 blip, then full recovery on rejoin — no data loss.
- **Minority side (e.g. a single node taken off the network — "laptop leaves the
  office").** That node can reach no quorum for *any* partition, so it becomes
  **write-unavailable**: every create/complete/activate fails fast (the orphaned
  leader steps down within ~one election timeout rather than hanging). Reads are
  still served from its **local applied state — consistent but frozen/stale** (no
  new writes land). Workers connected to it stay connected (same machine) but get
  errors/zero jobs; if that node is their only gateway address they are stuck until
  it rejoins. **No writes it attempted while isolated are ever acknowledged or
  retained.**
- **RF=1 (no replication, the default).** There is no quorum concept; each partition
  is single-homed. Losing a node keeps the **survivor fully serving its own
  partitions** (partition-level fault isolation), but the **dead node's partitions
  go offline** until it returns and its recent writes are not replicated anywhere.

### Rejoin

A returning node reconnects, catches up via Raft `AppendEntries` (or an
`InstallSnapshot` if it fell more than `NANOBPMN_RAFT_SNAPSHOT_LOGS` behind),
**discards any uncommitted entries it proposed while isolated** (overwritten by the
higher-term majority log), and resumes as a follower. Membership is not changed on
a transient outage, so no operator action is needed.

### Failover write path

So a single leader failure does not stall a closed-loop client, fast non-await peer
forwards (job complete/fail/throw, activation pulls, by-key reads/mutations, and
non-await create) use a **short per-attempt deadline** (default 2500 ms, just above
the election ceiling) instead of the 30 s general peer timeout; a forward racing a
leader failure fails fast and the create path **re-resolves the new leader and
retries** within a budget (default 5000 ms) before surfacing a retryable `503`.
`awaitCompletion` creates keep the long timeout (they legitimately block until the
instance finishes).

### Cluster configuration

| Variable | Effect |
| --- | --- |
| `NANOBPMN_NODES=<url,url,…>` | Comma-separated node base URLs, **index = node id** (e.g. `http://10.0.0.1:8080,http://10.0.0.2:8080`). Unset (or one entry) ⇒ single node. |
| `NANOBPMN_NODE_ID=<i>` | This node's id (index into `NANOBPMN_NODES`). Default `0`. |
| `NANOBPMN_RAFT=on` | Enable per-partition Raft replication. Off ⇒ the single-homed, byte-identical path. |
| `NANOBPMN_RF=<k>` | Replication factor: nodes per partition. Default `1`. Clamped to `[1, num_nodes]`. Use an odd node count with `RF=num_nodes` for fault tolerance. |
| `NANOBPMN_RAFT_HEARTBEAT_MS=<ms>` | Leader heartbeat interval. Default `250`. |
| `NANOBPMN_RAFT_ELECTION_MIN_MS` / `_MAX_MS` | Randomized election timeout window. Defaults `500` / `1000`. |
| `NANOBPMN_RAFT_SNAPSHOT_LOGS=<n>` | Snapshot every `n` applied entries (log compaction; also the catch-up→snapshot threshold). Default `5000`. |
| `NANOBPMN_PEER_TIMEOUT_MS=<ms>` | General peer-forward timeout (await-create, deploy, message publish). Default `30000`. |
| `NANOBPMN_WRITE_FORWARD_TIMEOUT_MS=<ms>` | Per-attempt deadline for fast non-await write forwards. Default `2500`. |
| `NANOBPMN_WRITE_FORWARD_RETRY_MS=<ms>` | Total leader-re-resolution retry budget for a forwarded create. Default `5000`. |

See [`docs/distributed-scaling-design.md`](docs/distributed-scaling-design.md) for
the full design rationale.

## Command stream (WebSocket)

Alongside the REST API, the server exposes a single **bidirectional WebSocket**
at `GET /command-stream` that multiplexes the whole client lifecycle — process
creation *and* the full job lifecycle — onto one persistent, credit-coordinated
socket. It funnels to the same engine command path as the REST handlers (the
engine core is untouched), so it is purely a more efficient ingress: no
per-request connection setup, no long-poll for jobs, and flow control by
**credits** instead of `429`/`503` + client retry.

Connect with an optional `worker` query parameter
(`/command-stream?worker=my-worker`). Frames are **JSON text frames**, each a
tagged union with a camelCase `"type"`. On connect the server sends a `welcome`
(initial submission window + heartbeat cadence) followed by a `submissionCredits`
grant. Idle sockets exchange `heartbeat` frames every 15 s.

The stream carries **two credit lanes** over the one engine thread:

- **Job push (demand/pull).** A client `subscribe`s to a job type with a credit
  count; a single server-side dispatcher leases jobs **round-robin across all
  subscribers** (reusing the REST activation + off-thread variable encoding) and
  pushes `job` frames while credits remain, topping demand back up via
  `jobCredits`. A pushed job carries the same shape as a REST `ActivatedJobResult`.
  **Lease expiry is the at-least-once guarantee:** a job pushed to a worker that
  never completes it is reclaimed by the existing periodic lock-expiry tick, so a
  dropped socket needs no special handling — the job is simply re-dispatched.
- **Submission (request/response).** `createInstance` is metered by a
  **submission-credit window** fed from the engine's processing headroom via the
  backpressure controller: under saturation the server simply **withholds
  credits** and the client stalls intake — no `503`, no retry storm, no thundering
  herd. When pressure clears, a top-up `submissionCredits` frame resumes the
  client. Job completions (`completeJob` / `failJob` / `throwError`) flow
  **unmetered** — draining backlog must never be throttled. A coarse, edge-
  triggered `pressure` frame (`level: "red"`/`"green"`) is broadcast on each
  transition so workers can coordinate without polling.

Client → server frames: `subscribe`, `jobCredits`, `createInstance`,
`completeJob`, `failJob`, `throwError`, `awaitInstance`, `heartbeat`.
Server → client frames: `welcome`, `job`, `commandResult` (`corr`-correlated
ack/result for every write), `instanceCompleted`, `submissionCredits`,
`pressure`, `heartbeat`.

**Await-completion and recovery.** `createInstance` may set
`awaitCompletion: true`; rather than holding the request, the server returns the
`commandResult` (with the `processInstanceKey`) immediately and later emits an
async `instanceCompleted` frame, correlated by the create's `corr`, when the
instance reaches a terminal state. If the socket drops before that arrives, the
client recovers by sending an **`awaitInstance`** frame on the new connection
with the `processInstanceKey` it persisted from the create ack. Because the read
model is durable history, an already-terminal (even evicted) instance resolves
**immediately**, so `awaitInstance` doubles as a completion poll.

> **Per-connection ordering vs. throughput.** A single connection's frames are
> processed in arrival order, each engine command awaited inline — so successive
> `createInstance`s on *one* socket serialize at journal fsync latency. Throughput
> comes from **concurrency across connections**: the group-commit journal writer
> batches many connections' appends into a single fsync. An application should
> therefore spread submission load over multiple sockets rather than pipelining
> one (see the recommended worker topology below).

**Recommended worker topology.** Because ordering is per-socket and throughput
scales with concurrent sockets, an application should **not** funnel everything
through one stream:

- **One stream per job-type worker.** Open a dedicated socket for each worker
  (i.e. each `subscribe` job type), as a Zeebe/Camunda client does with its job
  workers. Each gets its own demand-credit lane and its own reader task, so job
  delivery for different types proceeds in parallel.
- **Separate creation from work.** Use a distinct socket (or a small pool) for
  `createInstance` submission, kept apart from the job-worker sockets, so a burst
  of creates can't head-of-line-block job completions and vice versa. For high
  create rates, fan out across a **pool** of submission sockets — that is what
  lets the group-commit writer batch their appends and is where throughput comes
  from.
- **Rule of thumb for 10 job types:** ~10 worker sockets (one per type) **plus** a
  small submission pool (e.g. 2–8 sockets sized to your create rate) — not a
  single shared socket for everything.

| Variable | Effect |
| --- | --- |
| `NANOBPMN_STREAM_SUBMISSION_WINDOW=<n>` | Per-connection create-submission window (default `256`): how many `createInstance`s a client may have outstanding before it must wait for the server to replenish credits. |

**Stream durability: ack-before-fsync pipelining.** To maximize throughput on the
command stream, job lifecycle commands (`completeJob` / `failJob` / `throwError`)
use **pipelined commits**: the server applies the command to the engine (which
writes to the journal and establishes order), then **replies `200` immediately**
while fsync completes asynchronously in a detached task (~5ms later). This lets
multiple connections' completions batch into a single group-commit fsync, measured
at **4× higher throughput** (2280 vs 572 writes/s) than awaiting fsync inline.

The trade-off: if the server crashes in the ~5ms window after replying but before
fsync finishes, the completion is lost from disk and the job **re-activates** after
restart (its lock expires). This **preserves at-least-once semantics** — handlers
must already be idempotent (standard BPMN worker contract) — and the durability
window (~5ms) is negligible compared to typical job lock timeouts (30–60s). The
REST API `/jobs/{key}/completion` endpoint still awaits fsync before replying; only
the command stream pipelines. Analogous to Kafka `acks=1` or RabbitMQ async
confirms.

> **Multi-node durability:** With `NANOBPMN_RAFT=on` and `NANOBPMN_RF>1` (see
> [Clustering, replication, and availability](#clustering-replication-and-availability)),
> a partition's writes commit through Raft — replicated to a quorum of replicas
> before they are durable cluster-wide — instead of the single-node ack-before-fsync
> window described here. The stream's at-least-once contract is unchanged.

See [`docs/command-stream-design.md`](docs/command-stream-design.md) for the full
design rationale,
[`docs/command-stream.asyncapi.yaml`](docs/command-stream.asyncapi.yaml) for the
AsyncAPI 3.1 description of every client/server frame, and
`server/tests/command_stream_e2e.rs` for runnable examples of every frame
exchange.

### Node/TypeScript SDK

[`clients/node-stream`](clients/node-stream) publishes **`@nanobpmn/sdk`**, a
companion to `@camunda8/orchestration-cluster-api` that adds a typed
command-stream client and a streaming job worker. The worker auto-detects the
backend: it uses the command stream against nanobpmn and **falls back to Camunda
REST polling** against a Camunda gateway, so the same handler code serves both.
See [`clients/node-stream/README.md`](clients/node-stream/README.md).


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
server generator (run from the version-pinned `openapi-generator-cli` JAR via
the local Java runtime — no Docker required). It produces a self-contained
library crate (`nanobpm-gateway-rest`) with:

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
├── spec/                          # bundled OpenAPI spec (upstream, source of truth)
│   ├── rest-api.yaml              # entrypoint ($refs the sibling files)
│   └── *.yaml
├── spec-patches/                  # local overlays applied to the build copy
│   └── patches.yaml               # project-specific spec additions (spec/ stays pristine)
├── scripts/
│   ├── generate.sh                # end-to-end generation pipeline
│   ├── preprocess-spec.py         # sanitizes + overlays a temp copy of the spec
│   ├── postprocess-generated.py   # patches known rust-axum generator bugs
│   └── gen-stub-server.py         # generates the server's stub trait impls
├── server/                        # runnable stub server (binary crate)
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs                # ServerImpl, auth/error glue, bootstrap
│       └── stub_impls.rs          # generated trait impls (git-ignored)
├── build/                         # temp sanitized spec + cached generator JAR (git-ignored)
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
    │   ├── state.rs               # State + apply() — the sole mutator
    │   ├── engine.rs              # single-writer loop + processor
    │   └── ffi.rs                 # coarse C-ABI surface (feature "ffi")
    ├── scripts/verify-wasm-ffi.mjs # asserts the wasm FFI exports + a round-trip
    └── tests/public_api.rs
```

The `generated/` crate, `build/`, and `server/src/stub_impls.rs` are **build
artifacts** and are git-ignored. Regenerate them on demand with `make generate`.
The `spec/` tree is the committed source of truth.

## Requirements

- A Java runtime (JRE/JDK 11+) — runs the pinned `openapi-generator-cli` JAR,
  which is downloaded once into `build/tools/` (no Docker, no global install)
- A Rust toolchain (`cargo`, `rustfmt`)
- Python 3 with PyYAML (for the spec pre-processing step)
- `curl` or `wget` (to fetch the generator JAR on first run)

## Usage

```bash
# Generate the Rust REST layer + server stubs from spec/
make generate

# Generate if needed, then compile both crates
make build

# Build the optimized production server binary
make release
# -> server/target/release/nanobpm-gateway-rest-server

# Run the stub server (defaults to port 8080; override with PORT)
make run
PORT=18080 make run

# Lint / format
make clippy
make fmt

# Remove all generated artifacts
make clean
```

`make release` produces a single self-contained binary at
`server/target/release/nanobpm-gateway-rest-server`. Run it directly,
configuring it through the environment — `PORT` for the listen port,
`NANOBPMN_DATA_DIR` for the durable event-log + read-model directory (see
[Durability](#durability-event-log-replay) and
[Read model](#read-model-cqrs-eventual-consistency)), and `DEBUG_REST` to log
requests:

```bash
NANOBPMN_DATA_DIR=/var/lib/nanobpmn PORT=8080 \
  ./server/target/release/nanobpm-gateway-rest-server
```

Set `DEBUG_REST=1` (or `true`/`yes`/`on`) to log every REST request and
response — method, URI, status, latency, and a preview of both bodies — which
is handy when inspecting what a client is sending:

```text
INFO rest: --> POST /v2/process-instances [53 bytes] {"processDefinitionId":"demo","tenantId":"<default>"}
INFO rest: <-- POST /v2/process-instances 200 OK (39.7ms) [180 bytes] {"processInstanceKey":"3", …}
```

> The bodies are buffered to be logged, so leave `DEBUG_REST` off in
> production; it is unset (silent) by default.


> The generated REST layer under `generated/` is a build dependency, so
> `make release` runs `make generate` first if needed (which downloads and runs
> the `openapi-generator-cli` JAR with local Java — no Docker).
> Once generated, the binary itself has no run-time external dependencies. (At
> build time the vendored jemalloc allocator is compiled from source, so a C
> compiler — `cc`/`clang`, already present on macOS and most Linux toolchains —
> is required; the resulting binary is still self-contained.)

## Web console

A built-in web console for the self-contained single-node distribution is
available behind the **`console`** Cargo feature. It serves a single-page app at
`/console` and a JSON API under `/console/api/*`, both **additive and
feature-gated** — the default gateway build does not include them and is
unaffected.

```bash
# 1. Build the frontend bundle (also vendors Swagger UI + bundles the spec).
#    Required before any console build — the gateway embeds ../console/dist.
cd console && npm install && npm run build && cd ..

# 2a. Debug: rust-embed reads ../console/dist from disk at runtime, so frontend
#     rebuilds are picked up live without recompiling the gateway.
cargo build --features console --bin nanobpm-gateway-rest-server
NANOBPMN_DATA_DIR=./nanobpm.data PORT=8080 \
  ./server/target/debug/nanobpm-gateway-rest-server

# 2b. Release: `make release` builds the frontend and the console-enabled,
#     optimized single-file binary in the right order (it also forces a
#     re-embed so a rebuilt frontend is baked in). Equivalent to `make console`.
make release
NANOBPMN_DATA_DIR=./nanobpm.data PORT=8080 \
  ./server/target/release/nanobpm-gateway-rest-server

# open http://localhost:8080/console
```

> **Note:** the `console` feature is required. `make release` includes it; a
> plain `cargo build --release` (or `make release-gateway`) produces the default
> API-only gateway, which serves no console, landing page, or Swagger UI —
> `/console` returns 404.

When started with the `console` feature, the gateway prints the human-facing
URLs at startup:

```text
Nano BPM is up:
  Landing page   http://127.0.0.1:8080/
  Web console    http://127.0.0.1:8080/console
  API reference  http://127.0.0.1:8080/swagger
  REST API       http://127.0.0.1:8080/v2
  Metrics        http://127.0.0.1:8080/metrics
```

The root path `/` serves a small self-contained landing page (an inline canvas
particle effect, no external assets) linking to the console and the API
reference. `/swagger` serves an **offline** Swagger UI: the multi-file OpenAPI
spec under `spec/` is bundled into a single `openapi.json` at frontend-build
time and embedded alongside the UI, so nothing is fetched from a CDN. (These
root routes are part of the `console` feature; the default gateway build serves
neither and keeps its original startup output.)

The console has five tabs:

- **Topology** — cluster/partition/Raft overview.
- **Metrics** — a live performance dashboard (process starts/s, jobs/s, active
  processes, connected clients, commit pipeline depth, journal/fsync/commit-wait
  means, writer duty cycle) with inline sparklines. Throughput rates are derived
  client-side from the gateway's Prometheus surface (`/metrics`);
  active-process count is read on demand only while the dashboard is open, so it
  never perturbs a running load test. Handy for performance demos and debugging.
- **Modeler** — a bpmn-js editor backed by a workspace model library. Create,
  edit, deploy (idempotent), pull a deployed model back from the engine, and
  duplicate. Each model shows its deploy status relative to the engine
  (*not deployed* / *deployed & in sync* / *modified*).
- **Explorer** — a live process-instance explorer (variables, jobs, incidents)
  with BPMN XML.
- **Workers** — author TypeScript job workers in the browser and run them as
  sandboxed **Deno** subprocesses over the command stream, with a live
  "Running" fleet view (status, throughput, completed/failed, uptime, restarts)
  and streamed logs.

### Workspace vs cluster data

The console keeps the user's **authoring source of truth** in a *workspace*
directory, deliberately separate from the engine's data dir so that deleting
cluster data leaves your models and workers intact.

```text
<workspace>/
├── models/<name>.bpmn          # BPMN models (Modeler)
├── workers/<name>/             # one directory per worker (worker.ts, deno.json, …)
├── .nanobpm/worker-sdk.ts       # embedded Deno worker SDK (auto-written)
└── .deno-cache/                 # DENO_DIR for worker dependency caching
```

| Variable | Meaning |
| --- | --- |
| `NANOBPMN_WORKSPACE_DIR=<dir>` | Console workspace root (models + workers). Default `./nanobpm-workspace`. Survives deletion of `NANOBPMN_DATA_DIR`. |
| `NANOBPMN_DENO_BIN=<path>` | Explicit path to the Deno binary used to run workers. Default: `deno` on `PATH`, else `~/.deno/bin/deno`. |

### Embedded workers (Deno)

Each worker is a directory under `workers/<name>/` whose `worker.ts` imports the
embedded SDK and declares a handler:

```ts
import { defineWorker } from "@nanobpm/worker";

defineWorker({
  type: "my-job",
  maxParallelJobs: 10,
  async handle(job) {
    // job.variables holds the activated job's variables; npm libraries are
    // available via `npm:` specifiers.
    return { result: 42 };          // resolves -> completeJob({ result: 42 })
    // or: job.fail("boom") / job.error("CODE", "msg")
  },
});
```

The supervisor runs one sandboxed `deno run` subprocess per enabled worker
(`--allow-net`, read-only access to the workspace, writes confined to the Deno
cache) that speaks the command stream directly. **Deno is optional**: if it is
not installed the Workers tab still authors code but starting a worker reports
the runtime as unavailable.

## Engine (`engine-core`)

The embeddable BPMN engine builds and tests with plain `cargo` — no Docker, no
code generation:

```bash
make engine-test    # unit + integration + doc tests
make engine-build   # debug build
make engine-wasm    # prove it compiles for wasm32 (needs the wasm32 target)
make engine-wasm-ffi # build the FFI cdylib for wasm32 + verify exports & a round-trip (needs node)
```

See [`engine-core/README.md`](engine-core/README.md) for the architecture. The
engine also exposes a coarse C-ABI (`src/ffi.rs`, behind the `ffi` feature) for
embedding via UniFFI on mobile or as wasm exports in a browser; `make
engine-wasm-ffi` proves that wasm/FFI build end to end.

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
   request bodies such as `content: { application/json: {} }`), and any local
   overlays from `spec-patches/patches.yaml` are applied (see
   [Local spec overlays](#local-spec-overlays)). Files that need no change are
   copied verbatim to keep the transform minimal.
2. **Generate** — the version-pinned `openapi-generator-cli` JAR (downloaded once
   into `build/tools/` and run with local Java — no Docker) emits the crate into
   `generated/`.
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

## Local spec overlays

`spec/` is kept byte-for-byte identical to the upstream Camunda release so it can
be refreshed by simply replacing files. Project-specific additions to the API
contract live separately in `spec-patches/patches.yaml` and are applied to the
build copy (`build/spec/`) during preprocessing — `spec/` is never mutated.

Each overlay entry names a `file` (relative to `spec/`) and a dotted `target`
path inside it, then either deep-`merge`s a mapping or `append`s items to a list:

```yaml
- file: process-instances.yaml
  target: components.schemas.CreateProcessInstanceResult.properties
  merge:
    processCompleted: { type: boolean, description: "…" }
- file: process-instances.yaml
  target: components.schemas.CreateProcessInstanceResult.required
  append: [processCompleted]
```

This is how nanobpmn adds the `processCompleted` flag to
`CreateProcessInstanceResult` (it reports whether the returned variables are the
authoritative final result) without forking the upstream spec.
