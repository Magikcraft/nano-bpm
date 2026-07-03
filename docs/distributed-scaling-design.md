# Distributed Scaling — Design Proposal

> Status: analysis / proposal only. No code changes made by this document.
> Grounded in: `server/src/partition.rs` (Partitions router), `server/src/main.rs`
> (`correlate_message_everywhere`, `deploy_partition` / `replicate_deployment`,
> `try_activate`, tick fan-out), `server/src/journal.rs` (shared group-commit WAL,
> sync/async durability), `server/src/falcon.rs` (credit dispatch, Pressure
> frame), `engine-core/src/lib.rs` (`partition_of`, key encoding).

## 0. Context: the single-node envelope is characterised

Before going distributed we exhausted the single-node levers (see
`session-state/.../files/envelope-findings.md`, UPDATEs 1–16):

- **Envelope: ~33–34k process-instances/s, p50 ~150ms**, graceful to ~9× overload (no
  hard collapse) on a 14-core M4 Pro.
- The writer was freed (opt-in **async durability**), the dispatcher was **parallelised**
  (`buffer_unordered`, `NANOBPMN_DISPATCH_CONCURRENCY`), the engine hot path is O(1), and
  **completion-priority** + submission-credit flow control give graceful degradation.
- The remaining ceiling is the **single shared group-commit WAL writer** (P=1..8 all
  funnel through it) plus the closed-loop completion rate — i.e. the work that is
  intrinsically single-node.

Going past this **requires distributing the partitions across processes/nodes.** This
document is the plan for that.

## 1. The good news: the partition is already the unit of distribution

nanobpmn already implements Zeebe-style partitioning (`server/src/partition.rs`):

- Each partition is an **independent single-writer engine actor** (`EngineHandle`) — its
  own engine thread + journal stream.
- **Keys embed their owning partition in their high bits** (`partition_of(key)`), so any
  command that targets an existing key routes deterministically to exactly one partition
  (`Partitions::by_key`).
- A fresh `createProcessInstance` is balanced **round-robin** across partitions
  (`Partitions::for_create`); an instance lives on its creating partition for its whole
  life.
- `NANOBPMN_PARTITIONS=<n>` already runs `n` partitions in one process. Default `1`
  preserves historical behaviour exactly.

**Distributed scaling = running those same partitions across M processes/nodes instead of
M threads in one process.** The engine core does not change; the *connective tissue* does.

## 2. The unifying model: a partition is a replicated group with a movable leader

We want **both** higher aggregate throughput **and** fault tolerance. The expensive
requirement (replication) constrains the cheap one (routing): if we build naive
throughput-first routing and later add replication, we rework the transport, because
replication makes a partition's home **movable** (leaders fail over).

So we design the seam **once** for the replicated case and ship single-replica first:

> **Every partition is a Raft group. Exactly one node is its leader at any time. Commands
> route to the current leader. Ship at replication factor 1 first (no followers = today's
> durability, but already leader-addressed and relocatable), then raise RF to 3 for HA
> without touching the routing layer.**

This is the load-bearing abstraction. Everything below hangs off it.

## 3. What is process-local today and must become a network protocol

These operations are cheap in-process (thread fan-out across `Partitions::all()`) but
become distributed-systems problems across nodes. Each is a concrete code site:

| # | Concern | Today (process-local) | Distributed requirement |
|---|---------|----------------------|-------------------------|
| 1 | **Message correlation** | `correlate_message_everywhere` (main.rs:1471) **broadcasts** the correlate command to *every* partition, because it doesn't know which holds the waiting subscription. | A **subscription routing index** (correlation-key → partition) so a publish hits one node, not an all-nodes fan-out. The #1 scaling tax for message-heavy workloads. |
| 2 | **Deployment** | `deploy_partition()` = partition 0 owns + journals the deployment; `replicate_deployment` (main.rs:2589) installs the definition **in-memory** on every other partition. | Cluster-wide **deployment distribution** (replicated config / gossip), since in-memory replication doesn't cross process boundaries. |
| 3 | **Message-start / timer-start subscriptions** | Owned solely by partition 0 (`partition.rs:98–103`). | An **owner + failover** story (which node hosts the start-event subscriptions; what happens when it dies). |
| 4 | **Read model (queries)** | A **single shared read store**; queries never touch a partition (`partition.rs:9`). | A replicated/exported read store, or per-node shards with **scatter-gather** queries. |
| 5 | **Write-ahead log** | One **shared group-commit WAL** writer for all partitions (checkpoint 6: fixed fsync fragmentation). | Per-**node** log again (each node group-commits its own partitions). The fragmentation lesson still applies *within* a node. |
| 6 | **Job activation / ticks** | `try_activate` (main.rs:2682) and the periodic tick (main.rs:3724/3798/3858: timers, eviction, idle compaction, expiry) fan out across `all()` locally. | Node-local fan-out only touches that node's partitions; activation already routes by job key. Mostly fine — but cross-node job streaming needs the dispatcher to address remote partitions. |
| 7 | **Topology** | `TopologyResponse` hardcodes `cluster_size:1, partitions_count:1, replication_factor:1` (main.rs:1455). | Real cluster topology surfaced to clients (partition→leader map) so SDKs route directly. |

## 4. Two head-starts worth flagging

- **The Falcon protocol is already your inter-node RPC.** Workers subscribe, creates flow,
  credits + the **Pressure (red/green)** frame flow back (`falcon.rs`). The same
  protocol can carry node→node command routing — likely **no second transport needed.**
- **The shared-WAL work reverses direction cleanly.** We merged partitions onto one log to
  fix single-node fsync fragmentation; distributed, each node simply group-commits its own
  partitions. `Journal::open` / `open_partition` (single-partition path) is unchanged and
  becomes the per-node primitive.

## 5. The durability story changes shape

- **Single-node today:** durability = journal replay on restart (sync = ack after fsync;
  async = ack after page-cache write, fsync amortised, survives process crash via replay).
- **Distributed (RF=3):** durability = **replication**. A command commits when a **quorum**
  of replicas has the log entry. Leader failover promotes a follower that has the committed
  tail. The existing sync/async work becomes the **local disk tier** *under* the replicated
  log (how each replica persists its slice), not the commit authority itself.

This is the single biggest lift and the reason stage 0–1 must be designed leader-aware.

## 6. Distributed sensing — the adaptive envelope

This was the original framing of the session. The primitive already exists: the
dispatcher's **edge-triggered Pressure frame** broadcast over the Falcon protocol. Distributed
sensing extends it to a **cluster-wide load signal**: each node publishes its envelope
position from gauges we already instrumented —

- `nanobpm_journal_writer_busy_seconds` / `_idle_seconds` (writer duty cycle),
- `lo_len` create-queue depth (`pending_create_queue`),
- `inflight` active-instance backlog,

— and the system **adapts**: route creates toward nodes with headroom, and shed/redirect at
the boundary (the admission gates `NANOBPMN_ADMISSION_MAX_BACKLOG` /
`NANOBPMN_ADMISSION_MAX_CREATE_QUEUE`) instead of letting one partition collapse. It is
load-aware **admission + placement**, fed by the exact signals already in `metrics.rs`.

## 7. Staged roadmap

Ordered so nothing is thrown away (each stage is independently shippable/testable):

| Stage | Delivers | Key work |
|-------|----------|----------|
| **0. Partition addressing** | Foundation for both | A `PartitionRouter` indirection: key → current leader node. Today it resolves to "always local"; make it a seam. **Zero behaviour change single-node.** |
| **1. Network transport (RF=1)** | **Throughput** | Run partitions in M processes; route commands to the owning node's Falcon. Near-linear pi/s scaling. Each partition single-homed (node loss = that partition recovers from its journal). |
| **2. Fan-out fixes** | Throughput at scale | Subscription routing index (kills the §3.1 broadcast); distributed deployment (§3.2); start-event ownership (§3.3); real topology (§3.7). |
| **3. Per-partition Raft (RF=3)** | **Fault tolerance** | Replace single-node replay with a replicated log; commit = quorum-ack; leader failover. sync/async durability becomes the local disk tier. |
| **4. Distributed sensing** | Adaptive envelope | Cluster load signal + adaptive create placement + boundary shedding via the Pressure frame and admission gates. |

## 8. Recommended first concrete step

**Stage 0 — the `PartitionRouter` indirection** — as a pure refactor with **zero behaviour
change single-node**: every partition resolves to "local". It is low-risk, independently
testable, and the load-bearing seam for *both* throughput and HA. Everything else bolts onto
it.

Today the resolution points are `Partitions::by_key`, `for_create`, `deploy_partition`,
`activate_start`, and `all()`. Stage 0 routes each of these through a `Resolve(partition) ->
Location::{Local(EngineHandle), Remote(NodeId)}` seam, where `Location` is always `Local`
until stage 1 introduces remote nodes.

## 9. Open questions for later stages

- Partition **count vs node count** elasticity: partitions are fixed at boot
  (`NANOBPMN_PARTITIONS`); rebalancing partitions across nodes (without re-keying live
  instances) needs a membership/placement controller.
- **Query consistency** under a distributed read model (read-your-writes vs eventual).
- **Cross-partition call activities** (a process calling another that lands on a different
  partition/node) — currently in-process; becomes a routed command.
- **Membership & leader election** substrate (embed Raft, or lean on an external coordinator).
