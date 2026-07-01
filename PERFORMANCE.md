# nanobpmn Performance & Stress Tests

A running log of throughput / durability stress tests against the nanobpmn
engine. Newest runs first. Each entry records the environment, cluster
topology, load configuration, and observed results so runs stay comparable.

## Test process

Unless noted, the workload is `test-job-process.bpmn`: a single service task
with job type `test-job`. One job == one process instance, so **completed jobs/s
== completed process instances/s**. The load generator (`loadgen`, from
`ts-performance-matrix/rust-worker`) runs coupled producers + workers in one
process: producers create instances while holding server-granted submission
credits; workers stream-activate and complete `test-job`. `tput` is measured
completed jobs/s during the steady-state window; `producedRate ≈ tput` means the
cluster keeps up with the create rate (no growing backlog).

---

## 2026-07-01 — Ceiling diagnosed: the single per-node read-model exporter

Follow-up to the ceiling hunt below. The earlier sweep proved the ~12k PI/s/node
limit was **per-node and shared across a node's partitions**, but not *what* it
was. This run instruments and isolates it.

**The bottleneck is the read-model exporter.** Each node runs **one** exporter
thread (`spawn_exporter`, `main.rs`) that funnels *every* partition's events into
**one** `Mutex<Connection>` SQLite read store (`readstore.rs`). Completion
recognition, the backpressure in-flight gauge, hot-state eviction, and
`awaitCompletion` wakeups all flow through it — so its projection rate *is* the
observed throughput. Adding partitions never helped because they all re-serialize
here.

Added `NANOBPMN_EXPORTER_PROFILE` (logs the exporter's rate / busy% / share of
busy spent in `store.export` every 5 s) and an A/B on the read store's fsync mode
via `NANOBPMN_READ_SYNC`. Same 3× `c2-standard-16` cluster (3 partitions, RF=3,
pd-ssd), dedicated load box, 40 s steady window, build `0.0.3-walab`.

| `synchronous` | **Agg tput** | Per node | Exporter busy | In `export` |
|---|--:|--:|--:|--:|
| FULL (old default) | **39 373** | 13.1k | ~100% | 99.6% |
| NORMAL (WAL) | 38 272 | 12.8k | ~100% | 99.5% |
| OFF (no fsync) | 39 587 | 13.2k | 92–100% | 99.6% |

### It is CPU-bound on SQLite inserts, not fsync

The exporter thread pins **one core at ~100%**, **99.6% of it inside
`store.export`**, at **~250 000 events/s (~13k PI/s × ~19 events/PI)** — while the
other 15 vCPU idle. Throughput is **flat within noise across all three fsync
modes**: under flood the drain-everything batcher forms **huge batches (thousands
to ~80 000 events)**, so per-commit fsync is amortized to ~16 commits per 5 s and
becomes irrelevant. This is exactly why the earlier **tmpfs** run (fsync-free)
*also* held at ~36k: the wall was never fsync, it was single-thread projection
CPU. The "coordination-bound" label from the ceiling hunt resolves to **one
saturated core**.

### Outcome

- **Shipped (`37c6cf5`): file-backed read store now runs `journal_mode=WAL` +
  `synchronous=NORMAL`.** Correct and safe (the read model is derived and rebuilt
  from the journal on boot), and it *does* help the **low-batch / light-load /
  slow-disk** regime — a local single-node laptop A/B measured **+39%** (FULL
  672 → NORMAL 933 PI/s) where small batches left fsync on the critical path. But
  it does **not** lift the *flooded-cluster* ceiling, where batches amortize fsync
  away.
- **The real lever is per-partition sharding of the exporter + read store** (N
  threads, N SQLite shards) so projection CPU scales with cores instead of pinning
  one. This run is the empirical justification: relieve that single core and the
  ~13k/node ceiling should rise.

---

## 2026-07-01 — Ceiling hunt: dedicated load box + parameter sweep

Build: `0.0.3-stress` (commit `21d954a`), release profile. Same 3× `c2-standard-16`
cluster, plus a **dedicated 4th `c2-standard-16` load box** (`nano-loadbox`) so
the load generator no longer steals CPU from the engine. The load box runs **3
`loadgen` processes, one per gateway** (spread across nodes, per the design), all
targeting internal IPs.

**Goal:** find the true throughput ceiling and see which knob moves it.

### Sweep results

Each row is a ~60 s steady-state window; "CPU idle" is the mid-run node average
(16 vCPU each).

| Config | RF | Parts | Disk | loadgen procs | MAX_INFLIGHT | **Agg tput** | p50 | worst p99 | Node CPU idle |
|---|--:|--:|---|--:|--:|--:|--:|--:|--:|
| baseline (remote) | 3 | 3 | SSD | 3 | 6 000 | **36 258** | 333 ms | 2.2 s | 47% |
| more in-flight | 3 | 3 | SSD | 3 | 12 000 | 35 699 | 1 281 ms | 4.0 s | 46% |
| more in-flight | 3 | 3 | SSD | 3 | 18 000 | 36 001 | 3 238 ms | 5.6 s | 47% |
| more partitions | 3 | **9** | SSD | 3 | 6 000 | 35 799 | 331 ms | 2.3 s | 50% |
| more partitions | 3 | **9** | SSD | 3 | 12 000 | 37 346 | 978 ms | 4.9 s | 52% |
| no replication | **1** | 3 | SSD | 3 | 6 000 | 35 997 | 347 ms | 2.2 s | 48% |
| no replication | **1** | 3 | SSD | 3 | 12 000 | 35 947 | 957 ms | 5.3 s | 47% |
| more clients | 1 | 3 | SSD | **6** | 6 000 | 35 860 | 2 314 ms | 5.6 s | 47% |
| **RAM disk** | 1 | 3 | **tmpfs** | 3 | 6 000 | 37 014 | 333 ms | 2.2 s | 49% |
| **RAM disk** | 1 | 3 | **tmpfs** | 3 | 12 000 | 36 073 | 1 137 ms | 5.1 s | 47% |

### The ceiling is ~36k PI/s (~12k/node) and nothing above moved it

Throughput sat at **~36k PI/s regardless of**:

- **Replication factor** (RF=3 vs RF=1) — durability is ~free here; not the wall.
- **Partition count** (3 vs 9) — more writer actors did **not** help → the
  bottleneck is **per-node**, upstream/shared across a node's partitions.
- **Disk** (SSD vs tmpfs) — moving the data dir to RAM removed **all iowait**
  (5% → 0%) but **did not raise throughput** → not disk/fsync-bound.
- **Client load** (3 vs 6 loadgen procs, MAX_INFLIGHT 6k–18k) — extra in-flight
  work only **deepened queues** (p50 333 ms → 3.2 s, p99 → 5.6 s) with **zero**
  throughput gain → the client is not starving the server.

### Diagnosis: coordination-bound, not resource-bound

At the ceiling every node had **~50% CPU idle and 0% iowait**. `nano-gw` drew only
~6.5 of 16 vCPU. `top -H` showed **~12 tokio worker threads each ~40% busy — no
single hot thread** (so it is *not* a single-writer/single-thread serialization),
and the load box was **80% idle** (so it is *not* the client).

The signature — many threads each partially busy, half the cores idle, no hot
thread, no iowait, latency that grows with in-flight while throughput stays flat —
is a **coordination-bound service rate**: each create/complete awaits the
per-partition commit/apply pipeline (Raft batcher → engine actor → read-model
projection), and the cross-thread handoffs cap the node at **~12k PI/s** long
before the cores or disk saturate.

### Implication for scaling

Because throughput is coordination-bound per node with cores half-idle,
**scaling *up* (bigger machines) is not expected to help** — the extra cores would
sit idle. **Scaling *out* (more nodes) is the lever**: the cluster was linear at
~12k PI/s/node (3 nodes → 36k, 6 nodes → 65k — see below).

### Scale-out check: 6 nodes

Doubled the cluster to **6× `c2-standard-16`** (6 partitions, RF=3), load box
running **6 `loadgen` procs, one per gateway**, same baseline config
(`MI=6000/node`):

| Cluster | Nodes | Parts | **Agg tput** | per-node | p50 | Node idle | Loadbox idle |
|---|--:|--:|--:|--:|--:|--:|--:|
| 3-node | 3 | 3 | 36 258 | 12.1k | 333 ms | 47% | 80% |
| **6-node** | 6 | 6 | **64 923** | 10.8k | 348 ms | 41% | 45% |

**~1.8× throughput for 2× nodes** — near-linear, confirming the scale-out thesis.
Both nodes (41% idle) and the load box (45% idle) still had headroom, so the small
sublinearity is distributed-coordination overhead: with 6 partitions, create
placement forwards 5/6 of instances to a peer owner (vs 2/3 at 3 nodes). Latency
stayed healthy (p50 348 ms). Extrapolates to ~12 nodes → ~130k PI/s. (The single
load box is now ~55% used driving 6 gateways; pushing further needs a second load
box.)


### Tuning takeaways (users)

- **Throughput scales with node count, not machine size.** Prefer more, smaller
  nodes over fewer, larger ones for a create/complete-heavy workload.
- **`MAX_INFLIGHT` is a latency/throughput trade, not a throughput lever.** Once
  the node hits its service rate, raising in-flight only adds latency. ~6 000/node
  gave the best latency (p50 ~330 ms) at full throughput here; higher just queued.
- **Spread clients across all gateways** (one producer/worker set per node). A
  single gateway forwards the rest and bottlenecks.
- **RF=3 was ~free** on same-zone SSD nodes — keep durability on; it did not cap
  throughput.
- **Partitions:** 3 (one leader/node) already saturated the nodes; more partitions
  didn't add throughput but do add parallelism headroom for bigger machines /
  multi-task processes. Match partition count to node count as a starting point.

---

## 2026-07-01 — 3-node / 3-partition / RF=3 cluster, ~34k PI/s (30 min)

Build: `0.0.3-stress` (commit `21d954a`), release profile.

### Environment

- **3× GCP `c2-standard-16`** (16 vCPU, 64 GB), Debian 12, 200 GB `pd-ssd`, all
  in `us-central1-a` (same zone → low inter-node latency).
- Cluster: 3 nodes, 3 partitions, **RF=3** (every partition replicated to all
  three nodes). Each node leads one partition and follows the other two.
- Config per node: `NANOBPMN_NODES=<3 internal IPs>:8080`, `NANOBPMN_NODE_ID`,
  `NANOBPMN_RF=3`, `NANOBPMN_PARTITIONS=3`, `NANOBPMN_DATA_DIR` on the SSD,
  `PORT=8080`. Segmented multi-partition journal, snapshot/compaction every 60s.
- **Load placement:** `loadgen` **co-located on each node**, hitting
  `localhost:8080`. This is the design-intended topology — every node is a
  gateway, so clients spread across gateways and each node activates jobs on its
  own partitions locally (zero-hop hot path). Note: create *placement* still
  round-robins cluster-wide by design, so ~2/3 of creates are forwarded to peer
  partition owners over the Raft quorum-commit path (intentionally exercised).
- Load config per node: `WORKERS=256`, `PROD_CONNS=64`, `MAXPAR=4`,
  `MAX_INFLIGHT=6000`, `TRANSPORT=stream`, `DURATION_S=1800`.

### Results (30-minute steady-state window)

| Metric | node-0 | node-1 | node-2 | **Cluster** |
|---|--:|--:|--:|--:|
| Throughput (PI/s) | 11,236 | 11,572 | 11,623 | **34,431** |
| Instances processed | 20.23 M | 20.83 M | 20.92 M | **61.98 M** |
| p50 latency | 865 ms | 901 ms | 855 ms | ~875 ms |
| p90 latency | 2,286 ms | 2,325 ms | 2,263 ms | ~2.29 s |
| p99 latency | 2,810 ms | 2,857 ms | 2,794 ms | ~2.82 s |

Sustained throughput was flat across the run (sampled 34,142 → 34,233 →
34,431 PI/s), i.e. no degradation over 30 min / many compaction cycles.

### Resource usage (mid-run, under full load)

| Node | nano-gw RSS | loadgen RSS | System used (of 64 GB) | CPU idle (16 cores) |
|---|--:|--:|--:|--:|
| node-0 | 300 MB | 144 MB | 1.6 GB | ~43% |
| node-1 | 319 MB | 150 MB | 1.6 GB | ~43% |
| node-2 | 309 MB | 148 MB | 1.6 GB | ~43% |

- **Memory:** the engine holds a flat **~300–320 MB RSS** per node while
  leading one partition and replicating two peers' partitions at ~11.4k PI/s.
  Memory is not a constraint (jemalloc + idle-purge story holds under load).
- **CPU:** load average ~10/16, i.e. **~43% CPU idle**. nano-gw ≈ 6.9 cores,
  loadgen ≈ 1.3 cores → only ~8.2 of 16 cores busy.

### Disk / durability (post-run)

| File | Size / node | Behaviour |
|---|--:|---|
| `journal.jsonl` (live log) | **0 B** | Compacted to zero every 60s |
| `msnapshot.bin` (snapshot) | ~6.9 MB | Bounded, tiny |
| `read-model.sqlite` (projection) | 6.6–7.7 GB | Grows with history (~368 B/instance) |

- The **segmented journal + snapshot compaction bounds the durable log**: the
  live `journal.jsonl` truncates to ~0 and rolls into a few-MB snapshot each
  cycle. Mid-run the live segment was seen at 43–129 MB (accumulating between
  60s ticks) — bounded by the compaction interval, as designed.
- The **read-model projection is the disk-growth vector**, ~368 B per completed
  instance (≈ 7.7 GB for 20.9 M instances). This — not the journal — is the
  target for any history retention / rotation / exporter work. 179 GB was still
  free at the end (~6% used).

### Has the cluster hit its ceiling? **No.**

~34k PI/s was **not** the hardware ceiling — ~43% of CPU was idle and memory was
untouched. The run was bounded by the client-side concurrency knobs
(`MAX_INFLIGHT=6000/node`, worker/producer counts) and the resulting queuing
(p99 ~2.8 s), not by the machines. Two levers to push higher next time:

1. **Raise in-flight / producer concurrency** until CPU saturates.
2. **Move `loadgen` off the nodes** — co-location steals ~1.3 cores/node from
   the engine. A dedicated load generator (or a 4th box driving all three
   gateways) would free those cores.

Linear extrapolation from ~8.2 busy cores → 34k PI/s suggests **~50k+ PI/s** is
reachable on this same hardware before becoming CPU-bound.

### Reproduce

```bash
# Provision (us-central1-a, Debian 12, 200 GB pd-ssd)
gcloud compute instances create nano-node-0 nano-node-1 nano-node-2 \
  --zone=us-central1-a --machine-type=c2-standard-16 \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=200GB --boot-disk-type=pd-ssd --tags=nano-cluster

# On each node-N (N=0,1,2), launch the gateway (durable via systemd-run):
NANOBPMN_NODES='http://<ip0>:8080,http://<ip1>:8080,http://<ip2>:8080' \
NANOBPMN_NODE_ID=N NANOBPMN_RF=3 NANOBPMN_PARTITIONS=3 \
NANOBPMN_DATA_DIR=$HOME/run/data PORT=8080 ./nano-gw

# Deploy once, note processDefinitionKey:
curl -s -F resources=@test-job-process.bpmn http://localhost:8080/v2/deployments

# On each node, co-located load (localhost):
BASE_URL=http://localhost:8080 PDK=<key> WORKERS=256 PROD_CONNS=64 MAXPAR=4 \
MAX_INFLIGHT=6000 TRANSPORT=stream DURATION_S=1800 ./loadgen
```

## 2026-07-01 — Sharding shipped: exporter ceiling lifted 2.4× (~13k → ~31k/node)

The payoff for the diagnosis above. The read-model exporter + read store are now
**sharded per partition** (`4b7e24e`): a node owning *N* partitions gets *N*
`ReadStore` shards (each its own `read-model.p<pid>.sqlite`) and *N* exporter
threads, routed by partition. The single `Mutex<Connection>` that pinned one core
is gone. Projection CPU now scales with owned-partition count instead of
funnelling every partition through one thread.

### Environment

- Same GCP shape: **3× `c2-standard-16`** (16 vCPU / 64 GB, pd-ssd), us-central1-a,
  Debian 12, build `0.0.3-shard`. Segmented multi-partition journal (durable,
  60 s snapshot/compaction). Dedicated **`nano-loadbox`** (4th `c2-standard-16`)
  ran all `loadgen` — nothing co-located on the nodes.
- **Key change vs the earlier runs: 12 partitions instead of 3**, RF=3. With 3
  nodes that is **4 owned partitions → 4 shards → 4 exporter threads per node**.
  (The prior 3-partition topology gave only 1 shard/node — no parallelism to gain
  from sharding; the high partition count is what exercises it.)

### Results

| Topology | Agg tput | Per node | Bottleneck |
|---|--:|--:|---|
| 3 part / 1 shard/node (pre-sharding) | ~39k | ~13k | 1 exporter core @ 100% |
| **12 part / 4 shards/node (sharded)** | **~89–96k** | **~30–32k** | engine-actor / stream serialization |

- Clean plateau **~95k PI/s aggregate**; 60 s sustained window **~89k**
  (`producedRate ≈ tput ≈ 29.5k/node` — the cluster completes what it is fed).
  Pushing more producers (PPN 2–3, up to 9 loadgen procs) did **not** exceed the
  plateau while the loadbox stayed near-idle (load avg ~2/16), so the ceiling is
  the cluster, not the client.
- **2.4× per-node throughput** over the exporter-bound ceiling, on identical
  hardware.

### The exporter is no longer the wall

Under load, `perf` + per-thread sampling on a node show the former **single
100%-pinned exporter is gone**: `nanobpm-exporter` threads now sit at modest CPU
alongside `nanobpmn-journal-writer` and the `nanobpmn-engine` actors. At the new
ceiling the node runs at **~65% CPU (≈35% idle), only ~3% iowait**, with **one
hot thread** (engine-actor / command-stream `tokio` path, dominated by TCP
`sendmsg` syscalls) as the next serialization point — **not** the exporter and
**not** disk. So the sharding change did exactly what the diagnosis predicted:
relieved the projection core, and the ceiling moved on.

### Memory & disk

- **RSS flat at ~300–330 MB/node** despite each node now holding **4 shard SQLite
  DBs** instead of one — sharding did not inflate memory.
- Segmented journal still bounds the durable log (live `journal.jsonl` truncates
  each 60 s cycle into a small `msnapshot.bin`); read-model growth is now spread
  across the per-partition `read-model.p*.sqlite` shards.

### Next lever

The new ceiling is the **single-writer engine actor / command-stream path** (~35%
node CPU still idle, 3% iowait — a software-serial limit, not hardware). Options
to push further: shard/parallelise the command-stream dispatch, reduce per-event
TCP syscall overhead (batch stream frames), or scale partitions-per-node higher
to spread engine-actor work. Memory and disk remain non-constraints.

### Reproduce (delta from the run above)

```bash
# Build 0.0.3-shard; launch each node with 12 partitions, RF=3:
NANOBPMN_NODES='http://<ip0>:8080,http://<ip1>:8080,http://<ip2>:8080' \
NANOBPMN_NODE_ID=N NANOBPMN_RF=3 NANOBPMN_PARTITIONS=12 \
NANOBPMN_JOURNAL=segmented NANOBPMN_DATA_DIR=$HOME/nano-data PORT=8080 ./nano-gw

# From a dedicated load box, drive all three gateways (sweet spot):
#   WORKERS=70 PROD_CONNS=160 MAXPAR=80 MAX_INFLIGHT=16000 TRANSPORT=stream
# one loadgen per node IP → ~95k PI/s aggregate.
```
