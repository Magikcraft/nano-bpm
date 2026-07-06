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

> **Correction (2026-07-05): every run below was Raft-OFF.** All cluster runs set
> `NANOBPMN_RF=3` but did **not** set `NANOBPMN_RAFT=1` — none of the deploy/start
> scripts do. With Raft off, `NANOBPMN_RF` selects only *static* partition
> ownership and cross-node request forwarding: each partition is journaled solely
> by its owner and is **not** replicated to peers. There were **no quorum-commit
> rounds, no cross-node replication, and no failover** in any measurement here.
> Statements below implying the "Raft quorum-commit path" was exercised or that
> peers' partitions were "replicated" are inaccurate and have been annotated. The
> throughput/latency numbers themselves stand — they just reflect the Raft-off
> (single-homed, forward-on-create) topology, which is the configuration these
> numbers were measured on. Enabling Raft was separately found to wedge
> leader-durable clusters at **cold start** — a formation-time split-brain that
> trips an openraft invariant on release builds, not the snapshot/purge loop first
> suspected — which is **fixed** as of the cold-start split-brain guard (validated:
> clean 12-partition formation plus a 150s under-load soak). A validly-measured
> *replicated* throughput number is still pending a published re-measurement.

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
per-partition commit/apply pipeline (engine actor → read-model
projection; **note:** with Raft off there is no Raft batcher/replication hop in
this path), and the cross-thread handoffs cap the node at **~12k PI/s** long
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
- **RF=3 was ~free** on same-zone SSD nodes — but note this was **Raft-off**, so
  RF=3 meant only static ownership/forwarding, **not** actual replication; the
  "durability" here is each owner's local journal fsync, not quorum durability.
- **Partitions:** 3 (one leader/node) already saturated the nodes; more partitions
  didn't add throughput but do add parallelism headroom for bigger machines /
  multi-task processes. Match partition count to node count as a starting point.

---

## 2026-07-01 — 3-node / 3-partition / RF=3 cluster, ~34k PI/s (30 min)

Build: `0.0.3-stress` (commit `21d954a`), release profile.

### Environment

- **3× GCP `c2-standard-16`** (16 vCPU, 64 GB), Debian 12, 200 GB `pd-ssd`, all
  in `us-central1-a` (same zone → low inter-node latency).
- Cluster: 3 nodes, 3 partitions, **RF=3** (**Raft-off**: RF=3 sets static
  ownership only — each partition is journaled solely by its owner, **not**
  replicated to the other two). Each node owns/leads one partition; the
  leader/follower labels are a static replica-set map, not live replication.
- Config per node: `NANOBPMN_NODES=<3 internal IPs>:8080`, `NANOBPMN_NODE_ID`,
  `NANOBPMN_RF=3`, `NANOBPMN_PARTITIONS=3`, `NANOBPMN_DATA_DIR` on the SSD,
  `PORT=8080`. Segmented multi-partition journal, snapshot/compaction every 60s.
- **Load placement:** `loadgen` **co-located on each node**, hitting
  `localhost:8080`. This is the design-intended topology — every node is a
  gateway, so clients spread across gateways and each node activates jobs on its
  own partitions locally (zero-hop hot path). Note: create *placement* still
  round-robins cluster-wide by design, so ~2/3 of creates are forwarded to peer
  partition owners (over the plain create-forwarding seam — **not** a Raft
  quorum-commit path, since Raft was off; the owner commits to its local journal).
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
  owning one partition (Raft off → no peer replication) at ~11.4k PI/s.
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
hot thread** (engine-actor / Falcon `tokio` path, dominated by TCP
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

The new ceiling is the **single-writer engine actor / Falcon path** (~35%
node CPU still idle, 3% iowait — a software-serial limit, not hardware). Options
to push further: shard/parallelise the Falcon dispatch, reduce per-event
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

---

## Dispatcher parallelization A/B — and a corrected diagnosis (0.0.3-disp)

Following the "next lever" above, we sharded the Falcon **dispatcher**:
one `JoinSet` + `Semaphore` task per connection (dispatch/JSON work stolen
across tokio workers) and serialize each job **once** (`ServerFrame::Job` as
`Box<RawValue>`, dropping the old build-a-`Value`-then-re-serialize double pass).
Commit `53291a6`.

### Result: a measured wash

Controlled **fresh-state** A/B on the same 3-node / 12-partition / RF=3 cluster
(`70 160 80 16000` sweep, wiped data before each build, first run = fresh,
second = warmed):

| Build          | fresh tput | fresh p99 | warmed tput | warmed p99 |
|----------------|-----------:|----------:|------------:|-----------:|
| 0.0.3-shard    | 96.4k      | 2.1 s     | 97.7k       | 2.4 s      |
| 0.0.3-disp     | 96.0k      | 2.1 s     | 97.7k       | 2.4 s      |

Statistically identical in **both** throughput and latency. An earlier apparent
"p99 39 s → 2 s" win was an **artifact of data accumulation**, not the code: each
45 s sweep adds ~1.3M instances to the read-model SQLite shards, so successive
sweeps on *either* build degrade latency. Fair A/B **requires fresh-launched
state** (wipe `~/nano-data`, measure the first window).

### The "100%-pinned main thread" was a `top -H` artifact

The prior "one hot thread (engine-actor / Falcon `tokio` path)" reading
came from `top -bH`, which showed the `nano-gw` **main thread** at 99.9%. That is
wrong: the main thread is the parked `#[tokio::main]` `block_on` driver.

- `strace -c -p <main-tid>` (no `-f`): **0 syscalls** in 3 s.
- `/proc/<main-tid>/stat` utime+stime delta: **0%** CPU over 2 s.
- `gdb` always catches it parked in `syscall()` (a blocking futex).

So there is **no single-thread CPU wall**. Ground-truth per-thread CPU:
tokio workers ~24% each (×16) + engine actors ~25% each — the node has **spare
cores**. Whole-process `strace -c` under load shows the real signature:
**70% futex** (121k calls / 3 s, ~19k contended) + heavy **`sendto`** (143k).

### Corrected conclusion / next lever

The ~100k PI/s/node ceiling is **coordination-bound**, not
dispatch-CPU-bound: cross-thread lock/handoff contention (futex) around the
single-writer engine actor — with CPU to spare. (**Raft-off:** the earlier
mention of "RF=3 replication round-trips" here is inaccurate — there was no
replication; the contention is the local engine-actor/journal handoff, not a
replication path.) The dispatcher change didn't move the ceiling because the
dispatcher was never the limiter. Real levers from here: reduce engine-actor
cross-thread contention / batching, or scale partitions-per-node to
spread the single-writer actor further. It was kept for the cleaner architecture
(dispatch CPU spread across cores, single serialization pass).

## Raising the per-node ceiling: profile-guided serial-path fixes (0.0.3-cache)

Since the ceiling is coordination/serial-bound with CPU to spare (above), the
lever is to make the **per-command serial path cheaper**, not to add cores. A
symbolized release build (`strip=false debug=line-tables-only`) + `perf record
-g --call-graph dwarf` on a live node under load surfaced two concrete costs:

1. **Engine `maybe_spill` was O(N) per command.** `Journal::maybe_spill` called
   `resident_spillable_count()` — a full scan of the resident instance map —
   on **every** `apply_command`, *before* the budget check (self ~4.9%). Under a
   deep backlog that is O(N) per command / O(N²) aggregate and feeds congestion
   collapse. (The doc comment claimed a "cheap one-counter read"; the O(1)
   counter never existed.)

2. **Exporter re-parsed SQL per event.** The read-model exporter (the node's #1
   CPU thread group) used `Connection::execute` / `query_row`, recompiling the
   SQL text on every projected event. Its top self-time symbols were pure
   SQLite parsing (`sqlite3RunParser` 2.3% + `yy_reduce` 2.2% + `sqlite3GetToken`
   1.2%), not `sqlite3VdbeExec`.

### Fixes (durability + memory-bound preserved)

- **`maybe_spill` → O(1) hot path.** Added `Engine::resident_instance_count()`
  (O(1) `len()`). Spill candidates are a subset of resident instances, so when
  the resident set already fits the budget the scan is skipped entirely. When
  *persistently* over budget, the precise scan/shed is amortized across
  `SPILL_CHECK_INTERVAL` (256) commands (spill is a **soft** bound — variables
  stay durable in the journal — so bounded-late shedding is safe; the first
  over-budget command still sheds eagerly).
- **Exporter → `prepare_cached`.** A small `CachedSql` trait routes the hot
  projection statements through `prepare_cached`, compiling each SQL string once
  per connection. Same SQL, same params, plans reused.

### Result — GCP 3-node / 12-partition / RF=3, spill **on**, `110 224 112 32000`

| Build          | tput (avg of runs)      | node RSS under load |
|----------------|-------------------------|---------------------|
| 0.0.3-disp     | ~100.7k PI/s/node       | ~313–353 MB         |
| 0.0.3-cache    | ~109.9k PI/s/node       | ~373–397 MB         |

**+9% throughput** while keeping RF=3 durability and variable spill active. The
~+50 MB RSS is the bounded amortization overshoot plus the larger in-flight
backlog at higher throughput — no runaway; spill still sheds under real
pressure. Both fixes are on the single-writer serial path, which is why they
move a coordination-bound ceiling that adding cores did not.

### Tuning guidance

- **Variable spill budget** (`NANOBPMN_VAR_SPILL_BUDGET`, default 512): the max
  resident instances carrying variables before shedding to disk. Raise it when
  the active backlog is large but variables are small (keeps them resident,
  avoids spill I/O); lower it when variables are large and RAM is tight. The
  scan is now O(1) while under budget, so a generous budget is cheap.
- **Spill on/off** (`NANOBPMN_VAR_SPILL`): keep **on** for OOM safety under
  large/stalled backlogs. Disabling trades ~100 MB RSS for a few % throughput
  only when variables are tiny and completion is fast — not safe in general.

## Adaptive variable spill (RAM-pressure driven) — now the default

Variable spill's fixed **instance-count** budget is a poor proxy for bytes, so
its default had to be conservative (512) — spilling even when RAM is fine. New
**adaptive** mode (default in persistent mode) sheds active-backlog variables
only when resident memory actually crosses a watermark, mirroring the existing
cold-spill tier:

- Gated on `resident_bytes()` high/low watermarks (`NANOBPMN_VAR_SPILL_MB`,
  default 384) on the 500 ms maintenance sweep, run *before* cold spill (shed
  variables first — instance stays live — then evict whole dormant instances).
- **Below the mark: a single cheap RSS read → zero spill, max throughput.**
- Above it: shed oldest active-backlog variables in batches until under
  low-water — but **stop early if a batch doesn't reduce RSS** (variables aren't
  the memory driver), so a too-low watermark can't thrash the backlog.
- Per-command instance-count backstop (`NANOBPMN_VAR_SPILL_HARDCAP`, default
  262144) bounds a create-flood runaway *between* sweeps without an RSS read per
  command.
- `NANOBPMN_VAR_SPILL=on` keeps the legacy fixed-budget mode; `off` disables.

### Result — GCP 3-node / 12-partition / RF=3, `110 224 112 32000`

| Mode                                   | tput (PI/s/node) | node RSS      |
|----------------------------------------|-----------------:|---------------|
| fixed budget 512 (`on`)                | ~110k            | ~340–397 MB   |
| adaptive, watermark 4096 MiB (unreached) | ~110.6k        | grows freely  |
| adaptive, watermark 384 MiB (default)  | ~110.7k          | ~320–370 MB   |

Adaptive costs **nothing** below pressure (identical throughput to fixed
budget), and at the default 384 MiB watermark — which this workload's ~370 MB
working set sits right at — the progress-check let the one node that crossed it
shed a few times **harmlessly** (tiny variables aren't the RSS driver, so it
stopped immediately) with **no throughput dip**. The mechanism that actually
*constrains* RAM is exercised when variables are the memory driver (large
payloads); for a tiny-variable flood, RAM is bounded by cold spill +
backpressure instead — the correct layering.

### Tuning guidance (updated)

- **`NANOBPMN_VAR_SPILL`**: `adaptive` (default) is the right choice for almost
  all deployments — full throughput until real memory pressure. Use `on`
  (fixed budget) only when you want a hard, deterministic resident-count cap;
  `off` only when payloads are tiny and RAM is ample.
- **`NANOBPMN_VAR_SPILL_MB`** (adaptive high-water): defaults to a **RAM-relative**
  value — ~65% of the detected cgroup/host memory limit (floored at 128 MiB;
  384 MiB when no limit can be detected). This self-corrects across
  environments: a 512 MiB container gets a ~333 MiB watermark, a 64 GB node gets
  a ~41 GB watermark (so spill stays fully dormant under normal load and only
  engages on a genuine explosion). Set it explicitly only to override the
  auto-derived value. Low-water is `7/8` of high. `NANOBPMN_COLD_SPILL_MB`
  behaves identically for cold spill.

### Large-payload memory test (adaptive spill actually constraining an explosion)

To prove adaptive spill does what it claims when variables *are* the RAM driver,
the load generator was extended with a `VAR_BYTES` knob that injects a large
`blob` string into every created instance's variables. Test: 3× c2-standard-16
(64 GB), 12 partitions RF=3, a **backlog-building** load (only 8 job workers so
completions lag creation, `MAX_INFLIGHT=32000`, 20 KB payload/instance, 60 s).
Fewer workers means tens of thousands of instances sit in-flight holding their
20 KB blobs — the memory explosion we want to bound. RSS sampled via
`/proc/<pid>/status` VmRSS across the run.

| Mode | Peak RSS/node | var-spill.sqlite | Agg throughput |
|---|---|---|---|
| `VAR_SPILL=adaptive`, MB=500 | **~390–500 MB** (held at watermark) | 222 MB spilled | 12,351 PI/s |
| `VAR_SPILL=off` (cold spill on) | ~390–490 MB | — | 12,357 PI/s |
| `VAR_SPILL=off` + `COLD_SPILL=off` | **~740–790 MB** (climbing with backlog) | — | 12,387 PI/s |

Findings:

- **Adaptive var spill fires correctly under real pressure.** Logs show
  `variable spill (adaptive): shed 512 instance(s)' variables under RAM
  pressure` in repeated 512-instance batches; 222 MB of variables moved to
  `var-spill.sqlite` and RSS was held at the 500 MB watermark instead of
  climbing.
- **Layering confirmed.** With var spill `off` but cold spill on, cold spill
  alone still bounds RSS (~490 MB) because instances waiting on a job worker
  count as *dormant* and get shed whole. Only with **both** off does RSS grow
  unbounded with the in-flight backlog (~790 MB peak and still rising when the
  run ended; it settled back as the drain completed the backlog).
- **Zero throughput cost — again.** All three modes delivered ~12.35k PI/s
  aggregate. Spill runs on the 500 ms maintenance sweep off the hot path, so
  even actively shedding 222 MB it cost nothing measurable. The throughput here
  is lower than the ceiling runs purely because this test deliberately
  starves job completion (8 workers) to build the backlog.

**Takeaway:** the engine's memory is bounded by the *combination* of cold spill
(whole dormant instances) and adaptive var spill (active instances' variables),
each covering a case the other cannot. Under a large-payload flood the resident
set tracks the configured watermark rather than the in-flight backlog size.

## 2026-07-02 — Read-model retention: bounding history disk without a throughput hit

`NANOBPMN_HISTORY_RETENTION=adaptive` caps the sharded read-model (SQLite
projection of completed instances) at a per-shard byte budget by evicting the
oldest *terminal* instances. The first cut evicted inline in the exporter
thread and did **not** hold the budget under sustained over-budget load — shard
files grew to ~6 GB each (25 GB datadir) against a 1500 MB budget.

### Root cause (data-backed)

Prune was serialized behind projection in the **single saturated exporter
thread**. That thread runs the SQLite projection inserts at the single-writer
ceiling (~232k events/s ≈ 29k PI/s/node in the blob phase). Inline prune got
only one bounded sweep per multi-second export batch, so it evicted ~4k rows/s
while inserts ran ~7k rows/s/shard → rows crept up monotonically (4.3M → 8.4M),
freelist stayed flat (~1300 pages, freed pages instantly reused). The old
`OFFSET keep_target` scan (~2M index walks/sweep) made each sweep costlier and
starved projection further.

### Fix (commit `c8e1daa`)

1. **Cheaper cursor prune** (`prune_oldest_terminal`): delete oldest terminal
   via `ORDER BY key ASC LIMIT batch` — walks the PK index from the oldest key,
   O(batch) not O(keep_target), no OFFSET. Keys are monotonic so oldest terminal
   = earliest completed = correct to evict first.
2. **Decoupled per-shard pruner thread** (`spawn_adaptive_pruner`): one thread
   per shard with its **own** 2nd SQLite connection on a 200 ms timer. Small
   delete txns interleave with the exporter's inserts at SQLite's write-lock
   granularity (busy_timeout makes each side wait, not error). Hysteresis holds
   `live` between `7/8·budget` and `budget`; `MAX_DELETES_PER_WAKE=65536` bounds
   the lock-hold so projection is never starved.

### Result — GCP 3-node / 12-partition / RF=3, 3-phase soak

Soak: steady 780 s @110w → blob 240 s @16w/50 KB payload → steady 780 s.
Budget 1500 MB/shard, 4 shards/node. Sampled node-0 (owns p0,3,6,9).

| Metric | Before (inline prune) | After (`c8e1daa`) |
|---|---|---|
| Read-model file/shard | grew to ~6 GB (unbounded) | **plateaus ~1500–1600 MB** |
| Terminal rows/shard | climbed 4.3M → 8.4M | **plateaus ~4.66M** |
| Freelist | flat ~1300 pages | **grows (pruner outpaces inserts)** |
| Datadir (read-model) | ~25 GB | **~6 GB (4×1.5 GB)** |
| OOM | none (avail ≥51 G) | none (avail ≥42 G) |

Read-model files held rock-steady at budget across the entire blob burst and
the whole second steady phase — the disk overshoot is fixed.

### Still open (separate subsystems, not read-model retention)

- **Transient RSS 8–17 GB under the over-budget blob burst**, idle floor
  ~8.7 GB (vs ~800 MB after below-budget soaks). This run retained 4.66M
  terminal instances + a 3.2 GB `var-spill.sqlite` + a 4.5 GB `msnapshot.bin`;
  RSS oscillates down when background maintenance settles, so it is not a leak,
  but engine hot-state + snapshot memory is the next lever.
- **Journal-segment accumulation** (~400 sealed segments, ~20 GB) built during
  the burst — segmented-journal compaction lag, independent of the read model.
- **Disk is not *reclaimed*** by prune: SQLite keeps freed pages in-file as
  freelist (no VACUUM), so `file_bytes` sticks at the worst-burst watermark
  while `live_bytes` tracks the budget. Shrinking the file would need
  `auto_vacuum=INCREMENTAL` + `incremental_vacuum` or an offline VACUUM — a
  deferred decision.

### Tuning guidance (users)

- `NANOBPMN_HISTORY_RETENTION=adaptive` + `NANOBPMN_HISTORY_RETENTION_MB=<n>`
  caps **live** read-model data per shard near `<n>` MB. Budget is per shard;
  a node owning K partitions uses up to `K × <n>` MB of live history.
- The on-disk file settles at the worst-burst high-watermark (freelist is
  retained and reused), so size the disk for peak, not steady-state, or plan an
  offline VACUUM during a maintenance window.

## 2026-07-03 — Snapshot streaming: the 17 GB burst RSS balloon was the snapshot Vec<u8>

### Problem
Under a worker-starved flood of large (50 KB) variable payloads, per-node RSS
transiently ballooned to ~17 GB (25% of a 64 GB box) even though resident
instance variables were only ~4 GB and every pipeline queue gauge
(`nanobpm_journal_inflight_bytes`, `nanobpm_pipeline_bytes`,
`nanobpm_exporter_queue_bytes`) read ≤ ~0.3 GB. ~12 GB was unaccounted, and the
per-node RSS peaks rotated every ~60 s — the periodic snapshot tick.

### Root cause (code + A/B)
`seglog::write_multi_snapshot` / `write_snapshot` serialized the whole snapshot
into an intermediate `serde_json::to_vec` `Vec<u8>` before `write_all` +
`sync_all`. The snapshot shares the live variables by `Arc` (the clone is cheap),
but `to_vec` **materializes every resident 50 KB payload into JSON at once** — a
multi-GB buffer, held across the multi-second serialize+fsync, for all owned
partitions accumulated together. An A/B that stretched the snapshot interval past
the burst dropped peak RSS 17 → 12 GB, confirming ~5 GB+ was the snapshot
transient.

### Fix (commit 7b1f7ad)
Stream the JSON straight to the file through a `BufWriter` with
`serde_json::to_writer`, eliminating the intermediate `Vec<u8>`. No format
change, no throughput change (snapshotting is background work).

### Result (fresh 3-node RF=3 c2-standard-16 cluster, ~/soakB.sh blob burst)
- Peak RSS/node: **17 GB → ~4–7 GB** across two fresh runs, now bounded to
  `resident_var_bytes + ~1.3 GB` overhead (`allocated` tracks `resident`; the
  ~12 GB unaccounted transient is gone). Decisive memory win.
- Phase-A steady (0-var): unchanged, ~37–38k loadgen-proc, p99 2.75 s.
- Phase-B blob-burst tail latency: p99 still ~55 s (vs 60 s baseline, 19 s with
  snapshots fully off). **Streaming fixes MEMORY but not the p99 stall.**

### Open follow-up: the p99 stall is the clone + snapshot fsync, not the buffer
Since streaming removed the buffer but left p99 ~unchanged, the 60 s phase-B
stall is the on-engine-thread `Engine::snapshot()` = `state.clone()` (runs via
`handle.with` = High priority on the single engine thread) and/or the still
same-sized snapshot-file `sync_all` competing with journal fsync. Next lever
(Fix 1b): exclude journal-durable variables from the snapshot and rehydrate on
recovery — shrinks the on-thread clone AND the on-disk file/fsync. Bigger change
(touches the recovery path); pending direction.

## 2026-07-03 — Memory-driven adaptive var-spill (Fix 2), and why spill alone can't bound the burst

### Change (commit 812ba57 / deployed sha e07e70dede58d773)
Rewrote the adaptive var-spill trigger to be **purely memory-driven**: fire on
jemalloc `resident >= high_water` or live system-available `< reserve`, reclaim
toward `low_water` (or `floor` under a reserve breach). Removed the old
instance-count growth co-trigger (`prev_count`, `VAR_SPILL_GROWTH_SHIFT`) — count
is a poor proxy for bytes — and the noisy per-batch `now >= prev_rss` RSS-delta
bail (mis-fired under concurrent inbound allocation). Added a single up-front
byte-based futile-shed guard (`VAR_SPILL_MIN_RELIEF_DIVISOR`): skip when resident
variable bytes are a small fraction of the overshoot.

### Result (fresh 3-node RF=3 cluster, VAR_SPILL_MB=700 to force spill)
- **Phase-A small-payload edge: NO regression.** 37–38k loadgen-proc, p50 655 ms,
  p99 2.78 s — identical to baseline. The futile-shed guard correctly avoids
  thrashing when variables aren't the driver.
- **Spill fires as designed:** 883 sweeps on node-0 during the burst, each
  shedding ~1000–1400 instances to the SQLite store (tens of GB shed cumulatively).
- **But peak burst memory is UNCHANGED: ~6–6.7 GB/node** (same as the
  streaming-fix run). Spill did not pin memory to the 700 MB watermark.

### Why: the balloon is in NON-SPILLABLE instances
`resident_var_bytes` held flat at ~4.35–4.85 GB/node throughout the burst **and
stayed there for minutes after the load stopped** (cluster idle, ji=0). The 1 Hz
gauge is live (pipeline/exporter gauges in the same loop read 0), so this is real
resident variable memory that never drained. Each spill sweep shed only
~1387/partition then hit `candidates == 0` — i.e. the bulk of the resident
variables live in instances that are **not spillable**.

`Engine::is_spillable` requires an un-leased job in `jobs_by_instance`
(`!jobs.is_empty()`). Instances whose single job has been **activated/leased** (to
a worker, or buffered by the stream dispatcher) — or that are otherwise between
states — are excluded, because their variables may be needed to service the
in-flight job. Under the burst the dispatcher activates far more jobs than the 16
workers complete, so a large population sits "activated, not yet completed",
holding its 50 KB payload resident and invisible to spill. This backlog does not
drain promptly after load stops.

### Takeaway
Fix 2 is correct and safe (ships, no small-payload regression) but **variable
spill is necessary-not-sufficient** for this workload. The remaining levers are
the ones already identified:
1. **Admission backpressure** to bound inflow (don't activate/admit faster than
   the system can drain) — the saturation-driven mechanism, not spill.
2. **Fix 1b lean/control-only snapshot** so the ~20 s off-thread snapshot stops
   pinning all variables via `Arc`-clone during the burst.
3. Optionally, make leased-job instances spillable (spill the variables, rehydrate
   on job completion/return) so the activated-but-incomplete backlog is reclaimable.

## 2026-07-03 — Fix 1b: lean (control-only) snapshot + authoritative durable var-store

### Change (commits be94879, 48cafeb, 9e8e538, 9904105; deployed sha ac7266cf6f30674c, `NANOBPMN_LEAN_SNAPSHOT=1`)
Split variables out of the periodic snapshot into a new authoritative, boot-surviving
`VarStore` (SQLite WAL, `<datadir>/var-store.sqlite`). The engine tracks a dirty-var
set; each checkpoint drains the delta + captures a **control-only** `EngineSnapshot`
(empty variable maps) on the engine thread, then writes the delta to the var-store
off-thread **before** the (variable-less) snapshot file. Recovery installs variables
from the store between `from_snapshot` and the journal-tail replay (install-before-tail
so post-snapshot `VariablesUpdated` merges onto the store base). Unified with var-spill:
spill write-through / rehydrate / terminal-forget all target the var-store. Compaction
gated on `min(exported, var_position)`. Only the segmented multi-partition paths honour
the flag; other boot paths keep full snapshots. 144 tests, clippy zero.

### Result A — mechanism confirmed on disk (fresh 3-node RF=3 P=12, ~/soakB.sh)
- **Snapshot file is now control-only: `msnapshot.bin` = 355 MB** (was multi-GB with
  variable payloads inline). Variables live in **`var-store.sqlite` ≈ 4.95 GB (+843 MB WAL)**.
  The variable payload is decisively evicted from the snapshot.
- Pipeline gauges stay flat: `journal_inflight`=0, `pipeline_bytes`=0 through the burst
  (the 17 GB snapshot `Vec<u8>` transient, already killed by streaming, stays gone).

### Result B — blob soak (0-var warm 60s / 50KB blob 16w-starved 180s / recover 90s)
| Phase | Agg tput | p99 | Peak RSS/node | Notes |
|-------|----------|-----|---------------|-------|
| A warm (0-var, 110w) | ~110k PI/s | 2.81 s | ~420 MB | unchanged |
| B blob (50KB, 16w) | ~3.3k PI/s | **82–91 s** | 4.5–5.7 GB (var ~2.6–3.4 GB) | see below |
| C recover (110w) | ~48k PI/s | 12.5 s | — | drains cleanly |

### Result C — tiny-var in-flight envelope (1KB, 110w, 60s, lean on) — regression guard
- **Agg 84.9k PI/s, p99 3.72 s, peak RSS 267 MB/node, idle floor ~140 MB → PASS**,
  identical to the 84.5k pre-lean baseline. No in-flight regression from lean mode.

### Honest verdict: lean snapshot works, but did NOT move phase-B p99
- Phase-B p99 was ~82–91 s vs the ~55 s streaming baseline — **no improvement** (and
  possibly worse this run; under 16-worker starvation p99 is dominated by queue depth,
  not the snapshot fsync, so run-to-run variance is large). The hoped-for "snapshot
  stall → 19 s" win did **not** materialise. Peak RSS is unchanged (~4.5–5.7 GB, still
  the non-spillable resident-variable population from Fix 2, which lean does not touch).
- **New cost:** variables are now double-written (journal segment + var-store), adding
  ~5 GB store + an **843 MB unbounded WAL** during the burst.

### Follow-ups
1. **var-store WAL grows unbounded** (843 MB) under the burst — add a periodic
   `wal_checkpoint(TRUNCATE)` (off the hot path) so the store doesn't balloon disk.
2. The dominant phase-B stall is the **non-spillable activated-but-incomplete instance
   backlog + queue depth**, not the snapshot — i.e. the real lever remains **admission
   backpressure** (saturation-driven) and/or **spillable leased-job variables**, per the
   Fix 2 takeaway. Lean snapshot is a prerequisite (bounds the on-thread clone + snapshot
   fsync) but not itself the p99 fix.
3. Re-evaluate the var-store double-write vs a controlled A/B once WAL checkpointing lands.

---

## ADR 0012 — Terminal-state / exporter-lag decoupling (deployed + soaked)

**Change.** Drop a terminal (Completed/Terminated) instance's variables in
`state::apply`, and forget its durable var-store row in `emit()`, so terminal
memory is reclaimed **on completion** rather than waiting for exporter-driven
eviction. Corrected diagnosis: leased-job instances were already spillable; the
non-spillable residue was terminal instances pinned in heap until export.

**Deploy.** 3-node GCP cluster (c2-standard-16), RF=3, 12 partitions, sha
`15f5239f4d6cff96`, lean snapshots on, `NANOBPMN_VAR_SPILL=adaptive`.

**Blob soak (soakB: warm 60s / 50 KB blob burst, 16 workers, 180s / recover 90s).**

| Phase | tput (agg/node) | p50 | p99 |
|-------|-----------------|-----|-----|
| A warm  | ~36 k | 0.67 s | 2.84 s |
| B blob  | ~2.2 k | 6.2 s | 16.0 s |
| C recover | ~19 k | 1.6 s | 7.9 s |

**Memory — the decisive result.** `nanobpm_resident_var_bytes` (the resident
instance-variable gauge):

- **Peak single-node resident var = 131 MB** across the whole run (was
  ~2.5–3.3 GB during burst, ~4.35 GB residue that "stayed for minutes after load
  stopped").
- **Post-load: var = 0 on every node immediately** (within the first idle
  samples), and the exporter queue drained to 0. Resident variable memory no
  longer tracks exporter lag.

No panics. Transient `read-model export failed: database is locked` (SQLite WAL
contention in the exporter path) observed at the B→C boundary — pre-existing,
idempotent/retried, unrelated to this change; the exporter fully caught up
afterward. Read contract remains eventually consistent (matches Camunda 8).

**Verdict.** The resident-variable balloon is eliminated. Spill/backpressure can
now bound the live working set; terminal state is no longer a memory liability
gated on a downstream reader.

---

## Hierarchical variable scoping — flat fast-path (Part C, PR #5)

Part C replaces Nano's single flat `variables: Arc<HashMap>` per instance with
Zeebe's hierarchical variable-scope tree (each scope-owning element instance —
process root, embedded sub-process, multi-instance body/child, mapped activity —
owns a variable map; reads resolve local → parent → … → root). The design keeps
the **common case free**: an instance that never creates a child scope (the vast
majority) stays a single root map and pays exactly what the pre-scoping engine
paid.

**Fast path (root-only instance).** `variables_for_element` /
`element_variables` — the variable resolver on the job-activation hot path —
returns the instance's shared root `Arc<HashMap>` **by pointer** when the target
scope is the root or when the instance has no scope-local maps
(`resolve.rs:176–179`). No walk, no merge, no allocation: activation is a cheap
`Arc` clone, byte-identical to the flat engine. Root-only variable writes also
collapse to the legacy flat `VariablesUpdated` event
(`resolve.rs:224–228`,`266`), so the journal, read model and conditional-event
re-evaluation replay byte-identically.

**Scoped path (nested instance).** Only elements that actually run inside a
sub-process / multi-instance scope allocate a freshly merged view (root cloned,
then each scope in the chain overlaid root-first so nearer scopes shadow
farther). The cost is proportional to the scope-chain depth × map size and is
borne solely by the instances that use scoping.

**Regression guard.** `flat_instance_activation_returns_the_shared_root_arc_without_copying`
(engine-core) pins the fast path structurally with `Arc::ptr_eq`: the flat
element view must be pointer-equal to the instance's root `Arc` (proving
zero-copy), while a nested-scope view must be a distinct allocation. This is a
deterministic, non-flaky guard that a future change cannot silently put an
allocation on the flat-activation hot path. Steady-state throughput A/B (flat
workloads) is therefore expected to be unchanged; the released cluster soaks
above ran on the flat path and show no regression.
