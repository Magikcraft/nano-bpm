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

## 2026-07-07 — Durability A/B ceiling: the "journal-writer wall" is a **misdiagnosis**

**Question:** how much of the RF=3 ceiling is the journal-writer fsync barrier?
Ran a 3-arm stepped open-loop ceiling probe on the live GCP cluster (3× c2-std-16,
RF=3, 12 partitions, leader-durable, lean-snapshot, deployed v0.0.6 binary
`aa88ee0a` which honors the durability knobs). Harness: `~/dur-ceil.sh` — per arm,
clean staggered restart (stop → wait-for-exit → wipe → `node-launch-dur.sh <arm>`),
fresh fixture (PDK=4), then a per-node `RATE` ladder 2k→12k (offered 6k→36k
aggregate), `MAX_INFLIGHT=0` open-loop, 25 s windows. Recorded aggregate
completed/s (server `job_completions_total` delta), ratio, worst p99, **journal-
writer duty cycle** (`journal_writer_busy_seconds`/(busy+idle) delta, max of 3
nodes), and peak RSS.

| arm | durability | offered→ | 6k | 12k | 18k | 24k | 30k | 36k |
|-----|-----------|----------|----|-----|-----|-----|-----|-----|
| **baseline_sync** | sync, linger 0 | comp/s | 5556 | 11034 | 16759 | 21963 | **27249** | 10544 ✗ |
| | | ratio | .93 | .92 | .93 | .92 | **.91** | .29 |
| | | p99 ms | 99 | 99 | 143 | 333 | **349** | 30218 |
| | | writer % | 37 | 33 | 45 | 43 | **46** | 37 |
| **async** | async, flush 10ms/8MiB | comp/s | 5573 | 11187 | 14808 | 11535 ✗ | 1833 | 1679 |
| | | ratio | .93 | .93 | .82 | .48 | .06 | .05 |
| | | p99 ms | 99 | 118 | 660 | 30678 | 34887 | 47118 |
| | | writer % | 16 | 17 | 27 | 19 | 19 | 24 |
| **sync_linger** | sync, linger 500µs | comp/s | 5337 | 10932 | 15999 | 21026 | **26624** | 12682 |
| | | ratio | .89 | .91 | .89 | .88 | **.89** | .35 |
| | | p99 ms | 99 | 99 | 101 | 285 | **249** | 3911 |
| | | writer % | 76 | 62 | 74 | 48 | **41** | 46 |

(✗ = past the knee / collapsed.) All three arms formed cleanly (0 Shutdown
partitions, PDK=4). The `s+:` awk warning in the log is a cosmetic empty-scrape
transient; completed counts are monotonic and consistent.

**Findings (decisive):**
1. **The journal writer is NOT the throughput wall.** In the sync baseline the
   writer duty cycle never exceeds **~46%** at the ceiling — and is only **37%**
   at the 36k collapse. A component that is idle >50% of the time at the ceiling
   cannot be the bottleneck. The p99 cliff (349 ms → 30 s) between 30k and 36k
   offered is a classic **queue collapse when offered load exceeds the serial
   drain rate**, not fsync saturation.
2. **async durability makes it WORSE, not better.** Deferring fsync to the page
   cache *halved* writer duty (16–27%) exactly as designed — yet throughput did
   **not** rise, and the ceiling *dropped* from ~27k to ~11–15k with an earlier,
   harder collapse (already degrading at 18k, gone by 24k). Removing the fsync
   from the ack path buys nothing because fsync wasn't the constraint; the async
   flush machinery (10 ms/8 MiB bursts) adds its own bursty backlog under RF=3
   leader-durable replication. **Do not enable async here.**
3. **sync + 500µs linger ≈ baseline throughput** (26.6k vs 27.2k) with a *slightly*
   better tail at high load (p99 249 vs 349 ms) and a **much softer collapse**
   (3.9 s vs 30 s p99 at 36k). Linger coalesces entries into fewer, larger fsyncs
   (duty higher at low load, 76%, but *lower*, 41%, at the ceiling). A minor
   graceful-degradation / tail win, **not** a throughput lever.

**Verdict:** the ~27k PI/s RF=3 ceiling is **not** the fsync barrier. Durability
tuning cannot break it (writer <50% busy at the ceiling; async lowers it). The
real constraint is downstream of the journal: the **single-writer engine actor
(Deepthi) serial apply lane shared by create/complete/state-apply**, plus the
RF=3 quorum-commit round-trip. Breaking through means **parallelizing that serial
lane** (per-partition engine actors / a dedicated apply task so a create flood
can't starve commit+apply) or scaling out partitions/nodes — the same horizontal
answer Zeebe reaches. Keep `sync` as the default; `sync_linger=500µs` is a
candidate default purely for tail-latency/graceful-degradation, not throughput.

## 2026-07-07 — First validly-measured RF=3 **Raft-ON** ceiling (replicated, leader-durable)

The first throughput measurement with Raft actually **on** (`NANOBPMN_RAFT=1` +
`NANOBPMN_REPLICATION=leader-durable`), i.e. real cross-node partition ownership
with the observability fix from PR #52 (`nanobpm_raft_partition_shutdown` gauge)
live. This is the replicated re-measurement the correction note above was waiting
for.

### Environment
- 3× GCP `n2` nodes (`10.128.0.19/.20/.18`), `NANOBPMN_RF=3`, `NANOBPMN_PARTITIONS=12`,
  `NANOBPMN_RAFT=1`, `NANOBPMN_REPLICATION=leader-durable` (acks=1, single-voter
  async replication window), segmented journal, lean snapshots.
- Binary `nano-gw` sha `aa88ee0a1b532fb7`, release `--features console`, source ==
  `main` @ `7545d23` (#52) byte-for-byte.
- Load: `loadgen` (`ts-performance-matrix/rust-worker`), **RATE-paced with
  `MAX_INFLIGHT=0`** (client-side inflight gate disabled; the server's own
  submission-credit scheme is the only backpressure). One loadgen per node IP.
  `WORKERS=140–160`, `PROD_CONNS=96–128`, `MAXPAR=140–160`, `TRANSPORT=stream`.
- Method: stepped offered-load ramp; per step the **global** completion rate is
  measured from `/metrics` deltas across all 3 nodes (independent of loadgen
  per-instance accounting), and `Shutdown` partitions counted from `/debug/raft`.

### Results — offered vs. completed (global, RF=3 Raft-ON, leader-durable)

| Offered/s | Completed/s | ratio | p50 | p90 | p99 | Shutdown |
|----------:|------------:|------:|----:|----:|----:|:--------:|
| 600       | 596         | 0.99  | –   | –   | –     | 0 |
| 900       | 884         | 0.98  | –   | –   | –     | 0 |
| 1,200     | 1,178       | 0.98  | –   | –   | –     | 0 |
| 1,500     | 1,472       | 0.98  | –   | –   | –     | 0 |
| 1,950     | 1,910       | 0.98  | –   | –   | –     | 0 |
| 2,400     | 2,344       | 0.98  | –   | –   | –     | 0 |
| 3,000     | 2,928       | 0.98  | –   | –   | –     | 0 |
| **3,900** | **3,813**   | **0.98** | 22ms | – | –  | 0 |
| 4,800     | 4,280       | 0.89  | 22ms | 343ms | 1,541ms | 0 |
| 6,000     | 4,765       | 0.79  | 23ms | 247ms | 1,590ms | 0 |
| 7,800     | 5,999       | 0.77  | 25ms | 652ms | 2,858ms | 0 |
| 9,600     | 6,768       | 0.70  | 29ms | 578ms | 2,093ms | 0 |
| 12,600    | 8,581       | 0.68  | 30ms | 972ms | 2,838ms | 0 |
| *uncapped flood* | **862** | –  | –   | –   | –     | 0 |

### Findings
- **Sustainable (balanced, no backlog growth): ~3,800 completions/s**, p50 ~22ms —
  offered==completed at ratio ≥0.98 all the way from 600 → 3,900/s. One job == one
  process instance, so this is ~3,800 PI/s replicated.
- **Knee ~4,000–4,800/s** offered (ratio falls to 0.89 at 4,800). Above the knee
  the median stays low (p50 22–30ms) but the tail grows (p99 1.5–2.9s) as a
  server-side backlog builds.
- **Peak drain with queueing: ~8,600 completions/s** at 12,600/s offered — the
  engine's raw drain capacity, sustained only while working down backlog.
- **Congestion collapse under unbounded flooding → 862/s.** With producers
  uncapped (`MAX_INFLIGHT=0` *and* no `RATE`), create pressure starves the
  single-writer engine actor (creation and job activation/completion share it),
  cutting completion throughput ~4×. Always drive load **RATE-paced**.
- **Zero `Shutdown` partitions at every level, 600 → 12,600/s offered.** Raft-ON
  leader-durable never flatlined; the `nanobpm_raft_partition_shutdown` gauge
  (PR #52) read 0 throughout and correctly lit up 8/node during a deliberately
  mis-ordered restart, validating the alarm live.

> **Harness note.** A clean RF=3 restart must be **stop-ALL → wait-all-exit →
> wipe-ALL → start-ALL**. Per-node interleaved restart (stop+wipe+start one node
> at a time) races: a freshly-formed node replays membership from index 0 while a
> peer still holds a long divergent log for that partition → openraft Defensive
> `LogIndexNotFound{want:0}` → the partition's RaftCore enters `Shutdown`
> permanently. The per-node wait-for-exit guard is necessary but not sufficient;
> the collision is cross-node.

### Reproduce
```bash
# On each node (stop-all, wait-exit, wipe-all, then start-all — NOT interleaved):
NANOBPMN_RAFT=1 NANOBPMN_REPLICATION=leader-durable NANOBPMN_RF=3 \
  NANOBPMN_PARTITIONS=12 ... nano-gw   # see ~/node-start-raft.sh
# From the load box, one loadgen per node IP, RATE-paced, MAX_INFLIGHT=0:
for ip in <n0> <n1> <n2>; do
  BASE_URL=http://$ip:8080 PDK=$PDK WORKERS=140 PROD_CONNS=96 MAXPAR=140 \
    RATE=1300 MAX_INFLIGHT=0 TRANSPORT=stream DURATION_S=80 loadgen &
done
# Ceiling = highest RATE where global completed/offered stays ≥ ~0.97.
```

### SLA-mode A/B (`latency` vs `admission`) — the modes converge on this workload

Apples-to-apples A/B of `NANOBPMN_SLA_MODE=latency` (default; sheds admission to
preserve e2e latency) vs `admission` (keep admitting, accept latency), both with
`NANOBPMN_ADMISSION_MAX_BACKLOG=2000`, differing **only** in `SLA_MODE`. Same
cluster/binary as above, fresh **stop-all → wipe-all → launch-all** per mode,
RATE-paced open-loop (`MAX_INFLIGHT=0`), 25s steady windows at three offered
levels straddling the ~3,800/s knee. Per level: global completed/s, net backlog
added over the window (`Δcreate_frames − Δcompletions`), `ceiling_active{throughput}`,
peak node RSS.

| Offered/s | `latency`: comp/s · backlog · RSS | `admission`: comp/s · backlog · RSS |
|----------:|:---------------------------------:|:-----------------------------------:|
| 3,000     | 2,781 · 3,039 · 629 MB            | 2,679 · 3,083 · 638 MB              |
| 4,200     | 3,855 · 4,214 · 842 MB            | 3,700 · 4,277 · 873 MB              |
| 5,400     | 4,976 · 6,130 · 996 MB            | 4,737 · 6,224 · 995 MB              |

Loadgen e2e latency at 5,400/s offered (per-node): **`latency` p50 22 / p90 36 /
p99 58 ms vs `admission` p50 22 / p90 37 / p99 56 ms** — identical. `ceiling_active`
read 0 throughout both modes; peak RSS ≤ 1 GB; 0 `Shutdown`.

**Finding — the two modes are statistically indistinguishable** across throughput,
backlog growth, RSS, and latency percentiles. The `MAX_BACKLOG=2000` gate **never
engaged** in either mode (backlog freely grew past 2,000). This reproduces the
ADR 0013 "modes converge" result with fresh RF=3 Raft-ON data.

**Root cause of the dead gate (code-verified).** The latency-mode active-backlog
gate (`admission_shed`, `main.rs:8764`) keys off `self.inflight` — an atomic
seeded from `active_instance_count()` and updated **only in the read-model exporter
projection loop** as `created − completed` per projected batch (`main.rs:1108`).
For `test-job` (one service task, worker completes in ~22 ms) instances are created
and completed almost immediately, so each projected batch nets `created − completed
≈ 0` and `self.inflight` stays near zero — it never approaches 2,000. The gate
measures **parked active instances**, but throughput-bound pressure here is
**transient in-flight pipeline work** (uncommitted creates, queued/activating jobs)
that the gate cannot see. So latency mode is a no-op for a fast create→complete
workload; it would only bite when instances genuinely park active (slow/absent
workers, timers, waiting events). A latency/backpressure control that protects
throughput-bound clusters must trigger on a **saturation** signal (create-queue
depth, job-type activation-wait, journal-fsync latency, engine-mailbox delay),
not on projected active-instance count.

### Reproduce (SLA A/B)
```bash
# Per mode: stop-all -> wait-exit -> wipe-all -> launch-all with
#   NANOBPMN_SLA_MODE=<latency|admission> NANOBPMN_ADMISSION_MAX_BACKLOG=2000
# then RATE-paced open-loop at RATE=1000/1400/1800 per node (offered 3k/4.2k/5.4k),
# WORKERS=160 PROD_CONNS=128 MAX_INFLIGHT=0 TRANSPORT=stream DURATION_S=25.
# Compare comp/s, (Δcreate_frames − Δcompletions), ceiling_active, RSS, loadgen p99.
```

### Fix deployed + re-validated at scale (binary `aa88ee0a`, PR #54)

The admission fix (latency gate also trips on `pending_create_queue()`;
`admission_max_create_queue` adaptive default-**on** as an OOM guard;
`nanobpm_admission_shed_total{reason}` counter; `ceiling_state` mirror) was built
release, deployed to all three RF=3 nodes, and re-run. Cluster came up healthy
(12/12 leaders, 0 `Shutdown`). Findings — all with the **closed-loop** loadgen
(create↔complete coupled, `MAXPAR`-bounded per connection):

| offered/s | completed/s | loadgen p99 | node RSS | `ceiling_active{throughput}` | sheds |
|-----------|-------------|-------------|----------|------------------------------|-------|
| 5,400     | 4,800       | 55 ms       | 0.22 GB  | 0                            | 0     |
| 7,200     | 7,205       | 49 ms       | 0.51 GB  | 0                            | 0     |
| 9,600     | 9,581       | 52 ms       | 0.92 GB  | 0                            | 0     |
| 12,600    | 12,508      | 87 ms       | 1.06 GB  | 0                            | 0     |
| 15,600    | 15,523      | 58 ms       | 1.31 GB  | 0                            | 0     |
| 30,000    | ~18,000     | 133 ms      | 2.10 GB  | **1** (saturated)            | 0     |

**Corrected ceiling.** The earlier "~3,800/s" figure was **loadgen-limited**, not
the engine. With `PROD_CONNS` 160–256 across three loadgens the cluster sustains
**~15,500/s completions at p99 < 60 ms** and only presses its throughput ceiling
(`ceiling_active{throughput}=1`) at ~30k/s offered (≈18k/s completed, p99 133 ms).
The new `ceiling_active` LED tracks this correctly — dark below the knee, lit at it.

**Why no sheds fired — and why that is correct.** RSS stayed **flat (~2.1 GB, no
balloon) even at 30k/s offered**. A closed-loop client cannot build a server-side
backlog: unacked stream creates are throttled by TCP/reader-loop backpressure, so
`pending_create_queue()` never grows and the active set never runs away — there is
correctly nothing to shed. The admission rails are **belt-and-suspenders** for the
*open-loop* pathology (the original "memory climbing, no instances starting"
incident, driven by an unbounded producer). That path is validated **locally**:
30 direct creates with no workers → exactly 10 admitted (=`MAX_BACKLOG`), 20 shed,
`nanobpm_admission_shed_total{reason="active_backlog"} 20`; plus the unit test
`create_queue_cap_default_scales_and_clamps` pins the OOM-guard cap math.

**Takeaway.** First-line OOM protection is the client backpressure already inherent
in the stream transport; the create-queue rail (now default-on) and the fixed
latency gate are the second line that bounds memory when a misbehaving open-loop
producer defeats backpressure. Neither is the binding constraint for well-behaved
load, so throughput and latency are unaffected in the common case.

### 2026-07-10 — `admission` keeps the AIMD valve armed (open-loop collapse A/B)

The "modes converge" A/B above ran a **gentle** open-loop offered load; this one
drives the cluster **hard past** the drain ceiling to separate the two latency rails
`SlaMode` controls: the proactive **backlog governor** and the **AIMD concurrency
limiter**. Same RF=3 / 12-partition GCP cluster, bounded-spill build, three
open-loop loadgens (`WORKERS=400`/node, `PROD_CONNS=256`, `RATE=30000`,
`MAX_INFLIGHT=400000`), `SLA_MODE` flipped live via the console
(`PUT /console/api/config/server/sla`, broadcasts cluster-wide).

| Config | AIMD | backlog gov. | agg comp/s | p50 | p90 | p99 | per-node backlog |
|--------|:----:|:------------:|-----------:|----:|----:|----:|-----------------:|
| `latency` (default) | on | on | ~14,000 | 32 ms | 66 ms | 2.3 s | bounded (~652 avg) |
| `latency`, `MAX_BACKLOG=off` (AIMD only) | on | off | **~22,000** | 61 ms | — | — | bounded |
| `admission` **(old: both off)** | off | off | ~21,500 | **107 s** | 843 s | 857 s | **unbounded (828k)** |

**Finding.** Suppressing the AIMD limiter (what the old `admission` mode did on top
of dropping the backlog governor) bought **zero extra throughput** — ~21.5k/s is the
same single-writer ceiling the AIMD-on config already reaches at ~22k/s — while
**exploding p50 from 61 ms to 107 s** and letting the backlog grow **unbounded**
(828k instances/node). The AIMD limiter is not a throughput throttle; because creates
and completions share the single-writer actor, it is a **self-balancing valve** that
paces intake down to the drain rate, which is exactly what keeps the backlog bounded.

**Change (this build).** `admission` now **keeps the AIMD limiter armed** and relaxes
**only** the proactive backlog governor (`server/src/main.rs`
`create_process_instance_impl`, gate no longer conditioned on `sheds_for_latency()`).
So `admission` admits everything the engine can actually drain — throughput at the
ceiling, backlog bounded — paced via retryable `503`s, instead of 200-accepting into
an unbounded queue. This makes `admission` the AIMD-only column above (~22k/s @ p50
61 ms), a genuine higher-throughput / looser-but-bounded-tail point distinct from
`latency`'s tight-tail governor (~14k/s @ p99 2.3 s). **Secondary result:** even at
828k resident instances/node the bounded-spill fix held the writer to ≤141 ms holds
(vs 8.2 s pre-fix) — no congestion collapse at the extreme.

### 2026-07-07 — Open-loop 24k reject soak: the backlog gate bounds **memory**, not **throughput**

The closed-loop table above never builds a server backlog, so it never exercised
the shed path at scale. This run does: three **open-loop** loadgens
(`RATE=8000`/node = **24,000 offered PI/s**, `MAX_INFLIGHT=0` fire-and-forget, 600
workers total) against `NANOBPMN_SLA_MODE=latency` +
`NANOBPMN_ADMISSION_MAX_BACKLOG=300000` on the v0.0.7 shed binary
(`f43f94ea`), fresh 4/4/4 cluster. Loadbox sampler (60 s), `netBklog` = created −
completed, `rssMB` = max node jemalloc resident:

| mm:ss | comp/s | shed/s | offered | netBacklog | node RSS |
|-------|--------|--------|---------|------------|----------|
| 01:00 | 24,076 | 0      | 24,000  |    72,668  | 3.1 GB   |
| 02:04 | 21,926 | 0      | 24,000  |   217,254  | 5.5 GB   |
| 03:09 | 18,806 | 0      | 24,000  |   541,325  | 6.3 GB   |
| 04:13 | 12,171 | 6,561  | 24,000  | 1,305,389  | 11.2 GB  |
| 05:17 |      0 | 24,080 | 24,000  | 2,846,942  | 9.0 GB   |
| 06:21 |      0 | 24,061 | 24,000  | 4,386,409  | 9.0 GB   |

**24k offered is above the drain ceiling for this worker count.** Completion
starts decaying from the very first sample (24k → 22k → 18.8k) while backlog
climbs monotonically — offered simply exceeds what 600 workers can drain
(sustainable completion is ~15–18k/s, consistent with the closed-loop knee at
~15.5k/s). Once the backlog is large enough, the per-op cost of a bloated engine
hot-state (bigger snapshot clones, allocator pressure) drags the single-writer
apply lane and completion **collapses to 0** — the same positive-feedback spiral,
now measured end-to-end.

**The `active_backlog` gate fired, but too late to save throughput.** Shedding
engaged (`shed/s` 0 → 6.5k → 24k) only after backlog crossed the 300k threshold —
by which point completion had already decayed into the collapse. The gate then
sheds *new* admits but cannot drain the ~1.3M already-admitted instances that are
suppressing completion, so `comp/s` stays at 0. **A backlog gate is a memory
backstop, not a throughput regulator:** it prevents OOM but does not restore
throughput once you are over the drain ceiling.

**Memory was protected.** Node RSS peaked ~11 GB and *fell* to ~9 GB once
shedding engaged (vs the **20–25 GB** unbounded runs with the gate disabled) —
far under the 51 GB watermark; no node OOM'd even as backlog hit 4.4M. The
open-loop *loadbox* is the component that died (~06:21), OOM'd by its own
unbounded fire-and-forget create buffers — a client-side limit, not a server one.

**Takeaways.** (1) To *sustain* throughput, offer at or below the drain ceiling
(~15–18k/s here) or add workers — the server cannot manufacture drain capacity by
shedding. (2) `MAX_BACKLOG` should be set well below the collapse knee (300k was
too loose: by the time it trips, the decay has begun); a tighter cap keeps the
active set small enough that per-op cost stays flat and completion holds. (3) The
shed gate delivered on its actual contract — memory stayed bounded and no server
node fell over under a 24k open-loop flood.

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

---

## 2026-07-08 — The sustained-load "completion-freeze" is NOT the engine actor (instrumented + probed)

Earlier notes above attributed the RF=3 sustained-load throughput ceiling — under
any load ≥~10k/s, completions decelerate to **0** after ~3–4 min — to the
**single-writer engine actor (Deepthi) serial apply lane**. Direct instrumentation
of that actor, shipped to the live cluster and observed across a reproduced freeze,
**disproves that hypothesis** and repoints at the Raft/stream transport.

### What was instrumented (commit `7032663`)
Per-partition, lock-free `deepthi::ActorStats` (alive / jobs_total / current_job_ms
/ hi_depth / lo_depth) with an `AliveGuard` drop-guard that fires on clean exit
**or** panic-unwind, plus a global panic hook logging thread/location/payload/
backtrace via `tracing`. Exposed as `nanobpm_actor_*` gauges sampled ~1 Hz in the
monitor loop. Discriminator by design: `alive=0` ⇒ actor DEAD (panic/exit);
`alive=1` + jobs frozen + `current_job_ms` climbing ⇒ WEDGED in one job;
`alive=1` + all flat + depth 0 ⇒ IDLE (stall is upstream of the actor).

### What the freeze actually showed
Reproduced a freeze under a closed-loop soak (`MAX_INFLIGHT=20000`). At the freeze,
on every node:
- **`nanobpm_actor_alive=1`, `current_job_ms=0`, `hi_depth=0`, `lo_depth=0`, yet
  `actor_jobs_total` STILL CLIMBING (~160/s).** ⇒ the actor is **alive, idle, and
  fast** — servicing activate-polls that return empty. Not DEAD, not WEDGED, not
  starved. The "IDLE (stall upstream)" arm of the discriminator.
- `commit_inflight=0` on all nodes; `raft_log_entries` ~33.8k and **balanced**
  across the three nodes (logs not diverged).

### The decisive probe (server has spare capacity during the "freeze")
A manual REST round-trip against the leader **during the freeze**:
- `createProcessInstance` → **HTTP 200 in 6 ms**
- `activateJobs` → returns a fresh job immediately
- job completion → **HTTP 204 in 2.8 ms**

The full create→activate→complete pipeline is healthy and *fast* while the
closed-loop stream load reads 0 completions. The engine is not the bottleneck; it
is >50% idle with headroom to spare at the exact moment throughput reads zero.

### Where the stall actually is
Node journals during the freeze are flooded with openraft **`AppendEntries timeout
after 250 ms`**, **bidirectionally between all peers** (0↔1, 0↔2, 1↔2), while the
`type="raft"` stream-frame counter is nearly stalled (~21/s vs thousands of client
frames). Inter-node Raft replication has **collapsed under sustained load**, and the
closed-loop loadgen then deadlocks downstream (producer pinned at `MAX_INFLIGHT`
awaiting completions that never arrive; stream workers hold leased jobs and go
silent; the liveness reaper removes them ~10 min later — an effect, not a cause).

**Reframed verdict.** The sustained-load completion-freeze is a **Raft/stream
transport failure, not a Deepthi serial-apply-lane failure.** Parallelizing the
engine actor would not have fixed it. The open root-cause question is now: *why do
AppendEntries RPCs time out at 250 ms under load?* — leading candidates: the
inter-node Raft transport starved / head-of-line-blocked behind client frames on
the shared falcon stream, follower apply-lane ack latency, or a 250 ms deadline
that is simply too tight for a 12-partition (4 leader + 8 follower replicas/node)
Raft fan-out at load. That is where the next debugging round should focus. The
`7032663` observability is a keeper regardless: it converted a silent, ambiguous
"actor stall" into a decisively falsified hypothesis.

---

## 2026-07-08 (cont.) — ROOT CAUSE FOUND: raft-coupled activation lease-leak

The transport reframe above was half the story. The HOL fix `7d78b21` (spawn the
inbound `ClientFrame::Raft` dispatch instead of awaiting it inline in `reader_loop`)
was deployed and **did** unstick the raft lane — raft stream-frames went from ~21/s
to ~1428/s — **but the completion-freeze reproduced identically anyway.** So the
250 ms AppendEntries timeouts were a *symptom* of a downstream stall, not the cause.

### The decisive A/B
With the HOL fix in place, the *only* variable changed was activation coupling:

| variable | `REPLICATE_ACTIVATION=1` (default) | `REPLICATE_ACTIVATION=0` (leader-local) |
|---|---|---|
| completions at ~9 min | **0/s (frozen)** | **14,410/s (holding)** |
| active_backlog | pinned at 60,001, frozen | ~20k/node, flowing |
| admission_shed | 0 | 0 |

Same binary (`110381033f95864e`), same closed-loop loadgen (3× `MAX_INFLIGHT=20000`,
200 workers, stream transport), same cluster. Flipping activation from a Raft
proposal to a leader-local lock made the collapse **vanish** and throughput hold
sustained past the ~4-min mark where it had frozen every prior run.

### Mechanism (lease-leak)
Under `replicate_activation=true`, **every** worker activation is a Raft
`ActivateJobs` proposal (`try_activate` → `activate_on_raft`, main.rs:7706) that
LOCKS the leased jobs. Those locks are ephemeral, leader-only, and never
journaled/exported. During ramp, dispatch leases jobs via raft faster than workers
receive+complete them; when a worker's outbound channel fills, `send_job` drops
already-locked jobs → the lease leaks. Reclaim (`ExpireJobs`) is *also* a raft tick
on the same commit lane and cannot keep pace. The activatable pool drains
monotonically to 0 → permanent starvation. Meanwhile `activate_on_raft` **silently
returns empty on any propose error** (main.rs:7731-7732), so the freeze is invisible
in the read model (jobs show `state:"Created", worker:null` even while locked).
Leader-local activation (`activate_on_local`, main.rs:7688) locks directly on the
leader without a quorum round-trip and uses lenient follower completion, so
activation is never gated on the partition's commit budget and cannot leak this way.

### Fix direction
Make `replicate_activation=0` the default under `leader-durable` replication (the
completion path is already lenient there), OR bound leases to available outbound
room so dispatch never leases a job it cannot deliver, OR make lease reclaim
leader-local + fast. A locked/leaked-lease gauge would make this observable — none
exists today, so the leak was inferred from the A/B, not directly measured.

The keeper commits from this arc: `9744685` (actor liveness observability),
`7d78b21` (raft inbound HOL fix — real transport win regardless), and this finding.

## 2026-07-08 (cont. 2) — CONGESTION COLLAPSE is the real ceiling (1Hz DuckDB time-series)

Built a 1Hz Prometheus scraper (`/tmp/prom-scrape.sh`, all 3 nodes → long-format
`ts_ms,node,metric,labels,value` CSV) + a DuckDB analysis harness
(`session-state/.../files/promts.py`: counter rates via LAG, gauge summaries,
histogram avg-latency from Δsum/Δcount, knee detection). This replaced the broken
in-soak sampler (`soakclh.log` was garbage — `s+: syntax error`, used a nonexistent
create metric), which is why the ceiling had looked "flat" before.

### Finding: the saturation ceiling is NOT flat — it is congestion collapse
Time-series of two fresh 10-min instrumented soaks (mode-0 replicate_activation=0,
mode-1 =1) plus a decisive `MAX_INFLIGHT` sweep overturned the "flat ceiling" framing.

**Steady-state congestion curve (sec 60–170 avg, same binary `110381033f95864e`,
same closed-loop loadgen, only MAX_INFLIGHT/node varied):**

| MAX_INFLIGHT/node | completions/s | actorOps/s | active | µs per actor-op |
|------------------:|--------------:|-----------:|-------:|----------------:|
| 1,000  | 23,016 | 997,749 | 569    | 36  |
| 4,000  | 23,380 | 976,505 | 602    | 37  |
| 20,000 | 16,329 | 228,288 | 44,860 | 158 |

Latency (loadgen p50/p90): MI=1000 → 23ms/—; MI=4000 → 25ms/—;
MI=20000 → 247ms / 4,379ms (and sinks further to ~14k/s @ ~3.9s p50 over a full
10-min run as `active` keeps climbing to the ~58k cap).

**Mechanism — the engine actor's per-command cost is O(active).** As the active set
grows 569 → 44,860 (79×), cost-per-actor-op grows 36µs → 158µs (4.4×), so actorOps/s
collapses 998k → 228k (−77%) and completion throughput falls 23k → 16k (−29%). This is
a runaway negative feedback loop: active↑ → per-op cost↑ → throughput↓ → active↑,
which drives the system to the in-flight cap.

**Ruled out** (all measured, not inferred):
- Durability lane: `commit_wait` avg 85µs (clean) → 30µs (congested) — *faster* when
  slower; the raft commit path is not the bottleneck.
- Memory: 19–37GB of the 54GB watermark, climbs smoothly through the knee, zero
  shedding events.
- Raft transport: balanced log entries, `commit_inflight`≈0 in mode-0.

**Prime code suspect for O(active):** `engine-core/src/engine/mod.rs` `Command::ActivateJobs`
walk (~L811–854): designed O(max_jobs) but the `job_activatable` `.filter(...).take(max_jobs)`
degrades to O(scan depth) when the activatable-index front fills with locked/expiring
jobs — not yet proven with scan-depth instrumentation.

### Two distinct phenomena, now cleanly separated
1. **Congestion collapse (both modes)** — the real ceiling above. Fix = admission
   control on the *active/in-flight* backlog (not just the create queue), holding the
   cluster left of the knee (~few thousand active/node) to keep it at its ~23k/s peak;
   secondary = fix the O(active) activatable scan if confirmed.
2. **Raft-coupled activation lease-leak (mode-1 only)** — the hard freeze, fixed by
   `replicate_activation=0` (prior section). Under mode-1, active climbs monotonically
   from t=0 and hard-freezes at ~60k (comp=0, actor churns futile ~1,500 ops/s).
   `replicate_activation=0` removes the FREEZE but not the congestion ceiling.

### Actionable conclusion
Bounding in-flight recovers **+41% throughput (16.3k → 23.4k/s) and ~170× lower p50
latency (3.9s → 23ms)** vs the unbounded cap. The sweet spot is broad — anything
keeping cluster active below ~12k holds ~23k/s. Add admission control on active
in-flight backlog to pin the system at its peak.

## 2026-07-08 (cont. 3) — FIXES SHIPPED for congestion collapse + lease-leak

Three changes land the findings above (all in `server/src/main.rs`, `+` console/config):

1. **Tick pre-check no longer O(active).** `tick_partition_via_raft`'s per-tick
   `jobs_due` gate scanned `state.jobs.values()` (every job in the backlog) every
   ~500ms on the single-writer engine actor per led partition — an O(active) walk
   that starves activation/completion as the backlog grows. Replaced with an
   O(activated) walk over the `activated_jobs` index (only `Activated` jobs hold a
   lease deadline, and `ExpireJobs` already reclaims exactly that set), so the gate
   is bounded by concurrently-leased jobs, not total backlog.

2. **Active-backlog admission is on by default (adaptive backstop).**
   `NANOBPMN_ADMISSION_MAX_BACKLOG` now auto-derives a generous per-node cap from
   the detected memory limit (`active_backlog_cap_default_from_limit`, clamped
   `[50k, 1M]`), latency-SLA-mode only, env-overridable (explicit number, or
   `off`). This self-protects against an unbounded active runaway / OOM out of the
   box. It is a *safety* backstop above typical parked populations — to pin the
   ~23k/s throughput peak, set an explicit lower cap at the knee (a few
   thousand/node), which the sweep showed recovers +41% throughput and ~170× lower
   p50 latency.

3. **`replicate_activation` defaults to leader-local under `leader-durable`.**
   The raft-coupled activation lease-leak (phenomenon 2) only occurs when
   activation is a per-job Raft proposal. Under `leader-durable` replication the
   tier already acks leader-locally with lenient follower completion, so
   replicating activation adds only a lease-leaking quorum proposal. The default is
   now mode-dependent: leader-local (`false`) under `leader-durable`, fully
   replicated (`true`) under `quorum` (unchanged, node-loss-durable). Force with
   `NANOBPMN_REPLICATE_ACTIVATION=1` when failover must honor in-flight leases.

Tests: `active_backlog_cap_default_scales_and_clamps` added; full server unit suite
(183) + clippy `--all-targets` clean.

## 2026-07-08 (cont. 4) — LIVE VERIFICATION on the RF=3 cluster (new binary 8a45d06a)

Deployed the fixed binary to all 3 nodes under `leader-durable` with the NEW
defaults (activation env UNSET -> leader-local; admission as noted) and re-ran the
MI=20000 point with 1Hz capture.

| run | build / mode | tput/s | p50 | p90 | mean | steady active | actorOps/s | µs/op |
|-----|--------------|-------:|----:|----:|-----:|--------------:|-----------:|------:|
| pre-fix   | old, MI=20000, no admission | 18,119 | 247ms | 4379ms | 1989ms | 34,231 | 424,685 | 85 |
| **verifyA** | **new, MI=20000, admission off** | **19,425** | **41ms** | 4245ms | 1377ms | 12,985 | 796,788 | 45 |
| **verifyB** | **new, MI=20000, admission cap=4000/node** | **23,597** | **27ms** | **52ms** | **48ms** | ~757 (peak node ~4.9k) | — | — |

Reads:
- **Tick pre-check fix (verifyA vs pre-fix, same offered load):** the engine actor
  sustains ~1.9× the ops/s (797k vs 425k) at ~half the per-op cost (45 vs 85µs) and
  holds <½ the active backlog (13k vs 34k) — p50 latency 247 -> 41ms. The O(active)
  actor scan is gone.
- **Admission cap (verifyB):** an explicit per-node cap of 4,000 holds the ~23.6k/s
  peak at p50 27ms / **p90 52ms** even under a 20k-inflight flood (vs p90 4,245ms
  uncapped — ~80× lower). Peak single-node active stayed ~4.9k (the gate bites),
  avg cluster active ~757. This is the proven congestion-collapse remedy.
- **Lease-leak default:** the cluster booted and ran healthy under `leader-durable`
  with `NANOBPMN_REPLICATE_ACTIVATION` UNSET (new default = leader-local) — no
  freeze across all runs.

Deploy note: the GCP nodes are Linux x86_64; a macOS build is an Exec-format-error
there. Built the release binary natively on the loadbox (16-core x86_64) after
`rustup` + `build-essential`, then fanned `nano-gw-new` out to the nodes.

## 2026-07-09 — SELF-OPTIMIZING admission-backlog governor (build 0146c188)

Replaced the *static* per-node backlog cap with a self-tuning **backlog governor**:
the proven latency-driven `AimdLimit` (self-calibrated baseline, already used for the
create limiter) now also drives the admission-backlog cap between a floor
(`MIN_BACKLOG_GOVERNOR_CAP=2000`) and a memory-derived ceiling, stepped off the same
per-command engine-latency window. Enabled **by default** (`NANOBPMN_ADMISSION_MAX_BACKLOG`
unset → `auto`; a number pins a static cap; `off` disables). So the previously-proven
peak throughput is delivered out of the box, and the cap only grows while the engine is
*healthy and loaded* — never by shedding legitimately-parked instances.

**Parked-safety by construction:** the gate/governor signal is the **runnable task-job
backlog** = total jobs the engine holds (`jobs.len()`, Created + Activated), summed across
owned partitions. Only `ServiceTask` mints a job (`create_job_for`), so timer/message/
signal/conditional parks never appear. This replaces the old `self.inflight` (which
counted parked instances).

**Signal fix (this build):** the first cut summed only `activatable_jobs` (Created/waiting)
and read ~0 under worker starvation (jobs were *leased* into the activated set), so the
governor flew blind and active ran to 190k. Switched the signal to total `jobs.len()`
(`Partitions::job_backlog()`), which counts leased-but-uncompleted jobs — the real
O(active) congestion driver.

Verification (default/auto; two regimes):

| run | offered | workers/node | tput/s | p50 | p99 | engine actor max ms | runnable (steady) | active peak | collapse? |
|-----|---------|-------------:|-------:|----:|----:|--------------------:|------------------:|------------:|:---------:|
| **govAuto** (healthy) | MI=20000, 200 w | ~24,000 | ~low | — | ~0 | ~0 | ~150 | **no** |
| **starveAuto2** (flood) | RATE=20k×3, MI=400k | ~13.9k (worker-bound) | 43s | 50s | **428** | **~2000/node (floor)** | 669k (exporter lag) | **no** |

Reads:
- **Healthy (govAuto):** default/auto holds the **~24k/s peak** (matches the explicit
  cap=4000 verifyB run, beats uncapped verifyA 19.4k/s) with the governor **dormant at
  the floor** — runnable ~0, active ~150, zero spurious shedding. Peak by default.
- **Flood (starveAuto2):** with only 15 workers/node the throughput is worker-bound
  (~13.9k/s), but the **engine never collapses** — `actor_current_job_ms` maxed at
  **428ms** (p50 ≈ 0) and completions held steady ~13–17k/s across the whole sustained
  phase. The corrected signal held the **runnable backlog at the ~2000/node floor** by
  shedding creates (752,900 `active_backlog` sheds cluster-wide). The 447k–669k
  `active_backlog` is exporter-projected instance lag (admitted-but-not-yet-exported-
  complete), *not* live engine congestion — the job map (the O(active) collapse driver)
  is bounded.

Net: the governor delivers the proven +21% peak by default, is parked-safe by
construction, and bounds the engine's live backlog under a worst-case worker-starved
flood without any static tuning. 187 bin tests (incl. 4 new governor tests) + clippy
`--all-targets` clean.

## 2026-07-09 (cont.) — WORKER-CONCURRENCY GOVERNOR (dispatcher right-sizing)

The backlog governor tunes *how much work is admitted*. A well-provisioned high-worker
soak showed the actual ceiling on this architecture is elsewhere: **worker
over-provisioning of the single push dispatcher.** Dispatch is push-based (falcon.rs) —
workers subscribe per job type and park; one server-wide dispatcher fans jobs out per
connection, and each lease is a **High-priority** activation in the shared single-writer
engine mailbox. Fan a fixed job supply across too many subscribers and activation swamps
completions (engine threads measured ~16% busy at the knee — the dispatcher, not engine
CPU, is the wall).

### Sweep: over-provisioning roughly halves throughput (non-monotonic)
Same binary, fixed `MAXPAR=50`/`RATE=30000` flood, only workers/node varied:

| workers/node | cluster tput | max p99 |
|-------------:|-------------:|--------:|
| **50**       | **46,694/s** | 29.3 s  |
| 100          | 12,083/s     | 85.9 s  |
| 200          | 23,672/s     | 57.4 s  |
| 400          | 17,946/s     | 60.7 s  |

Knee ~50 workers/node; past it throughput ~halves and tail latency triples.

### Fix: a third shared-signal AIMD limiter
`AdaptiveController` now runs three limiters off the SAME partition-0 latency window:
create limiter + backlog governor (ADR 0014) + **worker governor**. The worker governor
(`with_worker_governor`, floor=16, ceiling=4096) tunes the **active dispatch width** —
subscribers fanned out per job type per pass — gated on the runnable task-job backlog
(grow only while there's work to drain, back off when latency inflates). Enforcement:
`dispatch_plan(per_type_cap)` truncates each type's targets to the width, round-robin
cursor rotating so excess subscribers are parked, never starved (`0` = no cap).
Advisory: edge-triggered `ServerFrame::WorkerAdvice { recommended_concurrency }`
broadcast on width change so cooperating SDKs can self-size. Gauge:
`nanobpm_active_worker_target`. Env `NANOBPMN_WORKER_CONCURRENCY=auto|<n>|off`.

3 new worker-governor unit tests (grow / back-off / gate-on-backlog) + clippy
`--all-targets` clean; release build green. See ADR 0017.

### Live validation: the width cap recovers the knee (floor recalibrated 16 -> 50)
First live run exposed a calibration bug: floor=16 pinned the fan-out *below* the
knee and made 400-worker/node WORSE than uncapped (~9k vs 18k) — under sustained
overload latency is always inflated, so AIMD can't grow to the knee from below; the
floor must BE the knee (as with the backlog governor). A fixed-width calibration at
400 over-provisioned workers/node found a sharp optimum at width 50:

| width cap | cluster tput | max p99 |
|----------:|-------------:|--------:|
| off       | 14,842/s     | 74.5 s  |
| **50**    | **41,996/s** | **9.3 s**|
| 100       | 13,828/s     | 71.2 s  |
| 200       | 13,515/s     | 64.8 s  |
| 800       | 17,058/s     | 74.9 s  |

Capping an over-provisioned fleet to 50 active subscribers/type nearly triples
throughput (14.8k->42k) and cuts p99 8x (74.5s->9.3s), matching the subscribed-worker
knee (50 workers/node -> 46.7k). `MIN_WORKER_GOVERNOR_WIDTH` set to 50.
