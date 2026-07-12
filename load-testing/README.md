# Load Testing

Scripts and runbooks for driving the nanobpmn Raft cluster under load on the GCP
soak environment. These are **operator tools**, not part of the server build —
they run from the "loadbox" host and drive a 3-node cluster over an internal
network.

They are checked in so that both the maintainer and future automated agents have
a durable, documented reference for reproducing performance/memory validation
runs (e.g. the byte-bounded Raft-log hot-window cache A/B, and backlog→recovery
self-heal verification).

---

## Environment topology

```
  your laptop ──IAP tunnel──▶ loadbox (runs these scripts + loadgen)
                                  │  ssh to internal IPs
                                  ▼
        ┌──────────────┬──────────────┬──────────────┐
        │ 10.128.0.19  │ 10.128.0.20  │ 10.128.0.18  │   RF=3, 12 partitions
        │  (leader)    │              │              │   port 8080, /metrics
        └──────────────┴──────────────┴──────────────┘
```

- **Reach the loadbox** (from the laptop):
  ```
  ssh -i ~/.ssh/google_compute_engine -p 2223 joshua.wulf@localhost
  ```
- **Nodes**: `10.128.0.19` (leader), `10.128.0.20`, `10.128.0.18`. Metrics at
  `http://<node>:8080/metrics`.
- **Load generator**: a Rust binary at `~/rw-build/target/release/loadgen`
  (source `~/rw-build/src/loadgen.rs`). Use the **release** build — the debug
  build is ~3.5× slower and will misrepresent the throughput knee. All scripts
  here assume that path.
- **Process fixture**: `~/test-job-process.bpmn` (single service task, job type
  `test-job`) deployed at the start of each run.

> These scripts hard-code the node IPs and the loadbox SSH key path
> (`/home/joshua.wulf/.ssh/google_compute_engine`). If the cluster is
> re-provisioned, update `IPS`/`NODES`/`SSHK` at the top of each script.

---

## The loadgen tool

`loadgen` drives one node. Every script fans it out to all three. It is
controlled entirely by environment variables:

| Env | Meaning |
|-----|---------|
| `BASE_URL` | node to hit, e.g. `http://10.128.0.19:8080` |
| `PDK` | processDefinitionKey to create instances of |
| `WORKERS` | job-worker pool size. **`0` disables workers** (pure producer). |
| `PROD_CONNS` | producer connection pool size. **`0` disables producers** (pure worker). |
| `MAXPAR` | worker max-parallel job activations |
| `RATE` | per-producer create rate cap. **`0` = unbounded** (credit-gated). |
| `MAX_INFLIGHT` | producer inflight cap. **`0` = unbounded**. |
| `VAR_BYTES` | per-instance variable payload size (e.g. `51200` = 50 KB) |
| `WARMUP_S` / `DURATION_S` / `DRAIN_S` | run phases (seconds) |
| `TRANSPORT` | `stream` (used everywhere here) |
| `PROGRESS` | `1` for per-second progress rows |

The `WORKERS=0` / `PROD_CONNS=0` switches are what make the backlog-recovery
scenario possible: inject with producers-only, drain with workers-only.

Prints a `RESULT ...` line at the end with aggregate throughput / latency
percentiles.

---

## Scripts

All live in [`scripts/`](scripts/). Run them **on the loadbox**.

### `restart-verify.sh` — clean-slate cluster restart
Wipes the Raft journal (`~/nano-data`) and restarts all three nodes from the
staged `nano-gw-new` binary, under `leader-durable` + `sync` durability with the
adaptive memory rails enabled. **Always run this between A/B arms** — residual
backlog from a prior arm is the #1 cause of spurious "wedges" (see
`RUNBOOK.md`).

```
./restart-verify.sh default        # adaptive admission backstop (normal)
./restart-verify.sh off            # disable admission backlog cap
./restart-verify.sh 100000         # explicit per-node backlog cap
```

The per-node systemd launcher it deploys is reproduced (decoded) in
[`scripts/node-launch-verify.reference.sh`](scripts/node-launch-verify.reference.sh)
for reference — the live script embeds it base64 so it can be shipped in one SSH
call. **Note it does NOT set `NANOBPMN_RAFT_LOG_RAM_BYTES`**, so the cache uses
its 64 MiB/partition default (×12 ≈ 768 MiB resident floor). Set that env in the
launcher to run an A/B baseline (`0` disables the cache).

### `backlog-recovery.sh` — backlog injection → recovery self-heal scenario
The headline reusable scenario. Proves the cluster self-heals from a large
standing backlog (the production concern behind worker outages / create bursts):

1. **INJECT** — flood creates with **no workers**; admission control must *cap*
   the backlog (shed > 0), not let it grow unbounded.
2. **SETTLE** — idle hold; backlog must stay **bounded** (delta ≈ 0).
3. **RECOVER** — workers-only, no producers; backlog must **drain to ~0**.
4. **NORMAL** — mixed load; throughput must return to its normal **knee**.

Emits a `VERDICT: PASS/FAIL`. PASS iff shed>0 AND settle-growth≈0 AND drained AND
knee>0. Self-contained (wipes+restarts first by default).

```
./backlog-recovery.sh                 # negligible payload, default knobs
PAYLOAD=51200 ./backlog-recovery.sh   # 50 KB payload — exercises memory rails
```

Knobs (all optional): `WIPE PAYLOAD INJECT_S SETTLE_S RECOVER_S NORMAL_S
INJECT_PC INJECT_RATE INJECT_MI REC_WORKERS REC_MAXPAR NORMAL_RATE DRAIN_FLOOR
INTERVAL`. Writes `~/backlog-recovery.log` (driver) and
`~/backlog-recovery-samples.log` (per-tick sampler, phase-tagged).

### `hotcache-mon.sh` — Raft-log memory-rail monitor
Samples every 30 s and reports, per node, `raft_log_bytes` (full on-disk tail)
vs `raft_log_ram_bytes` (resident hot window). This is the metric pair that
shows the hot-window cache working: full tail can be many GB while resident
stays near the per-partition budget × 12.

```
./hotcache-mon.sh <label> <iters>     # e.g. ./hotcache-mon.sh 50k 24
```

### `clean50k.sh` / `cleanneg.sh` — clean single-arm soaks
Fixed-config max-throughput soaks used for the hot-window-cache A/B. Both use the
`latency` self-optimizing SLA mode at the ~42k/s aggregate knee (RATE=14000/prod
× 3). `clean50k.sh` uses `VAR_BYTES=51200` (50 KB, 10 min); `cleanneg.sh` uses
`VAR_BYTES=0` (negligible, 5 min). Run `restart-verify.sh` before each, then
`hotcache-mon.sh` alongside.

```
./restart-verify.sh default && ./clean50k.sh & ./hotcache-mon.sh 50k 24
```

---

## Typical workflows

**A/B the hot-window cache (memory reduction):**
1. `restart-verify.sh default` (cache on, 64 MiB default) — or edit the launcher
   to set `NANOBPMN_RAFT_LOG_RAM_BYTES=0` for the baseline arm.
2. `clean50k.sh` with `hotcache-mon.sh 50k 24` alongside.
3. Compare `raftlog_gb` (full) vs `ram_gb` (resident) and process RSS.

**Verify self-healing after a backlog wedge concern:**
```
./backlog-recovery.sh                  # negligible
PAYLOAD=51200 ./backlog-recovery.sh    # 50 KB, stresses memory rails too
```
Expect `VERDICT: PASS`.

See [`RUNBOOK.md`](RUNBOOK.md) for the wedge post-mortem, root cause, and the
memory-rail interpretation notes.
