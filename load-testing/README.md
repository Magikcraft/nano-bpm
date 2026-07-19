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

## The test matrix

We validate the **out-of-the-box defaults** across two payloads × two durations:

| | negligible payload | 50 KB variable payload |
|---|---|---|
| **5 min** | `scenario.sh neg 5m` | `scenario.sh 50kb 5m` |
| **30 min** | `scenario.sh neg 30m` | `scenario.sh 50kb 30m` |

Each cell always runs against a **freshly built** binary from the feature branch
under test **with the web console embedded**, on a **clean-wiped** cluster with a
**disk-space preflight**. `neg` isolates the CPU/single-writer knee (~42k/s agg);
`50kb` exercises the durable-write path and memory rails (~2.4k/s agg, PD-bound).

---

## Scripts

All live in [`scripts/`](scripts/). The orchestrator + soak/monitor/deploy/guard
scripts run **on the loadbox**; `build-server.sh` runs **on the build host**
(node0 — it has `cargo` + `npm` + the repo checkout, and the resized disk).

### `scenario.sh` — run one matrix cell end to end *(start here)*
Build (optional) → stage → deploy (OOTB defaults, disk preflight) → soak +
background monitor → print `RESULT` lines and a monitor tail.

```
# reuse the already-staged binary:
./scenario.sh 50kb 5m
# rebuild the feature branch (console embedded) + stage first:
./scenario.sh 50kb 30m --build --branch jwulf/journal-value-codec
```

### `build-server.sh` — deterministic build WITH the console
Builds the console frontend, strips macOS junk, builds the server
`--features console`, and **hard-fails if the console is not actually embedded**
(the `console` feature is non-default and rust-embed silently embeds an empty
dir). Prints the binary path + sha256. Run on the build host.

```
CARGO=~/.cargo/bin/cargo ./build-server.sh --stage ~/nano-gw-new
```

### `stage-binary.sh` — fan the binary out to all nodes
Pulls `nano-gw-new` off the build host to the loadbox, then pushes it to all 3
nodes as `~/nano-gw-new` (verifying sha256). Handles node0 lacking the peer key.

### `deploy.sh` — clean-slate cluster restart (OOTB defaults)
Swaps in `nano-gw-new`, **wipes** `~/nano-data`, restarts all three nodes under
`leader-durable` + `sync` with the adaptive memory rails, and runs a **disk
preflight** (aborts if any node has < 40 GB free). Always run between arms.

```
./deploy.sh default    # adaptive admission backstop (normal)
./deploy.sh off        # disable admission backlog cap
./deploy.sh 100000     # explicit per-node backlog cap
```

The per-node systemd launcher it deploys is reproduced (decoded) in
[`scripts/node-launch-verify.reference.sh`](scripts/node-launch-verify.reference.sh);
the live script embeds it base64 to ship in one SSH call.

### `soak.sh` — the unified soak runner
`soak.sh <neg|50kb> <5m|30m|SECONDS> [label]`. Fans a producer+worker loadgen to
all three nodes at the validated knee (`RATE=14000/prod`, `MI=50000`). `neg` =
`VAR_BYTES=0`; `50kb` = `VAR_BYTES=51200 VAR_MODE=json` (honest ~6.7×-compressible
payload — **not** the `"x".repeat` mirage). Runs a background **disk watchdog**
and a mid-run disk-write/completes/backlog sample. Prints `RESULT` lines.

### `monitor.sh` — per-node memory + storage rails
`monitor.sh [label] [iters] [interval_s]`. One line per interval; per node:
`raftlog GB / RAM GB / RSS GB / backlog / shutdowns / **free GB**`. The free-GB
column is the early warning for the ENOSPC crash class.

### `disk-guard.sh` — disk-space safety rail
`report` / `preflight [MIN_FREE_GB=40]` / `watch [FLOOR_GB=15] [INT=30]`. Used by
`deploy.sh` (preflight) and `soak.sh` (watchdog) so a run can never silently fill
a disk and crash a node with ENOSPC. See RUNBOOK.md → *Disk sizing*.

### `disk-attribution.sh` — raft-log vs read-model write split
`sample [INTERVAL_S=10]` / `watch [INTERVAL_S=10] [ITERS=30]`. Per node, splits
disk writes into device gross (MB/s + IOPS), process gross, and per-store net
growth (`raft` / `rm` / `var` / `spill` / `jrnl`) so you can prove whether a soak
is raft-log-bound or read-model-bound. See RUNBOOK.md → *Disk Attribution Probe*
and *Remote read-model exporter*.

### `backlog-recovery.sh` — backlog injection → recovery self-heal scenario
Proves the cluster self-heals from a large standing backlog: INJECT (no workers)
→ SETTLE → RECOVER (workers only) → NORMAL. Emits `VERDICT: PASS/FAIL`.
Self-contained (wipes+restarts first).

```
./backlog-recovery.sh                 # negligible payload
PAYLOAD=51200 ./backlog-recovery.sh   # 50 KB — exercises memory rails
```

---

## Typical workflows

**Run the full matrix on a feature branch (build once, reuse across cells):**
```
./scenario.sh neg 5m  --build --branch <branch>   # builds + stages, then soaks
./scenario.sh 50kb 5m                             # reuse staged binary
./scenario.sh neg 30m
./scenario.sh 50kb 30m
```

**Verify self-healing after a backlog wedge concern:**
```
./backlog-recovery.sh                  # negligible
PAYLOAD=51200 ./backlog-recovery.sh    # 50 KB
```
Expect `VERDICT: PASS`.

See [`RUNBOOK.md`](RUNBOOK.md) for disk sizing, the wedge post-mortem, and the
memory-rail interpretation notes.
