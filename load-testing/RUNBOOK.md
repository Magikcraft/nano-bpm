# Load-Testing Runbook

Operational notes for running the nanobpmn soak/stress scenarios and interpreting
the results. Companion to [`README.md`](README.md) (topology + script reference).

---

## Golden rule: wipe the journal between runs

**Always** run `deploy.sh` (which `rm -rf ~/nano-data` and restarts all
nodes) between A/B arms or scenario runs. Residual backlog from a prior arm —
instances created but never completed because that arm's workers are gone — is
the #1 cause of spurious "wedges" in the next arm.

The `backlog-recovery.sh` scenario wipes automatically by default (`WIPE=1`).

---

## The standard test matrix

We validate the **out-of-the-box defaults** across payload × duration:

|          | negligible (`neg`, `VAR_BYTES=0`) | 50 KB (`50kb`, `VAR_BYTES=51200` JSON) |
|----------|-----------------------------------|----------------------------------------|
| **5 min** | `scenario.sh neg 5m`              | `scenario.sh 50kb 5m`                  |
| **30 min**| `scenario.sh neg 30m`             | `scenario.sh 50kb 30m`                 |

Each cell runs against a freshly built branch binary **with the console embedded**
(`build-server.sh`), on a clean-wiped cluster, behind a **disk preflight**. Use
`scenario.sh ... --build --branch <b>` to rebuild+stage; omit `--build` to reuse
the staged binary across cells. `neg` isolates the CPU/single-writer knee; `50kb`
exercises the durable-write path and memory rails.

---

## Getting source onto the build host (rsync — no git on the nodes)

The build host (node0, `10.128.0.19`) has **no `git`** installed and its
`~/build-console` tree is **not a git checkout** — so `scenario.sh --build
--branch <b>` (which runs `git fetch && git checkout`) does **not** work there.
Instead, sync the source from a machine that *does* have the repo (your dev box),
then build in place with `build-server.sh`.

The nodes are only reachable *through* the loadbox (they lack the peer SSH key and
sit behind the IAP tunnel), so sync in two hops: **dev box → loadbox → node0**.

```bash
# On the dev box, at the repo root, with main checked out at the commit to build:
git checkout main && git pull

# Only these crates change between builds; console/dist rarely does. --delete keeps
# the build host tree identical to the source (no stale files leaking into a build).
SSHK="-i $HOME/.ssh/google_compute_engine"
RSYNC_EXCLUDES="--exclude target --exclude .git --exclude nano-data"

# Hop 1: dev box -> loadbox (via the persistent IAP tunnel on localhost:2223)
rsync -az --delete $RSYNC_EXCLUDES \
  -e "ssh $SSHK -p 2223" \
  engine-core server generated \
  joshua.wulf@localhost:~/src-stage/

# Hop 2: loadbox -> node0 build-console (run from the loadbox)
ssh $SSHK -p 2223 joshua.wulf@localhost \
  'rsync -az --delete ~/src-stage/engine-core ~/src-stage/server \
     ~/src-stage/generated \
     -e "ssh -i ~/.ssh/google_compute_engine -o StrictHostKeyChecking=no" \
     10.128.0.19:~/build-console/'
```

Then build + stage + deploy + soak as usual:

```bash
# Build on node0 (embeds the console; --stage writes ~/nano-gw-new):
ssh $SSHK -p 2223 joshua.wulf@localhost \
  'ssh -i ~/.ssh/google_compute_engine 10.128.0.19 \
     "cd ~/build-console && CARGO=~/.cargo/bin/cargo \
        load-testing/scripts/build-server.sh --stage ~/nano-gw-new"'

# Fan the binary out, wipe+restart all nodes, then soak (on the loadbox):
ssh $SSHK -p 2223 joshua.wulf@localhost \
  '~/stage-binary.sh --from node0 && ~/deploy.sh default && \
   PROD_CONNS=256 MAXPAR=224 ~/soak.sh 50kb 30m my-soak'
```

Notes:
- Keep `--delete` on both hops so a merged/rebased `main` cannot leave a stale
  source file behind and produce a build that doesn't match the commit.
- `deploy.sh` has **no compression**; use `deploy-compress.sh` to launch with
  `NANOBPMN_RAFT_LOG_COMPRESS=1` when reproducing the compressed-soak conditions.
- If you prefer git on the build host, `sudo apt-get install -y git` on node0
  works (it has outbound internet), after which `scenario.sh --build --branch`
  becomes viable — but the rsync path avoids depending on node0 state.

---

## The node launcher (embedded base64 in `deploy.sh`)

Each node is started by a small systemd launcher, `~/node-launch-verify.sh`, that
`deploy.sh` writes to every node before starting it. **The launcher is stored
base64-encoded in the `LB64="..."` variable inside `deploy.sh`** (so the whole
deploy is a single self-contained script with no side files to copy). `deploy.sh`
does `echo "$LB64" | base64 -d > ~/node-launch-verify.sh` on each node, then runs
it via `nohup ... $MAXBKLOG $CAP $LIVENESS "$EXP_MODE" "$EXP_ENDPOINT"`.

A **decoded, human-readable copy is committed** at
[`scripts/node-launch-verify.reference.sh`](scripts/node-launch-verify.reference.sh).
**These two must stay in sync** — the reference is documentation only; the live
copy is the base64 blob.

### What the launcher pins (the OOTB cluster under test)

RF=3, 12 partitions, `NANOBPMN_RAFT=1`, `REPLICATION=leader-durable`,
`DURABILITY=sync`, `JOURNAL=segmented`, `DATA_DIR=$HOME/nano-data`, all adaptive
rails on (`VAR_SPILL`/`COLD_SPILL`=700 MB, `HISTORY_RETENTION`=6000 MB,
`EXPORTER_QUEUE`, `MEM_WATERMARK`), `SLA_MODE=latency`. Positional args:

| arg | var | meaning |
|-----|-----|---------|
| `$1` | `MAXBKLOG` | `default` (omit → adaptive) \| `off` \| N (explicit `NANOBPMN_ADMISSION_MAX_BACKLOG`) |
| `$2` | `CAP` | `NANOBPMN_ADMISSION_MAX_CREATE_QUEUE` (default 100000) |
| `$3` | `LIVENESS` | `NANOBPMN_STREAM_LIVENESS_MS` (default 600000) |
| `$4` | `EXP_MODE` | read-model exporter: `sqlite` (default) \| `tee` \| `remote` → `NANOBPMN_READ_EXPORTER` |
| `$5` | `EXP_ENDPOINT` | central exporter batch URL → `NANOBPMN_EXPORTER_ENDPOINT` |

`deploy.sh` forwards `$4`/`$5` from its own env `NANO_EXP_MODE` /
`NANO_EXP_ENDPOINT`, so the remote-exporter A/B (see below) needs **no
re-encoding** — just `NANO_EXP_MODE=remote NANO_EXP_ENDPOINT=... deploy.sh`.

### Editing the launcher (decode → edit → re-encode)

To change anything *other* than the parameterized args, edit the reference, then
regenerate `LB64`:

```bash
cd load-testing/scripts
# 1. edit node-launch-verify.reference.sh (skip its 5 header comment lines)
# 2. regenerate the blob from the body only:
tail -n +6 node-launch-verify.reference.sh > /tmp/launcher.sh
NEWB64=$(base64 -i /tmp/launcher.sh | tr -d '\n')
# 3. paste $NEWB64 into the LB64="..." assignment in deploy.sh
# 4. verify they round-trip:
grep -oE 'LB64="[^"]+"' deploy.sh | sed 's/LB64="//;s/"$//' | base64 -d \
  | diff - /tmp/launcher.sh && echo "in sync"
```

---

## Post-mortem: the "50 KB soak wedge" (2026-07-12)

A 50 KB soak time-series showed completions collapsing to ~0 mid-run while the
backlog climbed from ~300 to ~56k. It looked like a server stall.

**It was not a server defect.** Root cause: **cross-run contamination**. The A/B
arms were run back-to-back without wiping the journal. The prior arm left orphaned
instances (no workers) in the backlog; the loadgen does not back off, so the
residual load pushed the next arm past its admission cap and starved completions.

Clean, wiped runs never reproduce the wedge. Verified by:
- A clean 50 KB soak (10 min): healthy ~2,406/s aggregate, backlog bounded, 0 shed.
- A clean negligible soak (5 min): ~39.5k/s aggregate, p99 < 80 ms.

**Is production vulnerable?** No — the server is architecturally self-healing:
- **Completion-priority mailbox** (`server/src/deepthi.rs`): the engine actor
  drains High-priority messages (completions/reads) before Low (instance creates),
  so a create flood cannot starve completions/recovery. Mirrored in the Raft apply
  path (`server/src/raft.rs`).
- **Multi-rail admission shedding** (`server/src/main.rs` `admission_shed`): sheds
  *creates* when any rail trips — create queue, active backlog (auto-tuned to the
  throughput knee), exporter, pipeline bytes, or memory watermark. Backlog and RAM
  stay bounded; completions keep flowing.

A wedge only occurs if you both flood creates **and** keep workers offline
indefinitely — and even then backlog/RAM stay bounded and recovery is immediate
once workers reconnect. This is exactly what `backlog-recovery.sh` proves.

---

## Interpreting the backlog-recovery scenario

`backlog-recovery.sh` runs INJECT → QUIESCE → SETTLE → RECOVER → NORMAL and
prints a `VERDICT`. Reference results:

| Metric | Negligible payload | 50 KB payload |
|--------|-------------------|---------------|
| INJECT peak backlog | ~143k (count-bound) | ~50k (**memory-bound**) |
| Binding admission rail | active-backlog | **mem-watermark** |
| RECOVER drain | 6 s @ ~24k/s | ~170 s @ ~2k/s |
| NORMAL knee | ~40k/s | ~2.3k/s |
| Verdict | PASS | PASS |

Key insight: **with large payloads the memory rail binds first.** The count-based
backlog caps far lower (~50k vs ~143k) because RSS hits the memory watermark
(~17 GB of 50 KB × 50k resident instances) and admission sheds on memory, not
count. Drain and knee throughput are ~10× lower simply because 50 KB instances are
~10× heavier to process — not a regression.

### QUIESCE window (why it exists)
After INJECT kills the producers, creates already submitted to the pipeline keep
applying into `active_backlog` (there are no workers to complete them). With large
payloads this pipeline lag is large (~15k creates). Without a quiesce, the SETTLE
baseline is snapshotted mid-drain and the run falsely reports
"backlog-grew-while-idle". `QUIESCE_S` (default 15 s) lets the pipeline settle
into `created` before the SETTLE baseline, so the growth check measures true idle
drift. Increase `QUIESCE_S` for very large payloads / higher inject volumes.

---

## Interpreting the memory rails (hot-window cache)

Run `monitor.sh` alongside a soak/backlog run. The pair to watch, per node:

- `nanobpm_raft_log_bytes` — **full** on-disk Raft-log tail (can be many GB).
- `nanobpm_raft_log_ram_bytes` — **resident** hot window kept in RAM.

The byte-bounded hot-window cache keeps recent entries resident up to
`NANOBPMN_RAFT_LOG_RAM_BYTES` (default **64 MiB per partition** → ×12 partitions ≈
**768 MiB resident floor**) and demotes the colder tail to a
`(seg_start, offset, len)` descriptor, read back on demand.

Reference (50 KB backlog run): full tail **2.0 GB/node** while resident held at
**762 MB/node** (= the 64 MiB × 12 budget) — the cache clamps resident RAM at the
budget regardless of tail size, even under heavy backlog. In the sustained 50 KB
soak the effect is larger: **12.3 GB full tail vs 765 MB resident ≈ 17× reduction**.

Set `NANOBPMN_RAFT_LOG_RAM_BYTES=0` (in the node launcher — see
`scripts/node-launch-verify.reference.sh`) to disable the cache for an A/B
baseline arm.

---

## Disk sizing (avoid the ENOSPC crash)

A soak is a heavy durable writer. Per node it accumulates **Raft segments +
read-model SQLite + var-store**; a 20-min 50 KB soak consumed **~33 GB/node**. If
a node fills to 100% the server aborts with `No space left on device (os error
28)` — a **real crash by design** (`journal.rs` aborts rather than serve
non-durable state), *not* a bug. This first bit node0, which doubles as the build
host (its `~/build-console/target` adds ~7 GB).

Mitigations, both in place:

1. **Bigger disks.** The node boot disks were grown from 50 GB → **200 GB**
   (`pd-ssd`), giving ~150 GB free headroom. Note this buys **capacity, not
   throughput** — the ~276 MB/s write ceiling is the per-VM PD cap, not
   disk-size-derived. To resize:
   ```
   gcloud compute disks resize nano-node-N --size=200 --zone=us-central1-a --quiet
   ssh <node> 'sudo growpart /dev/sda 1 && sudo resize2fs /dev/sda1'
   ```
   (`growpart` is in `cloud-guest-utils`; the node images ship without it.)

2. **Guardrails in the scripts** (`disk-guard.sh`):
   - `deploy.sh` runs `disk-guard.sh preflight` after wiping — **refuses to start**
     a soak if any node has < 40 GB free (override `DISK_MIN_FREE_GB`).
   - `soak.sh` runs `disk-guard.sh watch` in the background — **aborts the soak**
     (kills loadgens) if any node drops below 15 GB free (`DISK_FLOOR_GB`).
   - `monitor.sh` prints a per-node **free-GB** column as an early warning.
   - `disk-guard.sh report` shows per-node `df` + `nano-data` size on demand.

Keep the build host (node0) tidy: `cargo clean` in `~/build-console` and remove
stale `~/nano-gw-*` / old `~/nano-data` between build cycles.

---

## Disk Attribution Probe (raft-log vs read-model)

`disk-attribution.sh` answers one question: **is a soak raft-log-bound or
read-model-bound on disk?** — which decides whether moving the read-model
exporter off-node (below) can lift the ceiling. Run it during a soak:

```bash
# on the loadbox, alongside a running soak:
~/disk-attribution.sh sample 10          # one 10 s window, per node
~/disk-attribution.sh watch  10 30       # 30 windows
```

Per node it prints, over the window:

| field | source | meaning |
|-------|--------|---------|
| `dev`   | `/sys/block/<dev>/stat` f7 | **gross device write MB/s** — the real ~276 MB/s PD-ceiling signal |
| `wiops` | `/sys/block/<dev>/stat` f5 | gross device write IOPS (the fsync/small-write signal) |
| `app`   | `/proc/<nano-gw>/io write_bytes` | gross logical write MB/s by the server process |
| `syscw` | `/proc/<nano-gw>/io syscw` | `write()` syscalls/s |
| `raft`  | `du nano-data/raft/` Δ | net growth MB/s of the **Raft log** |
| `rm`    | `du nano-data/read-model.*` Δ | net growth MB/s of the **read model** (the exporter offload target) |
| `var` / `spill` / `jrnl` | `du` Δ | var-store / var-spill / engine-journal net growth |

**Reading it.** `dev` is the absolute disk load (near the PD cap ⇒ disk-bound;
well below it while throughput won't rise ⇒ **CPU/raft-commit bound**). The
per-component columns are *net* on-disk growth, so their **sum is less than
`app`/`dev`** — the gap is write amplification + Raft/SQLite compaction churn.
Use the columns for **attribution** (which store grows fastest), `dev`/`app` for
the load:

- `rm`+`var`+`spill` dominate **and** `dev` near the cap → **read-model/payload
  disk is the wall** → remote-exporter mode should free it.
- `raft` dominates, or `dev` is low while throughput is capped → **raft/CPU
  bound** → moving the exporter will **not** help (this is the 0-byte regime).

Reference (idle cluster): all columns ~0 (sanity check that the probe attaches
to the device + `nano-gw` PID + `nano-data` paths correctly).

---

## Remote read-model exporter (Elasticsearch) — the off-node experiment

**Goal.** Measure how much removing per-node read-model projection frees the
cluster, by running nodes in `remote` mode against a central exporter VM that
projects to Elasticsearch. This only moves the needle in a **read-model-bound**
regime — use **50 KB payloads** (`scenario.sh 50kb ...`), not negligible; at
0-byte the nodes are raft/CPU-bound and there is nothing on the read-model disk
to offload (confirm first with the Disk Attribution Probe above).

Architecture (see the pluggable-exporter epic, issue #133, and `nano-exporter`):

```
 nodes (NANOBPMN_READ_EXPORTER=remote)          central VM: nano-exporter-0
   per-shard RemoteSink --HTTP POST /ingest-->  nano-exporter :9700 --_bulk--> Elasticsearch :9200
   (keeps only the compaction watermark)        (RemoteTarget = ElasticsearchTarget)
```

Node side ships each shard's event batch and advances only its local compaction
watermark (one integer write) instead of the full instance+variable projection.

### 1. Provision the central exporter VM (once)

```bash
gcloud compute instances create nano-exporter-0 \
  --zone us-central1-a --project camunda-researchanddevelopment \
  --machine-type c2-standard-8 --image-family debian-12 --image-project debian-cloud \
  --boot-disk-size 200 --boot-disk-type pd-ssd
# same VPC/subnet as the nodes (10.128.0.0/20) so nodes reach it on the internal IP.
```

Install Docker + run single-node Elasticsearch (security off for the experiment —
private subnet only), then run `nano-exporter` pointing at it:

```bash
# on nano-exporter-0:
sudo apt-get update && sudo apt-get install -y docker.io
sudo docker run -d --name es -p 9200:9200 \
  -e discovery.type=single-node -e xpack.security.enabled=false \
  -e "ES_JAVA_OPTS=-Xms8g -Xmx8g" docker.elastic.co/elasticsearch/elasticsearch:8.14.0
# stage the nano-exporter binary (built from server/, bin target `nano-exporter`)
NANO_EXPORTER_ES_URL=http://localhost:9200 NANO_EXPORTER_ES_INDEX=nano-events \
  NANO_EXPORTER_BIND=0.0.0.0:9700 nohup ./nano-exporter >~/nano-exporter.log 2>&1 &
curl -s localhost:9700/health   # -> ok
```

Build the `nano-exporter` binary the same way as the server (it is a second bin
in the `server` crate): `cargo build --release --bin nano-exporter`, stage the
artifact to `nano-exporter-0:~/nano-exporter`.

### 2. Run the A/B (wipe between arms — golden rule)

Let `EXP=http://<nano-exporter-0 internal IP>:9700/ingest`.

```bash
# Arm A — baseline (local SQLite exporter, current default):
~/deploy.sh default && PROD_CONNS=256 MAXPAR=224 ~/soak.sh 50kb 30m es-armA-local &
~/disk-attribution.sh watch 15 40 | tee ~/attr-armA.log     # capture per-node disk split

# Arm B — remote exporter (nodes project off-node to ES):
NANO_EXP_MODE=remote NANO_EXP_ENDPOINT="$EXP" ~/deploy.sh default \
  && PROD_CONNS=256 MAXPAR=224 ~/soak.sh 50kb 30m es-armB-remote &
~/disk-attribution.sh watch 15 40 | tee ~/attr-armB.log
```

Confirm the mode took on each node: the launch log line ends `exporter=remote`
(`ssh <node> 'tail -1 ~/nano-launch.log'`), and ES fills:
`curl -s "$ES/_cat/indices/nano-events?v"` shows a rising `docs.count`.

### 3. What to expect / how to read the result

- **Arm B node `dev` and `rm` should drop sharply** (read-model projection left
  the node; only the raft log + watermark remain) while **achieved creates/s
  rises** if the read-model disk was the binding constraint.
- If throughput is **unchanged** between arms, the nodes were **raft/CPU-bound**,
  not read-model-disk-bound — the exporter was never the wall (expected at low
  payload; re-run at larger payload to load the read-model path).
- Watch `nano-exporter-0`: if its ES `_bulk` can't keep up, `HttpBatchTransport`
  backpressures the node exporter thread (bounded queue, never drops) — that
  shows up as node-side create-accept backpressure, not data loss.

**Durability note.** In `remote` mode the compaction watermark advances on
*enqueue*, not on ES ack, so a node crash with batches still in flight loses that
downstream data (engine correctness is unaffected — that path is Raft-durable).
This is acceptable for the throughput experiment; ack-gated delivery is M3 in
#133. Use `tee` mode (local SQLite authoritative **and** mirror to ES) if you
need the read model to survive locally during the run.

---

## Metric glossary (gotchas)

- `active_backlog` — exporter-projected `created − completed`; the *parked
  instance* population. This is the backlog the scenario tracks.
- `pending_create_queue` (`pcq`) — creates submitted but not yet applied. Near 0
  in a healthy pipeline; a rising `pcq` means apply is the bottleneck.
- `runnable_backlog` — uncompleted service-task jobs; the *latency* rail's signal,
  NOT the parked population. Don't confuse with `active_backlog`.
- Standing-queue latency artifact: with `MAX_INFLIGHT=50000` at ~800/s the p50 can
  read ~60 s — that's `50000 ÷ 800 ≈ 62 s` residence time, not server latency.

---

## Environment gotchas

- Use the **release** loadgen (`~/rw-build/target/release/loadgen`). Debug is
  ~3.5× slower and misrepresents the knee.
- On macOS, `ps -o rss` under-reports; cluster RSS here is read from the Linux
  nodes' `ps` directly (accurate).
- The server crate is a bin: test with `cargo test --bin nanobpm-gateway-rest-server`.
