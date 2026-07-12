# Load-Testing Runbook

Operational notes for running the nanobpmn soak/stress scenarios and interpreting
the results. Companion to [`README.md`](README.md) (topology + script reference).

---

## Golden rule: wipe the journal between runs

**Always** run `restart-verify.sh` (which `rm -rf ~/nano-data` and restarts all
nodes) between A/B arms or scenario runs. Residual backlog from a prior arm —
instances created but never completed because that arm's workers are gone — is
the #1 cause of spurious "wedges" in the next arm.

The `backlog-recovery.sh` scenario wipes automatically by default (`WIPE=1`).

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

Run `hotcache-mon.sh` alongside a soak/backlog run. The pair to watch, per node:

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
