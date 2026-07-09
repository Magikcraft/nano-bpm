# ADR 0017 — Worker-concurrency governance (server-side right-sizing of the push dispatcher)

Status: **Accepted — implemented + validated.** Ships behind a default (`NANOBPMN_WORKER_CONCURRENCY=auto`)
that installs a self-optimizing governor; `off` restores the historical uncapped fan-out.
Unit-tested; clippy `--all-targets` clean; release build green.
Date: 2026-07-09.
Relates to: ADR 0001 (activation fairness), ADR 0002 (leader-local activation),
ADR 0014 (create-placement protection and load awareness), ADR 0016 (falcon protocol),
`PERFORMANCE.md`, `server/src/backpressure.rs`, `server/src/falcon.rs`,
`server/src/main.rs`, `server/src/metrics.rs`.

## Context

The self-optimizing admission-backlog governor (ADR 0014 / `ebe1ea3`) tunes how much
work is *admitted*. But a well-provisioned, high-worker soak revealed that the
throughput ceiling on this architecture is not admission and not engine CPU — it is
**worker over-provisioning of the single push dispatcher**.

Dispatch is push-based (ADR 0016 / `falcon.rs`): workers `Subscribe` per job type and
park; a single server-wide dispatcher task reacts to `jobs_available`/tick and fans jobs
out per connection. Idle workers cost ~nothing. But every subscriber the dispatcher
touches per pass adds O(streams) fan-out work, and each lease is a **High-priority**
activation in the shared single-writer engine mailbox — so activation across too many
subscribers *swamps completions*. Engine threads measured only ~16% busy at the knee:
the dispatcher, not the engine, is the wall.

A clean worker sweep (same binary, fixed `MAXPAR=50`/`RATE=30000` flood, only workers/node
varied) is decisive and **non-monotonic** — more workers actively hurt:

| Workers/node | Cluster throughput | max p99 |
|-------------:|-------------------:|--------:|
| **50**       | **46,694 /s**      | 29.3 s  |
| 100          | 12,083 /s          | 85.9 s  |
| 200          | 23,672 /s          | 57.4 s  |
| 400          | 17,946 /s          | 60.7 s  |

The knee is ~50 workers/node; past it throughput roughly halves and tail latency
triples. Operators cannot be relied on to hand-size worker pools to the knee (it shifts
with job mix, payload size, and cluster size), and the server already *knows* — from the
same per-command latency signal the admission governors read — when it is being
over-driven.

## Decision

Add **worker-concurrency governance**: the server bounds, and advertises, the active
dispatch width — how many subscribers each job type is fanned out to per pass.

### 1. A third shared-signal AIMD limiter

The unified control plane (`AdaptiveController`, `backpressure.rs`) now runs three
limiters off the **same** partition-0 per-command latency window:

- the create limiter (intake concurrency),
- the admission-backlog governor (how much runnable work to admit, ADR 0014), and
- the **worker governor** (how wide to fan that work out).

`with_worker_governor(floor, ceiling, backlog)` installs a `LatencyLimiter` whose signal
is the **runnable task-job backlog** (`jobs.len()`, Created+Activated — the same
parked-safe signal as the backlog governor). AIMD semantics: slow-start the active width
upward while there is backlog to drain *and* latency stays below the self-calibrated
baseline × congestion ratio; multiplicative back-off the moment latency inflates. It
converges the width on the completion-throughput knee. Floor `MIN_WORKER_GOVERNOR_WIDTH=50`
(**the measured drain knee**, not an arbitrary minimum — see Validation),
ceiling `MAX_WORKER_GOVERNOR_WIDTH=4096` (effectively "all subscribers").

`NANOBPMN_WORKER_CONCURRENCY`: `auto` (default, governor) / `<n>` (fixed width) /
`off`/`0` (no cap → historical fan-out-to-all).

### 2. Enforcement — truncate the fan-out

`Registry::dispatch_plan(per_type_cap)` truncates each job type's targets to
`width = per_type_cap.min(ids.len())` (`0` = no cap). The existing round-robin cursor
rotates the window each pass, so **excess subscribers are parked, never starved** — an
over-provisioned fleet is bounded even if clients ignore the advice. The dispatcher reads
`server.active_worker_cap()` each pass.

### 3. Advisory — server-advised client scaling

Because the server knows the target, it tells the fleet: a new edge-triggered
`ServerFrame::WorkerAdvice { recommended_concurrency }` is broadcast whenever the governor
moves its width. A cooperating SDK can idle its excess subscribers down toward the target
so they never reach the dispatcher at all (cheaper than server-side truncation).
Enforcement is authoritative; the advisory is a cooperative optimization. The SDK-side
consumer is cross-repo future work — the server emits the signal and enforces the cap
standalone today.

### 4. Observability

`nanobpm_active_worker_target` (IntGauge) publishes the live width ~1 Hz from the monitor
loop; watch it against the subscribed-worker roster to see how much of the fleet is being
parked. The console `/config` view surfaces `NANOBPMN_WORKER_CONCURRENCY`.

## Consequences

- **Over-provisioning is now safe.** A fleet sized well past the knee is throttled to the
  knee's active width rather than collapsing throughput; the operator no longer has to
  hand-tune worker counts.
- **One control plane, one signal.** The worker governor reuses `AimdLimit`/`LatencyLimiter`
  and the same latency window — no new signal source, no new hot-path cost (steps ~1 Hz
  off-path). `is_active()` and `record()` fan the window to all three limiters.
- **Global, not per-job-type.** The width is one global cap applied uniformly per job type,
  because the latency signal is global (partition-0). Per-type widths would need per-type
  latency windows — deferred as future work.
- **GROW rarely engages under pure single-writer overload — so the floor IS the knee.**
  As with the backlog governor, overload here surfaces as *latency* (not
  healthy-but-loaded backlog), so the AIMD grow path cannot climb from below and the
  governor pins at the floor. An initial floor of 16 was therefore actively harmful
  (it clamped the fan-out *below* the drain knee and roughly halved throughput). The
  floor was recalibrated to **50** — the empirically measured knee — so pinning there
  *is* the optimum. GROW remains unit-tested and engages if a workload ever presents
  genuine drainable backlog at healthy latency.

## Validation

- **Fixed-width calibration (400 over-provisioned workers/node, RF=3 cluster).** Vary
  only the per-pass width cap; measure sustained cluster throughput + tail:

  | width cap | cluster tput | max p99 |
  |----------:|-------------:|--------:|
  | off       | 14,842 /s    | 74.5 s  |
  | **50**    | **41,996 /s**| **9.3 s**|
  | 100       | 13,828 /s    | 71.2 s  |
  | 200       | 13,515 /s    | 64.8 s  |
  | 800       | 17,058 /s    | 74.9 s  |

  Width 50 is a **sharp, isolated optimum**: capping an over-provisioned fleet to 50
  active subscribers/type nearly triples throughput (14.8k → 42.0k) and cuts p99 8x
  (74.5s → 9.3s). It independently matches the subscribed-worker sweep knee (50
  workers/node → 46.7k/s), confirming the per-pass width cap ≈ effective worker
  concurrency. This is why the floor is 50.
- Unit tests (`backpressure.rs`): `worker_governor_grows_active_width_while_healthy_and_loaded`,
  `worker_governor_backs_off_to_floor_under_congestion`,
  `worker_governor_does_not_widen_without_backlog`, plus the `is_active` coverage.
- Subscribed-worker sweep establishes the knee and the over-provisioning penalty the
  governor removes.
- `cargo build --release` green; `cargo clippy --all-targets` clean; worker-governor tests pass.
