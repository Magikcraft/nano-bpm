# ADR 0022 — MessagePack serialization for the create/replication path: explored, measured, deferred

Status: **Explored and deferred.** The msgpack Raft-RPC codec swap was implemented,
benchmarked on the live GCP cluster, and **reverted** because it did not lift the
throughput ceiling. Work preserved on branch `explore/msgpack-raft-falcon`
(commits `3f98933`, `ef14ee4`) for a future revisit that pairs it with the
opaque-payload + lazy-follower-decode redesign (see "Path not yet taken").
Date: 2026-07-21.
Relates to: ADR 0016 (Falcon protocol), ADR 0020 (two-tier admission compression;
Tier-1 signal = raft-log fsync latency), ADR 0003 (write-path durability tiers),
`server/src/raft_net.rs`, `server/src/main.rs` (`dispatch_raft_rpc`),
`server/src/falcon.rs`, `engine-core/src/packed.rs` (on the explore branch),
`docs/performance-comparison.md`.

## Context

The nanobpmn create-throughput ceiling on a 3-node RF=3 cluster is ~**11.5k
creates/s aggregate (~3.8k/node)** at 50 KB variable payloads (vs ~48k/s at 1 KB,
~32k/s at 10 KB) — throughput falls sharply as the variable payload grows. Prior
profiling attributed a large share of node CPU to `serde_json` (de)serialization
of the `Command`'s `HashMap<String, serde_json::Value>` variables payload: it is
touched on roughly ten passes across the create path (ingest parse, leader
raft-encode, per-follower AppendEntries decode ×8, log-store persist, journal,
snapshot). The single documented hottest pass was the **8-follower AppendEntries
decode** (`dispatch_raft_rpc → serde_json::from_str::<RaftRpcRequest> →
Command::deserialize → Value::deserialize`).

Zeebe — the system nanobpmn draws its lineage from — uses **MessagePack** for its
record/variable encoding. That prompted the question this ADR settles: *can we
lift the 50 KB create ceiling by moving the hot serialization boundaries from JSON
to msgpack, doing the encode client-side where possible, while keeping JSON for
compatibility?*

Two distinct levers were on the table:

1. **Cheaper codec (msgpack over JSON).** msgpack (de)serialization is typically
   2–5× faster than `serde_json` for this shape: no string escaping/unescaping,
   binary-encoded scalars, length-prefixed strings. Swapping the codec at the hot
   boundaries makes every pass cheaper without changing the engine state machine.
2. **Fewer passes (opaque payload + lazy decode).** Zeebe's real win is not just
   the format — it is that followers **never inflate the variable document into a
   map**; they store the opaque bytes and decode lazily/partially only on read.
   That eliminates the 8-follower HashMap build entirely — a structural change, not
   a codec swap.

We deliberately scoped the first, cheapest, lowest-risk slice — lever (1) at the
Raft-RPC request boundary (the documented follower hotspot) — as a falsifiable
experiment before committing to the larger opaque-payload refactor.

## What was built (on `explore/msgpack-raft-falcon`)

Kept JSON as the default/compat path; added msgpack as an **opt-in, feature-gated**
fast path with zero code duplication:

- **`engine-core` variable codec** (`packed.rs`, behind a `msgpack` Cargo feature
  using optional `rmp-serde`): `pack_variables`/`unpack_variables`. engine-core
  stays dependency-free by default (verified `cargo tree` = 0 rmp deps) so the
  mobile/wasm targets are unaffected.
- **Opt-in msgpack `ClientFrame` over Falcon's binary channel** (commit
  `3f98933`): the `Message::Binary` arm decodes the *same* `ClientFrame` via
  `rmp_serde::from_slice` and routes through the identical `handle_client_frame`
  — the JSON and msgpack ingress share one handler.
- **Msgpack Raft-RPC request codec** (commit `ef14ee4`): only the RPC *request*
  path swapped `serde_json`→`rmp_serde` (`to_vec_named` on the leader send in
  `raft_net.rs`, `from_slice` on the follower `dispatch_raft_rpc` in `main.rs`).
  Because msgpack is binary it rides the existing JSON Falcon frame base64-encoded
  (large payloads still deflate). The RPC *response* stayed JSON (it carries no
  variable payload). Fully contained to `encode_rpc_payload`/`decode_rpc_payload`
  and their two call sites — no state-machine change, so all nodes still build
  identical `HashMap` state, just (hypothetically) decoded faster.

Correctness was validated end-to-end: `cluster_e2e` replicates real leader→
follower AppendEntries/Vote/InstallSnapshot over the wire through the msgpack
codec (including segmented-node recovery); `falcon_e2e`, the codec unit tests, and
the msgpack `ClientFrame` round-trip tests all pass; clippy + pinned-nightly fmt
clean.

## Decision

**Do not ship the msgpack RPC codec now; revert it and defer.**

A clean, same-session A/B on the live 3-node GCP cluster measured **no
improvement**:

| Arm | Binary | Aggregate creates/s | p99 create-accept |
|-----|--------|--------------------:|------------------:|
| A — baseline (JSON RPC) | `6480dd5` | **11,163** | 236 ms |
| B — msgpack RPC | `9a5213b8` | **11,018** | 235 ms |

Delta **−1.3%** — inside run-to-run noise; per-node was tight (3712/3712/3737 vs
3677/3677/3664). Both arms ran the identical ceiling condition: RF=3,
`var_spill=off`, `linger=0`, **remote blackhole exporter** (removes read-model
disk contention), `s3-flood` **WORKERS=200**, 18k offered, 50 KB payloads, clean
journal wipe between arms.

**Interpretation:** at this operating point the follower Raft-RPC decode is *not*
the binding constraint, so making it cheaper buys nothing. This corroborates the
ADR-0020-era finding that the Tier-1 wall is the **raft-log fsync / single-writer
write path** (fsync latency ~1.7 ms @9k → ~3.3 ms at ceiling, parking at the
adaptive knee), not RPC serialization CPU. A codec that only accelerates one of
many passes cannot move a ceiling set by a different resource.

The implementation is correct and cheap to re-apply, so it is preserved on
`explore/msgpack-raft-falcon` rather than discarded.

### Benchmarking caveat learned (recorded so we don't repeat it)

The first A/B attempt used the wrong harness (`xmasconc.sh`, WORKERS=50 + the
**local sqlite** read-model exporter) and showed ~5–6k/s — the local exporter's
disk contention caps there, and 50 draining workers compete with creates for the
shared single-writer engine actor. Reproducing the real ~11.5k ceiling **requires
the remote blackhole exporter + `s3-flood` (WORKERS=200, RATE-push)**. Any future
throughput A/B on this cluster must use that harness.

## Consequences

- The default wire format is unchanged: JSON everywhere. No compatibility or
  determinism risk introduced on `main`/the PR branch.
- The ~11.5k 50 KB ceiling is unchanged; the next throughput lever is the write
  path (raft-log fsync / group-commit), or the structural change below — **not**
  the RPC codec in isolation.
- engine-core remains dependency-free (the reverted `msgpack` feature and
  `rmp-serde` optional dep are gone from `main`; they live on the explore branch).

## Path not yet taken (why we may revisit)

The step-change is lever (2): make the variable payload an **opaque msgpack
document that followers never inflate**. Followers would persist and replicate the
bytes verbatim and decode lazily/partially only when an owner actually reads a
variable (nanobpmn already funnels instance reads through
`resolve.rs::variables()` / `visible_variables`, and instance state already has
spill/rehydrate infrastructure). That deletes the 8-follower HashMap build — the
actual per-follower cost — rather than merely speeding up one decode. It requires
switching the raft-log/journal/snapshot codec to carry opaque bytes (feasibility
of `rmp-serde` round-tripping openraft's `Entry<RaftConfig>` was already proven
during this exploration) and threading lazy decode through instance reads. If a
future write-path optimization lifts the fsync wall and re-exposes serialization
CPU as the ceiling, revisit lever (2) — with lever (1)'s codec as its foundation —
starting from `explore/msgpack-raft-falcon`.
