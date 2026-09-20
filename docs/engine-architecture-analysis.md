# Engine Architecture Analysis: `nanobpmn-engine-core`

**Status:** analysis complete; regression coverage verified sufficient for the
proposed refactoring (see §6). Work branch: `refactor/engine-layering`.

**Claim template** (post to the tracking issue once we have connectivity — see
`AGENTS.md` *Claim Your Task Before You Start*; no issue exists yet because the
analysis was done offline):

```
Claimed — worktree `nanobpmn`, branch `refactor/engine-layering`.
```

---

## 1. Why

This repository runs a dozen parallel agents in parallel worktrees. Two files
concentrate the whole engine's behaviour — `engine/mod.rs` (13.7k lines, a
single 12.5k-line `impl Engine`) and `engine/tests.rs` (27.5k lines) — and the
crate contains two intra-crate dependency cycles. That combination makes the
crate hard to read in isolation, guarantees merge conflicts between agents
touching unrelated concerns, and gives the layering no mechanical enforcement.

Goal: isolated modules, single-direction imports, one concern per file, with the
invariant machine-checked in CI.

## 2. Current state (verified from source)

`engine-core` is 77k lines, zero-dependency by design (std-only so it compiles
for server, wasm, iOS, Android). Module dependency graph, from every intra-crate
`use crate::…` edge:

```
L0 (leaves, depend on nothing):   model   xml   json   read_query
L1:                               feel ──→ model
                                  dmn ──→ feel, model, xml
                                  cluster_vars ──→ model
L2:  ⚠ state ──→ model, EVENT        agent ──→ model, STATE   lease ──→ STATE
L3:  ⚠ event ──→ model, STATE
L4:                               command ──→ model, state
L5:  ⚠ bpmn ──→ model, xml, VALIDATE
     ⚠ validate ──→ model, BPMN
L6:                               engine ──→ command, event, model, state, agent,
                                              feel, dmn, cluster_vars, lease,
                                              ⚠ bpmn (ISO-8601 parsers)
                                  ffi ──→ engine, bpmn, model, json
```

### 2.1 Two true cycles

- **`event` ↔ `state`.** `state::apply` consumes `Event`, while `event.rs`
  imports `crate::state::{Key, IncidentKind, IoMappingRedrive,
  MessageSubscriptionKind, TimerKind, default_job_priority, …}` for its
  payloads.
- **`bpmn` ↔ `validate`.** `parse_bpmn` *calls* `validate::run(ValidationInput)`,
  while every validator in `validate/` imports `crate::bpmn::ParseError`. The
  shared error type lives in the wrong layer.

### 2.2 One inverted (non-cyclic) edge

- **`engine/resolve.rs` → `bpmn`.** The runtime calls
  `crate::bpmn::parse_iso8601_duration` / `parse_iso8601_cycle` — the
  execution layer reaches into the deploy/parser layer for a primitive.

### 2.3 God files

| File | Lines | Smell |
|---|---|---|
| `engine/tests.rs` | 27,484 | one test file for the entire engine |
| `engine/mod.rs` | 13,742 | a single `impl Engine` block of **12.5k lines**, 153 fns |
| `bpmn.rs` | 10,058 | parser (~4.5k) + **5.5k lines of tests in the same file** |
| `state.rs` | 3,714 | `apply()` is **one ~1,800-line function** |
| `model.rs` | 2,946 | OK (registry + `ProcessBuilder`) |
| `event.rs` | 1,920 | OK (92-variant exhaustive enum; keep as one file) |

Largest methods inside `engine/mod.rs`: `plan_command_at` **2,521 lines**,
`run_activation_body` 503, `complete` 475, plus 150 more — all in one file.

### 2.4 Duplication / drift surfaces

1. **Two hand-rolled ISO-8601 duration parsers.**
   `bpmn.rs::parse_iso8601_duration` (milliseconds, unsigned, supports weeks,
   no fractions) and `feel/temporal.rs::DtDuration::parse` (nanoseconds, signed,
   supports fractional seconds) both char-loop over `P…DT…H/M/S`. They have
   already drifted: `PT1.5S` is a valid FEEL duration but an invalid BPMN timer
   (silently `None`).
2. **`use super::*` in every engine submodule.** `boundary.rs`, `memory.rs`,
   `agent_behavior.rs`, `api.rs`, `resolve.rs` all do `use super::*` — each sees
   the entire 12.5k-line `mod.rs` namespace (213 `state::` references in
   `mod.rs` alone). Nothing can be read or extracted in isolation; this is the
   mechanical root of the god file.
3. **Split-brain capture types.** `validate` defines `FlowRefCapture`,
   `RefSite`, `UnmodelledElement`, `TaskDefCapture` — all `#[allow(dead_code)]`
   because the *parser* (`bpmn`) populates them and the *validators* read them.
   Three modules, one data structure, no owning layer.
4. **Hand-rolled `Display` impls** (7 of them; `EngineError`'s alone is 330
   lines) — mechanical, low risk, but each is another variant-list drift spot.
5. **Good precedent already exists:** `json.rs` (one shared RFC-8259 escaper for
   `engine::api` + `ffi`) is exactly the right pattern — a leaf both JSON-writing
   sites import. The codebase knows how to do this; it hasn't been applied to
   temporal, the capture types, or the state/event shared types.

## 3. Target architecture — single-direction imports

All changes stay *inside* `engine-core` (it is `publish = false` and its own
workspace, so nothing here disturbs the wasm/iOS embed story or the published
`@nanobpm/engine-wasm` package).

```
L0  model  xml  json  read_query  temporal (new)    ← zero internal deps
L1  feel → model        dmn → feel, model, xml
L2  state → model        (state::apply additionally → event)
L3  event → model, state::types
L4  command · agent · cluster_vars · lease → L0..L3
L5  validate → model, validate::error
    bpmn → model, xml, validate
L6  engine → L0..L5      ffi → engine, bpmn, json
```

### The four moves that break every cycle and inversion

1. **Break `event` ↔ `state`.** Split `state.rs` into `state/types.rs` (record
   types + key machinery: `IncidentKind`, `IoMappingRedrive`, `TimerKind`,
   `MessageSubscriptionKind`, `PendingUserTaskTransition`, `Key`/hashing,
   defaults) and `state/apply.rs`. Rule: `event` may import `state::types`
   only; `state::apply` may import `event`. Pure type moves — the serialized
   form is untouched (golden serde guard in §6 proves byte-identical).
2. **Break `bpmn` ↔ `validate`.** Move `ParseError` into `validate/error.rs`
   (it is the *validator's* vocabulary; the parser merely returns it). The
   capture types stay in `validate` as its own input type, populated by the
   parser via a builder. Then `validate → (model + own error)` and
   `bpmn → validate`; the `#[allow(dead_code)]` split-brain goes away.
3. **Break `engine → bpmn`.** Extract `parse_iso8601_duration`/`_cycle` into a
   new `temporal` leaf, which also absorbs the overlap with `feel/temporal`
   (one canonical ISO-8601 parser; each layer's width — ms vs ns, weeks vs
   fractions — becomes an explicit, documented policy on top, decided per call
   site). `bpmn` and `engine/resolve` both import the leaf.
4. **Enforce it mechanically.** Rust happily allows intra-crate cycles, so the
   only guard is a lint. Add a **dependency-direction test**: a small
   script/test that parses every `use crate::` edge in `engine-core/src`
   against an allowed-edges table (the layer list above) and fails CI on any
   backward edge. The repo already lives on exactly this pattern (golden
   serde-drift guard, `console/scripts/merge-gates.test.mjs`, the procesos
   spec-parity test) — this extends it to module direction. **Landing this
   before the big file splits (§5) freezes the layering so later slices cannot
   regress it.**

## 4. File-level decomposition

- **`engine/mod.rs` → ~11 files**, each a private `impl Engine` (Rust allows
  multiple impl blocks per type; `mod.rs` keeps only `Engine`, `Step`,
  `EngineError`, `StepDriver`). Grouped by lifecycle concern:

  | New file | Contents |
  |---|---|
  | `lifecycle.rs` | activate / complete / `finalize_completion` / token walk |
  | `jobs.rs` | job activation, leases, retries |
  | `user_task.rs` | user-task lifecycle, changesets, task listeners |
  | `multi_instance.rs` | fan-out / fan-in / completion condition |
  | `call_activity.rs` | child spawn / output projection / depth guard |
  | `adhoc.rs` | ad-hoc container, tools, agent bridge |
  | `incidents.rs` | raise / resolve / retry, `io_mapping_incident` |
  | `timers_catch.rs` | timers, message / signal / conditional catch |
  | `compensation_escalation.rs` | throw events, handlers, re-open chains |
  | `listeners.rs` | execution + task listener chains (`AdvanceListener`) |
  | `migration.rs` | instance migration, remap, snapshot (de)serialization |

  Each file gets **explicit imports** — no `use super::*`.
- **`state::apply` (≈1,800 lines) → thin dispatcher** + `apply_element`,
  `apply_job`, `apply_user_task`, `apply_timer`, `apply_subscription`,
  `apply_instance`, `apply_process`. Mechanical: one group per event family
  (92 variants total).
- **Tests split by concern, mirroring the submodules:**
  `engine/tests/{lifecycle,jobs,user_task,multi_instance,incidents,adhoc,
  timers_catch,compensation,listeners,migration,boundary}.rs` instead of one
  27.5k-line file; `bpmn.rs` tests out into `bpmn/tests.rs`. This is the
  biggest per-contributor win: two agents touching user tasks and
  multi-instance no longer collide in one file.
- **`bpmn.rs`:** separate the `ProcessAcc` builder / streaming walk from the
  `ParseError`-returning surface once `validate` owns its inputs (final slice).

## 5. Slices, in order

Each step is a **pure code movement** guarded by the existing suite (§6); no
behaviour change, no serde-form change, no public-API change (the
`engine-core (clippy + test)` job with `warnings = "deny"` plus the
golden guards make drift visible; the wasm/`.d.ts`/server surfaces are
JSON-only and need no change because no `pub` signature moves).

| # | Slice | Risk | Notes |
|---|---|---|---|
| 1 | `temporal` leaf; dedupe the two ISO-8601 parsers | low | small, self-contained; immediately kills a real drift surface |
| 2 | `ParseError` → `validate/error`; capture types owned by `validate` | low–med | breaks cycle 2, no runtime change |
| 3 | `state` → `types` + `apply` (break `event ↔ state`) | med | confirm golden serde fixtures stay byte-identical |
| 4 | dependency-direction CI lint (allowed-edges table) | low | **freeze the layering before the big moves** |
| 5 | `engine/mod.rs` split into the ~11 impl files, explicit imports | med | mechanical but large; one concern per PR |
| 6 | `state::apply` per-family split | med | same discipline |
| 7 | test-file split (`engine/tests/*`, `bpmn/tests.rs`) | low | pure file moves; biggest merge-conflict relief |
| 8 | `bpmn.rs` parser / `ProcessAcc` / tests separation | med | last — `validate` owns its inputs by then |

## 6. Regression coverage audit (verified 2026-09-20, before starting)

### Suite run

```
engine-core, cargo test (feature-off)            770 passed, 0 failed
engine-core, cargo test --features serde         839 passed, 0 failed
```

(Extra 69 are the serde-gated golden drift / replay / forward-compat tests —
the `engine-core (clippy + test)` CI job already runs `--features serde`.)

### What exists, and what it protects per slice

| Guard | What it is | Protects slice |
|---|---|---|
| `engine/tests.rs` — **472 tests** | full command→event→state execution semantics | 5, 6 |
| `bpmn.rs` — 151 tests | parser incl. ISO-8601 duration/cycle parsing | 1, 8 |
| conformance corpus — **25 `.bpmn` models** with declarative Zeebe verdicts | deploy-validation parity, every `ParseError` category in exactly one of the two mapping tables (partition enforced by test) | 2, 8 |
| golden serde drift — byte-for-byte snapshot + event journal (v1, v2) | any serialized-shape change fails CI | 3 |
| golden replay — whole-journal replay parity under current code | event-frame replay safety (#1065/#1069 class) | 3 |
| `feel` 35 / `dmn` 14 / `validate` 40 / `state` 13 / `event` 7 tests | expression, decision, validator, applier units | 1, 2, 3, 6 |
| `engine-wasm` `surface_parity.rs` + e2e tests (JS) | compile-time exhaustiveness over `Command`/`ReadQuery`; TestEngine surface | all |
| server e2e — `journal_replay_e2e` (37), `falcon_e2e` (10), `cluster_e2e` (4) | engine behind the gateway: journal, replay, failover | all |

### Per-concern execution-test density (`engine/tests.rs`, by name)

Healthy for **every** concern that slice 5 splits out: agent 85, adhoc 83,
job 84, boundary 54, multi_instance 45, listener 43, incident 40,
call_activity 36, message 32, timer 29, user_task 17, migration 17, gateway 15,
escalation 13, lease 11, compensation 4, conditional 10, signal 5. (Names
overlap, so the sum exceeds 472.) Nothing will be moved that has no test
behind it; `compensation` (4) is the thinnest and its slice should land
last in the file-split ordering.

### Honest gaps (noted, deliberately non-blocking)

1. **FEEL internals are thinly tested** — `feel/{parser,lexer,eval,builtins,
   value}.rs` carry 0 inline tests (27 in `feel/mod.rs` + 6 temporal + 2
   regex). `feel` is a leaf the refactor does *not* touch (except the
   temporal dedupe, which is covered), so this does not gate the work — but
   a FEEL test-depth slice should follow, since it is the least-protected
   3.6k-line surface in the crate.
2. **The golden corpus is representative, not exhaustive** over the 92
   `Event` variants. Irrelevant to pure module moves (module moves cannot
   change the serialized form), and the golden replay test re-proves
   replay-parity on the corpus; noted for completeness.
3. **`state::apply` has only 13 direct tests** — but `apply` is the *sole*
   state mutator, so all 472 engine tests (each asserting post-command
   state queries) exercise it end-to-end. The per-family split (slice 6) is
   safe on that basis; each family's existing tests move with it.

### Verdict

**Coverage is sufficient to begin.** Every refactoring surface is guarded by
(a) a green, deterministic, 839-test suite that must pass first-run (no
retries, per repo policy), (b) byte-for-byte golden serde/replay fixtures for
the only shape-sensitive move (slice 3), (c) a Zeebe-verdict conformance
corpus for the parser/validator moves, and (d) downstream wasm-parity and
server e2e layers. The one shape-sensitive action to take during slice 3 is to
re-run `cargo test --features serde` and confirm the golden fixtures diff by
zero bytes; if they do not, the move changed the on-disk form and must be
reverted or version-bumped per the event-frame rules in `AGENTS.md`.
