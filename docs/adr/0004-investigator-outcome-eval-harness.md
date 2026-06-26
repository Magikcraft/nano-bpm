# ADR 0004 — Investigator outcome-eval harness (replay-as-gating-scorer)

Status: **Proposed.**
Date: 2026-06-26.
Relates to: `processos/src/investigate.rs`, `processos/src/harness/replay.rs`,
`processos/src/harness/replay_rank.rs`, `processos/src/experiment.rs`,
`processos/src/agent.rs`; prior art surveyed in `~/workspace/process-os/evals`
(camunda/process-os Inspect-AI suite).

## Context

ProcessOS ships an LLM-driven **investigator**: an agent loop
(`investigate.rs` + `agent.rs`) that, given a recorded trace corpus, authors and
mutates candidate BPMN models, then scores them with the deterministic replay /
simulate machinery (`harness/replay.rs`, `harness/replay_rank.rs`,
`experiment.rs`). The scoring layer is strong — `replay` backtests a candidate
against *real recorded instances* (Tier-1 creation inputs + Tier-2 job outputs)
and emits a fidelity **gradient** (validity → completion → per-job-type coverage
→ key-by-key boundary-conservation divergence); `replay_rank` ranks a whole
population by that gradient ("engine as fitness function").

What we do **not** have is any test that exercises the **investigator agent
end-to-end** and gates on the quality of what it produces. The crate has ~36
unit tests over the scoring primitives, but:

1. **No regression gate on the agent itself.** A prompt change, a model swap, a
   reasoning-control tweak, or a wire-context change (`wire_messages` clipping)
   can silently degrade the BPMN the agent authors. We only find out by eyeballing
   live investigations (e.g. "Investigation 4 is horizontal and named, but the
   gateway lost its conditions"). These observations are manual, unrecorded, and
   non-reproducible.
2. **No trend signal.** We have no committed record of token spend, turn count,
   wall-clock, or — critically — **achieved fidelity score** per scenario over
   time, so we cannot tell whether a change made the investigator better, cheaper,
   or worse.
3. **Stale-binary footguns recur.** Much of this session's manual debugging
   traced to testing a UI/agent built before the latest source edits. An
   automated harness that builds and drives the agent from a known corpus removes
   that whole class of false signal.

Meanwhile, the sibling project **`~/workspace/process-os`** (a Claude Code
plugin) has solved the *infrastructure* half of this problem well, and its design
is worth borrowing from deliberately:

- It uses **Inspect AI** to run the real agent end-to-end in a sandbox and grade
  *what the agent left on disk* — not a mocked loop.
- Scorers are split **gating vs diagnostic**, **deterministic-before-judge**: a
  cheap deterministic check decides pass/fail; an LLM judge is added only where a
  deterministic check cannot express the failure mode. Its strongest gate,
  `cpt_scorer`, deploys the generated BPMN to a live Camunda cluster and asserts
  runtime behaviour.
- It keeps a **schema-versioned trend store** committed in-repo as JSONL per
  `(test, sample)` — pass/fail plus token split (I/CW/CR/O), runtime, turns, tool
  categories, sub-agent breakdown, file diffs, and a BPMN element census — read
  back by PRs to compare against the main-branch trend.

The key realisation: **process-os had to *build* a behavioural verifier
(deploy-to-cluster CPT) because it has no recorded history to score against.
ProcessOS already *owns* a better behavioural verifier — `replay` against real
traces.** We have the hard part (the signal); we are missing the cheap part (the
harness and the trend store). This ADR proposes building the harness around the
verifier we already have.

## Decision (proposed)

Add an **investigator outcome-eval harness** to the `processos` crate that runs
the real investigator agent against a pinned trace corpus and gates on the
fidelity score produced by the existing `replay` evaluator. Concretely:

1. **A corpus of pinned eval scenarios.** Each scenario is a recorded
   trace dataset (the existing `northwind-bank/loan-approval` Tier-2 corpus is the
   worked reference) plus an investigation prompt and an expected-fidelity floor.
   Stored under `processos/evals/scenarios/<id>/` (data + `scenario.toml`),
   independent of the live `.processos-data` workspace.

2. **An eval runner** (`processos eval run [--scenario <id>] [--model <profile>]`,
   or a `#[ignore]`-gated integration test behind a `PROCESSOS_EVAL=1` env so it
   never runs in the normal `cargo test` path). It drives the investigator agent
   to completion against the scenario corpus, captures every candidate model the
   agent authored, and scores each with `harness::replay::replay_dataset`.

3. **Gating-vs-diagnostic scorers**, deterministic-before-judge, mirroring the
   process-os discipline but using our own primitives:
   - **Gate (deterministic):** the best candidate the agent produced must
     `validate_model` clean **and** achieve a replay `conservedRate ≥ floor` with
     `requiresNewWorkers == 0` (or within the scenario's allowed set). This is the
     replay fitness gradient used as a pass/fail threshold — the analogue of
     process-os's `cpt_scorer`, but grounded in real history rather than a
     hand-pinned synthetic contract.
   - **Gate (deterministic):** the authored BPMN must satisfy the structural
     lints we already enforce (`lint_task_definition_attribute`,
     horizontal-layout / has-names checks) — the recurring "vertical, unnamed,
     conditionless gateway" failures become a red gate, not a manual observation.
   - **Diagnostic (never gates):** token spend, turn count, wall-clock, and the
     full `ReplayReport` scorecard, recorded for the trend store.
   - **Optional judge (later):** an LLM-judge over the agent's closing narrative
     can be added behind the same gating/diagnostic split *only* where a
     deterministic check is insufficient — explicitly not the primary gate.

4. **A committed trend store**, schema-versioned JSONL keyed on the full
   **configuration tuple** `(scenario, model, pairing)` under
   `processos/evals/history/<scenario>/<model>/<pairing>/history.jsonl`, recording
   one row per run: pass/fail, achieved `conservedRate`, token split, turns,
   wall-clock, and BPMN census. A run reads back prior main-branch rows to report
   regressions, exactly as process-os's `core/history.py` + `RunRecord` do.

### Corollary: a model & pairing-mode leaderboard

Because the gate is the **same deterministic replay score regardless of who or
what authored the candidate**, every agent configuration is *commensurable* on one
scale. Holding the scenario corpus fixed and varying a single knob turns the
regression harness into a **comparative rater**:

- **Models.** Run a scenario across each sidecar profile (`--model <profile>`) and
  rank by achieved `conservedRate`, with token spend / latency as the cost axis.
  This answers the recurring "which local model authors the highest-fidelity BPMN,
  and at what cost" question objectively, instead of by eyeballing live
  investigations.
- **Pairing modes.** The investigator runs a primary persona then a chain of
  `PairStage` reviewers (`investigate.rs::run_chat_turn(pairs=…)`). Pairing
  configuration is just another eval axis: compare *no-pair* vs a single
  *pair-skeptic* vs *multi-pair* by their effect on achieved fidelity **and** on
  token/turn cost. This measures whether pairing actually improves the authored
  model or merely burns tokens — a question we currently cannot answer.

Hence the trend store keys on the full `(scenario, model, pairing)` tuple, not
just `(scenario, model)`: the same harness serves both as a **regression gate** (a
fixed config over time) and as a **leaderboard** (many configs over a fixed
corpus). The fidelity gradient is the common ranking key, exactly as
`replay_rank` already ranks *candidate models*; here it ranks the *agent
configurations that produce them*.

### Why replay, not deploy-to-cluster

process-os deploys to a real cluster because a markdown-skill plugin has no other
way to check runtime behaviour. ProcessOS's `replay` is **strictly more
informative for this purpose**: it scores against *observed production behaviour*
(boundary conservation, key-by-key), not against a synthetic contract a human had
to pin into the prompt. It is also pure, deterministic, in-process, and free — no
JVM, no cluster, no Docker, no Maven `.m2` warming. Adopting CPT-style
deploy-and-assert would be a strict downgrade for the investigator eval.

### What we deliberately borrow vs leave

- **Borrow:** the gating-vs-diagnostic scorer split; deterministic-before-judge;
  the committed schema-versioned per-`(test, sample)` JSONL trend store; treating
  edge cases as *samples* of one scenario, not separate tests; recording token
  spend + runtime as diagnostics that are *tracked but never gated*.
- **Leave:** Inspect AI / Python / Docker sandboxing. The investigator is an
  in-process Rust agent over local llama.cpp sidecars; a sandboxed cloud-agent
  bridge buys us nothing and adds a heavy second toolchain. The harness stays
  native Rust, consistent with the crate.

### Open implementation questions (explicitly not decided here)

- **Determinism of the agent under a local model.** llama.cpp sampling is
  non-deterministic; the floor-based gate (≥ threshold, not exact-match) absorbs
  some variance, but we may need `epochs > 1` with a median, or a fixed seed /
  greedy decode for eval runs. To be settled in the spike.
- **Model availability in CI.** The gate needs a running sidecar with a downloaded
  GGUF. First cut is **local/manual** (`PROCESSOS_EVAL=1` on a dev machine with a
  warmed sidecar); wiring it into CI (self-hosted runner with a pinned small
  model, or a cloud-model fallback profile) is a follow-on.
- **Where the agent loop is entered for a headless run.** `investigate.rs`
  currently assumes the cockpit SSE path; the runner needs a non-streaming
  entry point that returns the final transcript + all authored candidates.

## Consequences

- **A real regression gate on the investigator.** Prompt/model/wire-context
  changes get scored against real-history fidelity before they ship, replacing
  manual eyeballing of live investigations.
- **A trend signal.** We can see fidelity, cost, and latency move per change, per
  model — directly useful for the recurring "which model authors the best model"
  and "did this prompt change help" questions.
- **Doubles as a comparative rater.** The same harness, run over a fixed corpus
  while varying the `(model, pairing)` knobs, ranks models and pairing modes on
  one fidelity scale — turning "which local model / does pairing help" from a
  matter of opinion into a measured leaderboard (see the corollary above).
- **Reuses the strongest asset we have.** No new verifier to build; the eval is a
  thin harness over `replay` + `validate_model` + the existing lints.
- **New surface to maintain.** A `processos/evals/` tree (scenarios + history +
  runner) and a corpus that must be kept representative. Mitigated by starting
  with the single `loan-approval` scenario already in the workspace.
- **Not free at runtime.** A full agent run is minutes and (for cloud models)
  tokens; the harness is opt-in (`PROCESSOS_EVAL=1` / explicit subcommand), never
  on the default `cargo test` path, so it never slows ordinary development.
- **Scope boundary.** This ADR covers the *investigator agent* eval only. Unit
  testing of the scoring primitives themselves (`replay`, `replay_rank`,
  `experiment`) stays as-is; this harness sits *above* them.

## References

- ProcessOS code: `processos/src/investigate.rs` (agent loop + tool dispatch),
  `processos/src/agent.rs` (`wire_messages` context clipping, reasoning control),
  `processos/src/harness/replay.rs` (`replay_dataset` — the fidelity gradient /
  proposed gating scorer), `processos/src/harness/replay_rank.rs`
  (`CandidateModel`, population ranking), `processos/src/experiment.rs`
  (`simulate` / `compare_variants` with `limit`/`sampleSize`),
  `processos/src/bpmn_model.rs` (`validate_model`, `lint_task_definition_attribute`,
  `normalize_authoring`).
- Prior art: `~/workspace/process-os/evals` — `docs/testing/evals.md` (gating vs
  diagnostic, deterministic-before-judge, the sandbox+bridge model),
  `src/scorers/cpt.py` (deploy-and-assert gate we replace with `replay`),
  `src/core/runrecord.py` + `src/core/history.py` (the schema-versioned committed
  JSONL trend store this ADR mirrors), `scenarios/loan-approval/e2e.outcome-test.py`
  (the worked cross-skill outcome eval).
