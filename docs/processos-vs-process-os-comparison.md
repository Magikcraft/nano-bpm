# ProcessOS vs. process-os — Implementation & Eval Comparison

> Status: analysis / direction only. No code changes are made by this document.
> Compares **Nano ProcessOS** (this repo, `processos/` — a Rust trace-mining
> runtime) with **camunda/process-os** (`~/workspace/process-os` — a Claude Code
> plugin), with a focused deep-dive on the **eval approach** of each. Companion
> to [`processos-index.md`](./processos-index.md) and ADR
> [`adr/0004-investigator-outcome-eval-harness.md`](./adr/0004-investigator-outcome-eval-harness.md),
> which proposes porting the strongest ideas from process-os into Nano.

## TL;DR

The two projects share a name, a vendor lineage (Camunda), and even a
`loan-approval` worked example — but they are **different species of tool**, and
their "evals" sit at different layers of the stack and measure different things:

- **camunda/process-os** is a **breadth-first lifecycle authoring harness**: a
  Claude Code plugin of ~48 markdown skills that drive a cloud agent from process
  discovery → optimization → transformation → BPMN/artifact generation →
  deploy/test. Its eval suite (`evals/`) is **CI quality-gating on the agent and
  skills themselves**: it runs the real Claude Code agent end-to-end in a Docker
  sandbox and grades what it produced (files, final report, deployed-and-tested
  BPMN).
- **Nano ProcessOS** is a **depth-first runtime**: a Rust binary that mines
  *recorded production traces* and uses a local-LLM investigator to author and
  **backtest** candidate BPMN models against that real history. Its "eval" is not
  CI for the tool — it is the **product's own runtime fitness function**
  (`harness/replay.rs`), the deterministic verifier the investigator scores its
  own hypotheses with.

The headline: **process-os has the more mature eval *infrastructure*; Nano has
the more informative eval *signal*.** process-os had to *build* a behavioural
verifier (deploy-to-cluster) because it has no recorded history to score against;
Nano already *owns* a stronger one — replay against real traces. The actionable
conclusion (see ADR 0004) is to wrap Nano's verifier in process-os-style harness
infrastructure.

## 1. What each one is

| | **camunda/process-os** | **Nano ProcessOS** (`processos/`) |
|---|---|---|
| Form factor | Claude Code **plugin** — 48 `SKILL.md` slash-commands + 9 subagents | Rust **binary**: HTTP server + embedded `engine-core` + cockpit UI |
| Orchestration | Cloud Claude (Sonnet) drives skills through doc-driven phases | Local-LLM investigator agent over llama.cpp sidecars |
| Primary input | Org knowledge via specialists (web / filesystem / github / interview) | **Recorded execution traces** (Tier-1 creation + Tier-2 job stimuli) |
| Primary output | Documents + deployable Camunda artifacts (BPMN, Java Spring workers, DMN, Forms) | Scored / ranked candidate BPMN variants |
| Lifecycle scope | **Broad:** landscape → discovery → optimize → transform → BPMN → artifacts → deploy → test → CI | **Narrow & deep:** trace mining → hypothesize → simulate / replay → rank |
| Target runtime | Real Camunda 8 cluster (SaaS / Helm / c8run) | Its own offline embedded engine |
| Language / stack | Markdown skills (+ a Python `evals/` tree) | Rust |
| Network posture | Cloud model + live cluster | Local / offline-first |

## 2. Feature-set, side by side

| Capability | process-os | Nano ProcessOS |
|---|---|---|
| Process **discovery** from source material | ✅ multi-specialist iterative discovery | ➖ (assumes you already have traces) |
| Process **optimization** / **transformation** | ✅ optimize + 3-tier radical transformation (incremental/radical/moonshot) | ✅ pattern-driven optimization loop (design docs 1–5) |
| BPMN **generation** | ✅ `bpmn-generate` (multi-phase, subagents) | ✅ structured `edit_model` authoring tools (`bpmn_model.rs`) |
| BPMN **linting / compat** | ✅ bpmnlint, camunda-compat, dmnlint, forms-lint | ✅ `validate_model`, `lint_task_definition_attribute`, layout/name lints |
| Deployable **artifacts** (workers/DMN/Forms) | ✅ `artifact-generate` (Java Spring Boot) | ➖ (focus is model fidelity, not codegen) |
| **Recorded-history** ingest | ➖ | ✅ `import-camunda` folds C8/Zeebe exports → `traces.json` |
| **Simulation** of candidates | ➖ (judges + cluster instead) | ✅ `simulate` / `compare_variants` (distributional, `limit`/`sampleSize`) |
| **Replay / backtest** against real instances | ➖ | ✅ `replay_dataset` (deterministic boundary backtest) |
| **Population ranking** of candidates | ➖ | ✅ `replay_rank` ("engine as fitness function") |
| **Deploy-and-test** on a live cluster | ✅ CPT (`mvn test` against H2 cluster) | ➖ (replays in-process instead) |
| **Eval / regression harness** for the agent itself | ✅ Inspect-AI outcome evals + trend store | ➖ (proposed in ADR 0004) |
| **Local / offline** operation | ➖ (cloud model + Docker + cluster) | ✅ |

The two are largely **complementary**: process-os is strongest at the *front of
the lifecycle* (discover → author → deploy) and at *testing its own agent*; Nano
is strongest at the *back* (score a candidate against what actually happened in
production).

## 3. Eval approach — the deep dive

These are **not comparable layers**. One evaluates the *tool*; the other *is* a
scoring engine *inside* the tool.

### 3.1 process-os — outcome evals as a CI quality gate

Located in `evals/` (isolated; never ships in the plugin). Built on **Inspect AI**.

- **Runs the real agent end-to-end.** The `inspect_swe` claude_code bridge runs
  headless Claude Code inside a Docker sandbox; its model calls are proxied back
  to Inspect's provider. It grades *what the agent left on disk*, not a mocked
  loop. (`docs/testing/evals.md`.)
- **Scorers split gating vs diagnostic, deterministic-before-judge:**
  - deterministic gates: `assert_skill_loaded` (did the skill actually run),
    `checklist` (workspace file-shape), `bpmn_lint_clean`, and the strongest —
    **`cpt_scorer`**: `mvn test` deploys the generated BPMN to a **live Camunda
    cluster** and asserts runtime routing behaviour.
  - LLM-as-judge gates: `model_graded_qa` over the agent's final report **and** a
    custom `judge_bpmn_contract` over the generated XML artifact (the
    "judge-on-text vs judge-on-file" distinction).
- **Contract-pinning:** the prompt fixes exact ids/job-types/conditions so the
  deterministic CPT IT applies unchanged across runs (worked reference:
  `scenarios/loan-approval/e2e.outcome-test.py`).
- **Committed, schema-versioned trend store:** a `RunRecord` (schema 2) per
  `(test, sample)` appended to in-repo `evals/history/.../history.jsonl` —
  pass/fail + token split (Input / Cache-Write / Cache-Read / Output) + runtime +
  turns + tool categories + sub-agent breakdown + file diffs + BPMN element
  census. PRs read it back to compare against the main-branch trend;
  token/runtime are **tracked but never gated**.

**What it answers:** *"Does the agent, running the skill, produce a deployable and
behaviourally-correct artifact — and is it getting better / cheaper over time?"*
Graded against a *pinned synthetic contract* + a live cluster + LLM judges.

### 3.2 Nano ProcessOS — replay as a runtime fitness function

Located in `processos/src/harness/`. Pure Rust, **in-process, deterministic, no
LLM in the scoring loop**.

- **`replay.rs::replay_dataset`** backtests one candidate model against *real
  recorded instances*: the instance's creation inputs seed it, and each job the
  candidate issues is served from the **recorded Tier-2 output of that job type**.
  Routing therefore uses true historical variable values, and the end state is
  checked against the recorded boundary — a genuine backtest.
- The result is a **gradient, never pass/fail**: validity (compiler-class) →
  completion → per-job-type **coverage** (a job type the candidate issues that
  history never produced ⇒ *requires a new worker*) → key-by-key
  **boundary-conservation** divergence (terminal variables vs the recorded
  terminal).
- **`replay_rank.rs`** scores a whole **population** of candidates against the
  same dataset and ranks them by the fidelity gradient — feasible candidates sort
  ahead of invalid; among feasible, higher `conservedRate` wins, then lower
  latency, then fewer required new workers. Every candidate keeps its full
  `ReplayReport` for drill-down. This is *"engine as fitness function."*
- It is the **verifier inside the LLM hypothesis loop** (`investigate.rs`), not a
  CI gate. There is currently **no** end-to-end eval of the investigator agent
  itself — that is the gap ADR 0004 addresses.

**What it answers:** *"Would this candidate model have reproduced real production
history?"* Graded against *recorded traces*, deterministically, as a gradient.

### 3.3 The crux

| | process-os evals | Nano replay |
|---|---|---|
| Layer | CI gate on the *tool/agent* | runtime fitness function *inside* the tool |
| Ground truth | hand-pinned synthetic contract + live-cluster behaviour | **real recorded production traces** |
| Verdict shape | pass/fail (gating) + judge grades | **gradient** (validity → conservation), never a single verdict |
| LLM in the loop | yes (judges, alongside deterministic gates) | **no** (fully deterministic) |
| Cost to run | JVM + cluster + Docker + Maven; minutes + tokens | pure, in-process, free |
| Maturity | mature **infrastructure** (sandboxes, trend store, CI) | strong **signal**, but no agent-level harness |

process-os had to *build* a behavioural verifier (CPT deploy-and-assert) because a
markdown-skill plugin has no recorded history to score against. Nano's `replay` is
strictly **more informative for the same purpose** — it scores against *observed
production behaviour*, not a synthetic contract a human pinned into the prompt —
and it is pure, deterministic, and free.

## 4. Ideas worth porting (both directions)

### Into Nano, from process-os
1. **An outcome-eval harness for the investigator agent itself** (ADR 0004). Nano
   has unit tests over the scoring primitives but nothing that runs the *LLM
   investigator end-to-end* and grades the BPMN it authors — and Nano already owns
   the perfect gating scorer: wire `replay` fidelity as the deterministic gate.
   (Nano's verifier is *better* than CPT: real traces, not a pinned contract.)
2. **A committed, schema-versioned JSONL trend store** (fidelity score + tokens +
   turns + wall-clock per scenario/model over time) to catch investigator
   regressions — directly relevant to the recurring "which model authors the best
   model" and stale-binary questions.
3. **Gating-vs-diagnostic scorer discipline** and **deterministic-before-judge**:
   the recurring "vertical, unnamed, conditionless gateway" failures should be a
   red gate, not a manual live-investigation observation.

### Into process-os, from Nano
1. **Replay-against-real-traces as a fitness signal.** The `optimize` /
   `transform` skills currently justify changes with LLM rationale + judges; they
   have *no* data-grounded check that a transformed process still reproduces
   observed production behaviour. Nano's Tier-2 replay + boundary-conservation
   gradient would give those skills a hard fitness number.
2. **Fidelity-as-gradient ranking** for the three transformation tiers
   (incremental / radical / moonshot) instead of LLM verdicts alone.

### What to deliberately *not* port into Nano
- **Inspect AI / Python / Docker sandboxing.** The investigator is an in-process
  Rust agent over local llama.cpp sidecars; a sandboxed cloud-agent bridge adds a
  heavy second toolchain for no gain. The proposed harness stays native Rust.

## 5. References

- **Nano ProcessOS:** `processos/src/harness/replay.rs` (`replay_dataset`),
  `processos/src/harness/replay_rank.rs` (`CandidateModel`, population ranking),
  `processos/src/experiment.rs` (`simulate` / `compare_variants`),
  `processos/src/investigate.rs` (agent loop), `processos/src/agent.rs`
  (`wire_messages`), `processos/src/bpmn_model.rs` (`validate_model`,
  `normalize_authoring`, `lint_task_definition_attribute`),
  `processos/src/camunda_import.rs` (`import-camunda`). Design series:
  [`processos-index.md`](./processos-index.md).
- **camunda/process-os** (`~/workspace/process-os`): `README.md` (skill catalog),
  `evals/README.md` + `docs/testing/evals.md` (eval architecture),
  `evals/src/scorers/cpt.py` (deploy-and-assert gate), `evals/src/scorers/checklist.py`,
  `evals/src/core/runrecord.py` + `evals/src/core/history.py` (trend store),
  `evals/scenarios/loan-approval/e2e.outcome-test.py` (worked outcome eval).
- **Proposal:** [`adr/0004-investigator-outcome-eval-harness.md`](./adr/0004-investigator-outcome-eval-harness.md).
