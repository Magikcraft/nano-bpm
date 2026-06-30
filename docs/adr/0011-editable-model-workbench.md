# ADR 0011 — Editable model workbench (human-authored variants in investigations)

Status: **Proposed.**
Date: 2026-06-30.
Relates to: `processos/src/cockpit.html`, `processos/src/investigate.rs`,
`processos/src/bpmn_model.rs`, `processos/src/experiment.rs`,
`processos/src/harness/replay.rs`, `processos/src/harness/replay_rank.rs`,
`processos/src/main.rs`, `processos/assets/bpmn/*`. Builds on ADR-0010
(extensible tool surface) and the first-class LLM Pairings work.

## Context

ProcessOS investigations already revolve around a **variant + scorecard** loop.
The investigator/Experiment-Designer LLM forks the model and replays it against
the investigation's recorded dataset; each run surfaces in the cockpit's
**Simulations tab** as a variant model card beside a fidelity scorecard
(`cockpit.html` `simRunsHtml`/`suggestedModelsHtml`, ~2113–2660). The comment at
`cockpit.html:2625` already notes two authoring paths feeding that pool: the
agent's `edit_model` tool result, and raw `model` arguments to
`simulate`/`compare_variants`.

The BPMN canvas in the cockpit, however, is **read-only**: vendored bpmn-js
served at `/assets/bpmn/*` (`bpmn-navigated-viewer` + `bpmn-auto-layout` + ELK),
used purely to render suggested models. A human cannot propose or refine a model
directly; they can only watch the agent author variants.

This is a real gap for the project's stated loop — "hypothesize better variants,
prove them on real engines, roll winners to production in cohorts"
(`cockpit.html` console lede). An operator with domain knowledge often *knows*
the topology they want to test against a customer dataset, but has no entry
point. The question this ADR settles is **how a human-editable model interacts
with an investigation** without becoming a disconnected, parallel editor.

## Decision

Add an **editable model workbench as a mode within an investigation**, where a
human-edited canvas is a **first-class variant** scored by the same harness as
agent variants — not a separate app and not a new scoring path. Five parts.

### 1. The canvas authors variants, bound to the investigation

Swap the read-only bpmn-js viewer for a bpmn-js **Modeler** in a **Workbench
mode** toggled inside an open investigation (Chat ↔ Workbench). The model under
edit is bound to that investigation's **baseline model + dataset**, so a
human-authored variant draws on the same recorded traces the agent uses. Every
suggested-model card and the recorded/deployed model gains an **"Edit this
model"** action that forks it onto the canvas — the user never starts blank
against a real dataset.

The variant pool is unified: baseline, agent candidates (`edit_model` /
`simulate` / `compare_variants` args), and human edits are the same kind of
object, rendered by the existing card renderer and tagged by **provenance**
(`authored-by: user | agent | co-edited`) so the ranked table shows who proposed
the winner.

### 2. Lint-before-simulate gate (instant, no LLM)

A human drawing on a canvas will produce models the nano engine rejects unless
normalized (the `boundaryEvent` / `zeebe:taskDefinition`-as-child traps — see
ADR/bpmn-authoring memory). On every edit the workbench runs the **deterministic**
checks already in `bpmn_model.rs` — `validate_model` (parse errors, dangling
`errorRef`), `normalize_authoring`, and `lint_task_definition_attribute` — and
renders results as **error markers on the offending canvas elements**, with the
existing `deploy_fix_hint` text.

This is the "agent analyses the model for structural defects" capability, but
served **instantly and token-free** by the deterministic validator. It is a
**blocking gate**: Simulate is disabled until the model deploys cleanly, so the
operator never burns a replay run on a model that hits the scorecard's existing
`deployError` path.

### 3. Simulate against the customer dataset, honestly

A **Simulate** button runs the existing `simulate` tool against the
investigation's dataset, **staged** via the existing `limit`/`sampleSize`
(1 → 25 → full; `experiment.rs` returns `datasetTotal`/`sampled`). Results land
in the same Simulations tab.

The realism question — "what happens when a human proposes a model whose tasks
never ran in production?" — is already answered by replay scoring, which splits
uncovered job types (`harness/replay.rs`, `replay_rank.rs`):

- **`requiresNewWorkers`** — new tasks with *no recorded history*. The workbench
  surfaces these explicitly ("N new tasks have no recorded behavior — mock them
  to simulate") with inline **mock authoring** (constant output / sampled
  distribution / agent-suggested mock).
- **`divergentWorkers` / `structuralDivergence`** — history exists but the model
  reroutes it. Flagged as "fix topology, don't mock," with the diverging flows
  highlighted on the canvas.

A **Compare** action runs `compare_variants` (human edit vs baseline vs the
agent's best candidate) → the existing ranked-candidates table with fidelity
tiers and confidence bands.

### 4. The agent as collaborator on a human variant

Beyond the deterministic gate, the operator can request an LLM review of the
current canvas (the investigator persona, or a Pair persona). The reviewer may
run `simulate` itself and, crucially, return **counter-edits as `edit_model`
ops** that render as an **"Apply suggestion"** diff on the canvas. Edits flow
both ways: human → canvas, agent → applyable patch, both producing variants in
the same ranked set. This realises the unified two-source authoring the existing
code comment anticipates.

### 5. One investigation, two lenses; winners feed the existing pipeline

Workbench and Chat are **modes of the same investigation**, sharing one dataset
and one variant pool — not a separate route. A winning **human-authored** variant
is an ordinary candidate, so it flows into the **same experiment/cohort rollout**
the agent's winners use; the canvas only adds a human entry point to a loop that
already exists. Human variants persist in the transcript, are downloadable, and
deploy through the existing path.

## Consequences

- The cockpit gains a genuine human-in-the-loop authoring surface while
  **reusing** the entire downstream stack (validate → simulate/compare → ranked
  scorecards → experiment rollout). The only load-bearing new build is the
  editable canvas plus button wiring to tools already exposed.
- Two pieces carry the design: the **lint-before-simulate gate** (so
  human-drawn models are deployable on nano before any run) and the
  **provenance-tagged unified variant pool** (so human and agent proposals are
  ranked head-to-head on the same data).
- bpmn-js Modeler is a heavier asset than the vendored viewer; it remains
  `include_str!`-embedded and `no-store` like the other bpmn assets, so a browser
  reload still picks it up after a rebuild.
- Risk to manage: Modeler is a larger client bundle and a richer attack/UX
  surface than the viewer; mock authoring for `requiresNewWorkers` must be clear
  about the *fidelity caveat* of simulating never-recorded behavior.
- Future: live co-editing (agent edits the canvas mid-stream), a "diff against
  baseline" canvas overlay, and promoting a human variant directly into a cohort
  experiment from the workbench.
