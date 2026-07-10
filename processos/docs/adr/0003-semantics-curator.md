# ADR 0003 — Semantics Curator (LLM-assisted structural annotation)

Status: **Accepted**
Date: 2026-07-10
Supersedes: none
Extends: [ADR 0002 — Semantic-annotation pipeline](0002-semantic-annotation-pipeline.md)

## Context

ADR 0002 shipped the read/optimize half of the semantic-annotation pipeline
in eight slices. Slices 9–11 filled the human authoring gap with a
[Semantics Workbench] flow editor (slice 9), cluster editor (slice 10) and
renderer preview (slice 11). At the end of slice 11 the workbench lets a
human hand-author `SemanticAnnotations` end-to-end, but for real BPMN of
any size that authoring is tedious: naming the "happy" flow, grouping
related tasks into clusters, tagging roles across dozens of user tasks. We
want an LLM collaborator that proposes a first draft the human accepts or
rejects per item — the same diff-apply pattern slices 9 and 10 already use
for the working-copy Save flow.

[Semantics Workbench]: ../../../processos/src/semantics.html

## Decision

Add a **Semantics Curator** — a workbench-scoped LLM assistant — that
proposes annotations on **one structural axis at a time**: `flows`,
`clusters`, or `roles`. Proposals surface as a diff overlay in the
workbench with per-item ☑/☐ checkboxes; accepted items merge into the
working copy and flow through the normal Save path.

### The four locked-in decisions

| # | Aspect              | Decision                                                                                                    |
|---|---------------------|-------------------------------------------------------------------------------------------------------------|
| 1 | Interaction shape   | Per-axis batch pass + interactive chat refinement                                                           |
| 2 | Accept/reject       | Diff-apply overlay (per-item checkboxes, defaults all-checked)                                              |
| 3 | Provider config     | Reuse cockpit's LLM provider config; new `semantics-curator` persona in `personas.rs`                       |
| 4 | Scope               | **Structural only** — flows, clusters, roles. Cost/time stays human-only (numbers need real telemetry)      |

### Why costs and times are explicitly out of scope

Cost and time annotations feed the Pareto optimisation loop that slices
5–7 built. That loop is only trustworthy if its inputs are grounded in
real telemetry or explicit human estimates. Letting the LLM hallucinate
`p99Ms` values would silently corrupt every downstream ranking and every
"variant reduces p99 by 30%" claim the Experiment Designer makes.

The Curator therefore:

* **Never proposes** `costs` or `times` — the persona instruction forbids
  it and the server-side parser ([`curator::parse_proposal`]) strips
  those keys silently even if the model leaks them.
* **May read** the current `costs` and `times` (they are passed in as
  context) so it can use them as a signal for which nodes matter most.

### Why per-axis rather than one polymorphic proposal

A single "propose everything" call would be tempting — one round-trip
instead of three — but:

* Partial acceptance is cleaner per-axis. Mixing accepted flows with
  rejected clusters in one overlay is harder to reason about than three
  separate axis proposals.
* The persona instruction can be sharper when it names one axis at a
  time. "Flows want a narrative arc"; "clusters want cohesive
  groupings"; "roles want durable actor names". One prompt trying to
  cover all three dilutes.
* The UI can offer three focused "Propose X" buttons rather than one
  vague "Ask Curator".

### Why not extend an existing persona

The Experiment Designer persona is a read-heavy investigator whose whole
system prompt is about running simulations and *never inventing numbers*.
The Curator is authoring structural annotations from scratch — a
genuinely different job. Making it its own persona keeps both prompts
sharp.

### Why not use `run_chat_turn`

The `run_chat_turn` machinery in [`investigate.rs`] is deeply coupled to
dataset binding, DuckDB, and a tool loop. The Curator needs none of
that: it's a one-shot LLM call given the BPMN XML and the current
annotations, and it emits one JSON object. We call [`harness::llm::complete`]
directly, keeping the transport minimal.

## Implementation

Landed in slice 12 (single PR):

* **`processos/src/personas.rs`** — new `semantics-curator` builtin
  persona.  System prompt makes the axis-only + JSON-only + no-cost/time
  constraints explicit.
* **`processos/src/curator.rs`** (new) — `Axis` enum, `build_user_prompt`,
  `parse_proposal`. `parse_proposal` is the security border: it strips
  `costs`/`times`, keeps only the requested axis key, and validates the
  shape by round-tripping through `SemanticAnnotations`.
* **`processos/src/main.rs`** — `POST /api/curator/propose` route:
  resolves LLM config via the existing `resolve_llm` helper (env →
  active operator profile → per-request `llm` override), looks up the
  Curator persona, calls `harness::llm::complete`, parses the reply,
  returns the axis-subset payload + updated chat history.
* **`processos/src/semantics.html`** — collapsible Curator pane between
  the header btnrow and the workbench grid, toggled by a `🤖 Curator`
  button.  Per-axis "Propose" buttons + freeform text input + last-6-turn
  chat log + inline proposal overlay with per-item ☑/☐ checkboxes.
  Accepted items go through the existing `mutate()` path so undo,
  save-with-diff and preview auto-refresh all work unchanged.

## Consequences

### Positive

* Humans get an LLM first-draft on the tedious axes without exposing the
  optimisation loop to fabricated numbers.
* One new endpoint, one new module, no changes to the existing
  investigate/cockpit loop.
* The persona is a plain builtin: operators can copy it and author a
  variant with a tighter or looser system prompt through the existing
  persona library UI.

### Negative

* Ties the workbench to a configured LLM (`PROCESSOS_LLM_MODEL` or an
  operator profile). Fresh installs without an LLM see a graceful
  400 with an explicit "no LLM configured" message, but the button is
  always visible — an argument for hiding it when no LLM is set.
* One-shot LLM calls without streaming: for large models a Curator
  proposal can take a few seconds and the UI shows only a ⏳ spinner
  during that time. Adding streaming is future work; the axis payloads
  are small enough today that this hasn't bitten in practice.

### Follow-ups (deliberately deferred)

* **Streaming responses** — nice to have; not needed for typical
  <1 KB axis payloads.
* **Automatic proposal on load** — deliberately off; the operator opts
  in per axis.
* **Confidence scores / rationales** — can add once we see how noisy
  raw proposals are in practice.
* **Cross-axis reasoning** — the Curator sees the current annotations
  when proposing one axis, so it *does* implicitly coordinate; but we
  could add a "coordinate" pass that runs all three sequentially with
  each seeded by the previous acceptance. Not needed yet.
* **Cost/time proposals** — the negation is deliberate and permanent
  per Q4 of the design conversation. If a future data source (e.g. real
  telemetry ingestion) can ground the numbers, that becomes a separate
  system, not the Curator.

[`curator::parse_proposal`]: ../../../processos/src/curator.rs
[`investigate.rs`]: ../../../processos/src/investigate.rs
[`harness::llm::complete`]: ../../../processos/src/harness/llm.rs
