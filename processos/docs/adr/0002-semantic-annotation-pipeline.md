# ADR 0002 — A semantic-annotation pipeline (annotate → optimize → adopt)

Status: **Proposed.**
Date: 2026-07-08.
Relates to: `processos/src/layout/{mod,schema,solver,field,debug_svg}.rs` (the
semantic BPMN layout introduced in PR #57), `processos/src/investigate.rs` (the
`simulate` / `edit_model` / `compare_variants` tool specs surfaced to the
Experiment Designer LLM), `processos/src/cockpit.html` (the chat cockpit's
suggested-model cards and the ELK / Row-Bias / Field renderer drop-down added
in PR #59), and — prospectively — `engine-core/src/bpmn.rs` (`parse_bpmn`,
`ProcessDefinition`, `SequenceFlow`) for tolerant carriage of a new `nano:*`
extension namespace.

## Context

Two recent workstreams collided in a demo and revealed a
task-conflict problem worth naming.

The **semantic BPMN layout engine** (PR #57 — `layout::Solver::RowBias` and
`layout::Solver::Field` / "Fromme") takes a `SemanticAnnotations` sidecar
(flows, clusters, roles) that tells the physics/row-bias solver which nodes
belong to the happy path, which handle errors, and which escalate. The
sidecar is what makes the diagram *readable*: without it, both solvers
collapse every node to the primary band because everything defaults to
"unclassified, treat as primary." The demo case that motivated this ADR
authored a loan-approval variant, ran it through the Fromme renderer, and
saw a flat line — because the LLM authoring the variant had emitted zero
annotations.

The **chat renderer surface** (PR #59) tried to close the gap by adding an
optional `annotations` parameter to the three model-authoring tools
(`simulate`, `edit_model`, `compare_variants`) and asking the Experiment
Designer LLM to fill it out when proposing a variant. In practice, the
inspected session
(`chat-northwind-bank__loan-approval.json:sessions[3]`, id `s1783410242508-0`)
shows the LLM burnt 30 turns diagnosing the bottleneck (queue-time queries,
gateway coverage, worker analysis), then produced three model-authoring calls
— **all with `has_annotations=False`** — before the operator stopped it for
looping on a broken `edit_model` result:

```
[21] edit_model:  has_model=False has_annotations=False keys=['ops']
[23] simulate:    has_model=True  has_annotations=False keys=['limit', 'model', 'name', 'rationale']
[25] edit_model:  has_model=False has_annotations=False keys=['ops']
```

The failure is structural, not prompt-tuning:

1. **Diagnosing performance and authoring a correct variant are already two
   hard, orthogonal skills.** Adding "…and also produce a defensible
   narrative-role reading of the model" makes each variant more expensive
   and more error-prone. When context pressure rises, optional fields die
   first — exactly what we observed.
2. **Annotations are a property of a model, not of a chat turn.** Doing
   them inline in optimization re-does the work every time. A base process
   is typically annotated once; every derived variant inherits most of that
   annotation with node-level diffs for what changed.
3. **Most of what annotations encode is recoverable from structure
   alone.** For well-formed BPMN, 80–95% of a useful annotation set can be
   inferred deterministically — boundary error events dictate exception
   flows, `default="…"` picks out the happy path, `isForCompensation="true"`
   marks compensation, `exclusiveGateway` is by definition a decision, and
   so on. The remainder is naming disambiguation and business-domain
   judgement — a small, focused LLM task, not an implicit sub-task of
   optimization.

Beyond making the diagrams legible, annotations unlock a second, larger
prize: **they give the optimization LLM better priors.** Today the model is
told "find issues" and must intuit both what's wrong and what "wrong" even
means. With `cost` and `time` annotated on every task, the optimizer works
over an *explicitly-scored objective space* — sum the happy-path cost, spot
the tax, propose targeted changes. A much more tractable AI task than
free-form problem finding. In classical process-optimization literature
these two dimensions (plus quality/risk) are the primary objectives; adding
them to the metadata is uncontroversial from that direction, and it's the
practice mature BPMN modellers already follow before arguing about
redesigns.

The Experiment Designer today has no such scaffolding. It sees an unlabeled
graph and is asked to make it better on unspecified axes.

## Decision

Adopt a **three-stage pipeline** with explicit workbench separation, hybrid
encoding of annotations across model extensions and sidecar, and cost/time
as first-class optimization dimensions:

```
Stage 0: base BPMN (imported / drawn)
Stage 1: ANNOTATE  (Semantics Workbench)
           1a. algorithmic infer (structure + telemetry)
           1b. LLM refine (small focused prompt, ambiguous cases only)
           1c. human confirm (fast, accept-most workflow)
         persist: nano:* extensions in XML + JSON sidecar in workspace
Stage 2: OPTIMIZE  (existing chat cockpit)
           objectives derived from Stage 1 (cost, time, variance)
           variants inherit Stage 1 annotations; new nodes get
           algorithmic pre-fill
Stage 3: ADOPT     (comparison view)
           Pareto plot + side-by-side + one-click promote-to-baseline
```

Each stage owns one workbench. The chat cockpit stays as Stage 2. Stage 1
is new. Stage 3 can wait until Stages 1–2 prove out.

### Encoding: hybrid, with a clear rule

*If two experienced people would disagree, it's a sidecar. If the model
author would write it down as a fact, it's an extension.*

Split concretely:

**In the BPMN model** via a new namespace
`xmlns:nano="http://nano.camunda.io/schema/semantic/1.0"`:

| Extension | On | Why in-model |
| --- | --- | --- |
| `<nano:cost value="0.50" currency="USD" per="invocation"/>` | serviceTask | A credit-check costs $0.50 regardless of who's reading. Contract with the outside world. Travels with the file to Modeler, to a customer inbox, back. |
| `<nano:time p50="2s" p99="8s"/>` | serviceTask | Same contract. Recorded telemetry can override for replay. |
| `<nano:role>review</nano:role>` | task / gateway | Structural fact about node nature. |
| `<nano:flowClass>exception</nano:flowClass>` | sequenceFlow (or target node) | A flow either is the error branch or it isn't. |

**In a sidecar JSON** persisted per workspace-process:

- Cluster groupings (multiple valid clusterings exist for the same model —
  finance's cost view vs. ops's SLA view).
- Free-form operator notes and hypotheses.
- Provenance / confidence metadata (structural / heuristic / LLM / human,
  with `source` and `capturedAt` per entry).
- Per-workspace or per-user opinion overlays that shouldn't leak into the
  canonical file.

The Semantics Workbench presents them **unified**; the persistence layer
writes each to its rightful home. UX does not need to expose the split.

We publish a moddle descriptor for `nano:*` so bpmn-js round-trips the
extensions across save/load. Zeebe went through this exact door; the
mechanics are known.

### Objectives: cost + time + variance

Cost and time are the two dimensions the user asked for. We add
**variance** as a third almost for free — telemetry already gives us the
p50/p99 spread on service and queue times, and a task at mean 2s / p99 60s
is a fundamentally different optimization target from mean 2s / p99 3s.

This opens:

- **Pareto exploration**: rank variants by dominance across (cost, time,
  variance) and present the front. Trade-off cards ("−$0.12/inst, +2s p99,
  −80% variance") make the trade legible.
- **Path summarization**: sum cost/time along the happy path, along each
  exception path, weighted by branch probability from telemetry. The
  optimizer can identify which segment carries the tax before firing
  `simulate`.
- **Algebraic what-ifs on the client**: many optimizations (serial →
  parallel, batching, cache) have closed-form effects on cost/time. The
  LLM can reason about them without paying for a full replay run.

### UX: Semantics Workbench + slide-out bootstrap

**Semantics Workbench (Stage 1)** — anchored on the pattern the
Camunda-ecosystem operator already knows:

- Canvas: BpmnJS diagram with band-colour overlay for `flowClass` and
  role-icon badges. Live re-render with the renderer drop-down (ELK /
  Row-Bias / Field) so the operator sees the layout consequence of every
  annotation edit.
- Per-selection properties panel: Role · Flow Class · Cost · Time · Notes.
  Every field has an algorithmically-inferred default with a small
  `◎ inferred` tag; editing flips it to `✓ confirmed`.
- Global right-hand tabs:
  - **Flows** — proposed primary/exception/escalation/compensation flows
    with member nodes, confidence badges, accept/reject/edit. Ambiguous
    cases (branch with no `default`, neutral names) surfaced first.
  - **Costs & Times** — sortable table, missing values flagged, path totals
    for happy/exception summarised at the bottom.
  - **Provenance** — audit log (structural / heuristic / LLM / human, with
    timestamps).
- Bootstrap button "Run algorithmic + LLM pass" — one click, fills
  everything reasonable, leaves genuinely ambiguous cases for the human. A
  12-node model annotates in under a minute.
- Save writes `nano:*` extensions to the XML and the overlay data to the
  sidecar; a diff view precedes commit.

**Chat cockpit (Stage 2) enrichments** — no new workbench, additive:

- Model cards get a cost/time badge (e.g. "variant 3 — −$0.12/inst, −6s
  p99") from the returned XML's extensions.
- New Pareto tab beside Diagram/XML plots submitted variants on the chosen
  2D projection (default cost×time), baseline anchored, click-to-open.
- "Edit semantics" affordance on each card opens Stage 1 in-place with the
  variant pre-loaded.

**One-hour bootstrap UX** (ships before the standalone workbench):

Expose annotations as a **panel that slides out from the chat model
card** — click ◎ Semantics on any card, see the algorithmically-inferred
annotation for the variant, accept/reject inline, watch Row-Bias / Field
re-render live. Same interaction pattern; no new page. Proves the shape
before we invest in a route.

### LLM prompt surface change

Once Stage 1 exists and Stage 2's `/api/layout` server-infers annotations
by default (see rollout below), we **remove `annotations` from the
Experiment Designer tool schemas** (`simulate`, `edit_model`,
`compare_variants`). The LLM in Stage 2 goes back to authoring correct
variants only, no longer competing with a labelling task; the semantics
plane is owned by Stage 1.

We keep the parameter conceptually available as a hidden optional field
for the future case of a semantics-focused LLM handing off richer
annotations — the removal is from the schema description the primary LLM
sees, not from the parser.

## Consequences

**Positive.**

- The Fromme (Field) and Row-Bias renderers stop being "sometimes flat"
  the moment we land the server-side auto-infer default (rollout step 1) —
  no LLM work required to unblock the current demo.
- The Experiment Designer's context is freed of an optional-and-therefore-
  first-to-drop labelling task; every optimization turn has more room for
  the harder authoring problem.
- Cost/time-aware optimization becomes a well-posed problem instead of a
  fuzzy one, opening the door to Pareto-front presentation and algebraic
  what-ifs that skip `simulate` entirely.
- Annotations become reusable: annotate a base process once, every variant
  inherits until edits invalidate.
- Semantics live where they belong: task-intrinsic facts travel with the
  `.bpmn` file (interoperable with the wider Camunda ecosystem);
  reader-scoped opinions layer without polluting the canonical artefact.

**Negative / risks.**

- **Namespace commitment.** Publishing `nano:*` in customer models means
  we own that vocabulary. We treat v1 as `nano:semantic/0.1` and reserve
  the right to rev; a moddle descriptor / JSON-schema for validation ships
  alongside.
- **Cost/time drift from telemetry.** Algorithmic pre-fill is a snapshot.
  Each cost/time entry carries `source ∈ {manual, telemetry}` and
  `capturedAt`; a Re-sync button refreshes `source=telemetry` values only,
  manual entries stay untouched.
- **Variant inheritance staleness.** When a variant restructures the
  model, base-inherited cost/time on affected nodes are now lies. On
  structural edit we mark inherited annotations `stale` (not delete) and
  force a Stage-1 pass before Stage 2 will re-score.
- **New workbench maintenance surface.** We add a page and a persistence
  channel. Mitigation: the algorithmic pass is the heavy lifting; the
  workbench is thin above it.
- **Bloat.** Extensions add characters. Not meaningful at typical model
  sizes; worth measuring past a few hundred nodes.
- **Scope creep.** The moment we have annotations, someone will want
  risk / quality / regulatory-tag fields. **MVP is `cost`, `time`, `role`,
  `flowClass`.** Anything else waits until we've shipped and learned.

**Neutral / worth noting.**

- We deliberately keep the `annotations` parameter alive on the tool
  executor (just hidden from the LLM schema) so a future
  semantics-specialised agent can supply richer input without a schema
  change.
- The renderer drop-down (PR #59) becomes the primary visual feedback
  loop for Stage 1 edits — the two features that shipped together turn
  out to be complementary, not redundant.

## Rollout

The pipeline lands in ordered slices; each slice is independently useful.

1. **Auto-infer annotations server-side** as the default fallback in
   `POST /api/layout` when no annotations were supplied. The one-hour fix
   — zero risk, immediately unblocks the current demo. Fromme and
   Row-Bias renderers show real bands for any variant, LLM-authored
   annotations or not.
2. **`annotate::infer` module** (`processos/src/layout/annotate.rs`),
   with tests: structural pass first (boundary events → exception,
   `default` → primary, `isForCompensation` → compensation,
   `exclusiveGateway` → decision role, etc.), then heuristic pass
   (name/jobType regex for review/notification/external roles,
   shared-jobType clusters), then telemetry pass (`source=telemetry`
   time/variance defaults from the Analysis surface).
3. **Slide-out semantics panel on the chat model card** — algorithmic
   pass output shown inline, accept/reject controls, live re-render with
   the existing drop-down. Proves the interaction pattern before a
   standalone workbench.
4. **`nano:*` extension namespace**: engine parser tolerant of unknown
   extensions on tasks/flows (a small carrier struct), serialiser
   preserves them on round-trip, moddle descriptor published for bpmn-js
   / Camunda Modeler interoperability.
5. **Sidecar persistence**: per-workspace-process annotation store at
   `workspaces/<ws>/processes/<proc>/annotations.json`, versioned per
   process-model revision. LLM-inferred and human-confirmed entries
   carry provenance.
6. **Semantics Workbench** (Stage 1) proper: standalone route, canvas +
   per-selection properties panel + Flows / Costs & Times / Provenance
   tabs, save-with-diff. Uses everything from steps 2–5.
7. **Cost/time-aware optimization prompt** in Stage 2: the Experiment
   Designer sees annotations as first-class objective inputs, `simulate`
   scorecards start reporting cost and time deltas, and a Pareto tab
   joins Diagram/XML on the chat cards.
8. **Remove `annotations` from tool schemas** (LLM-facing description
   only) once step 3 or 6 covers the annotation channel end-to-end.

Steps 1–3 are days of work and deliver a compelling demo. Steps 4–5 are
the real infrastructure. Steps 6–7 are the payoff. Step 8 is cleanup and
can wait for confidence.
