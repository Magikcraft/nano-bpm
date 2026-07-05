# ADR 0001 — A reversible semantic IR for LLM model editing

Status: **Proposed.**
Date: 2026-07-05.
Relates to: `processos/src/bpmn_model.rs` (`definition_to_xml`, `edit_model`,
`apply_edit_op`, `node_view`, `analyze_model`, `validate_model`,
`inline_definition`), `processos/src/investigate.rs` (the `read_model` /
`read_model_xml` / `analyze_model` / `edit_model` tool specs + dispatch),
`engine-core/src/bpmn.rs` (`parse_bpmn`), `engine-core/src/model.rs`
(`ProcessDefinition`, `ElementKind`, `SequenceFlow`), and the replay harness
(`processos/src/harness/replay.rs`, `processos/src/experiment.rs`). This is the
first ADR housed in the `processos` crate.

## Context

The investigator LLM can inspect a process model (`read_model` emits a compact
JSON projection; `read_model_xml` the raw BPMN) and *mutate* it through
`edit_model`: a fixed catalogue of imperative, hand-written operations —
`set_task_job_type`, `insert_service_task_after`, `add_error_boundary`,
`remove_node`, `reroute_flow`, `set_flow_condition`, `add_exclusive_gateway`.
Each op is a bespoke `match` arm in `apply_edit_op` that owns the XML-level
correctness of its change.

This surface does not scale, and a concrete failure exposed why. A model tried to
mark a sequence flow as an exclusive gateway's **default flow** so a token has
somewhere to go when no condition matches. It could not — and the engine
*executes* default flows perfectly (`engine-core/src/bpmn.rs` parses
`default="flowId"` into `Node.default_flow`; `SequenceFlow.is_default` drives
`connect_default`). The gap is entirely in the **authoring layer**:

1. **No op sets it.** `edit_model` has no verb to mark a flow default; every
   construction site in `apply_edit_op` hardcodes `is_default: false`.
2. **The serializer drops it.** `definition_to_xml` emits
   `<bpmn:exclusiveGateway id="…"/>` with no `default="…"` attribute and ignores
   `SequenceFlow.is_default`, so even a flagged flow would lose its default on the
   round-trip to engine-parseable XML — the exact "tried but was unable to"
   symptom.

The irony: `analyze_model` *detects* the missing default
(`exclusive-no-default` warning) and instructs the model to fix the topology, but
hands it no tool to do so. The model is diagnosed into a dead end.

Generalising: every new BPMN capability we want the LLM to author demands (a) a
new imperative op, (b) its serializer support, and (c) teaching the model one
more verb. This is a combinatorial treadmill. We want the LLM to reason about the
model *generally* and express modifications at a high level, without us
enumerating every atomic mutation.

## Decision

Introduce a **reversible "compilation" between BPMN and a canonical, textual
semantic IR**, and let the LLM edit the IR as text rather than invoke imperative
ops.

The pivotal reframe: **the IR is the surface syntax of the engine's own
executable model (`ProcessDefinition`), not of the BPMN XML.** BPMN-the-XML is
large, namespace-noisy, and non-local (flows declared apart from nodes, diagram
interchange in a separate plane). BPMN-the-executable-subset — what the engine
runs — is small and regular, and Nano already has it as `ProcessDefinition` /
`ElementKind` / `SequenceFlow`.

### The compiler

The pipeline is a compiler whose two BPMN-facing ends already exist and are both
anchored to the engine's own model, so the IR **cannot drift from execution
semantics** (the same guarantee `bpmn_model.rs` already relies on):

| Stage | Direction | Status |
|-------|-----------|--------|
| Front end `parse_bpmn` | BPMN XML → `ProcessDefinition` | **exists** (engine) |
| Pretty-printer | `ProcessDefinition` (+ annotations) → IR text | new |
| Parser | IR text → `ProcessDefinition` | new — the core work |
| Back end `definition_to_xml` | `ProcessDefinition` → BPMN XML | **exists** |

`node_view` is the lossy read-only seed of the pretty-printer;
`analyze_model` / `validate_model` become the parser's type-checker.

### What "reversible" means — a lens, not XML-lossless

We do **not** aim for byte-identical XML round-trips. We aim for **semantic**
reversibility over the executable subset. Formally this is a **lens** between
BPMN and IR, and "deterministic in both directions" is the lens laws:

- **Executable core — a bijection** (the achievable, correct target):

  `semantics(xml) == semantics(toXml(parse(emit(fromXml(xml)))))`.

- **Everything the engine ignores** (diagram interchange / layout, cosmetic XML)
  is the **complement** the lens preserves or regenerates. Diagram interchange
  (DI) is kept as an opaque sidecar keyed by element id, re-attached for
  unchanged nodes and auto-generated for new ones (Nano already auto-lays-out via
  the `route_avoiding` router in `definition_to_xml`).

The boundary is elegant: the IR is lossless **exactly over what matters**
(execution) and lossy **exactly over what does not** (layout).

### Grammar requirements

1. **Canonical / normal form.** Guarantee `emit(parse(ir)) == ir` via stable
   ordering and canonical whitespace, so every edit re-normalises and diffs stay
   minimal (the gofmt/rustfmt idempotence property). This gives the LLM a stable
   target.
2. **Structural edges.** Flows are explicit `a -> b` edges with inline
   annotations. Default flow becomes grammar, not an op:

   ```
   review_gw -> auto_approve   when = amount < 1000
   review_gw -> manual_review  [default]
   ```

   The original failure dissolves at the pretty-printer with zero new ops.
3. **Carry human annotations even though the engine ignores them.** Names and
   documentation are the LLM's semantic anchors; dropping them on round-trip
   would gut reasoning. So the IR model is a slight *superset* of the executable
   core: executable semantics **+ preserved annotations**. FEEL conditions ride
   as opaque verbatim strings — preserved, never parsed.
4. **Validate on parse.** Reuse the existing static checks inside the parser:
   unique ids, every flow endpoint exists, an exclusive gateway's default flow is
   unconditional, service tasks have a job type, boundary `attachedTo` exists.
   Violations return as **compile errors** to the LLM — the typed-op API's safety,
   centralised, for free.

### The reversibility oracle (why this is more than a nice idea)

We already own an empirical proof of semantic reversibility: the **replay
harness**. On the real customer corpus we can assert, for each model `M`:

```
simulate(M) == simulate( toXml(parse(emit(fromXml(M)))) )
```

Identical replay outcomes mean the round-trip preserved everything that matters,
by construction — a far stronger signal than unit tests, and a CI gate for the
IR compiler.

### Mapping the possibility space: an engine-derived grammar

Rendering a model to IR shows the LLM a *sample* of the language, not its
*extent*. Reasoning from samples alone, a model can only recombine constructs it
has seen — so if no rendered model happens to contain a default flow, the model
may never invent one. That is precisely the default-flow failure that motivated
this ADR. The rendered model is one coordinate in the space; the **grammar is the
map of the space**, and it must be handed to the LLM explicitly.

Two complementary maps are required, and the default-flow case needs both:

| Map | Question it answers | Source |
|-----|--------------------|--------|
| **Context-free grammar** | "What *can* be expressed anywhere?" (default flows exist; here is the syntax) | derived from the engine's type surface |
| **Context-sensitive affordances** | "What is missing / available *here*?" (this gateway's branches are all conditional and it has no default) | `analyze_model` — already exists |

The LLM composes them: the grammar says `[default]` is a legal edge annotation;
`analyze_model` says *"review_gw has no default and every branch is
conditional"*; the model writes `review_gw -> manual_review [default]`. Neither
map alone suffices — the context-sensitive half already exists (the
`exclusive-no-default` warning); the context-free half is what this ADR adds.

**The grammar is generated from the engine surface, not hand-maintained.** The
engine's possibility space is a closed algebraic data type — `ElementKind` (its
variants and each variant's fields), `SequenceFlow` (`condition`, `is_default`),
boundary-event kinds, multi-instance, IO mappings. An ADT maps directly onto an
EBNF: variants → alternations, fields → attributes, `Option` → optional. The
space is finite and enumerable because the enum is closed. The single-source-of-
truth discipline (the same anti-drift guarantee the IR itself relies on):

- Derive a machine schema from the IR types (`#[derive(schemars::JsonSchema)]`)
  → exhaustive by construction; this is what the grammar tool returns.
- The **concrete notation** (keywords, layout) is authored once — an ADT yields
  *abstract* syntax, not surface syntax. What is derived-and-checked is
  **coverage**: the pretty-printer and parser are exhaustive `match`es over
  `ElementKind`, so the Rust compiler refuses to build if a new variant is added
  without a production. A parity test asserts every variant and field has a
  notation. The grammar therefore cannot silently fall behind the engine.

**The grammar does triple duty**, which is why generating it from the engine
surface pays off disproportionately:

1. **Tool result** — `describe_ir_grammar` returns the productions, mapping the
   latent space for reasoning ("here is everything you can say").
2. **Parser / validator** — the same grammar drives parse-time compile errors on
   write.
3. **GBNF constrained decoding** — converted to a llama.cpp GBNF grammar, it
   constrains the sampler so a local model *cannot emit a token sequence that is
   not valid IR*. For a 4–8B local model this eliminates invalid-syntax failures
   at the decoding layer rather than catching them post-hoc.

Caveat — **tier the grammar to protect small-model context.** A large grammar
dumped every turn degrades tool-selection and reasoning the same way too many
tools do. The default tool result is a compact one-page production cheat-sheet;
`analyze_model` does the contextual narrowing. The grammar tells the model that
default flows *exist*; the analyzer tells it *which* gateway needs one.

### The desirability dimension: guided search against an externalized objective

Legality and visibility (the two grammar maps above) constrain and illuminate the
space of valid models. They do not tell the LLM which move through that space is
an *improvement*. That is a third, orthogonal dimension. Three walls:

| Dimension | Question | Mechanism |
|-----------|----------|-----------|
| **CAN** (legality) | Is the edit valid? | grammar / GBNF / parser |
| **COULD** (extent) | What is expressible, and what is available here? | grammar-as-tool + `analyze_model` |
| **SHOULD** (desirability) | Which edit makes the model *better*? | an explicit objective function |

The design position: **do not encode desirability as an LLM prior; externalize it
as a measurable objective and structure the interaction as guided search.** The
grammar-constrained LLM is the *proposal distribution* (a smart, always-legal
mutation operator); a separate *evaluator* scores candidates; the LLM climbs the
gradient. The intelligence lives in the loop, not in a weak local model's priors —
which is what makes it robust on a 4–8B local model.

**"Better" is only defined relative to a stated objective.** There is no universal
"better BPMN"; a default flow is desirable only relative to a goal. The system
should *require or elicit* an objective rather than assume one. Given one,
desirability stratifies into a tiered fitness function — cheap to expensive,
mirroring the existing `limit:1 → 25 → full` replay sampling:

1. **Static well-formedness (cheap, ms).** `analyze_model` smell count. The
   `exclusive-no-default` warning is *already* a desirability signal, not merely a
   "you could": all-conditional branches with no default is a **liveness defect** —
   a token can get stuck. Adding the default *eliminates a defect*. Reducing
   warnings is a coarse monotone gradient, but a proxy: a model can be
   warning-free and still wrong.
2. **Behavioral fit — the replay oracle (expensive, the true gradient).** The
   signal ProcessOS uniquely owns, and the reason tier-2 capture exists. "Better"
   becomes *measurable*: replay a candidate against recorded stimuli and score it
   — does it reproduce observed outcomes and eliminate the divergence/incident
   that opened the investigation? `compare_variants` / `replay_rank`
   (`RankedCandidate`, confidence bands, divergent-vs-structural) is already this
   evaluator. Desirability = fit-to-reality + fault-elimination, empirically, not
   by prior.
3. **The investigation's goal (the target).** The loss is measured against intent
   ("loans over $X auto-approve when they should route to manual review"). Without
   a stated goal, dimension 3 collapses back to smell reduction.

**The LLM is a mutation operator, not an oracle.** This reframes model editing as
generate-and-test / guided search: the grammar-constrained LLM proposes legal
candidates, the harness scores them, the LLM climbs. ProcessOS already owns the
test half; the IR + grammar make the generate half tractable and legal, closing
the loop.

**Two dangers require regularizers:**

- **Objective-hacking / corpus overfitting.** If fitness is only "reproduce
  recorded traces," the LLM will memorize a finite corpus — special-case
  conditions that pass replay but generalize badly. Guard with: **minimal semantic
  diff** (surgical change, not a rewrite — this recovers the imperative API's one
  virtue, blast-radius containment, as a *regularizer*), a **held-out replay
  split** (train/validate on the trace corpus), and **preferring structural fixes
  over instance patches** — which replay already distinguishes
  (`requiresNewWorkers`/mock vs `structuralDivergence`/topology). Structural =
  general = more desirable.
- **"Better" is not scalar.** Fit-to-reality, fault-elimination, parsimony, and
  closeness-to-original trade off. Surface a **fitness vector / Pareto front**
  rather than letting the LLM silently collapse it; parsimony and minimal-diff are
  the regularizers that hold overfitting in check.

**Desirability replay cannot measure** — readability, naming, convention
alignment — needs a *normative* oracle: a best-practice lint catalog (a BPMN
"clippy": unnamed elements, single-flow gateways, implicit splits). It is
prior-based, so it belongs in an explicit, auditable, tunable rule set, never in
the model's head.

**The default-flow case, restated as desirability:** it is desirable when
*demonstrated* to be, via the fitness delta — *"adding `review_gw ->
manual_review [default]` removes 1 liveness warning, makes replay reproduce 3
previously-divergent instances, and is a 1-line diff."* Desirability is
**explained, not asserted**: the semantic diff plus the measured fitness change,
surfaced for the operator to accept or reject. For a reasoning/advisory agent the
human is the final objective, so making the *why* legible is part of the design.

Open concerns (carried, not yet settled): how to weight the multi-objective
vector; how to split a small customer corpus for held-out validation without
starving either side; and whether the normative lint catalog is authored,
learned, or both.

## Consequences

- **The per-op treadmill ends.** The LLM's loop becomes the read-modify-write
  loop it already runs on code: read IR → reason about the graph → write IR (or a
  patch) → compiler validates → deploy or return errors → iterate. A new BPMN
  feature is extended **once** in the grammar + the two mappings, and every
  mutation over it is then expressible.
- **Safety moves from vocabulary restriction to validation + diff + oracle.** The
  imperative API's one virtue was that a constrained op cannot touch what you did
  not name; a free-form IR rewrite can accidentally restructure untouched
  regions. We regain safety by surfacing a **semantic diff** before deploy and
  letting **replay** catch regressions — the right trade for a reasoning agent.
  For tighter blast radius on large models, add **one** generic textual-patch
  mechanism over the IR (replace-block-for-node / add-edge) — still general,
  still zero per-semantic-op code.
- **Scope is bounded; ~60% exists.** Reused: `parse_bpmn`, `definition_to_xml`,
  `node_view`, `analyze_model` / `validate_model`, the replay harness. New:
  (a) a canonical text pretty-printer, (b) a total, validating IR parser,
  (c) a DI-preservation sidecar.
- **Fidelity ceiling = the executable model's ceiling.** Anything
  `ProcessDefinition` does not capture, the IR cannot preserve — but anything the
  engine does not execute, the round-trip is *allowed* to drop. Scope the IR to a
  single executable process plus its called elements initially (via
  `inline_definition`), and be explicit that multi-pool / collaboration /
  message-flow constructs the engine does not run are out of scope.

## Alternatives considered

- **Keep adding imperative ops** (e.g. a `set_default_flow` op plus serializer
  support). Solves the immediate default-flow gap in ~1 op + 1 serializer line,
  but is the treadmill this ADR exists to escape. May still ship as a stopgap.
- **Let the LLM edit raw BPMN XML** (`read_model_xml` already exposes it).
  Rejected as the primary path: XML is verbose, namespace-noisy, non-local, and
  small syntactic slips yield invalid documents. The IR's entire value is raising
  signal-to-noise — LLMs make far fewer errors editing a ~30-line DSL than
  ~300 lines of XML.
- **A brand-new IR unrelated to `ProcessDefinition`.** Rejected: it would risk
  drifting from execution semantics. Anchoring the IR to the engine's own model
  is what makes reversibility provable.

## Rollout (phased)

1. Canonical pretty-printer `ProcessDefinition (+ annotations) → IR`.
2. Total, validating parser `IR → ProcessDefinition` (reusing `analyze_model`).
3. Corpus replay property test: `fromXml → emit → parse → toXml` preserves
   `simulate()` outcomes across the recorded customer corpus.
4. Expose `read_model_ir` (emit) and `write_model_ir` (parse + validate +
   deploy), optionally a generic `patch_model_ir`. Retire the `edit_model` verbs
   (or keep them as sugar over the IR).
5. DI-preservation sidecar so hand-laid-out customer diagrams survive a
   round-trip.
6. Engine-derived grammar: `#[derive(JsonSchema)]` on the IR types + a coverage
   parity test, exposed as a `describe_ir_grammar` tool result and a GBNF grammar
   for llama.cpp constrained decoding.
7. Guided search (the desirability dimension): make the fitness explicit and
   tiered — static smells → replay score against the investigation goal — with a
   minimal-diff regularizer and the fitness delta surfaced per candidate. Reuses
   `analyze_model` and `compare_variants` / `replay_rank`.
