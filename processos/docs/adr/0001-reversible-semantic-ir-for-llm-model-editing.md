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
(`gateway-no-default` warning — it covers both the exclusive/XOR and inclusive/OR
condition-routed gateways) and instructs the model to fix the topology, but
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
`gateway-no-default` warning); the context-free half is what this ADR adds.

**The grammar is emitted from the engine surface, not hand-maintained — with one
precise caveat about what is auto-derived.** The engine's possibility space is a
closed algebraic data type — `ElementKind` (its variants and each variant's
fields), `SequenceFlow` (`condition`, `is_default`), boundary-event kinds,
multi-instance, IO mappings. Two layers must be distinguished:

- **Abstract syntax (the possibility space)** — which element kinds exist, which
  fields each carries, required vs optional, value types — is **fully derivable**
  from the ADT via `#[derive(schemars::JsonSchema)]`. An ADT maps onto an EBNF:
  variants → alternations, fields → attributes, `Option` → optional. Finite and
  enumerable because the enum is closed. Nothing here can drift or be omitted.
- **Concrete syntax (the surface notation)** — `service`, `->`, `[default]`,
  `when = …`, layout — is **not inventable** from the ADT: the type system knows a
  flow *has* an `is_default: bool` but has no opinion that it renders `[default]`.
  This is authored **once**, as a compact notation table.

So the grammar *document* is emitted programmatically, from two inputs — the ADT
(auto) + the authored notation table. The emitter walks the ADT **exhaustively**,
so the authored half only answers "how does *this* construct render," never
"which constructs exist." Coverage is machine-enforced: the pretty-printer and
parser are exhaustive `match`es over `ElementKind` (the Rust compiler refuses to
build if a new variant lacks a production), and a parity test asserts every
variant and field has a notation. The grammar cannot silently fall behind the
engine.

**Preferred architecture — one notation table, four consumers.** Define the
concrete syntax as a single declarative table `(ElementKind variant → production
template)` and drive from it: (1) the pretty-printer (model → IR), (2) the parser
(or the grammar it is checked against), (3) the human grammar cheat-sheet (the
tool result), (4) the GBNF grammar (constrained decoding). One spec, many
emitters — a parser generator whose spec also yields the printer and the docs, so
the four cannot disagree. Pragmatic fallback if that metacompiler is too much up
front: hand-write printer + parser, auto-derive the JsonSchema possibility space,
and parity-test the three for coverage — same anti-drift guarantee, less
machinery.

**The grammar does triple duty**, which is why emitting it from the engine
surface pays off disproportionately:

1. **Tool result** — `describe_ir_grammar` returns the productions, mapping the
   latent space for reasoning ("here is everything you can say").
2. **Parser / validator** — the same grammar drives parse-time compile errors on
   write.
3. **GBNF constrained decoding** — converted to a llama.cpp GBNF grammar, it
   constrains the sampler so a local model *cannot emit a token sequence that is
   not valid IR*. For a 4–8B local model this eliminates invalid-syntax failures
   at the decoding layer rather than catching them post-hoc.

#### Grammar delivery: an on-demand, scoped tool — not a per-turn dump

Sending the whole grammar every turn wastes context and degrades small-model
reasoning (the too-many-tools failure mode). Deliver it through three channels
matched to need:

- **Pull — `describe_ir_grammar(kind?)`.** On-demand. No argument returns the
  compact one-page cheat-sheet; `kind: "exclusiveGateway"` returns *only* that
  element's productions plus the flow annotations it can carry (`[default]`,
  `when = …`). Scoped retrieval keeps every result small — it solves both "don't
  send every turn" *and* "don't send too much even when asked." This tool is a
  static, model-independent language reference; `analyze_model` remains the
  model-*aware* half.
- **Pair with the analyzer.** A context-sensitive finding names the context-free
  entry to consult: the `gateway-no-default` warning points at
  `describe_ir_grammar(exclusiveGateway)`. The two maps compose exactly at the
  moment of need.
- **Push-on-error.** `write_model_ir` parse failures echo the relevant production
  inline, so a bad write teaches the syntax on the error path — no separate call
  needed to recover.

A subtlety that reinforces the three walls: **GBNF and the tool are complementary
even at decode time.** GBNF guarantees the model cannot emit *invalid* IR, but at
a gateway it exposes all legal continuations without signalling that a default
flow is an option *worth choosing*. Legality (CAN) is not knowing-the-option
(COULD). GBNF enforces *form*; the tool and analyzer still drive *choice*.

The grammar tells the model that default flows *exist*; the analyzer tells it
*which* gateway needs one; GBNF ensures whatever it writes is syntactically valid.

### Status (Nov 2026): shipped

The grammar surface described above is implemented in
[`processos/src/ir_spec.rs`](../../src/ir_spec.rs):

* **`ELEMENT_KIND_SPECS`** — the single declarative notation table (one entry
  per `ElementKind` variant, plus shared element-level extras and flow
  annotations). Adding a new engine variant surfaces as a compile error in the
  exhaustive matches in `model_ir.rs` and a parity-test failure here — both
  doors must be walked. Two failure modes, one source of truth.
* **`emit_gbnf()`** — renders the table to a llama.cpp-compatible GBNF for
  constrained decoding. Run `processos emit-gbnf --out ir.gbnf` and hand the
  file to `llama-server --grammar-file`, or POST as the `grammar` field on
  `/completion`. GBNF over-approximates attribute permutation (attr multisets)
  to stay out of O(k!) production explosion — the parser catches
  duplicate-attr / missing-required errors with a better message. 80 % win at
  1 % of the grammar complexity.
* **`describe()`** — the `describe_ir_grammar` investigator tool. No argument
  returns the compact overview (every keyword + one-line doc, syntax skeleton,
  shared extras, flow annotations). Passing `kind:"<keyword>"` returns just
  that kind's productions — cheap enough to send per turn.
* **Parity harness** — `specs_match_pretty_printer`,
  `shared_attrs_match_element_renderer`, `gbnf_covers_every_kind`,
  `describe_returns_scoped_payloads`. All 21 variants are round-tripped
  through the real `render_kind_attrs` / `render_element_attrs` so drift
  fails loudly.
* **Compile-time variant witness** — a `#[cfg(test)] fn variant_witness`
  in `ir_spec.rs` is an exhaustive `match` over `ElementKind`. Adding a
  variant to the engine surfaces as a compile error *in this file* (not
  just in `model_ir.rs`), dropping the author directly onto the checklist
  of sibling edits — SPECS entry, sample instance, engine matches. Closes
  the gap where someone could add a variant + handle it in the engine
  matches but silently omit the SPECS entry.

Not yet wired: threading `grammar` through `harness/llm.rs` so the write path
is grammar-constrained automatically. `emit-gbnf` is the manual escape hatch
in the meantime.

### Status update: the drafting pair — grammar as a leveller

The write path is now wired end to end. Three pieces landed on top of the
grammar surface above:

* **Per-call grammar in the harness.** `LlmConfig.grammar` /
  `LlmOverride.grammar` + `apply_grammar()` inject a GBNF into the specific
  request that should be constrained. Grammar is applied *per call*, never
  per-profile — a config-wide grammar would break the prose / tool-call turns.
  The IR grammar is checked into the repo at
  [`processos/assets/ir.gbnf`](../../assets/ir.gbnf) (drift-tested against
  `emit_gbnf()`) and served at `GET /api/ir/grammar.gbnf`.
* **`PairMode::Drafter`** ([`pairings.rs`](../../src/pairings.rs)). A first-class
  Pair AI mode: a small local sidecar paired to write model IR under grammar
  constraint. It is the *app-level* sibling of `Speculator` (engine-level
  speculative decoding) — the secondary contributes no prose and runs no tool
  loop. Selecting a Drafter pairing in the composer offers the primary a
  `draft_ir` tool.
* **`draft_ir` tool** ([`investigate.rs`](../../src/investigate.rs)). One
  grammar-constrained completion: load `ir_gbnf()` into the drafter's cfg, show
  it the current model (as IR) + the primary's plain-language instruction (and
  an optional `base` IR to revise), and return the drafted IR — already
  parse-checked via `model_ir::analyze_ir` (GBNF guarantees the *form*; the
  parser still catches missing-required-attr / unknown-ref slips). The primary
  reviews the result and deploys it with `write_model_ir`.

The insight the mode banks on: **the grammar is a leveller.** A 1.5B model with
GBNF emits syntactically valid IR as reliably as an 8B without — the grammar
carries the syntactic weight, so the small model spends its budget on the
*modelling*, not on remembering IR syntax. That makes a tiny local drafter a
credible partner to a larger local planner across one or two machines.

Deferred to a follow-up: a retry-with-diagnostic loop when `analyze_ir` rejects
a draft (currently the parse error is surfaced to the primary, which re-instructs);
and a "draft with grammar" button in the Semantics Workbench.

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
   `gateway-no-default` warning is *already* a desirability signal, not merely a
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
6. Engine-derived grammar: a single declarative notation table
   `(ElementKind variant → production template)` driving the printer, parser,
   grammar cheat-sheet, and GBNF; the possibility space auto-derived via
   `#[derive(JsonSchema)]`; coverage held by exhaustive-match + a parity test.
   Exposed as a **scoped** `describe_ir_grammar(kind?)` tool (on-demand, not a
   per-turn dump), with `analyze_model` warnings pointing at the relevant grammar
   entry and `write_model_ir` errors echoing productions inline.
7. Guided search (the desirability dimension): make the fitness explicit and
   tiered — static smells → replay score against the investigation goal — with a
   minimal-diff regularizer and the fitness delta surfaced per candidate. Reuses
   `analyze_model` and `compare_variants` / `replay_rank`.
