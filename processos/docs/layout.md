# Semantic BPMN layout — annotation schema

Processos can lay out a BPMN model using *semantic annotations* about which
nodes belong to the happy path, which handle errors, which escalate, and which
compensate. This document describes the JSON shape the layout engine consumes.
It doubles as the reference an LLM sees when asked to generate annotations for
a model.

## Overall shape

```jsonc
{
  "flows":    [ /* array of AnnotatedFlow */ ],
  "clusters": [ /* array of Cluster        */ ],
  "roles":    { /* map of node_id -> Role  */ }
}
```

All three fields are optional. An empty `{}` is a valid document (and results
in the default non-semantic auto-layout).

## Flows

A **flow** is a named sequence of node ids the LLM claims belong to one
narrative thread. Each flow has a `kind` that picks the horizontal band its
members are attracted to:

| `kind`        | Where it lays out                     | Typical use |
|---------------|---------------------------------------|-------------|
| `primary`     | Horizontal centerline                 | The happy path |
| `exception`   | Below the centerline                  | Error handling, retries |
| `escalation`  | Above the centerline                  | Supervisor notifications, out-of-band alerts |
| `compensation`| Further below (own band)              | Compensating actions (undo side effects) |

```jsonc
{
  "flows": [
    { "id": "happy",  "kind": "primary",    "nodes": ["Start_1", "TaskA", "GwCheck", "TaskB", "End_ok"] },
    { "id": "on-err", "kind": "exception",  "nodes": ["HandleError", "End_err"] },
    { "id": "alert",  "kind": "escalation", "nodes": ["Notify_Manager"] }
  ]
}
```

**When a node appears in multiple flows**, the higher-priority kind wins:
`exception > escalation > compensation > primary`. Rationale: if a node is
technically on the happy path but *also* how you handle a specific error, the
diagram is more useful with the node off the centerline.

**A node not mentioned in any flow** keeps its default position (predecessor-
mean row) — you don't need to enumerate every node.

## Clusters

A **cluster** is a group of nodes you want to see visually together. The layout
engine pulls cluster members toward their shared centroid with a stiffness of
`affinity` (0..1, default 0.5).

```jsonc
{
  "clusters": [
    { "id": "validation", "nodes": ["ValidateInput", "CheckLimits"], "affinity": 0.8 }
  ]
}
```

Cluster attraction happens *after* the flow band assignment, so a cluster of
mixed-band nodes will pull them toward the midpoint of their bands. Use
sparingly — most models don't need clusters.

## Roles (reserved)

Per-node presentation hints. The v0 solver ignores these; the debug SVG uses
them to colour-code node fill.

```jsonc
{ "roles": { "GwCheck": "decision", "TaskReview": "review" } }
```

Valid roles: `decision`, `review`, `notification`, `compensation`, `external`.

## Guidance for LLM annotators

When asked to annotate a BPMN model:

1. **Read the model first** — identify the start event, follow the "no
   conditions / default branch" edges through to an end event. That's your
   `primary` flow.
2. **Follow non-default branches** — a `default` sequenceFlow marks the happy
   branch of an exclusive gateway; the *other* outgoing flows are usually
   `exception` (if they end in an error-handling task) or `escalation` (if they
   notify without changing the outcome).
3. **Boundary events**: an error boundary event's downstream nodes are
   `exception`; a timer/signal escalation boundary's are `escalation`.
4. **Compensation tasks** (marked with `<bpmn:compensateEventDefinition>` or a
   `<bpmn:isForCompensation="true"/>` task) go in a `compensation` flow.
5. **Include every node in the flow it belongs to** — even the start and end
   events. This is what pulls the whole path onto its band; if you only annotate
   the "interesting" middle nodes, the endpoints drift back to the centerline
   and the visual band breaks.
6. **Cluster only when it clearly reads better** — most models don't need it.
   A good candidate is a subflow of 3+ related tasks (a validation block, a
   payment block) that lay out badly by default.

## Running the layout

```
processos layout model.bpmn model.annotations.json \
    --out model.laid-out.bpmn \
    --debug-svg model.debug.svg
```

Open `model.debug.svg` in any browser or SVG viewer — bands are colour-coded,
cluster hulls are dashed purple boxes, node fills reflect the winning flow
kind. If the layout doesn't match the annotation intent, the schema is the
first place to look before touching the solver.

---

## Slice 15 — Layout quality tools (`conformance`, `gates`, `polish`, `guarded`)

Slice 15 ports four patterns from the [`camunda-consulting/bpmn-layout-bakeoff`](https://github.com/camunda-consulting/bpmn-layout-bakeoff) project so ProcessOS can *measure* what its solvers produce, not just eyeball it. All four live under [`processos::layout`](../src/layout/) and are exposed through two HTTP endpoints.

### The conformance score

[`layout::conformance::score`](../src/layout/conformance.rs) computes a weighted mean of 8 rules taken from the BPMN drawing literature (Silver *Method & Style*, 7PMG, Camunda / bpmn.io, Trisotech, Purchase, Effinger). Each rule returns a value in `[0, 1]`; the overall score is a weighted average using `RULE_WEIGHTS` (which sums to 1.0):

| Rule | Weight | Source | What it measures |
|---|---:|---|---|
| `flowDirection` | 0.22 | Silver M&S; Camunda; Effinger | % of sequence flows progressing left→right |
| `happyPathStraight` | 0.18 | Camunda; Trisotech | % of happy-path nodes within ½ row of the mean baseline |
| `gatewayAlignment` | 0.16 | Camunda; Purchase | split/join gateway y vs mean(branch y) |
| `flowOrientation` | 0.14 | Trisotech; Purchase | % of edge segments that are axis-aligned |
| `labelClearance` | 0.12 | Silver M&S; Trisotech | % of sequence flows that don't cut through unrelated nodes |
| `portDirection` | 0.08 | Camunda | % of edges that leave source right/bottom and enter target left/top |
| `artifactClearance` | 0.06 | Silver M&S | boundary events sit on their host, not on unrelated shapes |
| `canonicalSizing` | 0.04 | Camunda | tasks (100×80), events (36×36), gateways (50×50) at their canonical sizes |

### The correctness gates

[`layout::gates::evaluate`](../src/layout/gates.rs) implements two hard gates that catch broken layouts (`GATES` in the bake-off's `config/default.config.js`):

- **Coverage** — `drawn / declared` shape ratio; must be ≥ 0.95. Stops a solver from "winning" the conformance / crossings competition by simply drawing fewer elements.
- **Overlap ratio** — sum of pairwise overlap area between leaf nodes divided by the total leaf area; must be ≤ 0.08. Stops a solver from winning by piling shapes on top of each other.

When either gate fails, `gates.passed = false` and `gates.penalty = 0.35` — external ranking code should multiply the composite quality score by this to enforce the gate.

### The deterministic polish pass

[`layout::polish::polish`](../src/layout/polish.rs) runs after a solver produces its BPMN and nudges the layout toward the conformance rules by:

1. **Gateway centring** — moves each split/join gateway to the mean y of its branches.
2. **Grid-snap** — snaps movable node origins to a 10-unit grid so columns and rows read tidily.
3. **Happy-path align** — pulls all happy-path leaves toward a shared baseline y.

**Non-regressing guarantee.** Every candidate is scored by `conformance::score` and `gates::overlap_ratio` before being accepted; only the *first* candidate that improves-or-holds both is shipped. When both candidates regress, the base layout is returned untouched and `polish.reverted = true`. This mirrors the bake-off's `+polish` variant — polish is never worse than base.

### The crash-guarded engine wrapper

[`layout::guarded::run`](../src/layout/guarded.rs) wraps [`layout::layout_with`](../src/layout/mod.rs) with `std::panic::catch_unwind` + a coverage post-condition, so an experimental solver never takes down the request:

1. Run the requested solver. If it returns and coverage ≥ `MIN_COVERAGE`, return its output.
2. Otherwise, retry with `Solver::RowBias` (the deterministic baseline).
3. If even RowBias fails, surface the error.

The response includes `used_solver` (which solver was actually shipped) and `fallback` (the reason for downgrading, if any).

### HTTP surface

#### `POST /api/layout/score`

Score any BPMN document without modifying it. Useful for comparing renderer outputs (Nano's RowBias vs Field, or Nano vs a bpmn-js ELK-authored layout) objectively.

**Testing it against the tiny fixture:**

```bash
XML=$(cat processos/fixtures/layout/tiny.bpmn)
curl -sS -X POST http://localhost:8090/api/layout/score \
  -H 'content-type: application/json' \
  -d "$(jq -n --arg xml "$XML" '{xml: $xml}')" | jq .
```

Expected response shape:

```json
{
  "conformance": {
    "score": 0.75,
    "rules": [
      { "key": "flowDirection", "label": "Left-to-right flow",
        "weight": 0.22, "score": 1.0,
        "source": "Silver M&S; Camunda; Effinger" },
      "…"
    ]
  },
  "gates": {
    "coverage": 1.0, "overlap_ratio": 0.0,
    "passed": true, "penalty": 1.0, "failures": []
  },
  "declared_shapes": 5,
  "drawn_shapes": 5
}
```

#### `POST /api/layout` — extended

The existing renderer endpoint now accepts `"polish": true` and returns `conformance`, `gates`, `polish`, `used_solver`, and `fallback` alongside the BPMN XML:

```bash
XML=$(cat processos/fixtures/layout/tiny.bpmn)
curl -sS -X POST http://localhost:8090/api/layout \
  -H 'content-type: application/json' \
  -d "$(jq -n --arg xml "$XML" '{xml: $xml, solver: "rowbias", polish: true}')" \
  | jq '{used_solver, fallback, polish, conformance: .conformance.score, gates: .gates.passed}'
```

Expected:

```json
{
  "used_solver": "rowbias",
  "fallback": null,
  "polish": { "moves": 0, "aligns": 0, "reverted": false, "candidate": 0 },
  "conformance": 0.75,
  "gates": true
}
```

**Fallback in action.** If the Field solver panics on some pathological input, you'll see:

```json
{
  "used_solver": "rowbias",
  "fallback": { "Panic": { "requested": "field", "message": "…" } },
  "…": "…"
}
```

The response still contains a valid BPMN — the operator can see the layout and knows the experimental solver silently downgraded.

### What to test manually

After `make release` (or `cargo run --release --bin processos`), from a checkout root:

1. **Score endpoint** — the curl above against `tiny.bpmn` should return a conformance score around 0.75 and all gates passing.
2. **Polish improves a wobbly happy path** — pick a Northwind Bank model in the Semantics Workbench, POST it to `/api/layout` with `"polish": true`, and compare `conformance.score` with/without polish. `polish.reverted` should be false on well-behaved inputs.
3. **Guarded fallback** — force a Field panic (e.g. by handing an empty process definition), POST to `/api/layout` with `"solver": "field"`, and confirm the response has `used_solver: "rowbias"` and a populated `fallback` block instead of a 500.
4. **Semantics Workbench renderer parity** — the browser bpmn-js layout can be scored against Nano's by POSTing the bpmn-js-authored XML to `/api/layout/score` — this is the missing piece for the round-trip iteration loop we've been building toward.
