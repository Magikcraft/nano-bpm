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
