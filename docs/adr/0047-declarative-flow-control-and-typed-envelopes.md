# ADR 0047 — Declarative flow-control combinators and typed data envelopes

Status: **Proposed.**
Date: 2026-07-30.

Relates to:
ADR 0044 (`0044-code-first-durable-orchestration.md`, the `@nanobpm/workflow` SDK this extends),
ADR 0045 (`0045-code-first-workflows-rad-surface.md`, the RAD surface that hosts code-first flows),
ADR 0040 (`0040-fused-domain-model.md`, the `nano:shape` / data-envelope carrier this lifts into),
ADR 0033 §6 increment 12 (the server-side envelope scan that derives worker I/O),
and the code: `workflow/src/declarative.ts` (builder + graph compiler), `workflow/src/envelope.ts`
(typed envelope descriptor), `server/src/console/envelope_scan.rs` (the authoritative shape format),
`engine-core/src/model.rs:420-445` (XOR gateway + implicit-merge semantics the compiler targets).

## Context

ADR 0044 established `@nanobpm/workflow`, whose declarative surface (`defineFlow`) is — per the
2026-07-30 update — the **one true** code-first authoring surface (the imperative `defineWorkflow`
replay machinery is demoted to experimental/internal). But the shipped `defineFlow` emitted a
**strictly linear** chain: `Start → s1 → … → End`. It had `run` (locally-hosted service task),
`task` (external service task), and `signal` (durable message catch), and nothing else. It could not
express a **loop**, a **multi-way choice**, or a **guard** — so it could not model the real work the
single-user SDLC beachhead reaches for.

The forcing function is `urban-pr-review`'s `convergence-loop.bpmn`: a durable PR-review convergence
loop. Structurally it is **a loop wrapping a multi-way status switch with a nested max-rounds guard**,
with three edges looping back into the review step and two durable message catches
(`review-ready`, `escalation-answered`) correlated on `prKey`. Today that model is hand-authored BPMN;
the linear `defineFlow` cannot reproduce it. Code-first is only credible if it can express the loops
and conditionals real orchestrations need.

Second, ADR 0040's Fused Domain Model derives typed worker I/O from a **data-envelope carrier** in the
model (`nano:shape` declarations under `nano:shapes` on the process, plus
`io.nanobpm.dataEnvelope.in/out` `zeebe:property`s on service tasks and messages). Model-first authors
get typed I/O for free. Code-first authors, until now, did not — a `defineFlow` handler's `variables`
were untyped `JsonObject`, and the emitted `.bpmn` carried no shapes, so **ejecting a code-first flow
to the modeller lost its contracts**. There was a cliff between the two ends of the authoring axis.

## Decision

Two additions to the declarative surface, both compiling to primitives the engine already runs.

### 1. Control-flow combinators

Extend the flow from a flat step list to a **tree of nodes** (`FlowNode`), adding four structural
combinators alongside the three leaf activities:

- **`switch(subject, cases)`** — a multi-way exclusive choice. `subject` is a FEEL expression
  (usually a variable name); each case key is a value routed when `subject = value`. An optional
  `default` case is the unconditional fallback.
- **`branch(condition, { then, else? })`** — a two-way choice on a FEEL boolean. `then` is guarded by
  the condition; `else` is the unconditional fallback (omit it to skip to whatever follows).
- **`loop(body)`** — a durable back-edge. The body runs, then control returns to the top of the loop
  unless a branch calls `break()`.
- **`break()` / `continue()`** — exit the enclosing loop (routes to what follows it), or jump straight
  back to the loop head. Both are rejected at authoring time outside a `loop`.

Each compiles to BPMN the engine natively executes:

| Combinator | Compiles to |
| --- | --- |
| `switch` | one `exclusiveGateway`; a conditional `sequenceFlow` (`=subject = "value"`) per case; the default case is the gateway's unconditional `default` flow |
| `branch` | one `exclusiveGateway`; the `then` flow carries the `=condition`; the `else` flow is the gateway `default` |
| `loop` | a convergent `exclusiveGateway` (the **loop head**) that is a stable target for back-edges; the body's normal fall-through connects **back** to the head |
| `continue` | connects the current edges straight to the loop head |
| `break` | re-homes the current edges to the loop's **exit** danglers (they become sequence flows leaving the loop) |

This works because `engine-core` evaluates a gateway's outgoing conditions **in order, first match
wins**, treats an unconditional flow as the `default`, and treats **multiple incoming flows on a node
as an implicit XOR merge** (`engine-core/src/model.rs:420-445`). The loop head is exactly such an
implicit merge: `Start`, `continue`, and each back-edge all target it.

**Determinism is a non-issue.** These loops are *static BPMN back-edges* — the same construct the
hand-authored `convergence-loop.bpmn` uses — compiled once at author time, not the imperative
replay surface. There is no journal-ordinal discipline to respect; the engine runs the static graph.

The compiler is a **two-phase graph builder**: phase one walks the node tree emitting render-nodes and
pre-allocated edges (`emitNode(node, incoming, loop) → dangling out-edges`), back-patching gateway
`default`s and re-homing `break`/`continue`; phase two drops any edge that never got a target and
renders nodes + sequence flows. See `workflow/src/declarative.ts` (`class Compiler`).

### 2. Typed data envelopes, lifted into the model

Add an `envelope(name, fields)` descriptor (`workflow/src/envelope.ts`) that declares a **named, typed
payload contract in code** and carries two things at once:

- a **runtime schema** (ordered fields + scalar types: `string | integer | number | boolean |
  datetime`, each optionally `optional` / `list`), and
- a **phantom TypeScript type** inferred from the field spec, so handlers and payloads are statically
  typed at the call site (`typeof env.type` in type positions).

Envelopes are attached to a flow through a **contracts map keyed by step name**, passed as the second
argument to `defineFlow`:

```ts
const convergence = defineFlow(
  "convergence-loop",
  {
    "review-round": { in: ReviewRoundIn, out: ReviewRoundOut },
    "persist-round": { out: RoundState },
    "wait-review":  { in: ReviewReady },
  },
  (w) => {
    w.loop((b) => {
      b.run("review-round", async (job) => {
        // job.variables is typed ReviewRoundIn.type; the return is ReviewRoundOut.type
        return { status: classify(job.variables) };
      });
      b.switch("status", { /* … */ });
    });
  },
);
```

`run<K>(name, handler)` resolves the string-literal `name` against the contracts map: if `name` is a
key, `job.variables` is typed from that contract's `in` and the return from its `out`; otherwise both
fall back to `JsonObject`. The same map types `signal` payloads (from the contract `in`). This keyed
factory (rather than inline `{ in, out }` per call) is DRY and mirrors the console's generated
`worker-sdk.ts` pattern — the contract is declared once and reused wherever the name appears.

The compiler **lifts** every referenced envelope into the emitted model, exactly as ADR 0040's carrier
expects:

- each envelope becomes a `<nano:shape id="Name"><nano:extend name= type= optional= list= />…</nano:shape>`
  under `<nano:shapes>` on the process extension elements;
- each typed service task carries `io.nanobpm.dataEnvelope.in/out` `zeebe:property`s;
- each typed `signal` message carries the same `dataEnvelope.in` property;
- the `xmlns:nano="https://nanobpm.io/schema/shapes/1.0"` namespace is declared.

Only envelopes actually referenced by a step are lifted (unused ones never appear). The same envelope
name referenced with two different field sets is a compile-time (emit-time) error.

**This closes the eject-to-model-first cliff.** A code-first flow now emits the *same* typed carrier a
model-first author would draw. Opening the generated `.bpmn` in the modeller loses nothing: the shapes,
the envelope wiring, and thus the Fused Domain Model projection (ADR 0040) are all present. Code-first
and model-first are genuinely two ends of one axis with no discontinuity between them.

## Consequences

- **`urban-pr-review` is now expressible in code-first.** The `emit.test.ts` "urban golden" test
  reproduces the loop + status switch + nested max-rounds guard + two correlated durable waits + typed
  review-round I/O, and registers every `run` step as a hosted worker type. The linear-only SDK could
  not do this.
- **The public type surface changed** (generic `FlowBuilder<C>`, `Job<V>`, the contracts-map
  `defineFlow` overload) and the API is additive: the 2-arg `defineFlow(id, build)` form still works
  (empty contracts, untyped), so existing flows and the scaffold (`WORKFLOW_*` in
  `server/src/console/projects.rs`) are unaffected. This warrants a **minor version bump**
  (`@nanobpm/workflow` `0.2.x → 0.3.0`).
- **A `switch` with no `default`** still emits a synthesised unconditional fall-through edge, so a
  subject matching no case never deadlocks — it falls through to whatever follows the switch (or End).
- **Shared catch events are not yet expressible.** Two `signal` calls with the same name are rejected
  (names must be unique), so a single catch event reached from two branches (as `escalation-answered`
  is in the hand-authored urban model) must be authored as two distinct waits, or the shared wait
  hoisted after the choice. A future `join`/shared-node primitive can lift this; it is out of scope
  here.
- **Variable mutation is via handler results.** A loop that advances a counter (e.g. `persist-round`
  returning `{ round: round + 1 }`) mutates process variables through the step's `out` payload, which
  the engine merges — there is no separate assignment primitive. This keeps the surface small and
  matches how the engine already threads variables.

## Alternatives considered

- **Inline `{ in, out }` per `run`/`task` call** instead of a contracts map. Rejected: it repeats the
  contract at every reference of a step and does not match the console's generated-SDK ergonomics.
- **A structured expression AST** for conditions instead of raw FEEL strings. Deferred: FEEL strings
  match the proven imperative emitter and the engine's native condition language; a typed expression
  builder can layer on later without changing the compiled output.
- **Keeping code-first untyped** and relying on the modeller to add shapes after ejection. Rejected:
  that *is* the cliff this ADR removes — the whole point is that ejection loses nothing.
