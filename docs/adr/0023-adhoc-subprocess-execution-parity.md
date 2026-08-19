# ADR 0023 — Ad-hoc sub-process execution parity (agentic Tier-1)

Status: **Accepted; implementing.** Seams 1–3 (fixtures, model catalog, result
plumbing) and seam 2 (runtime activate/loop/complete) shipped in PRs #142/#146;
seam 5 (read model + metrics) is implemented — each tool activation is a
read-model element instance and the loop is metered via
`nanobpm_adhoc_events_total{kind=…}` (see `PERFORMANCE.md`). Seam 4 is
implemented for v1: the engine evaluates a declared `<completionCondition>` after
each tool completes (and honors the agent's `isCompletionConditionFulfilled`
flag), and applies each activated tool's `zeebe:ioMapping` (inputs on activation,
outputs projected into the container scope) — the pruned tool's mappings are
carried on the container catalog. Implemented for v1.1: the declarative `BPMN_TASK`
`activeElementsCollection` variant (an agent-less execution mode — see §Subset).
Remaining: the seam 6
E2E parity test.
Date: 2026-07-21.
Relates to: ADR 0022 (`0022-nano-rad-application.md` §E.1 — the parity strategy this ADR makes
concrete; **Tier-1** of that tier ladder), ADR 0005 (`0005-embedded-u-nano.md`, Bernd — the
embedded target the unmodified connector must run on), ADR 0016 (`0016-falcon-protocol.md`, the
job-completion transport that must carry the new result fields), and the engine hot path:
`engine-core/src/bpmn.rs` (`is_adhoc`, today's collapse+prune), `engine-core/src/{model,state,command,event}.rs`,
`engine-core/src/engine/mod.rs`, `server/src/main.rs` (`ActivatedJobResult`, complete-job REST),
`server/src/falcon.rs`. Camunda reference (`~/workspace/camunda/zeebe`):
`bpmn-model/.../zeebe/{ZeebeAdHocImplementationType,ZeebeAdHoc}.java`,
`protocol-impl/.../adhocsubprocess/AdHocSubProcessActivateElementInstruction.java`,
`protocol-impl/.../job/JobResult.java`.

## Context

ADR 0022 §E.1 established that Camunda's agentic orchestration is **not a new engine concept** —
it is the **ad-hoc sub-process** primitive driven by an **AI Agent connector that is an ordinary
job worker**, with tools = inner activities (+ MCP). It also established the switch-over-parity
rule and a three-tier ladder. **Tier 0 (deploy parity) exists today**; this ADR specifies the
engine work for **Tier 1 (execution parity)**: make the *unmodified* Camunda AI Agent connector
run identically on embedded Bernd and remote Nano, with tools executing as real engine token flow
visible in the read model.

### The Camunda contract we must satisfy (verified against source)

- An `adHocSubProcess` carries `zeebe:adHoc` with `implementationType ∈ {BPMN_TASK, JOB_WORKER}`
  (`ZeebeAdHocImplementationType.java`). The **agentic path is `JOB_WORKER`**: the container is
  backed by a job (the AI Agent connector). The declarative path (`BPMN_TASK` +
  `activeElementsCollection`, a FEEL list of element ids) is the non-agentic variant.
- The container's job returns a **`JobResult`** carrying `activateElements[]`
  (each `{elementId, variables}`), plus `isCompletionConditionFulfilled` and
  `isCancelRemainingInstances` (`JobResult.java`). Activation instructions are
  `AdHocSubProcessActivateElementInstruction {elementId, variables}`.
- `zeebe:adHoc` also declares `outputCollection` / `outputElement` (gather each activated tool's
  output into a collection variable — the agent's accumulated tool results / memory), and the
  container honors a standard BPMN `<completionCondition>` FEEL expression.

### Nano today (the gap)

`engine-core/src/bpmn.rs` marks the container `is_adhoc` and **prunes every inner element**
(the "tools"), keeping the ad-hoc as one opaque service job (`:1044` comment). There is **no
`activateElements` field** on Nano's job result, no inner-element execution, no
`completionCondition`, no `cancelRemainingInstances`. So an agent worker can only drive tools
out-of-band, invisibly to the engine and read model.

## Decision (proposed)

Implement ad-hoc `JOB_WORKER` **activate-element execution semantics** in `engine-core`, and
thread the three result fields through the completion transports, matching the Camunda REST/job
shape byte-for-byte so the existing connector is unmodified. Scope is bounded and honestly stated
(see §Subset). Work in five seams, each an additive extension of an existing one:

### 1. Model (`bpmn.rs`, `model.rs`) — stop pruning; keep tools as an inactive catalog

- Parse `zeebe:adHoc` (`implementationType`, `activeElementsCollection`, `outputCollection`,
  `outputElement`) and the container `<completionCondition>`.
- Replace the prune pass with a new `ElementKind::AdHocSubProcess { impl_type,
  tools: Vec<ElementId>, completion_condition: Option<FeelExpr>, output_collection,
  output_element, active_elements_collection }`. Inner elements are **retained** but marked
  **non-token-reachable** — they have no incoming sequence flow from the container's start and are
  activated *only* by an activate-element instruction. Sequence flows *between* tools inside the
  container are preserved (a tool may be a small graph).

### 2. Runtime state + commands (`state.rs`, `command.rs`, `event.rs`, `engine/mod.rs`)

- **Container activation** creates an ad-hoc **scope** and emits the **agent job** (type from the
  `JOB_WORKER` task definition). The job's payload advertises the **tool catalog** (inner element
  ids + their declared input schema/io-mapping) so the connector sees available tools — exactly
  what today's connector reads.
- **New command `AdHocActivateElements { scope, instructions: [{elementId, variables}] }`**:
  for each instruction, create a child element instance of `elementId` inside the scope, seeded
  with `variables` (applied through the tool's `ioMapping`). Children execute as normal token
  flow (service/user/connector tasks → jobs; sub-graphs run to their own end).
- **Loop**: when activated tool instances complete, map each output via `outputElement` into the
  `outputCollection` variable, then **re-emit the agent job** with accumulated results (memory in
  vars). The connector inspects results and returns the next `activateElements` (more tools) or
  signals done. Terminate the container when **`completionCondition` evaluates true**, or the
  agent job completes with `isCompletionConditionFulfilled=true` and no further activations, or no
  active children remain and none were requested.
- **`isCancelRemainingInstances`**: cancel any in-flight tool instances in the scope, then
  complete the container.

### 3. Job result plumbing (`server/src/main.rs`, generated models, `falcon.rs`)

- Extend the engine job-completion result with `activate_elements: Vec<{element_id, variables}>`,
  `completion_condition_fulfilled: bool`, `cancel_remaining_instances: bool`.
- Map them on **both** transports: the Camunda REST `POST /jobs/{jobKey}/completion` body
  (`result.activateElements` etc. — must match the C8 schema so the stock connector serializes
  into it) and the Falcon complete-job frame (ADR 0016). No new endpoint; existing complete-job
  gains optional fields (absent ⇒ today's behavior).

### 4. FEEL + IO (`engine-core` FEEL) ✅ (v1: completionCondition + ioMapping)

- Evaluate `<completionCondition>` in the scope's variable context on each tool completion.
  **Done:** the parser attaches the container's `<completionCondition>` to the catalog, and the
  runtime evaluates it (via `eval_bool`) against the container scope after every tool completes —
  overlaid with that tool's just-projected output mappings. When true the container completes at
  once, cancelling any tools still running (like a multi-instance body's early completion). The
  agent's `isCompletionConditionFulfilled` flag is honored the same way (it supersedes any
  activate-element instructions in the same result).
- Apply each activated element's `ioMapping` on activation (input) and completion (output→
  container scope). **Done:** each tool's `zeebe:ioMapping` is retained on the container catalog
  (the tool element is pruned from the executable graph, so it can't be read back by element id);
  inputs are evaluated on activation into the tool's local scope, outputs are projected into the
  container scope on completion.
- **Done (v1.1):** `activeElementsCollection` (declarative `BPMN_TASK` variant) as a FEEL list
  evaluated at container activation — a distinct (agent-less) execution mode. On activation the
  container evaluates the collection to the element ids to activate, seeds those tools directly
  (no container job is minted), and completes once they drain; ids absent from the container's
  tool catalog are dropped (raising the Camunda `NOT_FOUND`/`EXTRACT_VALUE_ERROR` incident is a
  separate validation gap).

### 5. Read model + metrics

- Each tool activation is a real element instance ⇒ it appears in the read model / trace (the
  audit-trail parity that Operate shows, and — see §Story below — the substrate ProcessOS needs).
- Add counters: tool activations, agent-job iterations, ad-hoc completions/cancellations.

### Subset (honestly stated, per the whitepaper's discipline)

Ship in this order; state each boundary in `PERFORMANCE.md`/feature matrix:

- **v1**: single-level ad-hoc, `JOB_WORKER` impl, tools = service/connector/user tasks and simple
  inner sub-graphs; `activateElements` + `completionCondition` + `cancelRemainingInstances` +
  `outputCollection/outputElement`.
- **v1.1**: the declarative `BPMN_TASK` `activeElementsCollection` execution mode.
- **v1.2**: nested ad-hoc (agent-of-agents) — a tool that is itself an
  `adHocSubProcess` stands up a real second-level container (its own agent job,
  tool catalog and `outputCollection`), and its completion (natural or
  `cancelRemainingInstances`) crosses the nesting boundary through the parent's
  tool-completion path, nesting correctly in the read-model element-instance tree
  (issue #631).
- **Deferred**: embedded `SUB_PROCESS` tools whose multi-element body runs by
  token flow, boundary events on tools, compensation inside ad-hoc.

## Phased plan

1. **Contract fixtures.** Import Camunda's `connectors` ad-hoc AI-agent example BPMN as golden
   fixtures; capture the exact complete-job REST body the connector sends.
2. **Model** (seam 1): parse + retain-as-catalog; unit tests over the fixtures.
3. **Result plumbing** (seam 3): fields on REST + Falcon complete-job (no behavior yet).
4. **Runtime** (seam 2): activate-element command, scope, loop, completion/cancel; engine tests.
5. **FEEL/IO** (seam 4) ✅ (v1): completion condition + tool ioMapping. ✅ (v1.1): the `BPMN_TASK`
   declarative `activeElementsCollection` variant.
6. **E2E parity test**: run a golden agentic BPMN on embedded Bernd driven by the **unmodified**
   Camunda AI Agent connector; assert per-tool instances appear in the read model and the run
   matches Camunda's outcome.
7. **Read model + metrics** (seam 5) ✅: each tool activation is a read-model
   element instance (standard `ElementActivated`/`ElementCompleted`, so tools
   appear in the console trace scoped to their container); the loop is metered on
   `/metrics` via `nanobpm_adhoc_events_total{kind=tool_activation|agent_iteration|completion|cancellation}`.
   Feature matrix + `PERFORMANCE.md` subset note updated.

## Consequences

- The unmodified Camunda AI Agent connector (and MCP Client connector) runs on Nano — the ADR
  0022 §E.1 Tier-1 target; Urban's embedded agent (Tier 2) becomes an *additive* default over the
  same contract, never a substitute.
- **First agentic feature to touch `engine-core`.** Unlike ProcessOS (strictly one-way,
  engine-untouched) and Tier 0/2, Tier 1 modifies the hot path; it must carry the same
  bounded-subset honesty and keep the completion fields optional so non-agentic completion is
  byte-unchanged.
- Per-tool execution produces read-model trace — the substrate ProcessOS can later mine to
  optimize agentic processes (prune/reorder tools, tighten the container).

## Open questions

- **Loop shape**: re-emit the *same* agent job key per iteration, or a fresh job per turn? Camunda
  semantics + connector expectations decide this — pin against the fixture.
- **Tool concurrency**: activate multiple elements in one instruction — run concurrently in the
  scope, or serialize? (Camunda allows a batch; confirm engine ordering guarantees.)
- **Cost/limits**: max iterations / max active tools per container as an engine guard (runaway
  agent protection) — engine-enforced, connector-enforced, or both?
- **MCP tools vs BPMN tools**: MCP-discovered tools are not inner BPMN elements — do they stay
  entirely connector-side (no engine change), and is that acceptable for audit-trail parity?
