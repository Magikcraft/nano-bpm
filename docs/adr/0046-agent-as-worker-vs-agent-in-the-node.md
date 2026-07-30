# ADR 0046 — Two agent topologies: agent-as-worker (native) vs agent-in-the-node (compat)

Status: **Proposed.**
Date: 2026-07-30.
Relates to:
ADR 0023 (`0023-adhoc-subprocess-execution-parity.md`, the executable ad-hoc sub-process — the
"agent-in-the-node" primitive this ADR positions), ADR 0044
(`0044-code-first-durable-orchestration.md`, code-first durable orchestration — the SDLC beachhead
whose agent step this ADR types), ADR 0045 (`0045-code-first-workflows-rad-surface.md`, the
code-first RAD surface that ships `w.task`), ADR 0006 (`0006-subagent-delegation-mode.md`, the
subagent/harness delegation seam), ADR 0022 (`0022-nano-rad-application.md`, Urban — the SDLC
context), and ADR 0025 (`0025-urban-trigger-runtime.md`, the I/O-trigger axis that also drives
external work). Grounded by the upstream Camunda `AgentInstance` record family
(`protocol-impl/.../agentinstance/*` in `camunda/zeebe`) and by the `c8ctl nano hire`/`work`
harness-as-worker flow.

## Context

Camunda is moving the AI Agent **into the engine**. The new `AgentInstance` record family
(scaffolded 2026-05, maturing through mid-2026) encodes an agent as a first-class, engine-driven
element of an `adHocSubProcess`:

- `AgentInstanceDefinition` = `model` / `provider` / `systemPrompt` — **the engine is the LLM
  caller**.
- `AgentInstanceTool` = `name` / `description` / **`elementId`** — tools are **inner BPMN elements**
  of the ad-hoc container: a closed, modeled catalog.
- `AgentInstanceLimits` = `maxTokens` / `maxModelCalls` / `maxToolCalls` — the engine **meters the
  reasoning loop**.
- `AgentInstanceMetrics` = input/output tokens, model/tool call counts.
- `AgentHistory` = `loopIteration` + `content[]` + `toolCalls[]` + metrics — the persisted
  conversation.
- Status lifecycle: `INITIALIZING → TOOL_DISCOVERY → THINKING → TOOL_CALLING → IDLE` — the engine
  **runs the loop**: call the model, receive tool-call requests, activate the matching inner
  elements, fold results back, repeat until a completion condition or a limit is hit.

This is a coherent, valuable model — but it makes one specific choice: **the agent is subordinate to
the process.** Its brain is engine-hosted, its powers are exactly the modeled tools, its budget is
engine-capped. That is exactly what you want for a *governed, bounded* decision loop over
pre-enumerated capabilities (a support agent that may "check order", "issue refund", or "escalate",
each a modeled service task): the value is **containment**.

Nano's own first beachhead (ADR 0044) is a different shape. The developer automating their own SDLC
wants to orchestrate **coding-agent harnesses** — Claude Code, Copilot CLI, and the like, turned
into external job workers via `c8ctl nano hire`/`work`. A concrete motivating pipeline:

> Run a soak test → measure performance → **if** there is a regression, dispatch an agent to
> investigate: it `ssh`es into the loadgen, reads logs, runs a spike, forms and tests a hypothesis,
> then raises an issue and opens a PR → a human approves the PR.

The investigating agent is a **general-purpose autonomous worker with its own reasoning loop and its
own open-ended tools** (ssh, shell, git, gh, an editor). The engine sees one step: `investigate` in,
`{issue, pr}` out. This does **not** map onto `adHocSubProcess`/`AgentInstance`, and forcing it there
is the wrong abstraction.

### The honest constraints

- **Drop-in migration is a commitment.** A Camunda user must be able to bring their `.bpmn` — including
  an AI-Agent ad-hoc sub-process — and have Nano run it. So Nano *does* owe engine-native
  `AgentInstance` support **as a compatibility surface**, eventually.
- **But compatibility is not the authoring model.** The model Nano reaches for to orchestrate its own
  SDLC agents must not be dictated by Camunda's containment choice. These are two orthogonal
  obligations that ADR 0044 previously ran together.

## Decision

Name and support **two distinct agent topologies**, and be explicit about which is native and which
is compatibility-only.

### 1. Agent-as-the-worker (Nano-native, differentiated)

The agent is a **process participant serviced through a job**. The process contains a *call to* an
agent (`w.task`, ADR 0045 — an external service task with a derived job type); the agent's tools and
reasoning are **its own and opaque to the engine**. The engine's role is to **route and await**, with
durable at-least-once delivery. This is what `c8ctl nano hire`/`work` already produces, and it rides
Nano's existing strengths (the command stream, long-lived job activation, message-correlated
human-in-the-loop waits).

The motivating pipeline decomposes with **no ad-hoc sub-process anywhere**:

```js
export const perfWatch = defineFlow("perf-watch", (w) => {
  w.run("soakTest",  async (job) => ({ metrics: await runSoak(job.variables) }));
  w.run("measure",   async (job) => ({ regression: detectRegression(job.variables.metrics) }));
  w.branch((v) => v.regression, {            // Slice-2 gateway combinator (declarative loops/conditionals)
    then: (b) => {
      b.task("investigate");                 // agent-as-worker: the coding harness, external
      b.signal("approvePR", { correlationKey: "prId" });   // human-in-the-loop gate
    },
  });
});
```

Deterministic and non-deterministic steps sit side by side; the engine stays the dumb-but-durable
router; the agent stays a black box behind a job.

### 2. Agent-in-the-node (Camunda `AgentInstance`, compatibility-only)

The engine hosts the reasoning loop; tools are modeled inner elements of an `adHocSubProcess`; LLM
config and budget live in the engine (per the record fields above). Nano treats this as a **drop-in
compatibility target** — a Camunda model that Nano must consume and (eventually) execute — **not** as
the authoring model for the SDLC use case, and **not** the surface the code-first SDK leads with.

### The distinction, at a glance

| | Agent-in-the-node (Camunda `AgentInstance`) | Agent-as-the-worker (Nano-native) |
|---|---|---|
| Reasoning loop | Engine-hosted | Inside the harness |
| Tools | Modeled BPMN inner elements (`elementId` catalog) | The agent's own, opaque |
| LLM config | Engine (`provider`/`model`/`systemPrompt`) | The harness |
| Budget | Engine-metered (`maxTokens`/`maxToolCalls`) | Harness-internal |
| Engine's role | **Runs** the agent | **Routes to & awaits** the agent |
| Primitive | `adHocSubProcess` | external job (`w.task`) |
| Good for | Governed, bounded, pre-enumerated capabilities | Open-ended, long-running autonomous work |
| Nano posture | **Compat** obligation (drop-in) | **Native / differentiated** |

### Why the ad-hoc/`AgentInstance` model fits the SDLC case badly

1. **Tools aren't enumerable.** "Arbitrary shell on a box" cannot — and should not — be modeled as
   inner BPMN elements. Open-endedness is the point; the `elementId` catalog fights it.
2. **Two reasoning loops collide.** The harness already has an agentic loop; `AgentInstance` wants the
   engine to run *the* loop and call the model. Nesting them is wasteful or contradictory.
3. **LLM config is in the wrong place.** Engine-side `provider`/`model`/`systemPrompt` assumes the
   engine calls the model. The harness owns the model, prompt, and context.
4. **Budget semantics don't transfer.** `maxToolCalls`/`maxTokens` meter the engine's LLM calls; the
   harness manages its own budget the engine can't see.
5. **Wrong durability shape.** Ad-hoc tool activations are short, in-turn. A coding investigation is a
   long-running (hours), crash-resumable job — external-worker/job-stream territory (heartbeats,
   timeout extension, at-least-once), not tool activation.

### `AgentHistory` as telemetry (steal the shape, drop the loop)

`AgentHistory` (`loopIteration`, `content[]`, `toolCalls[]`, `metrics`) is valuable in **both**
topologies — but decouple the **record shape** from the **execution model**. In Camunda it is a
byproduct of the engine-driven loop. In the agent-as-worker model the **worker reports** its
conversation, tool calls, and token metrics back on the job, turning an opaque agent step into an
**observable** one (what did it try, how many model calls, what did it cost) without the engine
driving anything. Nano adopts the history record as a **worker→engine telemetry channel** and leaves
the engine-as-LLM-caller machinery to the compat surface.

## Consequences

- **Positive:** the code-first SDK leads with the topology that actually serves the SDLC beachhead;
  the ad-hoc/`AgentInstance` complexity is scoped to a compat surface rather than pushed onto every
  code-first author. Nano occupies a niche Camunda's containment model serves poorly — *open-ended
  autonomous agents as first-class process participants* — which is a differentiator, not a gap.
- **Negative / cost:** Nano now owes two things over time: (a) long-running-job ergonomics good enough
  for hours-long opaque workers, and (b) an eventual engine-native `AgentInstance` execution path for
  drop-in. Neither is free.
- **Neutral:** the two topologies can coexist in one model — a governed in-node agent for a bounded
  decision and an external agent-worker for open-ended work are not mutually exclusive.

### Work items (agent-as-worker native surface)

These are the concrete gaps the SDLC use case needs; **none** involves `adHocSubProcess`:

1. **Long-running `w.task` ergonomics:** job heartbeat / timeout-extension so an hours-long job is
   not reaped; result streaming / progress. (Rides the command stream.)
2. **At-least-once safety with idempotency keys:** a redelivered `investigate` must not open a second
   PR. A per-job idempotency key + worker-side dedup is required before this leaves a toy.
3. **Rich context in / structured result out:** a first-class shape for the goal handed to the agent
   and the `{issue, pr, findings}` it returns.
4. **HITL gates:** already covered by `w.signal` (ADR 0045).
5. **Optional agent telemetry:** a worker-reported history/metrics record (the `AgentHistory` shape
   as telemetry) for observability of the black-box step.

### Non-goals

- **Not building engine-native `AgentInstance` now.** The compat execution path is a separate,
  later effort; this ADR only *positions* it.
- **Not modeling harness tools as BPMN elements.** The agent-as-worker's tools stay opaque by design.
- **Not a general connector framework.** This is about the topology and the `w.task` ergonomics, not
  a marketplace of agent integrations.

## Risks

- **Compat debt drift.** Camunda's `AgentInstance`/`AgentHistory` schema is actively evolving (see
  ADR 0023 follow-ups). Deferring the compat path risks a widening parity gap; the mitigation is to
  track the record shape now (telemetry adoption doubles as schema familiarity) even while deferring
  the engine-driven loop.
- **Idempotency is the real footgun.** Long, non-idempotent agent actions under at-least-once
  delivery are the highest-severity correctness hazard of the native surface; work item 2 is not
  optional.
- **Two-topology confusion.** If both surfaces are exposed without clear guidance, authors may reach
  for the wrong one. The naming table above and SDK docs must make the choice obvious ("does the
  agent bring its own tools and brain? → worker. Do you want the engine to pick among modeled
  process steps? → in-node.").

## Open questions

- **How far does `AgentInstance` compat go** — consume-and-execute (full engine-driven loop) or
  render/inspect-only first, degrading a live agentic model gracefully? (Ties to ADR 0023's
  execution-parity trajectory.)
- **Idempotency-key design:** engine-minted vs worker-supplied; where dedup state lives; interaction
  with retries/incidents.
- **Telemetry schema alignment:** how closely Nano's worker-reported history mirrors upstream
  `AgentHistory` so a future compat path and the native telemetry share one shape.
