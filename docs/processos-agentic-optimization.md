# ProcessOS × Agentic Orchestration — optimizing what the agent does

> Status: analysis / direction. Companion to the ProcessOS design series
> (`processos-index.md`) and to ADR 0023 (`adr/0023-adhoc-subprocess-execution-parity.md`,
> the engine work that makes agent runs observable) and ADR 0022 §E
> (`adr/0022-nano-rad-application.md`, the RAD/agent surface). **Direction only —
> no engine change.** Every implication here is a ProcessOS-side read of public
> surfaces plus a write through the existing public contracts; `engine-core` stays
> untouched, exactly as the rest of the series requires.

## 0. Thesis

ProcessOS optimizes **deterministic** processes today: it reads the event-sourced
trace + metrics, an LLM proposes a structure/binding change, a verifier and
statistics decide, and the engine ships the winner through deploy + routing
(`processos-design.md` §0). An **agentic** process — an ad-hoc container where an
LLM picks tools at runtime (ADR 0023) — is *the same optimization problem with the
nondeterminism moved inside a single node*. Once Tier-1 execution parity makes each
tool activation a real read-model element instance, the agent's behavior becomes
**trace**, and trace is precisely what ProcessOS eats.

So the two LLM loops meet at the trace, running in opposite directions:

| | Runtime agent (ADR 0023) | ProcessOS optimizer (this series) |
|---|---|---|
| Proposes | which **tool** to run next | which **process change** to ship |
| Bounded by | BPMN structure + `completionCondition` + DMN rails | replay-as-fitness + experiment statistics |
| Cadence | per instance, milliseconds | per corpus, offline/canary |
| Engine role | executes tools deterministically | untouched; reads/writes contracts |

Same thesis both ways — *the moat is not the LLM; the moat is the thing that bounds
it.* The runtime agent proposes behavior; ProcessOS verifies process.

## 1. What the agent trace gives the optimizer

Tier-1 emits, per ad-hoc instance: the **tool catalog** offered, the **sequence of
activations** (`activateElements`), each tool's input/output (`outputCollection`),
the **iteration count** (agent-job turns), the `completionCondition` outcome, model
identity/cost/latency, and the final result. That is enough to compute, per process
and per cohort:

- **Tool utility** — activation frequency, success rate, marginal contribution to
  the outcome; dead or rarely-useful tools.
- **Loop cost** — iterations-to-completion distribution, token/$ per instance,
  tail latency of the agent turn (which, per the single-writer engine, competes
  with instance creation — see the performance memory).
- **Stable sub-sequences** — tool orderings the agent converges on across many
  instances (the *latent process* hiding inside the agent).

## 2. The optimization patterns this unlocks

New members of the pattern taxonomy (`processos-latent-process-exploration.md` §7),
ordered by the series' **autonomy-vs-value** axis (safe→auto at the bottom, valuable→
advisory at the top):

1. **Deterministic crystallization** *(highest value, advisory→canary).* When the
   agent converges on a stable tool sequence for a recognizable input class,
   ProcessOS proposes **replacing that agent path with deterministic BPMN** — a
   plain sequence/gateway the engine runs with no LLM call. This is the payoff of
   *latent process exploration* stated exactly: **the agent discovers the process;
   ProcessOS crystallizes it.** Value is huge (removes per-instance model cost +
   nondeterminism); autonomy stays low (a human confirms the semantics).
2. **Container tightening** *(canary→auto).* Prune dead tools from the ad-hoc
   catalog, add cheap FEEL preconditions, reorder the catalog, or tighten the
   `completionCondition` — a smaller, cheaper tool space with the same outcomes.
3. **Guard tuning** *(auto).* Derive a max-iteration / max-active-tools cap from the
   observed distribution (runaway-agent protection), and pick the cheapest model
   that holds outcome quality — cohort-routed and A/B-verified.
4. **RAG/grounding hints** *(advisory).* Spot repeated failed tool calls that a
   knowledge/vector tool would have short-circuited; suggest adding it.

All four are **structure/binding transforms** already in the ProcessOS transform
space — no new engine verb. Crystallization and tightening ship via the existing
**deploy** contract; guard tuning via **routing** (cohort→model/version).

## 3. The eval wrinkle — nondeterministic fitness

Replay-as-fitness (Tier B deterministic recorded-input replay,
`process-optimization-design.md`) assumes a deterministic step function. An agent's
LLM calls are not deterministic, so agent-process eval must use the **outcome-eval**
approach ProcessOS already reasoned about for its *own* investigator LLM (ADR 0004,
`0004-investigator-outcome-eval-harness.md`): either **replay against recorded model
responses** (turn the run deterministic by pinning the LLM outputs, then diff the
crystallized deterministic candidate against the recorded agent outcome), or evaluate
an **outcome distribution** over repeated runs. Crystallization is the friendly case:
the candidate is deterministic, so only the *baseline* (the agent) needs distributional
eval, and the win condition is "same outcome, no model call."

## 4. Boundary discipline (unchanged)

The one-way dependency holds without exception. ProcessOS reads the agent trace via
the public trace/metrics export and writes only deploy + routing. It never reaches
into the agent loop at runtime; it observes completed instances and proposes the next
version. The engine (Tier-1 ad-hoc included) builds and runs with ProcessOS absent.
The single novelty versus the deterministic series is *where the nondeterminism to be
optimized lives* — inside one ad-hoc node rather than across a routing cohort — and
that is a property of the input data, not of the contract surface.

## 5. Why this matters to the story

Camunda's agentic story is authoring + connectors; nobody is *closing the loop* on
agent processes with a verifier that turns discovered agent behavior back into cheap
deterministic flow. That loop is ProcessOS's whole thesis, and agentic execution
parity (ADR 0023) is what feeds it. Nano can therefore offer something adjacent to,
not just compatible with, Camunda: **run the Camunda AI Agent unchanged, then let
ProcessOS distill it** — parity in, optimization out.
