# ADR 0044 — Code-first durable orchestration for the single-user SDLC (Camunda Nano)

Status: **Proposed.**
Date: 2026-07-29.
Relates to:
ADR 0023 (`0023-adhoc-subprocess-execution-parity.md`, the executable ad-hoc sub-process — the
agent-loop primitive this builds on), ADR 0040 (`0040-fused-domain-model.md`, the fused domain
model — the typing story a code-first surface projects against), ADR 0043
(`0043-bojtos-demo-framework.md`, the in-browser engine — the same substrate a frontend
`useJourney` twin would ride), ADR 0022 (`0022-nano-rad-application.md`, the baked-in RAD that
produced `urban-pr-review` — the SDLC-automation context this serves) and ADR 0003
(`0003-write-path-durability-tiers.md`, leader-durable replication — the durability tier this
leans on). Grounded by the de-risking spike in `spikes/durable-workflow/` (crash-resume proof +
the code-first façade proven this session).

## Context

Nano's long game is **Camunda Nano** — a drop-in, upgrade-path replacement for Camunda 8 / an
eventual "Camunda 9" — but the first beachhead is narrower and concrete: **a developer automating
their own SDLC** (agents, PRs, testing, integration environments, soak tests) on a single machine
(a workstation, or an always-on RPi behind an ngrok tunnel). The `urban-pr-review` application is
the first artifact of exactly this use case, built with the server's baked-in RAD.

For that developer, the current authoring path — model a BPMN diagram, wire each service task to a
job type, wire the data payloads, register workers, connect them — is the same ceremony Temporal
imposes (workflow definition + worker code + worker connection + task queues) and Camunda imposes
(model + task-type wiring + payload wiring). We have spent significant effort *deriving* this
wiring away across the system (the fused domain model, envelope-derived worker I/O, element
templates). The question this ADR answers: **can we offer a Temporal-style "code-first durable
execution" authoring experience on top of Nano's existing engine, for the single-user SDLC use
case, without a new engine and without the ceremony?**

Two findings reframe the effort from "build a durable-execution engine" to "add a thin authoring
layer":

1. **Nano already IS a durable-execution substrate.** The mapping to Temporal's model is
   near-total: event history → the raft journal; replay → the engine is itself replay-from-journal;
   activity → a job + worker (with retries/incidents); signal → message correlation; timer → a BPMN
   timer; child workflow → a sub-process. The durability crown jewel — an in-flight workflow
   surviving an engine crash and resuming without re-running completed steps — is a property Nano
   already has.

2. **Nano already shipped an executable agent loop.** ADR 0023's executable ad-hoc sub-process lets
   a worker decide, each turn, which inner steps to activate, fold their outputs into an
   accumulator, and loop to a completion condition. That is structurally an agent/durable-iteration
   primitive, already in the engine.

**The spike (this session) confirmed both empirically.** In `spikes/durable-workflow/`, a linear
A→B→C workflow was driven on a dedicated engine, the engine was `SIGKILL`'d after B committed, then
restarted cold against the same data dir; A and B were **not** replayed and C ran **exactly once**
(ledger `{a:1,b:1,c:1}`). A negative control that wipes the journal on restart correctly fails to
resume — so the proof is not a tautology. The durability is real and requires no new machinery.

So the missing piece is **not** durability — it is **authoring ergonomics + wiring derivation**.

### The honest constraints

- **The determinism sandbox constraint binds only the *orchestration function*, not activities.**
  A common misconception (worth stating plainly because it shaped the strategy discussion): if we
  ever compile an orchestration to a replayed/deterministic function, *that function* must be
  side-effect-free — but the **activities it invokes run outside the sandbox** and may freely do DB
  access, network calls, shell commands, and LLM calls. This is Temporal's exact split (workflow vs
  activity). It does **not** cripple SDLC automation, which is almost entirely activities.
- **At-least-once, not exactly-once.** If the engine dies *between* an activity's side effect and
  its job completion (not the scenario the spike tests, but a real one), the job is redelivered on
  restart and the side effect repeats. **Activities must be idempotent.** This is the standard
  durable-execution contract and must be documented on the authoring surface.
- **Single-node durability only, for v1.** The spike and v1 target a single machine. Scale,
  resilience, and multi-node reclaim are the separate (and already deeply explored) raft work; v1
  explicitly does not depend on them.

## Decision

Offer a **code-first authoring façade over the existing engine** — `@nanobpm/workflow` — for the
single-user SDLC use case, and grow it in increments. Take from Temporal what removes ceremony;
reject the ceremony itself.

### What we take from Temporal

- **Author the workflow as code**, not as a diagram the developer draws.
- **A first-class "local activity"** (`ctx.run(name, fn)`): a durable step whose handler is ordinary
  code doing real work.
- **Durable waits as code** (`ctx.signal`, `ctx.sleep`): a human-in-the-loop approval or a timer is
  one line, not a modeled event the developer wires by hand.
- **"It just resumes."** The developer never thinks about the journal; crash-resume is implicit.

### What we reject

- **Task queues and explicit worker registration.** A workflow's steps derive their own job types
  (`${workflowId}:${stepName}`); a single generic worker dispatches by that derived type. No
  hand-wiring, no registry.
- **Separate model + payload wiring.** The model is *generated* from the code; correlation keys and
  message subscriptions are *derived* from the `ctx.signal` declaration.
- **Multi-language SDK ceremony (for now).** One TypeScript/JS surface, targeting the SDLC user.

### The authoring surface (v1)

The proven v1 increment is a **declarative builder that compiles to a model** (Strategy A below),
because it reuses the engine's durability directly with no replay/determinism discipline:

```js
export const prReview = defineWorkflow("pr-review", (w) => {
  w.run("fetchDiff",  async (job) => ({ diff: await gh.diff(job.variables.prId) }));
  w.run("autoReview", async (job) => ({ findings: await llm.review(job.variables.diff) }));
  w.signal("humanApproval", { correlationKey: "prId" });   // durable wait
  w.run("merge",      async (job) => ({ merged: await gh.merge(job.variables.prId) }));
});
```

From this the façade **derives**:

- an executable **BPMN model** (`start → steps → end`; a `serviceTask` per `run`; an
  `intermediateCatchEvent` + `<bpmn:message>` + `zeebe:subscription correlationKey` per `signal`) —
  no diagram authored;
- the **job types** (`pr-review:fetchDiff`, …) — no worker↔task-type wiring;
- the **message name + correlation** for signals — no correlation plumbing;
- a **single generic worker** dispatching activated jobs to the declared handlers.

The resulting process instance is an ordinary Nano instance, so it inherits the proven crash-resume
durability. The **spike's façade demonstrates this end to end**: a code-first crash-resume test
(`resume-code-first.mjs`) passes the same `SIGKILL`/restart/exactly-once assertion as the
hand-written model, and a demo (`demo.mjs`) runs `fetchDiff → autoReview → [durable wait] →
humanApproval → merge` to `COMPLETED`, sending the approval as a correlated message.

- **`ctx.run`** = a local activity (an engine service task; the durability unit).
- **`ctx.signal`** = a durable wait for an external/human event = the `useJourney` "action" surface
  (ADR 0040/0043 frontend twin): the set of currently-valid, typed actions on an instance.
- **`ctx.sleep`** = a BPMN timer (later increment).
- An **agent loop** maps to ADR 0023's ad-hoc container; a **linear flow** maps to a generated
  linear model. Everything downstream (types, actions, stages) is *derived*, per the fused domain
  model.

### v1 durability model

v1 workers are **stateless per turn**: each handler reads the engine-accumulated instance state
(variables folded from prior steps) and returns new variables. There is no in-process workflow
heap to keep alive — the engine's journal is the durable state. This is why v1 is the honest
minimal path: a distinct engine step per `ctx.run`, no replay.

### The backend / frontend / diagram duality

The **same instance** is: a backend `workflow()` the developer authored in code; a frontend
`useJourney({ process, key })` twin observing its stages/actions (ADR 0040/0043); and, because the
model is real BPMN, a diagram anyone can open. One durable object, three views. This is Nano's
differentiator over "graphical Temporal" and over "code-only Temporal": we are neither, we are
both, over one artifact.

### Strategy trajectory (what we build vs. describe)

- **Strategy A — compile a declarative workflow to a model (BUILD NOW).** The `defineWorkflow`
  builder above. Cheap, reuses durability directly, no determinism discipline. **This is v1, and
  the spike proves it.**
- **Strategy B — replayed imperative function = the true Temporal model (DESCRIBE, don't build
  yet).** `async (ctx) => { const d = await ctx.run("fetchDiff", …); … }`, replayed against the
  engine log. The **beautiful insight**: ADR 0023's ad-hoc `outputCollection` accumulator **is**
  the replay log — each activated-tool output is a durably-recorded step result, which is exactly
  what a replay engine folds back into a function's local state. Strategy B is the trajectory; it
  needs the determinism discipline and is not undertaken in v1.
- **Strategy C — wasm-as-determinism-sandbox (OUT OF SCOPE).** Compiling the orchestration function
  to wasm to *enforce* determinism is the differentiated long-term moat but requires the
  side-effect-free-orchestration constraint and heavy machinery; explicitly a non-goal here.

## Consequences

- **Positive:** the SDLC developer authors a durable workflow in ~30 lines of code with zero
  diagram/wiring/registration ceremony, and it inherits crash-resume for free. The façade is a thin
  layer (a builder + a BPMN emitter + a generic worker + deploy/start/signal helpers over the REST
  v2 API), not an engine. It rides shipped machinery (jobs, message correlation, ad-hoc loop). The
  authoring surface *is* the frontend `useJourney` action contract, unifying three product surfaces
  on one instance.
- **Negative / cost:** a new public package surface (`@nanobpm/workflow`) to maintain and version.
  Activities must be idempotent (at-least-once) — a real footgun that documentation and helper
  patterns must make hard to hit. Strategy A cannot express arbitrary imperative control flow
  (loops/branches beyond the declared shape) — that awaits Strategy B or the ad-hoc container.
- **Neutral:** the emitted model is DI-less (no diagram layout); that is fine for code-authored
  workflows and consistent with the DI-less rendering path already in the console.

### Non-goals (v1)

- No wasm determinism sandbox (Strategy C).
- No multi-language SDKs.
- No scale/resilience/multi-node reclaim guarantees — single machine only.
- Best-effort versioning only; changing a running workflow's shape mid-flight is not solved here
  (see open questions).

### Risks

- **Versioning / version-skew** is the killer risk of any durable-execution system: an in-flight
  instance authored against workflow vN must not be resumed against a structurally different vN+1.
  v1 punts (best-effort); a real story is required before this leaves the single-user niche.
- **Determinism leak** if/when Strategy B lands: any non-determinism in the orchestration function
  (clock, RNG, direct I/O) corrupts replay. The sandbox (Strategy C) is the eventual guard.
- **At-least-once idempotency**: the single biggest correctness burden pushed onto the user.

## Open questions

- **Versioning:** how does an in-flight instance pin to the workflow version it started under, and
  what is the migration story when the code changes? (Blocking before multi-user.)
- **Where does the façade live?** A standalone `@nanobpm/workflow` npm package, or baked into the
  server RAD alongside the domain SDK codegen? The SDLC single-machine target argues for baked-in
  first, published later.
- **`ctx.sleep`/timers and cancellation** ergonomics — likely trivial to derive (BPMN timer), but
  cancellation/compensation semantics need design.
- **When to graduate to Strategy B** (replayed imperative), and whether the ad-hoc
  `outputCollection`-as-replay-log insight can be realized without the full wasm sandbox.
