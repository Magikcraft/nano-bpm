# Runtime Process Optimization — Design Proposal

> Status: analysis / proposal only. No code changes are made by this document.
> Grounded in: `engine-core/src/event.rs` (the element-granular `Event` stream),
> `engine-core/src/command.rs` (`Command` = the only driver of the engine),
> `engine-core/src/engine.rs` (`apply_command_at(cmd, now)` — clock is *injected*,
> never read; `EngineSnapshot`), `server/src/journal.rs` (append-only event log +
> the **async exporter channel** `set_exporter`), `server/src/main.rs`
> (`now_millis()` host clock, the single-writer `engine.with(|e| …)` actor),
> `server/src/console/mod.rs` (Prometheus + metrics surface), the WASM build of
> `engine-core`, idempotent deployment + process versioning, and the create-path
> leader-forward router (`server/src/partition.rs`).

## 0. Vision

Nano BPM is a high-performance, low-resource, drop-in Camunda replacement. That is
the floor, not the ceiling. The deliberate low-resource design opens a second
space: **closed-loop runtime process optimization.** Put the engine in a loop with
static-analysis tools and LLMs that reason over how a process actually performs —
which tasks throw incidents, which take too long, what each task *costs* (compute,
$, LLM tokens) and what an SLA violation costs the business — let that reasoning
network form hypotheses, deploy **canary** optimized process versions, and measure
the effect either against the customer's real traffic or in a **simulated** harness.

The thesis of this document: **the moat is not the LLM. It is (1) a faithful,
queryable execution trace, (2) deterministic recorded-input replay, (3) cheap
massive simulation (the WASM engine), and (4) a *verified* transformation space.**
The LLM only proposes; a verifier and statistics decide. Everything downstream
(simulation, canary, reasoning) depends on the first two, so we build them first.

## 1. The good news: the engine is already event-sourced

We are surfacing a substrate that already exists, not inventing one.

- **The engine emits an element-granular event stream.** `engine-core/src/event.rs`
  already produces `ElementActivating` / `ElementActivated` / `ElementCompleting`
  / `ElementCompleted` (each carrying `instance_key`, `element_instance_key`,
  `element_id`, `scope`), `JobCreated` / `JobActivated` / `JobCompleted` /
  `JobFailed` / `JobLockExpired`, `IncidentRaised` / `IncidentResolved`,
  `SequenceFlowTaken`, `TimerCreated` / `TimerTriggered`, message events, and
  `ProcessInstanceCreated` / `Completed` / `Terminated`. **This is a trace.**
- **Events are immutable facts and already replay state** (`event.rs` header:
  "the event stream is a replayable log"; `Journal::read_events` rebuilds state).
- **There is already an async export seam.** `journal.rs` carries an
  `exporter: Option<Sender<Arc<Vec<Event>>>>` and `set_exporter()` — the
  Zeebe-style async exporter. Trace export hangs off this with **zero hot-path
  cost** (it is already off the commit-ack path).
- **The clock is injected, not read.** Every mutation goes through
  `apply_command_at(command, now)` with `now = now_millis()` supplied by the host;
  the engine *never* reads a wall clock and contains "no threads, locks or I/O"
  (`engine.rs`). **This is the determinism property that makes replay and
  simulation exact** — feed the same commands with the same `now` and you get the
  same events, bit for bit.

What is missing is small and specific: a **trace projection** off the exporter, a
**recorded-input bundle** for counterfactual replay, a **cost annotation +
realization** layer, a **canary router + simulation host**, and a **reasoning /
control plane** on top. The engine core does **not** change.

## 2. The load-bearing abstraction: two capture tiers

There are two distinct needs, and conflating them is the classic mistake:

| Tier | Question it answers | Source | Cost |
|------|---------------------|--------|------|
| **A. Trace export** | *What happened?* (observe, measure, report) | the existing **event** exporter | ~free (async, off hot path) |
| **B. Recorded-input replay** | *What would happen to a **different** process version on the **same** inputs?* (counterfactual / simulate) | the **command** stream + injected `now` + external nondeterminism | small, opt-in |

The subtlety: the **event** log records the outcomes of the *deployed* version, so
replaying events only reconstructs *that* version. To ask "would v4 have done
better than v3 on Tuesday's traffic?" you must replay the **inputs** (commands +
clock + external payloads), because a different process definition produces a
*different* event stream from the same inputs. Tier B captures exactly the inputs
the engine consumes:

- the `Command` (intent), and the `now` value supplied to `apply_command_at`;
- the external nondeterminism each command injects: **job-completion variables**
  (`CompleteJob { variables }`), **message payloads** (`PublishMessage`),
  **timer fire instants** (already a function of injected `now`), and any RNG seed
  (today exclusive-gateway conditions are pure functions of variables → already
  deterministic; record a seed only if/when stochastic constructs are added).

Given a Tier-B bundle, **any** process version can be re-executed deterministically
in an ephemeral engine — on the server, in a sandbox, or in **WASM** in a browser.

> **Design rule:** Tier A is always-available and cheap (it is observability). Tier
> B is opt-in per process/tenant (it stores inputs, so it has a privacy + storage
> cost), and it is the substrate for everything counterfactual.

## 3. The canonical execution trace (Tier A)

A projection task subscribes to the exporter and folds the flat `Vec<Event>` into
a **per-instance, element-keyed trace** — the shape an LLM and a static analyzer
both consume. Stable identity comes from BPMN `element_id` (within a version) plus
`element_instance_key` (per token); cross-version comparison anchors on
`element_id` with a semantic-anchor fallback (see §8).

```jsonc
// one process instance trace
{
  "instanceKey": "...", "processId": "order", "version": 7,
  "tenant": "...", "businessId": "...",
  "startedAt": 1719000000000, "endedAt": 1719000004200,
  "outcome": "completed",                 // completed | terminated | incident-open
  "elements": [
    {
      "elementId": "ScoreRisk", "type": "serviceTask",
      "elementInstanceKey": "...", "scope": "root",
      "enteredAt": 1719000000100, "exitedAt": 1719000002600,
      "job": {
        "type": "risk-score", "worker": "...",
        "createdAt": ..., "activatedAt": ..., "completedAt": ...,
        "queueMs": 40,          // activatedAt - createdAt  (wait in the broker)
        "serviceMs": 2460,      // completedAt - activatedAt (worker time)
        "attempts": 1, "incidents": 0,
        "cost": { "usd": 0.0021, "tokens": 1840, "model": "…" } // realized, §6
      },
      "varsRead": ["amount","country"], "varsWritten": ["riskScore"] // lineage, §6
    }
    // … one entry per element instance, in causal order
  ],
  "incidents": [ { "elementId": "...", "kind": "...", "raisedAt": ..., "resolvedAt": ... } ],
  "path": ["Start","ScoreRisk","Gateway","Approve","End"]   // realized token path
}
```

Aggregations the reasoner needs (and the metrics surface should expose,
dimensioned by `process / version / element / tenant`):

- per-element **duration histogram**, split **queueMs vs serviceMs** (broker wait
  vs real work — different fixes);
- per-element **incident rate**, **retry/attempt count**, **abandonment**;
- **bottleneck path** and critical-path contribution;
- **variable→outcome correlations** (which inputs predict incidents / long tails).

Export formats: an **OpenTelemetry span tree per instance** (one span per element
instance; job activate/complete as child spans) for interop with existing
tracing/back-ends, plus the native JSON above for the reasoning layer. The OTel
mapping is the pragmatic default — it drops into Tempo/Jaeger/Honeycomb for free.

## 4. Deterministic recorded-input replay (Tier B)

A second, opt-in exporter-adjacent tap records the **input bundle** per instance
(or per partition window). Because the engine is a pure `(state, command, now) →
(state', events)` function, replay is exact:

```
replay(processVersion, inputBundle) -> Vec<Event>   // deterministic, no I/O
```

Seam (host-side, engine untouched): the single-writer actor in `main.rs` already
funnels every mutation through `engine.with(|e| e.apply_command_at(cmd, now))`. A
thin recorder wraps that closure to also append `(cmd, now, external-payload)` to
the Tier-B sink when recording is enabled for the instance's process/tenant. No
engine-core change; the recorder is a host decorator on an existing choke point.

Uses unlocked immediately:
- **Time-travel debugging** of any instance (step the recorded inputs).
- **Counterfactual replay**: run a candidate version against recorded inputs and
  diff the event streams / outcomes / costs.
- **Seeding simulations** (§5) from real arrival + service-time distributions.

Privacy/retention: input bundles contain business data, so Tier B is opt-in,
per-tenant, with TTL and field-level redaction hooks. Tier A traces can be stored
with variables reduced to **lineage + hashes** by default.

## 5. Simulation harness — the WASM payoff

`engine-core` already builds for `wasm32` and is "Send, no threads/locks/I/O." That
makes it cheap to instantiate **thousands of ephemeral, deterministic engines** in
a sandbox (server worker pool *or* the browser) and run Monte-Carlo experiments
**before touching production**:

- **Trace-driven replay sim:** drive a candidate version with Tier-B bundles
  sampled from real traffic → high-fidelity estimate of effect on *this* workload.
- **Distributional sim:** fit per-job service-time + arrival + branch-probability
  distributions from Tier-A aggregates, then generate synthetic load → explore
  load/scale regimes that have not occurred yet.
- **Virtual clock:** simulations advance `now` themselves (the engine already takes
  `now` as a parameter and drives timers via `trigger_timers(now)`), so a day of
  process time runs in milliseconds and timer-heavy processes are exact.

This is the differentiator. Few engines can spin up high-fidelity, faster-than-real
deterministic replicas of themselves at this density; a JVM + Elasticsearch stack
cannot. It turns "deploy and pray" into "simulate, rank, then canary."

## 6. Cost / value model — the objective function

Optimization needs an objective, and a *modeled-only* objective is garbage. Two
layers:

1. **Annotation (intent):** attach to each element — via BPMN extension elements or
   a side store keyed by `(processId, elementId)` — `execCost`, `latencyBudgetMs`,
   `slaMs`, `slaViolationCost` (cost-to-business), and, for LLM-backed tasks,
   candidate **model options with per-call $ + token + latency priors**.
2. **Realization (truth):** workers report **actual** cost back on completion. The
   command stream already carries `CompleteJob { variables }`; reserve a
   structured `__cost` channel (usd / tokens / model / latency) so realized cost
   lands on the job event and into the trace (`elements[].job.cost` in §3). Expected
   vs realized is itself a signal the reasoner uses.

A single tunable **utility** combines them: `U = wₜ·throughput − w_$·cost −
w_L·latencyPenalty(p99) − w_I·incidentRate`, with SLA penalties as hard or soft
constraints. The optimizer maximizes `U`; the weights are the customer's policy.

**Variable/data lineage** (`varsRead` / `varsWritten` in §3) is what lets the
reasoner connect inputs to outcomes and reason about whether a transformation is
data-safe (§8).

## 7. Experimentation plane — canary, shadow, guardrails

We already have **process versioning** and **idempotent deployment** (byte-identical
redeploy is a no-op; a changed model is a new version) and a **create-time router**
(`partition.rs`, leader-forward). Canary is a small, well-placed step.

- **Traffic-split router** at `CreateInstance`: a policy selects the process version
  per instance by **cohort** (tenant, business-key hash, or %) — e.g. 5% to the
  candidate. Routing already happens at create; this adds version selection.
- **Guardrails + auto-rollback:** watch the candidate's incident-rate / p99 / cost
  vs the incumbent and auto-revert on regression. Use **sequential tests / CUSUM**
  so a decision needs far fewer samples than a fixed-horizon A/B. (This is the same
  instinct as the existing `Pressure` back-pressure control loop — sense early, act
  before collapse.)
- **Shadow execution:** run the candidate in parallel on *mirrored* inputs with
  workers in **dry-run** mode, compare outcomes without side effects. Requires
  **idempotency keys** on external effects so shadow/canary cannot double-charge a
  payment or re-send an email.
- **Experiment design:** holdouts and variance reduction (CUPED-style using
  pre-period covariates) to survive **nonstationarity** — a business environment
  drifts, and a naïve before/after will credit seasonality as a win.

## 8. Reasoning interface — LLM-in-the-loop, safely

Three hard rules keep autonomy tolerable:

1. **The LLM consumes a report, not a firehose.** Give it a token-budget-aware
   **performance report** per version (the §3 aggregations: bottleneck path,
   incident clusters, queue-vs-service split, var→outcome correlations) with
   drill-down to a handful of **worst-case exemplar traces** — not raw events.
2. **The LLM proposes from a constrained, typed transformation space**, never free-
   text BPMN edits. Each transform is individually checkable:
   - parallelize provably-independent sequential tasks (split a sequence into a
     parallel gateway when there is no data dependency);
   - introduce a cache / short-circuit gateway for a pure, repeated computation;
   - swap a task's **implementation or LLM model** (cheaper/faster model where the
     quality bar still holds);
   - tune **retry / timeout / boundary-timer** parameters;
   - **batch** compatible jobs.
   These change *performance*, not business outcomes — which is the whole point.
3. **A static-analysis verification gate runs before any canary deploys.** It must
   prove the candidate is (a) **sound** (no deadlock, token-safe, no unbounded
   loop) and (b) **data-flow equivalent to the incumbent modulo the intended
   change** — i.e. for the same inputs it reaches the same business outcomes, only
   faster/cheaper. The §6 lineage + the deterministic replay (§4) make this
   checkable: prove it statically where possible, and **back it empirically** by
   replaying recorded bundles and diffing outcomes. **The verifier and the
   statistics are authoritative; the LLM is only a hypothesis generator.**

Cross-version element anchoring (so "task A in v3" maps to "task A in v4" after a
restructure): primary key is BPMN `element_id`; when a transform renames/splits an
element it emits an **anchor map** (old id → new id(s)) so trace comparison and
attribution survive the edit.

## 9. Control plane — closing the loop over time

- **Policy object:** *"optimize process X for cost, subject to p99 < N and
  incident-rate < M; autonomy = suggest | canary | auto; blast-radius = 5%;
  rollback-on regression."* Autonomy defaults to **suggest/canary with a human
  gate**; full auto is opt-in per process.
- **Feedback ledger:** every cycle records `hypothesis → evidence → transform →
  experiment outcome → decision`, immutably, linked to the deployed version. This
  is the **"over time"** capability: the system accumulates priors, stops re-testing
  dead hypotheses, and produces an audit trail for *why* a process changed — which
  is mandatory for autonomous edits to real business processes.

## 10. Staged rollout

Each stage is independently useful and strictly enables the next.

| Stage | Ships | Depends on | Value on its own |
|------:|-------|-----------|------------------|
| **T1** | **Trace export (Tier A)** off the existing exporter → native JSON + OTel spans; per-element metrics | exporter (exists) | observability, debugging, demos |
| **T2** | **Recorded-input replay (Tier B)** + a CLI/replay endpoint | T1, the `engine.with` choke point | time-travel debug, exact reproduction |
| **T3** | **Simulation harness** (WASM ephemeral engines, virtual clock, trace-driven + distributional) | T2 | "what-if" before prod |
| **T4** | **Cost model** (annotation + realized `__cost`) + lineage in the trace | T1 | the objective function; cost dashboards |
| **T5** | **Canary router** + guardrails/auto-rollback + shadow | versioning (exists), T1/T4 | safe A/B of process versions |
| **T6** | **Verification gate** (soundness + replay-backed equivalence) + typed transform space | T2/T3 | autonomy becomes safe |
| **T7** | **Reasoning + control plane** (report API, policy, feedback ledger) | T3–T6 | the closed loop |

**Start with T1.** It is the lowest-risk, highest-leverage step — largely surfacing
the event-sourced core that already exists — improves debugging and demos
immediately, and *every* later capability is downstream of being able to faithfully
record what happened.

## 11. Invariants this must not break

- **`engine-core` stays lean and dependency-free.** Trace/replay/cost/sim live in
  the **host** (server) and tooling, hanging off the existing exporter and the
  `engine.with` actor seam. The pure `(state, command, now) → events` function is
  untouched — it is the very property that makes replay and simulation exact.
- **`generated/` stays byte-for-byte from `spec/`**; any new REST surface (e.g. a
  trace/replay endpoint) is a nanobpmn extension applied via `spec-patches`, or
  lives under the console-gated `/console/api` namespace, never hand-edited.
- **Zero hot-path cost when disabled.** Tier A is async (already off the commit-ack
  path); Tier B and cost capture are opt-in per process/tenant. A deployment that
  uses none of this performs identically to today.
- **Determinism is sacred.** Nothing may introduce a wall-clock read or hidden I/O
  into the engine; all time enters via injected `now`. This is non-negotiable —
  every counterfactual depends on it.

## 12. Open questions / risks

- **Semantic-preserving optimization is the crux.** Most "optimizations" change
  outcomes, not just performance; the provably-safe subset (§8) is narrower than it
  first looks. Constrain the transform space hard and let the verifier gate it.
- **Confounding & nonstationarity.** Real environments drift; require proper
  experiment design (holdouts, sequential tests, variance reduction), not before/
  after deltas.
- **Cost attribution.** Realized cost (LLM tokens, external API $) must be measured
  (§6), or the objective is meaningless.
- **Side effects under shadow/canary.** Idempotency keys / dry-run worker contracts
  are mandatory before mirroring real traffic.
- **Autonomy is high-stakes.** Default to suggest/canary + instant rollback +
  immutable audit; full auto only where the verifier + simulation give high
  confidence and the blast radius is bounded.
- **Trace volume & privacy.** Tier A at 28k inst/s is a lot of spans — sample,
  aggregate-by-default, store exemplars; Tier B holds business data — opt-in, TTL,
  redaction.

---

*This document proposes a direction and the seams to build it on. It changes no
code. The first concrete step (Stage T1) is to add a trace-projection consumer on
the existing `Journal` exporter that folds events into the §3 schema and exposes it
as OTel spans + a `/console/api` trace endpoint.*
