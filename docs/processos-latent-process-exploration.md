# ProcessOS — Latent Process Optimization Exploration

> Status: analysis / direction only. No code changes are made by this document.
> Companion to `docs/process-optimization-design.md` (the "what/why" of runtime
> process optimization) and `docs/processos-design.md` (where the optimizer
> lives). This document widens the lens from the two levers in flight today —
> **cost** and **latency**, with LLM tasks routed to different models/providers —
> to the broader space of optimization *patterns*, the *data* needed to assess
> each one in a scenario, and the harder question: **how do we discover
> optimization patterns we did not hand-code?**
>
> Grounded in: the canonical execution trace (`process-optimization-design.md`
> §3), the cost/value model (§6), the typed transform space (§8), deterministic
> recorded-input replay (Tier B, §4), the WASM/native simulation harness (§5,
> §7), and the ProcessOS internal architecture (`processos-design.md` §2, §7).

## 0. Framing: the safety spectrum

Optimization is not turning the engine into a model router. It is turning
ProcessOS into a *process optimizer*. The starting observation: today we tune
**cost** and **latency**, and LLM-executed tasks can be routed to different
models or providers as their prices and speeds change. (That routing could even
live client-side — workers calling an organisation router that picks an executor
— rather than in the process. The pattern is the same either way: the process,
or a layer under it, chooses a cheaper/faster executor for a task whose business
outcome is unchanged.)

Every pattern below sits somewhere on a **safety spectrum**, and where it sits
dictates both the autonomy it earns and the data it costs to assess:

- **Performance-preserving** — provably the same business outcome
  (`process-optimization-design.md` §8's safe subset). Can be auto-applied behind
  the verifier.
- **Quality-affecting** — changes outcomes within a tolerance (model swaps,
  auto-approval, sampling). Needs a *correctness signal* + canary + human policy.
- **Side-effecting** — touches external state (speculation, hedging, batching of
  writes). Needs idempotency / dry-run worker contracts first
  (`processos-design.md` §1.3, §9.4).

The data you must collect grows as you move down that list. Keep this mapping in
mind throughout — it is the through-line that connects §1 (patterns), §2 (data),
and §3 (discovery).

A generative observation that runs through the whole catalogue: **most known
optimization patterns are compiler, database, distributed-systems, or queueing
optimizations applied to a BPMN graph.** §8's existing list (parallelize, cache,
model-swap, retry/timeout tune, batch) is exactly that — compiler/DB passes over
a process. That analogy is the key both to enumerating patterns (§1) and to
discovering new ones (§3.5, cross-domain transfer).

## 1. The optimization pattern catalogue

Grouped by family. The flag in parentheses is the dominant point on the safety
spectrum; many patterns can sit in more than one class depending on the task.

### A. Control-flow / structural (mostly performance-preserving)

- **Parallelize** provably-independent sequential tasks (in §8 today).
- **Reorder / fail-fast ("predicate pushdown"):** move a cheap, highly-selective
  discriminator earlier so expensive work is skipped when its result would be
  thrown away.
- **Speculative / eager execution:** start a likely branch's *pure* work before
  the gateway resolves; discard on mispredict. Latency↓, cost↑ — side-effect-free
  tasks only.
- **Short-circuit / early-exit:** when an early result already determines the
  outcome, skip the remainder of the path.
- **Common-subexpression elimination:** two tasks computing the same pure thing →
  compute once, reuse.
- **Loop-invariant hoisting:** pull work that does not change out of
  multi-instance / loop scopes.
- **Dead-branch / redundant-gateway pruning:** paths never taken in practice.
- **Sync → async + late join:** overlap an independent external call with
  downstream work instead of blocking on it.
- **Subprocess inline / extract:** inline hot tiny subprocesses; extract cold
  ones for clarity and independent scaling.

### B. Data / computation (performance- or quality-affecting)

- **Memoization / caching** of pure repeated computation, keyed on the input
  tuple (in §8 today).
- **Batching** compatible jobs to amortize a *fixed* per-call cost (in §8 today).
- **Prefetch / precompute** data a downstream task will need, in parallel with
  current work.
- **Payload reduction** (compress / project variables): variable size drives both
  broker and worker cost.
- **Approximation / strength reduction:** a cheaper computation where the
  precision bar still holds (the model-swap we do today is the special case).
- **Request coalescing / dedup:** identical external calls within a window
  collapse to one.

### C. Routing / sourcing (quality-affecting — today's focus, generalized)

- **Model / provider routing on cost / latency / quality** (have it).
- **Cascades / tiered routing:** cheap model first, escalate to the expensive one
  *only* on low confidence — often the single biggest LLM-cost lever.
- **Difficulty-gated routing:** estimate input hardness, route hard cases up.
- **Geo / residency / spot-vs-on-demand routing.**

### D. Reliability / tail (performance- and side-effect-affecting)

- **Retry / timeout / boundary-timer tuning** (in §8 today): set the timeout at a
  real latency percentile, not a guess.
- **Hedged requests:** fire to two providers, take the first — cuts the p99 tail
  at marginal extra cost.
- **Circuit breaker / fallback path:** a degraded-but-cheap service when the
  primary is slow or flaky.
- **Idempotency-key insertion:** a *meta-pattern* — it does not optimize on its
  own, it *enables* safe retry / hedge / speculation.

### E. Scheduling / capacity (performance-preserving, infra-level)

- **Worker-pool sizing & concurrency** per job type (the queue-vs-service split
  tells you whether this even helps).
- **Priority scheduling** for SLA-critical instances.
- **Admission control / load-shedding** of non-critical work under pressure.
- **Affinity routing** to warm-cache / warm-model workers.

### F. Temporal / arrival-shaping

- **Off-peak deferral** of non-urgent work (cost arbitrage).
- **Debounce / coalesce** duplicate triggers.
- **Window-batching** of timer-driven work.

### G. Business-policy / human (outcome-affecting — highest autonomy bar)

- **Auto-approval thresholds:** skip manual review under a risk bound.
- **Sampling-based QA:** sample instead of reviewing 100%.
- **Escalation / deadline tuning.**

These need ground-truth outcomes and a human policy gate; they are the patterns
where the verifier alone is insufficient.

## 2. What data each pattern needs to be *assessable*

The §3 canonical trace already provides the *diagnosis* layer: per-element
**queueMs vs serviceMs** (broker wait vs real work), incident / attempt counts,
the realized **path**, **variable lineage** (`varsRead` / `varsWritten`), and §6
realized **cost** (usd / tokens / model). That is enough to *diagnose* where a
process hurts. It is **not** enough to decide *applicability and expected gain*
per pattern. The cross-cutting gaps:

| Signal | Unlocks which patterns | Status today |
|---|---|---|
| **Side-effect / purity profile per task** (pure / idempotent / external-mutating) | parallelize, cache, speculate, hedge, reorder — *all the safety proofs* | **Missing.** Variable lineage shows data deps but not external writes. Needs a worker-declared or observed effect class. |
| **Quality / correctness ground-truth** (was the outcome *right*?) | every quality-affecting swap: model tier, cascade, auto-approval, sampling | **Missing & hardest.** Often *delayed* (rework, chargebacks, complaints). Needs joining traces to downstream business outcomes with lag. |
| **Full distributions, not means** (per-element latency/cost histograms, queueMs vs serviceMs, branch probabilities, arrival process) | timeout/retry (percentile), hedging (tail shape), batching (arrival rate), scheduling | Partly in §3 aggregations; need **tails (p99/p999)** and **branch probabilities conditioned on inputs**. |
| **Input feature vectors** (the variable *values*, not just hashes) | difficulty-gated routing, reorder selectivity, auto-approval feature→decision learning | **Gated by privacy** — §4 stores lineage + hashes by default; learning routing needs the values (Tier-B, opt-in, redacted). |
| **Cross-instance joins** (input-tuple recurrence; job co-occurrence; provider-failure correlation) | caching (hit-rate), batching (compatible-job density), hedging (are provider failures independent?) | **Missing** as a first-class projection. |
| **Cost decomposition** (fixed vs marginal; $ vs tokens vs latency) | batching (only helps with a fixed component); approximation (needs the marginal curve) | §6 has $/tokens/latency; needs the **fixed/marginal split**. |
| **Alternative-option priors + cheap probes** (cost/latency/quality of the option you *didn't* pick) | all routing / swap patterns — you cannot rank an alternative you have never observed | §6 annotation layer + **shadow a sample** through alternatives. |
| **Contention context** (concurrency, queue depth, CPU at the time) | distinguishes "queueMs fixable by scaling" from "needs a structural change" | **Missing** time-correlated resource snapshot. |

The shape of it: the **diagnosis** data we largely have; the **safety** data
(purity / side-effects) and the **outcome-truth** data (correctness, with delay)
are the two big investments — and they are exactly the data that lets a pattern
graduate from "suggest" to "auto."

## 3. Discovering latent patterns

Do not hand-curate the catalogue forever. Mine new transforms from the corpus
and the simulator. Five complementary engines, unified by one definition:

> **A pattern = ⟨precondition predicate over trace + structure, a typed graph
> rewrite, an expected-effect model, a safety proof obligation⟩.** "Discovering a
> pattern" = learning a new such tuple that *generalizes* across scenarios, not a
> one-off edit.

### 3.1 Static analysis enumerates candidates for free

Data-flow + effect analysis over the BPMN *proves* opportunities with no runtime
data at all: independent tasks (parallelizable), pure repeated subexpressions
(cacheable), loop-invariant work (hoistable), unreachable branches (dead),
redundant gateways. Static passes give the *candidate set*; runtime cost data
merely *prioritizes* it by realized impact. This is the cheapest discovery
channel and it is provably safe — it dovetails with §8's soundness gate.

### 3.2 Corpus / motif mining — learn rewrite rules from the population

Represent each process as a graph annotated with execution stats. Mine
frequent subgraph **motifs that correlate with bad outcomes** (high queue,
incidents, cost) — and motifs *elsewhere* that do the same job better. A new
transform is then "rewrite motif X → motif Y," where Y was observed (potentially
cross-tenant) with better stats. Production's natural variation is a giant
experiment that already ran: **cluster instances of the same logical job and find
Pareto-dominated clusters.** The delta between a dominated cluster and a
dominating one *is* a latent, discoverable optimization ("path A is always
cheaper for the same outcome — why does anyone take B?").

### 3.3 Simulate-and-generalize — the meta-loop

Use Tier-B replay (§4) + the SimRunner (§7) as a cheap experiment generator:
apply structured perturbations from the typed transform DSL and measure their
effect on the objective across *many* scenarios. Edits that repeatedly win get
**anti-unified** — generalize the concrete successful edits into a parametric
rule. A candidate pattern only "graduates" into the library if it (i) passes the
verifier (outcome-preserving) and (ii) yields gains over a *distribution* of
cases, not a single one. The §9 feedback ledger
(`hypothesis → evidence → transform → experiment → decision`) is the substrate;
survivors are promoted to named transforms with their precondition attached as
the applicability test.

### 3.4 LLM as pattern *proposer*, harness as *validator*

Point the LLM at the §3 performance report + the existing transform library and
ask it to **generalize new transform schemas** — not one-off BPMN edits. Each
proposed schema is auto-applied across scenarios in the SimRunner and gated by
verify + statistics. This preserves the load-bearing one-way boundary
(`processos-design.md` §8): the LLM is a hypothesis generator; the verifier +
SimRunner + sequential tests decide. The LLM's real leverage here is **cross-
domain transfer** — systematically porting compiler / database / distributed-
systems / queueing optimizations into BPMN rewrites.

### 3.5 Diagnosis taxonomy as an index — and a gap detector

Classify *why* each element is bad, and let the class **index into the applicable
pattern family**:

- queue-bound → scheduling / capacity (E)
- service-bound → swap / cache / approximate (B, C)
- retry-bound → reliability (D)
- branch-misprediction-bound → reorder / speculate (A)
- payload-bound → payload reduction (B)

A diagnosis with **no matching pattern** is precisely the signal to *invent* one
— route it to §3.3 / §3.4. Complement this with **sensitivity sweeps**: vary each
tunable (timeout, retry, batch size, concurrency, model) in the SimRunner and
build response curves. Flat regions = no lever; steep regions = high-value lever;
curves that only move *together* reveal pattern **interactions** you would never
hand-specify.

## 4. Synthesis — assessing applicability in a scenario

For a given scenario, compute an **applicability score** per known pattern:

```
score = precondition_match(collected data)
      × expected_gain(priors / sim)
      × safety_confidence
```

That yields a ranked backlog of candidate optimizations per process — exactly the
"Insights → hypotheses" output ProcessOS wants (`processos-design.md` §7.2 step
2). Patterns whose preconditions *cannot be evaluated* because a signal is
missing tell you **what to instrument next** — the data backlog falls out of the
pattern backlog for free.

### 4.1 Highest-leverage data investments

If forced to rank the data work by how much of the pattern space it unlocks:

1. **A per-task side-effect / purity profile.** It is the safety gate for nearly
   every structural pattern (A) and for speculation / hedging / cache (B, D).
   Without it those patterns can be *proposed* but never *auto-applied*.
2. **A delayed correctness / quality signal joined back to traces.** Without it,
   every quality-affecting routing decision (C, G) is flying blind, and — more
   dangerously — the *discovery* engines (§3.2–§3.4) cannot tell a real win from
   an undetected outcome change.

Everything else (distributions, tails, cross-instance joins, contention context)
is a projection off the existing event stream and the §6 cost channel.

## 5. The load-bearing caveat

Discovery engines §3.2–§3.4 are only as trustworthy as the verifier and the
*outcome-truth* data behind them. A mined "win" that is really an undetected
outcome change is the dangerous failure mode of autonomous pattern discovery. So
the correctness signal (§4.1 item 2) is not merely a routing input — it is the
foundation that makes *discovering* patterns safe at all. This is the same
invariant as `process-optimization-design.md` §8 and `processos-design.md` §8:
**the verifier and the statistics are authoritative; the LLM — and any mining
heuristic — only proposes.**

---

*This document widens the optimization design's transform space (§8) into a
pattern taxonomy, maps each pattern to the data needed to assess it, and proposes
a discovery loop for patterns we did not hand-code. It changes no engine code;
the Nano-side additions it implies (richer trace signals, a side-effect/purity
profile, an idempotency/dry-run worker contract) are all ProcessOS-driven
extensions to Nano's public surfaces, never engine-core changes — preserving the
one-way dependency of `processos-design.md`.*
