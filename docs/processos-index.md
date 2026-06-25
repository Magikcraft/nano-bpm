# ProcessOS — Design Series Index

> Status: index / reading guide. No code changes. Points at the design documents
> for **runtime process optimization** and **ProcessOS**, the separate component
> that hosts the optimization loop. All documents are analysis/direction only and
> change no engine code; every Nano-side implication is a public-contract
> enrichment, never an `engine-core` change.

## What this is

Nano BPM is a lean, low-resource, WASM-able BPMN engine on the production hot
path. **ProcessOS** is the separate, one-way-dependent component that reads Nano's
public surfaces (trace + metrics) and writes only through public contracts
(deploy + routing table, and — proposed — a scaling contract) to **optimize
processes at runtime**: an LLM proposes, a verifier and statistics decide, and the
engine stays untouched.

The series spans the full stack a running process is realized through — from the
business meaning at the top, down through the BPMN graph, the task bindings, the
worker internals, and the physical resources/deployment at the bottom.

## Read in this order

| # | Document | What it covers | Start here if… |
|--:|----------|----------------|----------------|
| 1 | [`process-optimization-design.md`](./process-optimization-design.md) | The **substrate**: event-sourced trace (Tier A), deterministic recorded-input replay (Tier B), the WASM/native simulation harness, the cost/value model, the canary/experimentation plane, and the typed **structure/binding transform space**. The "what & why." | you want the foundations and the core thesis ("the moat is not the LLM"). |
| 2 | [`processos-design.md`](./processos-design.md) | **Where the optimizer lives**: the ProcessOS crate, its three public **contracts** (read: trace/metrics; control: deploy + routing table), internal module pipeline, and the MVP evaluation harness (`SimRunner`, golden-model meta-eval). The "where." | you care about the component boundary, contracts, and crate layout. |
| 3 | [`processos-latent-process-exploration.md`](./processos-latent-process-exploration.md) | The **pattern taxonomy** (7 families, on a performance/quality/side-effect safety spectrum), the **data** needed to assess each pattern, and how to **discover patterns we did not hand-code** (static analysis, corpus mining, simulate-and-generalize, LLM-as-proposer, diagnosis-as-index). | you want the breadth of optimization patterns and how new ones are found. |
| 4 | [`processos-deployment-cooptimization.md`](./processos-deployment-cooptimization.md) | Structure, binding, and **resourcing** as coupled layers of one process→deployment mapping; the **observability/actuation regimes** (offline engine-sim / queueing-model-sim / live act-and-measure); the full layer taxonomy incl. **cross-process/portfolio** contention; and the ProcessOS additions implied (a third **scaling** control verb, a second **queueing** simulator, portfolio-scoped ingest). | you're reasoning about cluster/worker resources, scaling, and why they escape isolated-engine simulation. |
| 5 | [`processos-worker-and-semantic-layers.md`](./processos-worker-and-semantic-layers.md) | The two **ends** of the stack: **worker-internal logic** (SDK instrumentation → OTel sub-spans, auto-harvested side-effect/purity safety data, observe→configure→transform actuation) and the **business-semantic layer** (where the verifier's safety model inverts, autonomy drops to advisory, and a *grounded* LLM becomes a business-redesign copilot). | you want the bottom (worker internals) and top (business redesign) of the space. |

### Companion: implemented features

| Document | What it covers |
|----------|----------------|
| [`processos-camunda-import.md`](./processos-camunda-import.md) | **Implemented.** The `import-camunda` transformer: fold a **Camunda 8 / Zeebe** record export (Elasticsearch/Opensearch/debug-log JSON) into a Nano `traces.json` dataset, so a customer's existing C8 history loads into a workspace `DatasetSource` with no Nano engine and no Java. Includes the record→trace mapping, fidelity tiers, and the path to an in-engine Zeebe exporter. |

## The mental model that unifies them

A deployed process is a **mapping from a logical specification down to a physical
running deployment**, optimized under one policy objective and closed by a control
loop (doc 4, §0). Two axes place every layer:

- **Autonomy** (suggest → canary → auto), and
- **Observability/actuation regime** (offline engine-sim → queueing-model-sim →
  live act-and-measure).

The load-bearing insight (doc 5, §3): **autonomy and value run in opposite
directions.** The biggest prizes (business-semantic redesign) are exactly where
the decision is least automatable — so the system automates the *analysis and the
evidence*, never the *judgment*. The patterns and regimes recur **fractally**
across the process, the deployment, inside the worker, and above the process.

The stack, top (most value / least autonomy) to bottom (most autonomy):

```
  business semantics      (doc 5 §2)  advisory — human decides, numbers are evidence
  process structure       (docs 1,3)  verifier-gated transforms
  task binding            (docs 1,3)  model/provider routing, quality-gated
  worker internals        (doc 5 §1)  observe → configure → transform
  resources / deployment  (doc 4)     simulate (queueing) → canary → scale
```

## Invariants every document upholds

- **One-way dependency, build-enforced.** Nano never imports ProcessOS; ProcessOS
  touches only Nano's public contracts. Nano builds and runs with ProcessOS
  absent.
- **`engine-core` stays lean and unchanged.** Trace/replay/cost/sim/optimization
  live in the host + ProcessOS, hanging off the existing exporter and the
  `engine.with` actor seam.
- **The verifier + statistics are authoritative — within their reach.** The LLM
  (and any mining heuristic) only proposes; nothing reaches a canary without
  passing `verify`. At the semantic layer, where the verifier cannot reach,
  changes are explicitly advisory.
- **Determinism is sacred.** The logic regime is the exact
  `(state, command, now) → events` engine; the queueing regime is a *statistical*
  model, labelled as such.
- **Zero hot-path cost when disabled; absent-safe.** A cluster using none of this
  performs identically to today.

## Status at a glance

- **Implemented (per `processos-design.md` §7.4):** the `processos/` crate, the
  native `SimRunner` (M0), the baked candidate generator + ranking (M1), and the
  LLM hypothesis step (M2). Next: `ClusterRunner` (M3) + the live production path.
- **Design-only (these docs):** the deployment/resource co-optimization layer, the
  worker-instrumentation and business-semantic layers, the pattern-discovery loop,
  and the proposed scaling control contract + queueing simulator.

---

*This index is a reading guide to the ProcessOS design series. The series proposes
directions and the seams to build them on; it changes no engine code. The first
concrete build step remains Stage T1 of `process-optimization-design.md`: a
trace-projection consumer on the existing `Journal` exporter.*
