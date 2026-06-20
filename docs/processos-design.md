# ProcessOS — Design Proposal

> Status: draft / direction. Companion to `docs/process-optimization-design.md`
> (the "what" and "why" of runtime process optimization). This document is the
> "where it lives": ProcessOS is the **separate component** that hosts the
> optimization loop, so that **Nano handles production and ProcessOS handles
> optimization** — a clean separation of concerns.

## 0. Thesis

Nano is a lean, low-resource, WASM-able BPMN engine on the production hot path. The
optimization loop (reasoning, simulation, experiment statistics, an LLM, static
analysis, cost/feedback ledgers) is heavyweight, network-egressing, and key-holding.
Bolting it into the gateway would forfeit the very property that makes Nano
interesting. So it lives in its own crate with its own webserver:

```
                 public contracts only (HTTP)
   ┌──────────┐  ───────────────────────────▶  ┌──────────────┐
   │   Nano   │   read:  trace + metrics export │  ProcessOS   │
   │ (engine, │   write: deploy + routing table │ (optimizer,  │
   │ gateway) │  ◀───────────────────────────   │  webserver)  │
   └──────────┘                                  └──────────────┘
   handles PRODUCTION                            handles OPTIMIZATION
```

**The dependency is one-way and enforced as a build rule.** ProcessOS depends on
Nano's public surfaces; Nano never depends on ProcessOS and builds/runs with it
absent. The single engine-side concession is the **policy-free routing primitive**
(see `process-optimization-design.md` §7) — cohort→version selection at
`CreateInstance`, configured by a routing table ProcessOS edits over the public
control endpoint. Nano stores and applies a table; it has no concept of
"experiment", "candidate", "guardrail", or "rollback".

## 1. Boundary — the contracts (design these first)

Everything ProcessOS does crosses one of three contracts. Pin them, version them
(`X-ProcessOS-Contract: 1`), and treat them as the API the two components evolve
against independently.

### 1.1 Read contract (Nano → ProcessOS)

Already exists or is the natural extension of Stage T1:

- **Trace export** — `GET /console/api/traces` (list), `/console/api/traces/{key}`
  (the §3 canonical trace), `/console/api/traces/{key}/otel` (OTLP/JSON spans).
  Plus a **streaming** form for ingest (SSE/WS tail of the exporter, or an OTLP
  push endpoint ProcessOS exposes and Nano's exporter is pointed at). ProcessOS
  prefers *push* so it never polls the hot path.
- **Metrics** — `GET /console/api/metrics`, `/console/api/cluster/metrics`
  (throughput, p99, active instances, per-node).
- **Process catalogue** — the existing Camunda `process-definitions` + the
  console models API (versions, deployed XML via `/v2/process-definitions/{k}/xml`).
- **(T4) Realized cost** — the `__cost` lineage carried in the trace (token spend,
  external-API $), per element/version.

ProcessOS treats these as **read-only and authoritative**. It never reaches into
Nano's journal, read DB, or engine state directly.

### 1.2 Control contract (ProcessOS → Nano)

Only two verbs, both already-public or thin nanobpmn extensions:

- **Deploy** — the standard Camunda `POST /v2/deployments` (idempotent; a changed
  model is a new version). This is how a candidate process gets onto the cluster —
  *exactly the path a normal client uses*, no backdoor.
- **Routing table** — a new policy-free control endpoint (nanobpmn extension under
  `/console/api/routing` or via `spec-patches`). Body is plain data:
  `{ processId, rules: [{ cohort: <predicate|weight>, version }], default }`.
  Setting 5% → candidate is a `PUT`; auto-rollback is a `PUT` back to 100%
  incumbent. Journaled, durable, failover-safe. **This is the only new write
  surface the optimization story adds to Nano.**

### 1.3 No fourth contract

ProcessOS does **not** get privileged access. If it needs something Nano can't yet
express (e.g. shadow-execution dry-run worker semantics, idempotency keys), that is
a *Nano public-API* addition designed on its own merits, not a ProcessOS hook.

## 2. Internal architecture (inside the ProcessOS crate)

A pipeline of independently-testable stages, mirroring the optimization design's
Tiers and §6–§9:

| Stage | Module | Responsibility | Leans on |
|------:|--------|----------------|----------|
| Ingest | `ingest` | Subscribe to the trace/metrics push stream; normalize to the §3 schema | read contract |
| Store | `store` | Durable ledgers: traces/exemplars, cost, **feedback ledger** (`hypothesis→evidence→transform→experiment→decision`), routing history | `rusqlite` (bundled), like the gateway |
| Report | `report` | Token-budget-aware **performance report** per version: bottleneck path, incident clusters, queue-vs-service split, var→outcome correlation, worst-case exemplars | store |
| Simulate | `sim` | Ephemeral engines on virtual clock: trace-driven replay (Tier B) + distributional "what-if" | **`engine-core`** (native) / `engine-wasm` |
| Reason | `reason` | LLM **hypothesis generator** over a constrained **typed transform space**; emits candidate model + anchor map | pluggable LLM provider |
| Verify | `verify` | Static soundness (no deadlock, token-safe, bounded) + **replay-backed equivalence** (same inputs → same outcomes, faster/cheaper). Authoritative gate. | `sim`, `engine-core` |
| Experiment | `experiment` | Compile a verified candidate into routing-table edits; run canary/shadow; **sequential tests/CUSUM** guardrails; auto-rollback | control contract |
| Control | `control` | Per-process **policy** (objective + constraints + autonomy + blast radius); orchestrates the loop; writes the feedback ledger | all of the above |
| Web | `web` | axum server: ProcessOS REST API + (optionally) its SPA; serves reports, experiments, ledger | `axum` |

**Reuse, don't re-implement.** `sim` and `verify` embed `engine-core` directly
(path dep, `features=["serde"]`) for *exact* native replay/simulation — the same
pure `(state, command, now) → events` function the production engine runs, which is
what makes counterfactuals trustworthy. The browser modeler's WASM test-run
(`engine-wasm`) is the same engine; ProcessOS can also drive headless WASM for
sandboxed execution of machine-generated processes (the §5.3 safety boundary).

## 3. Tech stack & crate layout

A new top-level crate, sibling to `engine-core` / `server` / `engine-wasm` (no root
workspace — they stay independent, per repo convention):

```
processos/
  Cargo.toml          # axum 0.8, tokio 1, rusqlite 0.32 (bundled), serde,
                      # reqwest (talk to the gateway), an LLM client trait
  src/
    main.rs           # webserver bootstrap, config, the control loop tick
    contracts/        # typed clients/types for the 3 §1 contracts (versioned)
    ingest.rs store.rs report.rs sim.rs reason.rs verify.rs experiment.rs control.rs
    web/              # REST handlers (+ optional embedded SPA, rust-embed)
  Cargo.toml deps:  engine-core = { path = "../engine-core", features=["serde"] }
```

- **Webserver:** `axum` (match the gateway) on its own port (e.g. `PROCESSOS_PORT`).
- **Talking to Nano:** `reqwest` against the gateway base URL (`NANO_BASE_URL`).
- **Storage:** `rusqlite` bundled (self-contained, like the gateway's read DB) for
  the ledgers; trace exemplars + cost + feedback + routing history.
- **LLM:** a `ReasoningProvider` trait with an **OpenAI-compatible** HTTP impl, so it
  works against a **local llama.cpp** server *or* a hosted model — no hard
  dependency on any vendor. Keys/egress live entirely in ProcessOS's boundary.
- **Determinism for sim/verify:** inherited free from `engine-core` (time via
  injected `now`); ProcessOS adds no wall-clock into any replayed run.

## 4. UI / operational surface

Decision (revisitable): **console-proxy primary, standalone optional.**

- ProcessOS serves its own REST API and a small SPA, but the **primary UX is a new
  "Optimization" tab in the existing Nano console**, which proxies to ProcessOS
  (one pane of glass; the console already aggregates cluster views). If ProcessOS is
  not running, the tab shows a "not configured" state — the console never hard-
  depends on it.
- Standalone ProcessOS UI remains available for deployments that run it detached
  from the console.

This keeps operators in one place while preserving the clean process/security
separation underneath.

## 5. Deployment topology

- **Separate process / service**, co-located or remote. Scales independently of the
  gateway (the reasoning/sim plane is bursty and CPU/GPU-heavy; the gateway is the
  steady hot path).
- **Read path is push:** Nano's exporter is pointed at ProcessOS's OTLP/stream
  ingest, so optimization never adds latency or polling load to production.
- **Security boundary:** LLM keys, outbound network egress, and business-data-bearing
  Tier-B bundles live only inside ProcessOS — a different blast radius than the
  engine. mTLS / token between the two; ProcessOS authenticates to the gateway as a
  normal API client for deploy + routing writes.
- **Absent-by-default:** a Nano cluster with no ProcessOS behaves exactly as today.

## 6. Mapping to the staged rollout

ProcessOS materializes the optimization design's stages T1–T7 as it grows; each is
useful alone:

| Stage | ProcessOS gains |
|------:|-----------------|
| T1 | `ingest` + `store` + a read-only **Insights** report (surfaces existing traces/metrics) |
| T2/T3 | `sim` (native + WASM replay/what-if) — "explain this instance", "what-if this change" |
| T4 | cost ledger + objective-function dashboards |
| T5 | `experiment`: canary via the **routing-table** control endpoint + guardrails/auto-rollback (needs the §7 routing primitive in Nano) |
| T6 | `verify`: soundness + replay-backed equivalence gate; typed transform space |
| T7 | `reason` + `control`: LLM hypotheses, policy objects, the closed feedback loop |

**Start at T1**: stand up the crate, ingest the existing trace/metrics contracts,
and render a report. It is immediately useful (a richer Insights view) and forces us
to nail the read contract before anything autonomous is built on it.

## 7. Invariants ProcessOS must honour

- **One-way dependency, build-enforced.** Nano never imports ProcessOS; ProcessOS
  only touches Nano's three public contracts.
- **No privileged access.** Deploy and routing go through the same public API a
  client uses; reads go through the export endpoints. No reaching into journal/state.
- **The verifier and statistics are authoritative; the LLM only proposes.** Nothing
  reaches a canary without passing `verify`; nothing is promoted without the
  sequential-test guardrail clearing.
- **Determinism preserved.** `sim`/`verify` reuse `engine-core` with injected time;
  no hidden I/O enters a replayed run.
- **Absent-safe.** Removing ProcessOS leaves a fully functional production cluster.

## 8. Open questions

1. **Routing-table endpoint shape** — predicate language for cohorts (tenant /
   business-key hash / %): how expressive before it becomes policy-in-the-engine?
   Lean minimal (weights + a small fixed predicate set).
2. **Ingest transport** — OTLP push vs an SSE/WS tail of the console trace feed.
   OTLP is the standards-friendly choice and decouples volume from the console.
3. **Trace volume/privacy at scale** — sampling + aggregate-by-default + exemplar
   retention live in ProcessOS's `store`; Tier-B bundles need TTL/redaction here,
   not in the engine.
4. **Shadow execution prerequisites** — idempotency-key / dry-run worker contract is
   a *Nano public-API* design (§1.3), gating real-traffic mirroring.
5. **Multi-cluster** — one ProcessOS per cluster vs a fleet view; the contracts are
   per-gateway, so federation is a ProcessOS concern, not Nano's.

---

*This document proposes where the optimizer lives and the seams it binds to. It
changes no engine code; the only Nano-side addition the loop requires is the
policy-free routing primitive of `process-optimization-design.md` §7. First concrete
step: scaffold the `processos/` crate at Stage T1 — ingest the existing trace/metrics
contracts and serve an Insights report.*
