# ADR 0060 — Institutional memory: model-grounded context for agent sessions

Status: Proposed
Date: 2026-08-17
Relates to: ADR 0051 (agent authoring brief / `/agent` + `/llms.txt`), ADR 0053 (derivation is a shared library), ADR 0040 (fused domain model / `nano:meta`), ADR 0033 §3 (worker I/O deriver), ADR 0056 (agent relay / command-stream plane), ADR 0059 (supervisor enrolment), ADR 0061 (ecosystem knowledge plane — the budgeted retrieval boundary; *planned/TBD, not yet merged*)
Repo: nanobpm/nano-bpm (`server/src/console/agent_brief.rs`, `spec-app/`), nanobpm/nano-ide (`packages/urban`), nanobpm/nano-workforce (retro / feed-forward)

## Context

Coding agents solved *generation*. The residual bottleneck is **retrieval of institutional
knowledge**: an agent (or a newly-onboarded engineer) makes decisions that are technically correct
but operationally wrong because it does not know *the system it is working inside* — who owns the
upstream service, what depends on this process, why it was built this way. That knowledge is not
missing; it is scattered across chat threads, stale READMEs, and the heads of the three people who
built the service. This is a **retrieval problem, not a documentation problem** — the framing
Spotify's **Xirp** (an "agentic development environment with institutional memory," built on the
Portal/Backstage catalog) makes explicit. Xirp grounds every agent session in the service catalog:
ownership, dependencies, and architectural decisions, in-context, every session.

Nano already has most of the substrate Xirp assembles, but wired for the *node/platform*, not for
*an app's own domain*:

- **`/agent` + `/llms.txt`** (ADR 0051, `server/src/console/agent_brief.rs`) render a **live**,
  node-scoped agent brief: how Nano works, how to author and link an app, this node's live facts
  (installed packs, scaffold templates, endpoints). It grounds an agent in *the platform*. It does
  **not** describe the domain of any *installed app* — its processes, service ownership, the
  service-task call graph, or the decisions the app encodes.
- **Urban Gen derivers** (ADR 0053; nano-ide `packages/urban/src/toolkit/derivers/`) already scan an
  app's BPMN models to emit typed artifacts: `worker-io.d.ts` (service-task task types + data
  envelopes, ADR 0033 §3), `meta.ts` (model-level `nano:meta` key/values, ADR 0040 §5), message and
  domain bindings. **All the raw material for a domain-level system brief is already parsed** — it is
  simply emitted as *types*, never as *agent-readable context*.
- **nano-workforce** already runs a retro / feed-forward loop (L1 process-signal retro; L2
  domain-signal plane; the feed-forward bridge), i.e. "every session should make the next one
  smarter." But that loop reasons over *process signals*, blind to the *artifact structure* of the
  app the epic touched, and its output is not surfaced as grounding for the next planning wave.

So the gap is precise: **an agent working on a Nano app starts blind to that app's own system
model.** It sees the file it is editing; it does not see the system that file is part of. Every
session re-discovers ownership and dependencies from scratch, or guesses.

We considered three non-starters:

1. **Hand-authored per-app docs.** This is the exact anti-pattern the retrieval framing rejects:
   READMEs and architecture diagrams go stale while the real work moves. A brief must be *derived*
   from the source of truth (the models + manifest), not written and left to rot.
2. **Bolt ownership/deps onto the node `/agent` brief.** The node brief is node-scoped and live; an
   app's system model is app-scoped and should travel *with the app* (it is deployed, versioned, and
   moved between nodes). Cramming per-app domain context into the node brief couples them and does
   not survive the app moving to another node.
3. **A bespoke catalog service (a Nano "Portal").** Overkill, and it recreates a second source of
   truth that drifts from the models. The models *are* the catalog; we should read them, not mirror
   them.

## Decision

Add an **institutional-memory layer** with three parts. It reuses the derivation-is-a-shared-library
discipline (ADR 0053) so the system brief is a *derived artifact*, kept honest by the same
`urban gen --check` drift gate as every other generated file.

### 1. A model-analysis deriver: `system-brief`

A new pure Urban Gen deriver (nano-ide `packages/urban/src/toolkit/derivers/system-brief.ts`,
peer to `worker-io.ts` / `meta.ts`) folds the app's BPMN models + manifest into **two artifacts**:

- `nano-generated/system-brief.md` — an **agent- and human-readable** brief of the app's domain: its
  processes, the service-task call graph (task type → worker → downstream), the decisions
  (`businessRuleTask` → DMN) it encodes, its messages/correlation surface, and the **ownership /
  dependency / decision** context it can extract.
- `nano-generated/system-brief.json` — the same content **machine-readable**, so the runtime and the
  retro/feed-forward loop can consume it without re-parsing prose.

The deriver reads, from data **already scanned** by the sibling derivers plus a small extension:

- **Processes & call graph** — from `scanModelWorkers` (worker-io.ts): every service task's task
  type + in/out envelope, grouped by process; the implicit dependency edges (this process's tasks →
  the task types they invoke).
- **Decisions** — `businessRuleTask` + `zeebe:calledDecision` per process (a small scan mirroring the
  worker scan).
- **Messages / correlation** — from the message-bindings scan.
- **Ownership & "why"** — from **`nano:meta`** (ADR 0040 §5) under reserved keys the deriver
  recognises: `owner`, `team`, `slack`, `runbook`, `adr` (one or more ADR references), `since`. These
  are authored in the model (the source of truth), so they travel and version with it and cannot go
  stale independently. Absence is fine — the brief degrades gracefully to structure-only.

The deriver is **pure** (models in, artifacts out) and therefore testable and drift-checkable,
exactly like its siblings; it is wired into `gen.ts` alongside the others and emitted into the same
`nano-generated/` domain.

### 2. Serve it: a per-app `/app/agent` brief (mirror of the node `/agent`)

The runtime serves `system-brief.md` at **`/app/agent`** (and the JSON at `/app/agent.json`), the
app-scoped analogue of the node's `/agent` (ADR 0051). The node `/llms.txt` and `/agent` gain a
**forward link** to each installed app's `/app/agent`, so an agent that lands on a node can discover
"and here is the *domain* brief for each app running on it." This closes the discovery loop: node
brief → app brief → the specific process/decision the agent must touch, all live, all derived.

### 3. Feed it back: the system brief is context for planning & retro

The machine-readable `system-brief.json` becomes a **first-class input** to two existing loops:

- **Session grounding.** When an agent (nwf worker or an interactive session) opens work on an app,
  the app's `system-brief.json` is injected into the prompt as *system context* — the Xirp move:
  "every session starts grounded." No agent re-discovers ownership/deps from scratch.
- **Retro / feed-forward (nwf).** The L2 domain-signal plane consumes `system-brief.json` so retro
  can reason over *artifact structure* (which process/decision/owner an epic touched), not only
  process signals — and the next planning wave is seeded with it. This is the "living documentation,
  built automatically, fed into the next session" property, but sourced from the *models* (which
  cannot lie) rather than from prose an engineer was asked to stop and write.

### Scope / non-goals

- **No new catalog service, no new datastore.** The models + manifest are the catalog; the brief is
  derived, gitignored (`nano-generated/`), and regenerated. One source of truth.
- **Engine and Falcon untouched.** This is a toolkit + runtime + app-tier concern, consistent with
  ADR 0056/0059 keeping the agentic plane out of the engine.
- **`nano:meta` ownership keys are optional and additive.** Structure (call graph, decisions,
  messages) derives with zero authoring; ownership/"why" enriches it when present.

## Consequences

- **Positive — agents start grounded.** Every session on an app carries that app's system model:
  ownership, dependency edges, decisions, and the "why" — the Xirp value proposition, on substrate we
  already own (models + `/agent` + derivers + retro plane).
- **Positive — no staleness.** The brief is derived and drift-checked (`urban gen --check`), so it
  cannot rot the way a README does; if the model changes and the brief is not regenerated, CI fails.
- **Positive — reuses existing machinery.** A new deriver peer to `worker-io`/`meta`, a runtime route
  mirroring `/agent`, and a new input to the existing nwf retro/feed-forward loop. No new subsystem.
- **Positive — closes the nwf L1→L2 gap.** Retro gains artifact-structure awareness without asking
  anyone to author docs.
- **Cost — reserved `nano:meta` keys become a soft contract.** `owner`/`team`/`adr`/… acquire meaning
  the deriver depends on; needs documenting in the app schema guidance and the agent brief. Low risk
  (additive, optional).
- **Cost — prompt budget.** Injecting `system-brief.json` per session costs tokens. Mitigated by the
  brief being *derived and compact* (call graph + owners, not full model XML) and by injecting only
  the sub-graph relevant to the touched process where the runtime can scope it.

## Open questions

1. **Scoping the injection.** Whole-app brief per session, or only the sub-graph reachable from the
   process/service the session touches? Start whole-app (simple, compact); add reachability-scoping
   if prompt budget bites. (The budgeted, placement-aware retrieval boundary this shares with all
   agent prompts is specified in ADR 0061 §4 — *planned/TBD, not yet merged*.)
2. **Ownership vocabulary.** Which reserved `nano:meta` keys are blessed (`owner`, `team`, `slack`,
   `runbook`, `adr`, `since`) and how are multi-valued ones (multiple ADR refs) encoded — repeated
   `nano:meta` vs. a delimited value? Lean on repeated entries (matches the existing scan, no new
   parse rule).
3. **Cross-app dependency edges.** The call graph is within-app today (task types the app's own
   processes invoke). Should the brief resolve edges that cross into *other installed apps'* task
   types (a node-level, cross-app dependency graph)? Relates to ADR 0059 (the node already enumerates
   installed apps); a natural follow-up, out of scope here.
4. **Retro authorship of "why".** Should the nwf retro loop be allowed to *write back* discovered
   ownership/"why" as `nano:meta` on the model (closing the loop: sessions enrich the catalog), or is
   the brief strictly read-only over author-supplied meta? Write-back is powerful but mutates the
   source of truth from an automated loop — defer until the read path is proven.
