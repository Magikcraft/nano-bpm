# ADR 0059 — Supervisor enrolment: app-driven fleet configuration

Status: **Proposed.**
Date: 2026-08-13.

Relates to:
ADR 0056 (`0056-agent-relay-command-stream-plane.md`, the Nano agentic protocol — this ADR **lifts**
its per-worker `REGISTER`/`SERVE` handshake to a **per-machine, multi-app** supervisor enrolment and
names the fleet-configuration/discoverability problem 0056 left implicit),
ADR 0051 (`0051-nano-workforce.md`, the crew orchestrator — one of the two worked-example apps),
ADR 0046 (`0046-agent-as-worker-vs-agent-in-the-node.md`, **agent-as-worker** — the topology whose
workers a machine runs),
ADR 0028 (`0028-urban-app-user-auth-identity-authorization.md`, the identity a machine enrols under),
ADR 0058 (`0058-openapi-endpoint-surface.md`, the Urban **`api` primitive** — the contract-first
endpoint surface this ADR's enrol/vocab/registry endpoints are *generated* by, not hand-rolled),
ADR 0052 (`0052-urban-runtime-decoupled-manifest-interpreter.md`, the Urban runtime whose primitives
— pages, workers, connectors, `api` — this ADR joins with a generic **enrolment capability**),
ADR 0035 (`0035-observability-config-and-standalone-console.md`, runtime console profiles the
supervisor already threads through),
`c8ctl-plugin.js` in the `jwulf/c8ctl-plugin-nano` repo (the `supervisor` daemon and the `nano work`
matrix this ADR reconfigures — `jobTypeMatrix`, `diffJobTypes`, the profile-watch `reconcile` loop,
and `supervisor add`).

## Context

ADR 0056 collapses the **token** explosion — a job-type per model × per machine — by moving
capability→token resolution off the worker and into an app-tier registry: a worker declares its
capability once (`REGISTER`), the app returns the leaf tokens it should open (`SERVE`), and the worker
runs dumb 1:1 engine pollers over that (small, demand-gated) set. That solves *"what tokens does **one
worker** open?"*

It leaves a second, orthogonal problem untouched: **fleet configuration and discoverability.**

An operator installs and runs several Urban apps — the Nano Workforce crew, plus apps other authors
wrote for domains we never imagined. Each app *demands* work (its deployed models' task-definition
leaves). The operator owns a handful of machines of differing capability. Somebody has to decide **which
machine serves which app's which roles**, and keep that decision current as models change, demand
shifts, and new apps arrive.

Today that somebody is a human with `c8ctl`:

- `hire` persists a named profile (`rank`, `capabilities[]`, `model`, `command`).
- `work <name>` self-computes that profile's `rank × capability` matrix **locally** (`jobTypeMatrix`)
  and opens one long-poll poller per token.
- `supervisor add <profile> [--instances N]` runs a fleet of those `nano work` children from one
  daemon, restarting and reporting them.

Nothing in this chain knows **which app a machine is serving.** The mapping *app → roles → machines* is
hand-derived: read each app's job types, craft matching hire profiles, `supervisor add` the right
profile on the right box — for **every app × every role × every machine** — and re-derive the whole
matrix whenever a model is swapped or an app is installed. The worker-side win from 0056 does not reach
this layer: even with a perfect per-worker resolver, an operator still has to *point* each machine at
the right work by hand. Discoverability ("what can I serve, and what needs serving?") and configuration
("make this machine serve that") are manual, per-app, and per-machine.

## Decision

Add **supervisor enrolment**: an operator points a machine's supervisor at an **Urban app's enrolment
endpoint** — *"work on this"* — and the **app drives that machine's fleet configuration.** The
supervisor declares the machine's capability once, subscribes to one or more apps' demand, and
reconciles its `nano work` children to exactly the tokens those apps resolve for this machine. This is
0056's `REGISTER`/`SERVE`, **lifted from per-worker to per-machine and made multi-app.**

The engine and its transport are untouched — this is entirely app-tier, above the frozen C8 core, and
the `nano work` children remain dumb 1:1 engine pollers.

### 1. Enrol a machine against an app

```bash
c8ctl nano supervisor enrol http://host:8080/apps/nano-workforce
c8ctl nano supervisor enrol http://host:8080/apps/annotate
c8ctl nano supervisor unenrol http://host:8080/apps/annotate   # drain
```

`enrol` records the app URL in the supervisor's state and opens an enrolment session against it;
`unenrol` gracefully drains the pollers that only that app demanded. Enrolment is **per machine**, held
by the supervisor daemon — not per `nano work` child.

### 2. Declare the machine's capability once

The machine has **one capability**, not a per-role profile: `{ family, cognition, weight, host }`
(e.g. `{ family: opus, cognition: frontier, weight: heavy, host: mac-studio }`). A pre-existing hire's
`rank`/`capabilities` may *become* the machine capability, so the two models coexist during migration.
Capability is declared to the app at enrol and **never appears in a routing token** (ADR 0056 §7).

### 3. The app resolves demand × capability → served tokens

Each app is the **hub** for its own work (ADR 0056 §4): it owns a versioned **vocab artifact** (its
networks/roles/requirements) and a live **demand** view (its deployed leaves). On enrol it returns the
leaf tokens this machine both **qualifies for** (capability satisfies the role's `requires`) and the
app **currently demands** — and pushes an updated set whenever demand, the vocab, or the fleet changes.
The demand view is **not hand-maintained**: it is *derived from the app's models* by Urban Gen (§9), so
"what work exists" stays authoritative-from-model.

```
supervisor → app:  ENROL   { capability: { family, cognition, weight }, host }
app        → supervisor: SERVE  [ resolved leaf tokens for THIS machine ]   (+ live pushes)
supervisor → engine (per token): activateJobs(<token>)   # unchanged dumb pollers
```

### 4. The supervisor merges apps and reconciles its fleet

A machine may be enrolled against **several apps at once.** The supervisor **merges** every app's SERVE
list into a single deduped desired-token set and reconciles its `nano work` children to it — spawning
pollers for added tokens, draining pollers for removed tokens — reusing the **existing profile-watch
reconcile loop** (`diffJobTypes` + `reconcile`), lifted from "one profile's matrix" to "the union of my
enrolled apps' SERVE lists." A SERVE push from **any** enrolled app re-triggers the same reconcile that
a local profile edit triggers today.

This keeps hub connections at **machines × apps**, not workers × apps: the supervisor enrols on the
machine's behalf and owns the children; the children never talk to a hub.

### 5. Concurrency is the operator's call, via the supervisor

How many concurrent pollers/instances a machine runs for a served token is **decided by the operator at
the supervisor** — the existing `--instances N` / capacity knob — **not** dictated by the app. The app
expresses *demand* (a role needs serving); the operator decides *how much of their machine* to spend on
it. The app may *hint* demand depth (queue length) for the operator's benefit and for the console board,
but the supervisor is the authority on this machine's concurrency. (A future app-tier auto-scaler is an
open question, §Open questions — deliberately not now.)

### 6. Scope and trust

Enrolling against an app means running that app's demanded work, so:

- The machine enrols under its **ADR 0028 identity** plus a capability credential (the pattern the
  blackboard already uses).
- The operator may **scope/trim** what they will serve for an app (a role/prefix allow-list, a cost
  cap) — *"serve `annotate.*` but hold `label.adjudicate`."* The resolver intersects the app's SERVE
  with the operator's scope. Consistent with ADR 0056 §9 ("the operator may scope/trim the resolved
  set").

### 7. Global placement stays in the app; the supervisor is per-machine

Cross-machine concerns — **seat placement** (`#red` on a distinct machine/family from `#blue`), the
**diversity SLO**, and the **"missing agent type"** board — are **global** and live in the **app hub's
registry** (ADR 0056 §10, §11), not in any one machine's supervisor. The supervisor is a per-machine
actor that declares capability, receives SERVE, and manages children; it does not make fleet-wide
placement decisions. This is the natural home for ADR 0056's open **"mirror vs matchmaker"** question,
which multi-machine seats now force: assigning distinct-family seats across machines is app-tier
placement policy.

### 8. Discoverability: the cross-app board

Each app publishes **demand × supply** (ADR 0056 §11); the **host console aggregates across every
installed app** into one board — *"`annotate.label.adjudicate` is demanded, no frontier machine
enrolled"* in red; *"`planning.spar` seats are both `opus` — diversity degraded"* in amber. The
operator's answer to *"what needs serving, and what can I serve?"* is one screen, spanning all apps.

### 9. The endpoints are generated, not hand-rolled — scaffold + Urban Gen

The enrol/vocab/registry endpoints are a **generic Urban enrolment capability** (ADR 0056 §4 — "the
way apps get pages and workers"), delivered through the toolchain apps already use, so **no app author
writes a line of enrolment plumbing:**

- **`create-urban-app` scaffolds the OpenAPI endpoint.** A new Urban app is scaffolded with the
  enrolment `api: { spec }` fragment (ADR 0058) already wired — the enrol/vocab/registry contract the
  supervisor points at. Because the endpoint surface is generated from OpenAPI by the existing `api`
  primitive, this is just one more shipped spec fragment + its capability-provided delegates, not a new
  runtime HTTP primitive.
- **Urban Gen generates what backs the endpoint by analyzing the model.** The **demand** view (and the
  model-derived slice of the vocab — the actual task-type leaves each process demands) is produced by a
  **deriver in `urban gen`**, extending the same model-scanning pass that already exists: the `worker-io`
  deriver "scans each process for service tasks and extracts the Zeebe task type" (ADR 0033 §3 / ADR
  0053). A companion **demand deriver** folds those leaves — across every deployed BPMN *and* code-first
  `defineFlow` (which compiles to the same BPMN service tasks) — into the demand artifact the enrolment
  endpoint serves. Re-running `urban gen` after a model change re-derives demand; the endpoint reflects
  it on next `SERVE` push.

So the split is: **capability (requirements, seats, cognition) is author-declared** in the vocab, but
**demand (which leaves actually exist) is derived from the models by Urban Gen** — the same authored-
semantics / generated-DI split the toolkit already draws elsewhere. The supervisor sees one coherent
endpoint; the app author sees generated plumbing plus a small vocab they own.

## Worked example — two apps, three machines

**App A — `nano-workforce`** (the coding crew, ADR 0051). Vocab (excerpt):

```jsonc
{ "networks": {
  "planning":       { "roles": { "spar": { "requires":"frontier", "seats":["red","blue"], "seatsDistinctFamily":true },
                                  "finalize": { "requires":"frontier" } } },
  "implementation": { "roles": { "rust": { "requires":"standard", "weight":"workstation" } } },
  "qa":             { "roles": { "review": { "requires":"standard" } } },
  "ci":             { "roles": { "fix": { "requires":"standard" } } } } }
```

**App B — `annotate`** (a *third-party* dataset-labeling app — a different domain, the **same schema**;
it reuses *seats* for **inter-annotator agreement**, not sparring):

```jsonc
{ "networks": {
  "label": { "roles": {
    "triage":     { "requires":"knowledge" },
    "annotate":   { "requires":"standard", "seats":["a","b"], "seatsDistinctFamily":true },
    "adjudicate": { "requires":"frontier" } } } } }
```

**Three machines**, each with a declared capability:

- `mac-studio` — Opus 4.8, **frontier**, heavy
- `linux-box` — Kimi-K2, **frontier**, workstation
- `nuc` — Qwen-3.8 (local), **standard / knowledge**

**Enrolment** — the operator types five commands, no role→machine mapping by hand:

```bash
mac-studio$  c8ctl nano supervisor enrol http://host:8080/apps/nano-workforce
mac-studio$  c8ctl nano supervisor enrol http://host:8080/apps/annotate
linux-box$   c8ctl nano supervisor enrol http://host:8080/apps/nano-workforce
linux-box$   c8ctl nano supervisor enrol http://host:8080/apps/annotate
nuc$         c8ctl nano supervisor enrol http://host:8080/apps/annotate
```

**Resolution** — the app registries match demand × capability → what each machine serves:

| Machine (capability)          | `nano-workforce` serves                              | `annotate` serves                         |
|-------------------------------|-----------------------------------------------------|-------------------------------------------|
| `mac-studio` (opus, frontier) | `planning.spar#red`, `planning.finalize`, `decide`  | `label.adjudicate`                        |
| `linux-box` (kimi, frontier)  | `planning.spar#blue`                                | `label.annotate#b`                        |
| `nuc` (qwen, standard)        | `qa.review`, `implementation.rust`, `ci.fix`        | `label.triage`, `label.annotate#a`        |

The diversity SLO holds automatically: `spar#red = opus ≠ spar#blue = kimi`; `annotate#a = qwen ≠
annotate#b = kimi` — the registry placed distinct-family seats on distinct machines. The operator never
mapped a single role to a machine: they declared three capabilities and typed five `enrol` commands.

**Change a model tomorrow** — swap `nuc`'s local model, or replace `linux-box`'s Kimi with Qwen: re-declare
that machine's capability. *Every* enrolled app re-resolves live; the BPMN in either app does not change,
and no other machine changes. **Install a third app** — `supervisor enrol <url>` on whichever machines
should help; its demand appears on the board, and qualifying machines start serving it.

## Endpoint contract (app-side)

These endpoints are **not hand-written per app** — they are generated (§9): a **generic Urban enrolment
capability** ships the endpoints, the registry DataLayer schema, the capability→token resolver, the live
SERVE stream, and the demand×supply page, so an author supplies **only its vocab artifact** (and its
models, from which demand is derived). The capability is built on the existing Urban **`api` primitive**
(ADR 0058): the enrol/vocab/registry contract is a shipped OpenAPI fragment the runtime already mounts +
validates under a framework-constant prefix, with capability-provided delegates behind each
`operationId` — **no new bespoke HTTP-routing primitive** in the runtime, just a packaged `api` surface
+ DataLayer migration + resolver + page + Urban Gen deriver, versioned with the capability.

```
GET  /apps/<app>/vocab                    → { networks, requirements, version }
POST /apps/<app>/enrol { capability, host }
     → { serve: [tokens], demandVersion, leaseTtl }   + an SSE/WS stream of SERVE pushes
GET  /apps/<app>/registry                 → demand × supply (for the console board)
```

Because the mount is a **framework constant** (like `/app/api`), the supervisor points at
`http://host:8080/apps/<app>/` and hits the well-known enrolment path — no per-app URL scheme to guess,
which is also what makes an app **directory** (Open questions) a thin add: list installed apps, each
already exposes the same contract. `enrol` is idempotent per (app, machine); the stream delivers a
fresh `serve` set on any demand/vocab/fleet change. The supervisor holds one session per enrolled app,
intersects each `serve` with the operator's scope (§6), unions across apps, and reconciles.

## Consequences

- An operator configures a fleet by **declaring machine capabilities and pointing supervisors at apps**,
  not by hand-maintaining an *app × role × machine* matrix. Adding an app or swapping a model is a
  one-line change that re-resolves live across every enrolled app.
- The **app owns "what work exists,"** the **registry owns "who can serve it,"** and the **operator owns
  "how much of my machine to spend"** — three clean responsibilities, none on the engine.
- c8ctl's model shifts from **per-hire `rank:cap` profiles** toward a **machine-level capability** with
  app-assigned roles. The two coexist (a hire's rank/caps seed the machine capability), so the shift is
  incremental.
- New app-tier surface: the supervisor's enrolment client + multi-app SERVE merge (small — it reuses the
  existing reconcile loop), and each app's enrol/vocab/registry endpoints (a thin layer over the ADR 0056
  registry it already needs).
- The enrolment endpoints are **generated, not authored**: `create-urban-app` scaffolds the OpenAPI
  fragment and Urban Gen's demand deriver analyzes the models — so every Urban app gets a supervisor-
  pointable endpoint for free, and demand stays authoritative-from-model rather than a hand-kept list.
- The engine stays a frozen, agent-oblivious C8 router; `nano work` children stay dumb 1:1 pollers.

## Open questions

- **App-tier auto-scaling.** For now (§5) the **operator** sets concurrency via the supervisor. Should an
  app later *drive* instance count from demand depth (an app-tier autoscaler), and if so, how does that
  reconcile with the operator's capacity/cost cap — hint-only, or authoritative within a ceiling?
- **Matchmaking vs mirror** (inherited from ADR 0056 §Open questions, now forced by multi-machine seats).
  Does the registry stay a read-only mirror, or grow placement policy that actively fills an unserved
  role and holds a seat's demand until a distinct family enrols?
- **Capability declaration source of truth.** A machine-level capability file, promotion from an existing
  hire profile, or probe/auto-detect (installed model, host class)? How do capability edits propagate to
  every open enrolment?
- **App discovery.** The examples hard-code app URLs. Should the host expose an **app directory**
  (installed apps + their enrol endpoints) so `supervisor enrol` can offer a pick-list instead of a URL?
- **How much vocab is derivable vs authored.** Demand (which leaves exist) is derived from the models
  (§9), but role *requirements* (`requires: frontier`, seats, `seatsDistinctFamily`) are author-declared.
  Could requirements be partly derived too — e.g. from `zeebe:property` / headers on the task, so a model
  that tags a task `requires=frontier` needs no separate vocab entry — and where is the authored residue
  kept so Urban Gen never clobbers it?
- **Scope expression.** The shape of the operator's allow/deny scope (§6) — prefix globs, per-role cost
  caps, budget ceilings — and where it is persisted (supervisor state vs the app registry).
- **Cross-supervisor coordination.** Seat placement is global (§7); the supervisor is per-machine. Does
  the app hub alone arbitrate seats, or do supervisors need to observe each other for anti-affinity
  beyond what the registry sees?
