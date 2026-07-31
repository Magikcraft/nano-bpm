# ADR 0049 — Guided journeys: onboarding chosen by the entry point, not the build profile

Status: **Proposed.**
Date: 2026-07-31.
Relates to:
ADR 0034 (`0034-console-build-profiles.md`, the `studio` / `observe` split this ADR argues is *not* a
persona axis) and ADR 0035 (`0035-observability-config-and-standalone-console.md`, the standalone
operator console); ADR 0044 (`0044-code-first-durable-orchestration.md`, which names the beachhead —
"a developer automating their own SDLC" — and whose decision update ships **`w.task`** +
`externalJobTypes(flow)`, the external-worker seam journey 0 teaches), ADR 0045
(`0045-code-first-workflows-rad-surface.md`, the code-first RAD surface and its derived-model view),
ADR 0046 (`0046-agent-as-worker-vs-agent-in-the-node.md`, agent-as-worker: coding harnesses as
external job workers via `c8ctl nano hire`/`work`) and ADR 0006 (`0006-subagent-delegation-mode.md`,
the delegation seam); ADR 0022 (`0022-nano-rad-application.md`, Urban — the RAD thesis that *the
binding is the product*), with ADR 0027 (the app manifest), ADR 0024 (the datasource abstraction) and
ADR 0042 (the page composer) as the three parts journey 2 names; ADR 0007/0008
(`0007-rad-extension-system.md`, `0008-polyglot-language-packs.md`, the npm pack contract this ADR
extends with `tours[]`). Grounded by the driver.js spike (issue #393, draft PR #395, branch
`feat/console-product-tour`: `console/src/lib/tour/{steps.ts,useProductTour.ts,tour.css}`; a second
spike lives on `feat/console-tour-reactour`), by `console/src/lib/profile.ts`,
`console/src/views/Projects.tsx` (the template gallery and the empty state), the derived-model view in
`console/src/views/ProjectWorkspace.tsx`, `console/src/views/Workers.tsx` (the visibility gap in §6),
the `c8ctl nano hire`/`work` harness-as-worker flow in `jwulf/c8ctl-plugin-nano`, and the two
reference applications `~/workspace/urban-pr-review` (model-first) and
`~/workspace/urban-pr-review-codefirst` (code-first).

## Context

The console has a first-run product tour at spike stage: driver.js, a `localStorage` once-flag
(`nano.tour.v1.seen`), stable `data-tour` anchors, and a step list chosen by `CONSOLE_PROFILE` —
five steps for `studio` (welcome → projects nav → new project → run → observe nav), four for
`observe` (topology → explorer → metrics). The spike proved the mechanics that matter: it spans
react-router routes, waits for targets that mount asynchronously over CSS-transformed canvases
(bpmn-js/monaco), and skips steps whose target never appears.

The mechanics are right. The **content model** is wrong, in three specific ways.

**1. The build profile is not the persona.** `studio` vs `observe` is a build-time constant. But the
users who matter most all land in the *same* `studio` build and want materially different first
sessions. A single `studioSteps` array cannot serve them, and no amount of step-writing fixes that:
the branch is at the wrong level.

**2. The steps describe the UI instead of producing an outcome.** "Explorer, Traces, Metrics and
Workers give full visibility into running instances" names four tabs and teaches nothing that
survives the session. A first run should leave a *result* behind — a project that ran, an instance
that completed, a served page that answered.

**3. One step is a promise the console already knows it cannot keep.** The `run` step says "hit Run
to boot the engine and execute it". On a host with neither Node ≥ 22.6 nor Deno, Run cannot work —
and the console knows: the same `listProjects()` response that feeds the tour carries
`denoAvailable` / `nodeAvailable`, and `Projects.tsx` already renders a warning banner from them.
Missing-toolchain is the most likely first-run failure, and today the tour walks the user into it
while claiming otherwise.

### The three journeys we are actually onboarding

The product's first targets, in priority order, and the surface each rides:

| # | Target | Rides on | Reference |
|---|---|---|---|
| **0** | **Automate your own agentic SDLC** by orchestrating coding harnesses | `workflow-starter` (`defineFlow`) with **`w.task()`** — a step serviced by an *external* worker, its contract published by `externalJobTypes(flow)`; `c8ctl nano hire`/`work` turns Claude Code / Copilot CLI into a job worker over a **rank × capability** job-type matrix (`senior:code-review`), with per-job clone/branch/push and PR reconciliation | `urban-pr-review-codefirst` — `review-round` is a `w.task` aimed at a harness; `wait-answer` is a `w.signal` human gate |
| **1** | **A Camunda-compatible engine, headless, for local development** — a good subset | Single binary, no runtime deps, C8 v2 at `/v2`, offline `/swagger`, a `demo` process pre-deployed at startup, disposable (`stop --purge`), embeddable via `@nanobpm/nano-bernd` for tests | Mission 2 in `vision.md` — drop-in replacement, "migration measured in hours" |
| **2** | **Rapidly prototype a Camunda solution** — 90s RAD, fullstack | `urban-starter`: `nano.app.json` + `main.ts` (webserver serving the page runtime) + `pages/*.page.json` (declarative screens) + `db/migrations/` with typed `@nanobpm/domain` SQLite accessors + `workers/*/worker.ts` | `urban-pr-review` — ADR 0022's Delphi lineage: the *binding* is the product |

These three differ along an axis the spike has no concept of: **how much of the journey happens
inside the console.** Target 1 is headless by definition — the console is a debugger it visits when
something breaks, never part of the loop. Target 0 is split: authoring is in the console, but
`hire`/`work` is a terminal command. Only target 2 is substantially in-console, and only it fits a
classic spotlight tour.

That axis, not the build profile, is what the framework has to model.

## Decision

### 1. Journeys, not a tour

Replace the profile-keyed step arrays with a registry of named **journeys**:

```ts
interface Journey {
  id: string;                     // "agentic-author", "localdev", "rad-prototype"
  title: string;
  profiles: ConsoleProfile[];     // a filter, no longer the branch
  preconditions?: Precondition[]; // journey-level gates (see §3)
  steps: Step[];
  successEvent: Predicate;        // what must actually have happened (§4)
  nextJourneys?: string[];        // offered on completion, gated by their own preconditions
}
```

Each journey is **≤ 5 steps**. A journey that needs a detour outside the console is *split at that
boundary* rather than padded (see journey 0a/0b below). The spike's five-step overview survives, but
demoted: it becomes the `overview` journey behind a "just show me around" affordance, and it is no
longer what a first-time user gets by default.

### 2. The entry point chooses the journey — it already knows the persona

The console cannot infer a persona on a fresh install: every signal (no projects, no packs, one node,
a pre-deployed `demo`) reads identically for all three targets. But **the command the user ran
already encodes it.** So:

- Journeys are addressable by deep link: `…/console?tour=<journeyId>`.
- `c8ctl nano start` prints a console URL today; it prints `?tour=localdev` instead.
  `c8ctl nano hire` prints `?tour=agentic-author`. The CLI knows which persona you are *because of
  which command you ran*, so the console never has to ask.
- Documentation and marketing links carry the same parameter.

Self-selection is the **fallback** for someone who arrives cold, not the primary mechanism, and it is
**not a modal**: the three journey cards render as the Projects (or Topology) **empty state**, which
today reads only "No projects yet. / Create one to get started." They cost no friction, trap no
focus, and disappear once the user has projects — an empty state that needed the help anyway.

Second-tier selection is free: journeys 0 and 2 both begin by choosing a template, and the template
gallery *is already* the question "what do you want to build?". A journey pre-highlights its card
rather than duplicating the choice, via a per-template anchor (`data-tour="template-<id>"`), which is
cheap now that the gallery is data-driven from the template list.

### 3. Two new step kinds: **handoff** and **precondition/repair**

**Handoff steps** are the net-new capability, and journeys 0b and 1 are impossible to express
honestly without them: a copyable command or external URL, plus either an explicit "I've done it" or
a predicate that auto-advances when the console observes the result.

```ts
type Step =
  | { kind: "spotlight"; selector: string; route?: string; … }
  | { kind: "note"; … }                                    // anchorless, centered
  | { kind: "handoff"; copy: string; verify?: Predicate; … } // a terminal command / an external URL
```

**Preconditions with repair.** `skipMissingElement` answers "the target is absent"; it cannot answer
"the target is present but the action would fail". Every step may declare a precondition resolving to
`ok | skip | repair`, where `repair` substitutes a different step:

| Precondition | Failing case | Repair step |
|---|---|---|
| a JS runtime exists | `!denoAvailable && !nodeAvailable` | how to get one — never "hit Run" |
| `c8ctl` is reachable | journey 0b with no plugin | install the plugin first |
| a cluster exists | single node | "here is what changes at RF=3", instead of an empty cluster view |
| traces exist | fresh install | skip the Traces step rather than show an empty table |

This is the difference between a tour and a *guided* tour, and it is where the "Run promise" defect
(§Context 3) is fixed structurally rather than by rewording.

### 4. Advance on observed state, not on clicks

Journeys end in a **success event** — a predicate over real application state, evaluated from a small
context object. One `listProjects()` call already supplies most of it (`projects`, `denoAvailable`,
`nodeAvailable`, `templates`, `extensions`); run state, node count, and the current route complete it.

| Journey | Success event |
|---|---|
| 0a | the workflow project's worker host is up (deployed + polling) |
| 0b | an instance reached the agent step **and** parked on the human-approval catch event |
| 1 | the `/v2` base URL was taken, and Explorer was reached at least once |
| 2 | the served app answered on its port, and an instance exists that the UI started |

"Waiting for your first instance to complete…" is the step that makes journeys land, and clicks
cannot express it.

### 5. Resumable, versioned state

Replace the `nano.tour.v1.seen` boolean with a versioned record — persona (when known), and per
journey a status, step index and completion time — so a journey survives reloads and wandering, can
be resumed where it was left, and can be reset from Config.

### 6. The three journeys

**0a — Author a durable agent loop** *(all console; the agent does the work, the engine owns the
waiting)*
1. Note: what you are about to build — a durable multi-round loop around a coding harness, with a
   human approval gate.
2. `/projects` → New project, **Code-first workflow** pre-highlighted.
3. The flow file: the three verbs — `w.run` (app-hosted), **`w.task` (external — your harness)**,
   `w.signal` (durable human wait). This is the conceptual payload of the whole journey.
4. **The derived model view**: "you did not draw this — the BPMN was derived from the code, and that
   catch event is a real, engine-visible durable wait." The differentiator over code-only
   durable-execution engines.
5. **Run** (runtime-gated, with repair) → deployed + worker host running.

**0b — Hire an agent and close the loop** *(console + terminal; offered on 0a completion)*
1. **Handoff:** `c8ctl nano hire …` then `c8ctl nano work …`, explaining that the rank × capability
   token *is* the job type the `w.task` step emits. Verify: the job is picked up.
2. Start an instance → Explorer: it reaches the agent step, then parks on the human signal.
3. Resume the signal → the instance completes. Exit to `urban-pr-review-codefirst` as the full
   reference app.

**1 — Local Camunda, headless** *(3 steps, and it ends by getting out of the way)*
1. Note: the engine is running, `demo` is already deployed, there is nothing to install or configure.
2. **Handoff:** copy `http://127.0.0.1:8080/v2` + the offline Swagger link — "your existing C8 client
   works unchanged; this is the only line you change."
3. Explorer: "when something breaks, this is where you look — variables, jobs, incidents, BPMN XML
   per instance", plus disposability (`stop --purge`) and the subset boundary (§Open questions 2).

It deliberately offers **no** follow-on journey. Respecting a headless user's time is the design.

**2 — Prototype a fullstack solution** *(the most console-native)*
1. New project → **Urban App** pre-highlighted.
2. The file tree names the four parts: the process, the screen (`pages/*.page.json`), the app's own
   SQLite (`db/migrations/` + typed `@nanobpm/domain`), the code (`workers/`). "The model is the
   form, the handler is the code, the binding is derived."
3. Edit a page or form — the RAD moment: change a screen without writing a frontend.
4. **Run** (runtime-gated) → **open the served UI**. The payoff.
5. Deploy → Explorer to see the instance the UI started. Teaser: **Compile** to a single binary.

### 7. Packs contribute journeys

Add an optional `tours[]` to `nano-ide.ext.json`, mirroring how `templates[]` and `components[]`
already work, so the onboarding surface scales with the pack ecosystem instead of living in a
hardcoded array: the MQTT trigger pack teaches "start a process from a broker message"; each
throughput example teaches its own REST-vs-Falcon A/B. Pack journeys are subject to the same
preconditions (a pack journey that needs a toolchain is not offered without it).

### 8. Anchors, analytics, accessibility

Anchors today cover `nav-projects`, `new-project`, `run`; the journeys above additionally need
template cards, the output console, Deploy, the derived-Model tab, the served-app link, and a trace
row. Step ids stay stable and emit start/step/skip/repair/complete/abandon so per-step drop-off is
measurable. Journeys never trap focus, always exit on `Esc`, and respect
`prefers-reduced-motion`. Tour copy is **not** sourced from `USERGUIDE.md`, which still describes a
five-tab console that no longer matches the nav (issue #388); the ADRs and the two reference-app
READMEs are the accurate sources.

## Consequences

- The profile stops being the onboarding branch and becomes a filter. `observe` keeps a journey only
  if we decide it has one (§Open questions 4).
- `c8ctl` gains a small, high-leverage responsibility: printing the console URL with `?tour=`. This
  is the only coupling the design adds, and it replaces persona guessing entirely.
- Journeys can encode work that happens outside the console, which is what makes targets 0 and 1
  addressable at all. The cost is that a handoff step's completion is either self-reported or
  inferred from state; it is never directly observed.
- Success predicates mean the framework needs read access to app state it already fetches, plus a
  small predicate/event bus. This is the main net-new machinery.
- **The code-first starter does not yet demonstrate the seam journey 0a teaches.** The scaffolded
  `workflows/pr-review.ts` uses `w.run` and `w.signal` only — its `review` step is a local handler
  with an "e.g. hand the diff to an LLM" comment, not a `w.task` aimed at a harness job type. Either
  that step becomes a `w.task`, or an agentic variant template is added. Journey 0a is blocked on it.
- Each journey carries its own success metric rather than one global funnel: instances reaching a
  human-approval gate with an external agent round completed (0); base URL taken and the console
  left behind (1); served UI opened with a UI-started instance (2).

## Open questions

1. **The console cannot see a hired agent.** `hire`/`work` connects over the SDK, but `Workers.tsx`
   is about *console-authored* worker directories (`createWorker({name, jobType})` + Monaco). So 0b
   cannot show "your agent is polling `convergence-loop:review-round`" — precisely the moment that
   makes the topology click. Recommendation: add an engine-side "who is polling what" panel; it is
   independently the most-asked operational question ("is my worker connected?"). Without it, 0b's
   first step stays terminal-only and unverified.
2. **Target 1 needs a published subset boundary.** "A good subset" is exactly the claim that requires
   a documented edge. There is `docs/dmn-feel-parity-audit.md` but no overall compatibility matrix,
   so journey 1's third step has nothing to link to — and that is the step where trust is won or lost.
3. **`hire` vs `recruit`.** The plugin README documents `c8ctl nano hire`; `urban-pr-review`'s README
   says `c8ctl nano recruit`. Tour copy needs one name; `hire` appears current.
4. **Does `observe` get a journey?** None of the three targets is an operator, yet `observe` is the
   one profile where build *does* equal persona, and the operator tour is the one the spike gets
   structurally right. Out of scope for this round, or a fourth journey?
5. **driver.js vs `@reactour/tour`** remains undecided (issue #393). This ADR is deliberately
   runner-agnostic: journeys, preconditions, handoffs and predicates are the contract; the runner
   renders it. The `waitForElement` / `skipMissingElement` behaviour the spike relies on becomes one
   precondition kind among several.
