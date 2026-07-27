# ADR 0041 — Importing an Urban App by reference: external project pointers + headless run

Status: **Proposed.**
Date: 2026-07-27.
Relates to:
ADR 0009 (`0009-gui-application-projects.md`, the console **project** — the unit this ADR imports; a
project already carries `nanobpm.project.json`, deploys its models on Run, and starts its workers),
ADR 0022 (`0022-nano-rad-application.md`, **Urban** — an Urban app *is* an `app: "urban"` project),
ADR 0027 (`0027-urban-app-manifest-spec.md`, `nano.app.json`; §1 the project/manifest boundary, **§4
the three fail-closed validation gates that already run in the IDE** — this ADR adds **no** fourth),
`server/src/console/projects.rs` (`project_dir` `:342` = `projects_root().join(name)`, safe-name only —
the constraint this ADR relaxes; `supervisor().run()` `:2816`, "main.ts deploys processes + starts
workers" `:13`; `auto_deploy` sweep POSTs each model to `<deployTarget>/v2/deployments` `:401`),
`server/src/console/mod.rs` (`project_run` `:3150` — `POST /console/api/projects/{name}/run` = deploy
+ start; the path a headless run reuses).

## Context

The concrete goal: a persistent Copilot job-worker running headless on a machine, receiving GitHub
**PR-review-convergence** work as a durable BPMN loop, shipped as a *separate versioned Urban app* the
maker has **checked out on disk** — loaded into a running server **by pointer, not by copy**. The maker
also fixed two constraints:

1. **"Do it as an Urban app, not in the core."** The app ships in its own repo.
2. **"Import from my filesystem — I have the repo checked out. We already have distribution via the npm
   extensions surface. And we can already *run* apps in the Nano IDE."**

That last observation is decisive and shrinks this ADR. An Urban app is already a **console project**,
and the console **already runs one**: `supervisor().run()` compiles/starts the project, whose entrypoint
"deploys processes + starts workers", with the `auto_deploy` sweep POSTing each model to the engine's
`/v2/deployments` — and the manifest is already **validated** at the IDE's authoring/compile/boot gates
(ADR 0027 §4). So the deploy pipeline, the worker lifecycle, and validation **all already exist**. An
earlier draft of this ADR proposed a server-side App Registry with its own validator sidecar and deploy
pipeline; that duplicated the run path and is **rejected**.

What is *actually* missing is narrow and mechanical:

- **A project can only live at `<workspace>/projects/<name>`** (`project_dir` = `projects_root().join(name)`,
  gated by `is_safe_name`). There is no way to point a project at the maker's **external checkout** and
  read it **live** (no copy) for the dev loop.
- **Run is console-driven only.** There is no first-class **headless** invocation to start a project on a
  server without the IDE UI — the persistent-worker use case.

## Decision (proposed)

### 1. Import-by-reference = register an external directory as a project pointer

Add a project **source** indirection so a project name can resolve to an **external absolute directory**
(the maker's checkout) instead of a subdir of `projects_root()`. Import stores a **pointer**, never a
copy; the server reads `nano.app.json` + models **live** from that directory, so edits in the checkout
are picked up on the next Run/reload — the tight dev loop the maker asked for.

```jsonc
POST /console/api/projects/import
{ "name": "pr-review-convergence", "path": "/abs/to/urban-pr-review" }   // path source (increment 1)
```

Mechanically: a small **project-reference** record (name → resolved source) held in the projects root
(e.g. a `<name>.project-ref.json` alongside the existing project dirs, or a `source` field the resolver
consults). `project_dir(name)` gains a lookup: a registered reference returns its external path;
otherwise the existing `projects_root().join(name)` behaviour is unchanged. **No** project data moves.

**Guardrails (the external-path relaxation must stay safe):**
- The pointer is an operator action (local API), so it may resolve outside the workspace root — but the
  resolved path is **canonicalised and stored absolute**, and all *within-project* file access stays
  confined to that root by the existing `is_safe_name`/path-join checks (no `..` escape).
- A missing/again-unreadable pointer surfaces as a clear "project source not found" error, not a panic.
- `list_projects()` includes references, tagged with their `source` (`workspace` | `path`) so the console
  shows where a project actually lives.

**Distribution is unchanged.** Shipping an app to *another* machine already goes through the npm
extensions surface (`nano-ide-ext-*`, `extensions.rs`); an installed pack dir is just a future `source`
kind resolving the same way. This ADR adds **no** bundle format and **no** git importer. (A future `git`
source could reuse the c8ctl `provisionRepo` clone primitive, but that is out of scope here.)

### 2. Headless run reuses the supervisor — no new pipeline

Running an imported app on a server is the **existing** `supervisor().run(name)` path (compile → deploy
models via `auto_deploy` → start the run config's workers). This ADR adds only a **headless entrypoint**
so a server can start a project without the console UI driving `POST …/run`:

- a startup option / small CLI verb (e.g. `--run-project <name|path>`, or a `c8ctl` subcommand) that, on
  boot, registers the pointer (if a path) and calls the same `supervisor().run()`.

Because it is the same supervisor path, headless run inherits **everything** already built: deploy,
run-config selection, `resolve_run_env` layering, logs SSE, stop/lifecycle. No validator, no deploy code,
no worker manager is re-implemented.

### 3. Where the workers run — the supervisor hosts them (this *is* supervised mode)

In this reframe, "run the project" already **starts the project's workers** in-process under the
supervisor. So the server-hosts-workers ("supervised") behaviour the maker asked for is simply the
default of the run path — no separate mode is needed. The alternative remains available and unchanged: a
maker can instead run the workers **externally** with a persistent `c8ctl nano work <profile>` daemon
pointed at the same engine (that daemon already has persistence, `--max-parallel` concurrency, and
per-job multi-repo `provisionRepo` cloning). Register-by-reference does not force either choice.

### 4. Validation stays where it is — the IDE gates

No server-side validator is added. The manifest is validated by the **single** `spec-app/` TS validator
at the IDE's authoring/compile/boot gates (ADR 0027 §3–§4), preserving the one-implementation anti-drift
rule. A headless run inherits the **boot gate**: the app's entrypoint validates the manifest before it
starts the engine/triggers/surfaces and refuses to start on an invalid manifest — exactly as today.

## Consequences

- **A checked-out Urban app runs on a server by pointer**, read live, with a tight edit→reload loop and
  no copy — the maker's stated requirement, achieved by relaxing one path constraint plus a headless
  entrypoint.
- **Almost no new surface**: deploy, worker lifecycle, and validation are all reused. The change is
  `project_dir` gaining a reference lookup, an `import` endpoint, `list_projects` source-tagging, and a
  headless run entrypoint.
- **The engine stays Zeebe-pure** and **distribution is unchanged** (npm extensions).
- **No drift**: validation remains the single TS implementation at the existing gates.

## Open questions

1. **Reference storage** — a per-project `<name>.project-ref.json` in the projects root, or a single
   `references.json` registry? (Lean: per-project file, mirrors how projects already live as dirs.)
2. **Headless surface** — a server boot flag (`--run-project`) vs. a `c8ctl` subcommand vs. both. (Lean:
   boot flag first; it is the persistent-worker deployment shape.)
3. **Reload trigger** — Run already re-reads the live dir; do we also want a filesystem watch for
   auto-redeploy on change in dev? (Lean: manual re-Run first.)
4. **Name collisions** — importing a path under a `name` that already exists as a workspace project.
   (Lean: refuse with a clear error; the operator picks another name.)
