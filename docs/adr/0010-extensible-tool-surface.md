# ADR 0010 — Extensible tool surface (user-defined investigation tools)

Status: **Proposed.**
Date: 2026-06-30.
Relates to: `processos/src/investigate.rs`, `processos/src/agent.rs`,
`processos/src/analysis.rs`, `processos/src/pyrunner.rs`,
`processos/src/pairings.rs`, `processos/src/main.rs`,
`processos/src/cockpit.html`. Builds on ADR-0006 (subagent delegation) and the
first-class LLM Pairings work.

## Context

The investigator LLM drives a tool-using agent loop (`agent.rs`). A tool is two
things welded together in Rust: a **declaration** (`ToolSpec { name, description,
parameters }`, pure JSON-schema data) returned from `ToolBox::specs()`, and an
**implementation** dispatched by a hardcoded `match name { … }` in
`ToolBox::call()` (`investigate.rs`). The baked-in catalog is:

`query_traces`, `discover_flow`, `run_python`, `read_model`, `read_model_xml`,
`analyze_model`, `validate_model`, `edit_model`, `simulate`, `compare_variants`,
`conformance_check`, `delegate`.

Two limitations follow:

1. **The surface is fixed.** An operator with a domain-specific check (a
   regulatory validator, a bespoke metric, an external lookup) cannot offer it
   to the model without editing and recompiling Rust.
2. **The surface is undiscoverable and uniform.** Operators can't see what the
   model *can* do, can't curate it per investigation, and the primary and a
   paired second model are handed the identical tool list.

The declaration half is trivially data-driven. The hard half is execution: we
cannot let users ship Rust. The key observation is that two **general-purpose
executors are already exposed as tools** — `run_python` (arbitrary Python over
the exported trace CSVs) and `query_traces` (arbitrary read-only DuckDB SQL). A
user-defined tool is therefore a **named, curated, parameterised wrapper** over
an executor, plus a third executor — an **external subprocess** — that matches
this project's "simple CLI tools over MCP" philosophy.

## Decision

Add a **data-driven, file-backed tool library** plus **per-model / per-investigation
tool selection**, in four parts.

### 1. `ToolDef` + three backends

A custom tool is a `ToolDef { id, name, description, parameters (JSON schema),
backend, template, command, timeoutMs, tags, enabled, builtin }`. The `backend`
is one of:

- **`python`** — `template` is Python source. The named arguments are injected
  as a `params` dict (and `{{name}}` placeholders interpolated) and run through
  the existing `pyrunner` sandbox against the same private CSV workdir.
- **`sql`** — `template` is a single DuckDB `SELECT`/`WITH`. Arguments are
  rendered as **safe SQL literals** (numbers verbatim, strings single-quote
  escaped) into `{{name}}` placeholders and run through `Analysis::query`, which
  already enforces single-statement, read-only (`guard_read_only` rejects `;`
  and every mutating keyword). The model already has full SQL via `query_traces`,
  so this adds no new authority — only curation.
- **`subprocess`** — `command` is a fixed **argv array** the *author* sets
  (never a shell string, so no `sh -c` injection). Only declared params
  interpolate into argv elements, each as exactly one argument (never
  word-split). The full argument object is also delivered as **JSON on stdin**.
  The trace dataset directory and model XML path are exposed via the
  `PROCESSOS_DATASET` / `PROCESSOS_MODEL` environment variables. Execution is
  bounded by `timeoutMs` and the captured stdout is length-capped; stderr is
  surfaced on failure.

Custom tools live in a file-backed `ToolStore` at `<config_dir>/tools.json`
(open/list/upsert/delete/persist), mirroring `PersonaStore` / `PairingStore`.

### 2. Built-in catalog (read-only)

The store also surfaces the baked-in tools as read-only `builtin: true` entries
(name + one-line summary + the capability that gates them) so operators can see
the full surface and **clone a built-in as the starting point** for a custom
tool. Built-ins are never executed through the store — they remain Rust.

### 3. Per-model & per-investigation selection

`run_agent` / `run_agent_streaming` are generic over `T: ToolBox + ?Sized`, so a
lightweight **`ScopedTools<'a> { inner: &AnalysisTools, allow: Option<Set<String>> }`**
wrapper filters `specs()` and guards `call()` by an allowlist **without cloning
the DuckDB connection or the Python workdir** (`None` = all, preserving today's
behaviour). The same shared `AnalysisTools` can thus present a different surface
to the primary and to a paired second model.

- **Pairing config** gains `primaryTools` and `secondaryTools`
  (`Option<Vec<String>>`): the operator (de)selects which tools each model in a
  pairing may use.
- An **investigation "Configure tools" button** sets the primary's allowlist for
  the current investigation, sent per turn.

Capability gating still applies underneath the allowlist: a tool that needs a
model/Python/recorded dataset stays hidden when its prerequisite is absent, even
if allowlisted.

### 4. Import / export

The store supports exporting a selected subset of `ToolDef`s as a JSON bundle and
importing one (with id-collision handling), so operators can share toolsets.
Bundles are tagged/categorised for browsing, consistent with the extension
marketplace.

## Security model

This is a **local, single-user desktop** surface. `run_python` already grants
arbitrary local code execution, so the `python` and `subprocess` backends add no
*new class* of risk — they curate and name authority the operator already holds.
The deliberate constraints are:

- the **LLM chooses argument values, never commands**: argv/template are
  author-fixed; only declared, schema-validated params interpolate; subprocess
  uses no shell and passes args as discrete argv entries + stdin JSON;
- **SQL stays read-only/single-statement** via the existing guard, with literal
  escaping;
- custom tools are **opt-in per investigation** and **clearly marked** in the
  catalog, so a small local model is never silently flooded with tools (too many
  tools degrades tool-selection quality on 4–8B models).

## Consequences

- The tool surface becomes a first-class, shareable, per-investigation,
  per-model concern instead of a recompile.
- One more file-backed store and one CRUD panel, consistent with personas /
  pairings — low marginal complexity.
- `ScopedTools` makes "different tools for different models" a two-line wrap, and
  leaves all existing call sites (`None` allowlist) unchanged.
- Future: per-tool rate/row budgets, HTTP backend, signed/trusted bundles,
  marketplace distribution of toolsets.
