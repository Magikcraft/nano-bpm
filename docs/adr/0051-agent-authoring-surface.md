# ADR 0051 — The agent authoring surface (`/agent`): "point your agent here"

Status: **Proposed.**
Date: 2026-07-31.
Relates to:
ADR 0022 (`0022-nano-rad-application.md`, the **Urban** App — the artifact an agent authors and
the "binding is the product" thesis this surface teaches a machine to reproduce),
ADR 0027 (`0027-urban-app-manifest-spec.md`, `nano.app.json` — the declared-data manifest the brief
points an agent at, and the `$schema` it validates against),
ADR 0041 (`0041-urban-app-import.md`, **import by reference** — the "link it in" mechanism the brief
instructs an agent to drive, either via `POST /console/api/projects/import` or a
`<name>.project-ref.json` drop),
ADR 0049 §7 (`0049-guided-journeys.md`, packs contribute guided journeys — the *human* on-ramp; this
ADR is the *agent* on-ramp, the symmetric other half),
the published-schemas convention (App JSON Schema `$id` at `https://nanobpm.io/spec-app/nano-app.schema.json`),
`server/src/console/agent_brief.rs` (the renderer added here),
`server/src/console/mod.rs` (`/agent`, `/agent.md`, `/llms.txt` routes + `request_base_url`),
`server/src/console/projects.rs` (`projects_root`, `project_templates` — live facts the brief reads),
`server/src/console/extensions.rs` (`all_extensions` — installed packs the brief lists),
`console/src/views/Projects.tsx` ("Build with an agent" affordance).

## Context

Nano is a Rapid Application Development environment (ADR 0022): a person authors an **App** —
processes, decisions, forms, pages, data — and it compiles to a single binary. The console ships
editors for all of it, and ADR 0049 added *guided journeys* to teach a **human** the product from
inside the UI.

Two things that environment did **not** have:

1. **An agent could not author an app.** A user with a coding agent (Claude Code, Copilot CLI, any
   MCP-driven assistant) had no way to say "build me a Nano app" — the agent had no description of
   what a Nano App is, where files go, or how to get one into a running node. Everything it needed
   was scattered across ADRs, a JSON Schema, and an OpenAPI spec, none of them addressed to a machine
   reader and none discoverable from the running product.

2. **Nothing told an agent how Nano works.** Users increasingly ask *their agent* "how does this
   work?" rather than reading docs. If the agent has no grounding, it hallucinates.

The user's insight collapses both into one: **if you can explain the thing to an agent, the agent can
explain it to the user.** A single well-formed brief — written *for the agent* — satisfies "let an
agent build me an app" *and* "have an agent tell me how this works". The remaining problem is
**discoverability**: the brief has to live at a URL a user can hand over — *"point your agent here."*

The load-bearing facts that already exist:

- **Import by reference (ADR 0041)** is exactly the "link it in" primitive: register a pointer to an
  external directory, read live, no copy. `POST /console/api/projects/import {name, path}` or a
  `<name>.project-ref.json` in the projects root.
- **The manifest is declared data (ADR 0027)** with a **published JSON Schema** at a stable owned URL —
  an agent can fetch it to author and validate `nano.app.json`.
- **The node already serves human-facing pages** (`/`, `/docs`, `/swagger`, `/asyncapi`) from
  hand-wired routes in `server/src/console/mod.rs`. Adding one more text route is idiomatic.

## Decision

Serve a **live, per-node agent brief** at **`GET /agent`** (and `/agent.md`), rendered as Markdown,
plus a **`GET /llms.txt`** discovery index. Surface it in the console as a **"Build with an agent"**
affordance in the Projects view that reveals the copyable `<origin>/agent` URL. Document the whole
surface in this ADR and the README.

### Why a served endpoint, not a checked-in file

The brief must be **actionable for this node with zero other context**. A static file cannot know:

- the **base URL** the caller reached this node on (behind a tunnel/proxy/LAN address) — so it cannot
  print an import `curl` the agent can actually run;
- the **projects root on this host's disk** — so it cannot tell the agent where a `.project-ref.json`
  may be dropped;
- the **installed packs** and **scaffold templates** this node offers.

So `render(base_url)` (in `agent_brief.rs`) reads these at request time. The base URL is reconstructed
from the request (`X-Forwarded-Proto` + `Host`, falling back to `http` / `localhost`) by
`request_base_url` in `mod.rs`. The document a machine reads is therefore true for *that* running node.

### What the brief contains

Addressed to the agent in the second person, in this order:

1. **Framing** — you were pointed here; here are the two jobs (author + explain); Nano in one paragraph
   (a Rust distillation of Zeebe; Urban = the RAD binding).
2. **This node (live)** — base URL, console URL, on-disk projects root, import endpoint, schema URL,
   OpenAPI/AsyncAPI/docs URLs, server build; the installed packs and scaffold templates.
3. **What a Nano App is** — the `nano.app.json` binding, the directory layout, a minimal manifest with
   the `$schema` line.
4. **Author an app (outside the IDE)** — the steps, BPMN/DMN/FEEL notes, "validate by linking it in".
5. **Link it in (ADR 0041)** — the `curl` against *this node's* import endpoint, and the ref-file drop
   into *this node's* projects root.
6. **How Nano works (so you can explain it)** — engine, jobs/workers, native DMN, FEEL, μ-nano test,
   deploy/run, Bernd.
7. **Machine-readable references** — schema, OpenAPI, AsyncAPI, docs, and `/agent` itself.

`/llms.txt` follows the emerging convention: a short, link-first index pointing at `/agent` (the full
brief) and the node's specs, so agent tooling that probes `/llms.txt` finds the surface without a
human in the loop.

### Console affordance

Projects view gains a **"Build with an agent"** button beside "Import by reference" (the human
counterpart of the same "link it in" edge). It reveals a card with the `<origin>/agent` URL, a **Copy**
button (reusing the existing secure-context-tolerant `copyText` helper), an **Open** link, and the
one-liner a user pastes to their agent: *"Read `<origin>/agent` and build me an app."*

### Non-goals / boundaries

- **No new mutation surface.** The brief instructs the agent to use the **existing** import endpoint;
  `/agent` and `/llms.txt` are **read-only** and safe under every console profile (including observe).
- **No `eval`, no code in the manifest** (ADR 0007/0027 hold). The agent writes declared data + model
  files; it does not gain a code-execution path it did not already have via the filesystem.
- **Not an MCP server.** This is a *document* an agent reads, not a tool protocol. An MCP server that
  wraps the console API can come later (and would cite this brief); it is out of scope here.
- **Not in the OpenAPI spec.** Like `/docs` and `/swagger`, these are hand-wired static-ish text routes,
  intentionally excluded from the generated `/console/api/*` surface.

## Consequences

- A user can hand any coding agent a single URL and get either an authored, linked-in app or a correct
  explanation of Nano — the agent grounds itself from the running node, not from stale training data.
- The brief cannot drift from reality: base URL, projects root, packs and templates are read live, and
  it points at the generated OpenAPI + the published JSON Schema rather than restating them.
- Discovery has two doors: the human-facing console button and the machine-facing `/llms.txt`.
- Future work: an MCP server fronting the console API (would reference `/agent`); a "one-click ask my
  agent" that deep-links a local agent; enumerating example apps in the brief once a gallery API exists.
