# ADR 0058 — OpenAPI endpoint surface (contract-first controllers, ejectable to imperative)

Status: Proposed
Date: 2026-08-09
Relates to: ADR 0055 §3 (app-authored action handlers), ADR 0040 (fused domain model), ADR 0042 (page/screen composer), ADR 0053 (derivation is a shared library), ADR 0027 (App manifest + fail-closed validator)
Repo: Magikcraft/nano-bpm (`spec-app/`), nanobpm/nano-ide (`packages/urban`)

## Context

An Urban app exposes HTTP endpoints today through the **`actions[]`** surface (ADR 0055 §3).
Each declaration binds a route to a handler module:

```jsonc
// nano.app.json
"actions": [
  { "path": "/app/actions/register-invoice", "module": "actions/registerInvoice.ts" }
]
```

```ts
// actions/registerInvoice.ts
export default async ({ body }, app) => {
  // body is `unknown`. Every handler re-does its own parsing and checks — or skips them.
  const total = (body as any).total;
  if (typeof total !== "number" || total < 0) return { status: 400, body: { error: "bad total" } };
  // …business logic…
};
```

The runtime JSON-parses the body and hands it to the handler as `unknown`. There is **no request
schema, no constraint validation, no typed request/response, and no generated API documentation**.
Every endpoint hand-rolls (or silently omits) its own validation, error shapes drift from handler to
handler, and the app's HTTP contract lives implicitly inside imperative code where it can neither be
reviewed as a diff nor consumed by a client generator. This is the *"adhoc imperative code"* problem.

This runs against the grain of everything else in Urban, where **the spec is authoritative and the
code is derived**:

- the domain model is *fused/derived*, not authored (ADR 0040),
- pages are *declarative JSON* (ADR 0042),
- BPMN **semantics** are authored while the **DI** diagram is generated,
- worker I/O types are *derived from the data-envelope* (ADR 0033 §3 / ADR 0053).

Nano's **own** console already proves the target pattern end-to-end: `spec-console/console-api.yaml`
is the single source of truth from which `@hey-api/openapi-ts` generates the client SDK and
`server/src/console/generated_api.rs` generates the controller layer, leaving humans to write only
the delegated implementations. Urban apps should get the same contract-first surface Nano dogfoods.

## Decision

Add an **OpenAPI-first endpoint surface** to Urban. An author writes an OpenAPI document; the Urban
toolkit **derives** the controller layer (typed request/response contracts + runtime validators +
route table) from it, and the author writes only the **delegated implementation** per operation —
receiving an already-validated, typed request and the injected `AppApi` (the same contract a worker
or action handler gets today).

It **coexists** with `actions[]` and is **ejectable**: an author can drop back to raw imperative
handling for any operation (or the whole surface) without leaving the framework.

### The binding

A new optional manifest block names the spec and its generation/eject policy (ADR 0027 shape;
`spec-app/nano-app.schema.json` `$defs/apiBinding`):

```jsonc
"api": {
  "spec": "openapi.json",         // OpenAPI 3.x document, app-root-relative (JSON first; YAML fast-follow)
  "dir": "operations",            // where per-operationId delegate modules live (default "operations")
  "base": "/app/api",             // route prefix the derived paths mount under (default "/app/api")
  "validateResponses": "dev",     // "dev" | "always" | "never" (default "dev")
  "eject": false                  // true = mount validated routes but pass the RAW request to every
                                   //        delegate (whole-surface opt-out; see "Ejecting")
}
```

**`operationId` is mandatory** on every operation — it is the delegate module key and the type name
stem. `urban check` fails closed (ADR 0027 §4) when the referenced spec does not parse, an operation
omits `operationId`, or two operations collide on one.

### What is derived (Urban toolkit — `derivers/api.ts`, a peer of the domain/worker-io derivers)

From the one document, per operation, into the gitignored `nano-generated/` drift domain:

1. **Typed contracts** (`api-io.d.ts`) — `Request`/`Response` types per `operationId` from the
   operation's parameters, `requestBody`, and responses schemas. Authoring-time types + red
   squiggles, exactly like `worker-io.d.ts`.
2. **Standalone validators** — request (path/query params + body) and, when enabled, response
   validators compiled from the OpenAPI JSON Schemas to **zero-runtime-dependency** functions
   (host-agnostic; see Consequences). Urban already carries JSON Schema for the manifest itself
   (ADR 0027), so this reuses in-house machinery.
3. **A typed `defineOperation` wrapper** (`operations.ts`) keyed by `operationId`, mirroring the
   generated typed `defineWorker` (ADR 0033 §3), so a delegate's input and result are typed from the
   spec.
4. **The route table** — `(operationId → method + path)` consumed by the runtime mount.

All of it is **derived, never hand-edited**, regenerated on spec change, and guarded by the same CI
drift gate that protects the DI and domain-rows artifacts. Delegate **stubs** for
not-yet-implemented operations are scaffolded once (like `create-urban-app` templates) and then
owned by the author.

### What the runtime does (Urban runtime — `mountApi`, a peer of `mountActions`)

`mountApi(ctx, app)` reads `manifest.api`, loads the spec + generated route table, and mounts one
route per operation through the existing first-match-wins `router.ts` — **before** the generic pages
action routes, **alongside** `mountActions`. Each generated controller:

```
parse body → validate (params + body) → 400 with a structured error on failure
           → resolve + call the operationId delegate (validated, typed input + AppApi)
           → (optional) validate response → serialize
```

Delegate resolution mirrors `resolveActionHandler` (default export, else named `handler`), loaded
lazily and cached, so a missing/malformed module is a clear `500` on that route rather than a boot
failure. A structured, consistent `400` (`{ error, issues: [{ path, message }] }`) replaces the
per-handler ad-hoc error shapes.

### Coexistence with `actions[]`

`actions[]` **stays**. It does a genuinely different job — wrapping the *generic engine*
start/cancel/message routes (BPMN process control) — from app-owned REST endpoints. Both mount
together; `actions[]` continues to shadow the generic pages routes. An app can use either, both, or
neither. No migration is forced; nothing about `actions[]` changes.

### Ejecting (opt-out / additively imperative)

The whole point of a RAD framework is that it must never trap you. Three escape hatches, in
increasing order of control:

1. **Additive imperative in the delegate.** The delegate already *is* your code. Beyond the typed,
   validated input it receives the raw `req` (headers, method, streaming body) and the full `AppApi`
   (datasource + engine + sdk + host), and it returns an arbitrary `status`/`headers`/`body`. Any
   logic the generated controller does not express, you write here.
2. **Per-operation eject.** Mark an operation `x-urban-eject: true` (an OpenAPI vendor extension) to
   keep the route + docs but **skip generated validation** and hand the delegate the raw request —
   for endpoints that stream, negotiate non-JSON content, or validate by hand.
3. **Fall through to `actions[]`.** For a route that should not be spec-described at all, declare it
   in `actions[]` (or set `api.eject: true` to opt the whole surface out of validation). Because the
   router is first-match-wins and both surfaces mount together, an `actions[]` route simply shadows.

Ejection is **additive, never destructive**: you keep the generated types and docs and opt out only
of the machinery you are replacing.

## Consequences

- **Validation and error shapes become automatic and consistent** instead of hand-rolled per
  handler. The 400 contract is one shape across every endpoint.
- **One artifact drives many outputs** — server controllers, typed contracts, a client SDK, and
  human docs (Swagger UI at `/app/api-docs`) — the same "one source, many derived outputs" win
  Urban already sells for the domain model.
- **The HTTP contract becomes reviewable as a diff** in the spec, rather than buried in imperative
  handlers.
- **Generation lives in the TS toolkit** (`@nanobpm/urban`), a peer of the existing derivers, not in
  the Rust host — consistent with ADR 0053/0054 pulling derivation into the shared library.
- **Validators must be host-agnostic.** The Urban runtime targets both Node and Deno (ADR 0038 /
  0052, the `HostContext` seam), so we generate **standalone compiled** validator functions (e.g.
  Ajv `standalone` mode) rather than shipping a validator runtime into every app. This also keeps
  the emitted validators in the same "generated code" model as the rest of `nano-generated/`.
- **A supported OpenAPI profile, not all of OpenAPI.** The first cut supports JSON request/response
  bodies, path/query parameters, one `requestBody` content type, and response codes. Callbacks,
  links, XML, and exotic `oneOf`/discriminator composition are out of the initial profile and are
  documented + gated by `urban check`, the same way the manifest validator gates a profile today.
- **A cross-repo publish edge.** The `api` binding type ships in `@nanobpm/nano-app-schema`
  (this repo, `spec-app/`); `@nanobpm/urban` consumes it. The schema republishes first (this ADR's
  PR), then Urban folds the field into the manifest type.

## Open questions

- **YAML specs.** Authors expect YAML (the console spec is YAML). JSON-first keeps the starting
  slice dependency-free; do we add a pure-JS YAML parse to the host read, or a build step that
  normalizes YAML → JSON at derive time?
- **Response validation default.** `"dev"` (validate in the IDE/Run, skip in production) balances
  safety and perf; is a per-operation override warranted?
- **Security schemes.** OpenAPI `securitySchemes` could derive guards onto Urban's existing
  `security` manifest block rather than being re-authored — in scope for a later slice.
- **Client SDK.** Should `urban gen` emit a typed client for the app's own API from the same spec
  (dogfooding `@hey-api/openapi-ts` as the console does), or is that a separate tool?
- **Generic engine actions as a shipped spec.** Longer term, could the generic start/cancel/message
  actions be expressed as a shipped OpenAPI fragment so *all* endpoints share one description
  mechanism — collapsing `actions[]` into the same surface rather than merely coexisting?
