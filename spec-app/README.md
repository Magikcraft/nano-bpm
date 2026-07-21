# spec-app — the Urban App manifest (`nano.app.json`)

This directory is the **single source of truth** for the Urban App manifest, the
declared-data document that binds an Urban RAD application together (models,
data, triggers, surfaces, workers, llm, security). See
[`docs/adr/0027-urban-app-manifest-spec.md`](../docs/adr/0027-urban-app-manifest-spec.md).

It is the sibling of [`spec-console/`](../spec-console/): one hand-authored
schema drives generated types, so hand-written DTOs can never drift from the
wire shape.

## Files

| Path | Role |
|---|---|
| `nano-app.schema.json` | The canonical **JSON Schema** (draft 2020-12) for `nano.app.json`. Source of truth **and** the `$schema` an editor uses for autocompletion. |
| `gen/nano-app.d.ts` | **Generated** TypeScript types (`AppManifest`, …). Committed; do not edit by hand. |
| `examples/*.nano.app.json` | Valid example manifests (fixtures — must pass validation). |
| `examples/invalid/*.nano.app.json` | Fixtures that must be **rejected** (proves the schema is fail-closed, ADR 0027 §4). |

## TypeScript-only, by design

The Urban App is a **Deno/TypeScript** binary, so the manifest is consumed only
by TypeScript — the console **App panels** (authoring) and the **Deno App
loader** (running). It is *not* read by the Rust server, which handles project
files as opaque bytes (ADR 0027 §1). So this schema generates **TypeScript**
types only; there is no Rust emitter.

## Env & secret substitution

Any string value may be a `${VAR}` or `${VAR:-default}` reference, resolved at
App boot / IDE Run and **never persisted** (ADR 0027 §5). Consequence: a
committed `nano.app.json` carries no secrets. The schema validates the *shape*
of such a reference (via the `envTemplate` `$def`), not its resolved value.
Control-plane enums that the ADR examples never template (e.g. `runtime.engine`,
`security.mode`) stay strict; the fields the ADRs actually template — datasource
`driver`/`url`, credentials, model ids, paths — accept templates.

## Regenerate & validate

From the repo root:

```sh
make generate-app-manifest
```

or directly (from this directory):

```sh
npm install          # first time
npm run validate     # validate every example against the schema
npm run gen          # regenerate gen/nano-app.d.ts
npm test             # validate + assert gen/ is up to date (CI gate)
```

`npm run gen -- --check` (and `scripts/generate-app-manifest.sh --check`) fail if
the committed `gen/nano-app.d.ts` is stale — run it in CI after any schema edit.
