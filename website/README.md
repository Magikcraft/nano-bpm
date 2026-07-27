# website — the `nanobpm.io` publishing site

Generates the static site served at **https://nanobpm.io**, the canonical home
for every schema and namespace this repository publishes. The site is
**generated from the in-repo sources of truth on every deploy** — there are no
hand-maintained copies — so a published URL can never drift from the artifact it
names.

## What is published

| URL | Source of truth | Kind |
|---|---|---|
| `https://nanobpm.io/spec-app/nano-app.schema.json` | [`spec-app/nano-app.schema.json`](../spec-app/nano-app.schema.json) | JSON Schema (the `$schema` editors fetch for `nano.app.json`) |
| `https://nanobpm.io/schema/shapes/1.0` | [`console/src/moddle/nanoShapes.ts`](../console/src/moddle/nanoShapes.ts) | BPMN `nano:` moddle namespace (ADR 0040) |

Each artifact's **output path is derived from its own declared identity** (the
schema's `$id`, the moddle descriptor's `uri`). The build asserts that identity
resolves to `nanobpm.io`, and a repo-wide guard fails the build if any tracked
file still references the legacy `.dev` domain — so the URLs, the `$id`s and the
served files can only ever agree.

## Build

```sh
node website/build.mjs   # Node >= 23.6 / 24 (imports the .ts descriptor directly)
```

Output lands in `website/_site/` (git-ignored). CI builds it on every PR (drift
guard) and deploys it to GitHub Pages on merge to `main` — see
[`.github/workflows/pages.yml`](../.github/workflows/pages.yml).

## One-time hosting setup (repo admin)

1. **Settings → Pages → Build and deployment → Source: GitHub Actions.**
2. DNS for `nanobpm.io` at the registrar:
   - apex `A`/`ALIAS` → GitHub Pages IPs (`185.199.108–111.153`), or an
     `ALIAS`/`ANAME` to `<org>.github.io`.
   - `www` `CNAME` → `<org>.github.io` (optional).
3. The `CNAME` file is emitted into `_site` by the build, so Pages keeps the
   custom domain across deploys. Enable **Enforce HTTPS** once the cert issues.
