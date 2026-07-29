# Releasing the @nanobpm/workflow npm package

Publishes the code-first durable-orchestration SDK (ADR 0044) to npm as
`@nanobpm/workflow`.

Once published, a developer can `npm install @nanobpm/workflow` and author
durable workflows against any running nanobpmn gateway (see the quickstart in
`workflow/README.md`).

## How it works

- `@nanobpm/workflow` is pure TypeScript with **no wasm build** and **no internal
  `@nanobpm/*` dependencies**, so — unlike the Bojtos release — there is nothing
  to build from wasm and no `file:` link to rewrite. **`scripts/workflow-release.mjs`**
  is a plain `npm publish` in `workflow/`; `prepack` rebuilds `dist/` from source
  so the tarball is always built from the tagged tree.
- Authentication is **npm OIDC trusted publishing** — there is no `NPM_TOKEN`.
  CI mints a short-lived publish token from GitHub OIDC; the package must be
  bound to this repo + workflow on npmjs.com (one-time, below).
- Provenance is disabled (`NPM_CONFIG_PROVENANCE=false`) — it requires a public
  source repository, and nano-bpm is private.

## One-time setup

A Trusted Publisher can only be configured on a package that already exists, so
the first publish is done locally by a maintainer with `@nanobpm` publish rights.

1. Build, test and publish locally:

   ```bash
   make workflow                  # builds workflow/dist from source
   cd workflow && npm test        # 8 unit + (if a gateway binary is present) 2 integration
   npm login                      # if not already authenticated
   npm publish --dry-run          # sanity-check what will be packed
   npm publish                    # real first publish (creates the package)
   ```

2. On npmjs.com, for `@nanobpm/workflow`: **Settings → Trusted Publisher →
   GitHub Actions**:

   | Field               | Value                        |
   |---------------------|------------------------------|
   | Organization / user | `Magikcraft`                 |
   | Repository          | `nano-bpm`                   |
   | Workflow filename   | `release-workflow-npm.yml`   |
   | Environment         | *(leave blank)*              |

After that, tagged releases publish automatically with no secret.

## Cutting a release

1. Bump `version` in `workflow/package.json`.
2. `make workflow` — rebuilds `workflow/dist` so the committed artifact matches
   the new version.
3. Commit, open a PR, merge to `main`.
4. Tag from `main` and push:
   ```bash
   git tag workflow-npm-v0.1.0
   git push origin workflow-npm-v0.1.0
   ```
5. The `release-workflow-npm` workflow builds/tests the package, verifies the tag
   matches `workflow/package.json` version, and publishes via OIDC.
6. Verify on npmjs.com: <https://www.npmjs.com/package/@nanobpm/workflow>

## Manual dry-run (local, no publish)

```bash
make workflow
cd workflow && npm publish --dry-run
```
