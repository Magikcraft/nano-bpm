# Releasing the Bojtos npm packages

Publishes the three packages of the Bojtos in-browser BPMN demo framework
(ADR 0043) to npm, in dependency order:

```
@nanobpm/engine-wasm  →  @nanobpm/bojtos-kit  →  @nanobpm/bojtos-react
```

Once published, anyone can build their own Bojtos demo outside this monorepo
(see the quickstart in `bojtos-react/README.md`).

## How it works

- In the repo the inter-package dependencies are local `file:` links so in-tree
  builds, the console, and `website/demo` resolve them with no registry. npm
  cannot publish `file:` deps, so **`scripts/bojtos-release.mjs`** rewrites each
  internal `@nanobpm/*` dependency to `^<version>` at publish time and restores
  the committed `file:` link afterwards.
- Authentication is **npm OIDC trusted publishing** — there is no `NPM_TOKEN`.
  CI mints a short-lived publish token from GitHub OIDC; the package must be
  bound to this repo + workflow on npmjs.com (one-time, below).
- Provenance is disabled (`NPM_CONFIG_PROVENANCE=false`) — it requires a public
  source repository, and nano-bpm is private.

## One-time setup (per package)

A Trusted Publisher can only be configured on a package that already exists, so
the first publish of each package is done locally by a maintainer with
`@nanobpm` publish rights.

1. Build and publish all three locally:

   ```bash
   make bojtos                    # builds the engine wasm + both dists from source
   npm login                      # if not already authenticated
   node scripts/bojtos-release.mjs --dry-run   # sanity-check what will be packed
   node scripts/bojtos-release.mjs             # real publish, dependency order
   ```

2. On npmjs.com, for **each** of `@nanobpm/engine-wasm`, `@nanobpm/bojtos-kit`
   and `@nanobpm/bojtos-react`: **Settings → Trusted Publisher → GitHub
   Actions**:

   | Field               | Value                     |
   |---------------------|---------------------------|
   | Organization / user | `Magikcraft`              |
   | Repository          | `nano-bpm`                |
   | Workflow filename   | `release-bojtos-npm.yml`  |
   | Environment         | *(leave blank)*           |

After that, tagged releases publish automatically with no secret.

## Cutting a release

1. Bump the version in all three packages to the same value:
   - `engine-wasm/pkg.package.json` (source of truth for `@nanobpm/engine-wasm`;
     `make console-wasm` copies it into `engine-wasm/pkg/package.json`)
   - `bojtos-kit/package.json`
   - `bojtos-react/package.json`
2. `make bojtos` — rebuilds the wasm + both dists so the committed artifacts
   match the new version.
3. Commit, open a PR, merge to `main`.
4. Tag from `main` and push:
   ```bash
   git tag bojtos-npm-v0.1.0
   git push origin bojtos-npm-v0.1.0
   ```
5. The `release-bojtos-npm` workflow builds the wasm from source, builds/tests
   both dists, verifies the tag matches all three versions, and publishes them
   in dependency order via OIDC.
6. Verify on npmjs.com:
   - <https://www.npmjs.com/package/@nanobpm/engine-wasm>
   - <https://www.npmjs.com/package/@nanobpm/bojtos-kit>
   - <https://www.npmjs.com/package/@nanobpm/bojtos-react>

## Manual dry-run (local, no publish)

```bash
make bojtos
node scripts/bojtos-release.mjs --dry-run
```

This runs `npm publish --dry-run` for each package (with deps temporarily pinned
to `^version`) and prints the tarball contents without publishing.
