# Releasing `@nanobpm/engine-wasm`

Publishes **`@nanobpm/engine-wasm`** — the wasm-pack (`--target web`) build of
the Rust in-browser engine — to npm.

> **The Bojtos framework packages moved.** `@nanobpm/bojtos-kit` and
> `@nanobpm/bojtos-react` were extracted to the standalone public repo
> [`nanobpm/bojtos`](https://github.com/nanobpm/bojtos) and are published from
> there (see that repo's `docs/releasing.md`). They consume `@nanobpm/engine-wasm`
> from npm. This repo only publishes `engine-wasm`, whose source lives here
> (built from the Rust engine via `make console-wasm`).

## How it works

- `@nanobpm/engine-wasm` ships the prebuilt wasm + generated `.d.ts`/`.js`. The
  publish is driven by **`scripts/bojtos-release.mjs`**, which packs and publishes
  the single `engine-wasm/pkg` package.
- Authentication is **npm OIDC trusted publishing** — there is no `NPM_TOKEN`.
  CI mints a short-lived publish token from GitHub OIDC; the package is bound to
  this repo + workflow on npmjs.com (one-time, below).
- Provenance is disabled (`NPM_CONFIG_PROVENANCE=false`) — it requires a public
  source repository, and nano-bpm is private.

## One-time setup

A Trusted Publisher can only be configured on a package that already exists;
`@nanobpm/engine-wasm` is already published, so this is done. For reference, on
npmjs.com for `@nanobpm/engine-wasm`: **Settings → Trusted Publisher → GitHub
Actions**:

| Field               | Value                     |
|---------------------|---------------------------|
| Organization / user | `Magikcraft`              |
| Repository          | `nano-bpm`                |
| Workflow filename   | `release-bojtos-npm.yml`  |
| Environment         | *(leave blank)*           |

After that, tagged releases publish automatically with no secret.

## Cutting a release

1. Bump the version in `engine-wasm/pkg.package.json` (the source of truth for
   `@nanobpm/engine-wasm`; `make console-wasm` copies it into
   `engine-wasm/pkg/package.json`).
2. `make console-wasm` — rebuilds the wasm so the committed artifact matches the
   new version.
3. Commit, open a PR, merge to `main`.
4. Tag from `main` and push:
   ```bash
   git tag bojtos-npm-v0.1.0
   git push origin bojtos-npm-v0.1.0
   ```
5. The `release-bojtos-npm` workflow builds the wasm from source, verifies the
   tag matches the package version, and publishes via OIDC.
6. Verify on npmjs.com:
   - <https://www.npmjs.com/package/@nanobpm/engine-wasm>

## Manual dry-run (local, no publish)

```bash
make console-wasm
node scripts/bojtos-release.mjs --dry-run
```

This runs `npm publish --dry-run` for `engine-wasm/pkg` and prints the tarball
contents without publishing.
