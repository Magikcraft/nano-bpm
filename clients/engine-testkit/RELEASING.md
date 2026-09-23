# Releasing `@nanobpm/engine-testkit`

The engine-facing `assertThat*` DSL (Tier A of issue #894), co-located with
`engine-wasm` so it asserts over the same read-model build. Pure TypeScript,
**zero runtime dependencies**, no wasm — so releasing is just build + test +
publish (no engine build / wasm sync, unlike `../nano-bernd`).

## One-time setup

Publishing uses **npm OIDC trusted publishing** — there is no `NPM_TOKEN`
secret. Instead the package is bound to this repository + release workflow on
npmjs.com, and the CI job mints a short-lived publish token via GitHub OIDC.

### 1. Publish the initial version once, locally

A Trusted Publisher can only be configured on a package that already exists, so
the first publish is done by hand by a maintainer with `@nanobpm` publish
rights:

```bash
cd clients/engine-testkit
npm ci
npm run build
npm test
npm login                 # if not already authenticated
npm publish --access public
```

### 2. Configure the Trusted Publisher on npmjs.com

On <https://www.npmjs.com/package/@nanobpm/engine-testkit> → **Settings** →
**Trusted Publisher** → **GitHub Actions**, set:

| Field                | Value                             |
|----------------------|-----------------------------------|
| Organization / user  | `nanobpm`                         |
| Repository           | `nano-bpm`                        |
| Workflow filename    | `release-engine-testkit-npm.yml`  |
| Environment          | *(leave blank)*                   |

After this, every `engine-testkit-npm-v*` tag publishes automatically with no
secret. Provenance stays disabled (it requires a public source repository).

## Cutting a release

1. Update `clients/engine-testkit/package.json` `"version"` to the new release version.
2. Run `npm install` in `clients/engine-testkit/` so `package-lock.json` stays in sync (CI verifies).
3. Merge to `main`.
4. Tag from `main`:
   ```bash
   git tag engine-testkit-npm-v0.1.0
   git push origin engine-testkit-npm-v0.1.0
   ```
5. The `release-engine-testkit-npm` workflow runs automatically. It will:
   - Verify the tag matches `package.json`.
   - `npm run build` (tsc) + `npm test`.
   - `npm publish --access public` — publishes the tarball to npm.
6. Verify on <https://www.npmjs.com/package/@nanobpm/engine-testkit>.
