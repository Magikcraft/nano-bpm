# Releasing `@nanobpm/nano-bernd`

Publishes the npm counterpart to `io.github.jwulf:nano-bernd`. See [`../nano-bernd-jvm/RELEASING.md`](../nano-bernd-jvm/RELEASING.md) for the Maven side.

## One-time setup

Publishing uses **npm OIDC trusted publishing** — there is no `NPM_TOKEN`
secret. Instead the package is bound to this repository + release workflow on
npmjs.com, and the CI job mints a short-lived publish token via GitHub OIDC.

### 1. Publish `0.2.0` once, locally

A Trusted Publisher can only be configured on a package that already exists, so
the first publish is done by hand by a maintainer with `@nanobpm` publish
rights:

```bash
cd clients/nano-bernd
make -C ../.. engine-wasm-ffi-dist
npm ci
npm run sync-wasm
npm run build
npm test
npm login                 # if not already authenticated
npm publish --access public
```

### 2. Configure the Trusted Publisher on npmjs.com

On <https://www.npmjs.com/package/@nanobpm/nano-bernd> → **Settings** →
**Trusted Publisher** → **GitHub Actions**, set:

| Field                | Value                          |
|----------------------|--------------------------------|
| Organization / user  | `Magikcraft`                   |
| Repository           | `nano-bpm`                     |
| Workflow filename    | `release-nano-bernd-npm.yml`   |
| Environment          | *(leave blank)*                |

After this, every `nano-bernd-npm-v*` tag publishes automatically with no
secret. Provenance stays disabled (it requires a public source repository).

## Cutting a release

1. Update `clients/nano-bernd/package.json` `"version"` to the new release version.
2. Run `npm install` in `clients/nano-bernd/` so `package-lock.json` stays in sync (CI verifies).
3. Merge to `main`.
4. Tag from `main`:
   ```bash
   git tag nano-bernd-npm-v0.2.0
   git push origin nano-bernd-npm-v0.2.0
   ```
5. The `release-nano-bernd-npm` workflow runs automatically. It will:
   - Rebuild `nano_engine.wasm` from source.
   - Sync it into `clients/nano-bernd/wasm/`.
   - Verify the tag matches `package.json`.
   - `npm run build` + `npm test`.
   - `npm publish --access public` — publishes the tarball to npm.
6. Verify on <https://www.npmjs.com/package/@nanobpm/nano-bernd>.

## Manual dry-run (local, no publish)

```bash
cd clients/nano-bernd
make -C ../.. engine-wasm-ffi-dist
npm ci
npm run sync-wasm
npm run build
npm test
npm pack --dry-run    # shows what would be published
```

## Version policy

`@nanobpm/nano-bernd` and `io.github.jwulf:nano-bernd` share a version stream keyed to the FFI ABI they wrap. Tag both when you cut a release for either.
