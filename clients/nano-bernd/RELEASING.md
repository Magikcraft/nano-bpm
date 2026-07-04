# Releasing `@nanobpm/nano-bernd`

Publishes the npm counterpart to `io.github.jwulf:nano-bernd`. See [`../nano-bernd-jvm/RELEASING.md`](../nano-bernd-jvm/RELEASING.md) for the Maven side.

## One-time setup

### 1. Create an Automation npm token

- <https://www.npmjs.com/settings/{you}/tokens/new> → **Automation** token.
- Scope: `@nanobpm` (or the org that owns the package).

### 2. Add the repository secret

In <https://github.com/jwulf/nano-bpm/settings/secrets/actions>:

| Name        | Value                     |
|-------------|---------------------------|
| `NPM_TOKEN` | Automation token from #1  |

Provenance is signed via GitHub OIDC — no other secret needed. The workflow already grants `id-token: write`.

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
   - `npm publish --provenance --access public` — the tarball is published with an OIDC-attested provenance link back to this workflow run.
6. Verify on <https://www.npmjs.com/package/@nanobpm/nano-bernd>. The provenance badge should point at the tagged commit.

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
