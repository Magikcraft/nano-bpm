# Releasing `@nanobpm/nano-app-schema`

Publishes the canonical Urban App manifest contract to npm: the JSON Schema
(ADR 0027), the generated TypeScript types, the project symbol index (ADR 0029),
and the fail-closed validator (ADR 0027 §4). Downstream consumers — the console
Monaco intellisense and the `@nanobpm/urban` App loader — depend on this package
instead of vendoring their own copy, which eliminates schema drift.

## One-time setup

Publishing uses **npm OIDC trusted publishing** — there is no `NPM_TOKEN`
secret. Instead the package is bound to this repository + release workflow on
npmjs.com, and the CI job mints a short-lived publish token via GitHub OIDC.

### 1. Publish the initial version once, locally

A Trusted Publisher can only be configured on a package that already exists, so
the first publish is done by hand by a maintainer with `@nanobpm` publish
rights:

```bash
cd spec-app
npm ci
npm run build
npm test
npm login                 # if not already authenticated
npm publish --access public
```

### 2. Configure the Trusted Publisher on npmjs.com

On <https://www.npmjs.com/package/@nanobpm/nano-app-schema> → **Settings** →
**Trusted Publisher** → **GitHub Actions**, set:

| Field                | Value                              |
|----------------------|------------------------------------|
| Organization / user  | `nanobpm`                          |
| Repository           | `nano-bpm`                         |
| Workflow filename    | `release-nano-app-schema-npm.yml`  |
| Environment          | *(leave blank)*                    |

After this, every `nano-app-schema-npm-v*` tag publishes automatically with no
secret. Provenance stays disabled (it requires a public source repository).

## Cutting a release

1. Update `spec-app/package.json` `"version"` to the new release version.
2. Run `npm install` in `spec-app/` so `package-lock.json` stays in sync.
3. Merge to `main`.
4. Tag from `main`:
   ```bash
   git tag nano-app-schema-npm-v0.1.0
   git push origin nano-app-schema-npm-v0.1.0
   ```
5. The `release-nano-app-schema-npm` workflow runs automatically. It will:
   - `npm ci` + verify the tag matches `package.json`.
   - `npm run build` + `npm test`.
   - `npm publish --access public` — publishes the tarball to npm.
6. Verify on <https://www.npmjs.com/package/@nanobpm/nano-app-schema>.
