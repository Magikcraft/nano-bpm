# Releasing nano-bpm

This repo ships **three independent release trains**, each with its own tag scheme, workflow, and destination. Cutting a "complete" release means tagging all three from the same commit on `main`, in sequence.

| # | Product | Tag | Workflow | Destination |
|---|---------|-----|----------|-------------|
| 1 | Nano gateway (`nanobpm-gateway-rest-server`) + ProcessOS binaries | `v*` | `publish-c8ctl-binaries.yml`, `publish-processos-binaries.yml` | Public plugin repo (`jwulf/c8ctl-plugin-nano`) + this repo's releases + S3 mirror |
| 2 | `@nanobpm/nano-bernd` (embedded engine, npm) | `nano-bernd-npm-v*` | `release-nano-bernd-npm.yml` | npmjs.com |
| 3 | `io.github.jwulf:nano-bernd` (embedded engine, JVM) | `nano-bernd-jvm-v*` | `release-nano-bernd-jvm.yml` | Maven Central |

Details on each are in the per-package `RELEASING.md` (see below); this document is the orchestration checklist.

## Version streams

The three trains share one `nano_engine` codebase (compiled to native for #1, wasm for #2 and #3) but move at different cadences:

- **Gateway + ProcessOS** (`v*`) is the engine's own SemVer. Bump on any user-visible change to the gateway server or ProcessOS binary.
- **nano-bernd** (`nano-bernd-{npm,jvm}-v*`) is versioned by the **FFI ABI** its host wraps, not by the engine crate:
  - **Major**: ABI break.
  - **Minor**: additive ABI change (this is why the current release is `0.2.0` — ABI v1 → v2 added the job worker surface).
  - **Patch**: host-side fixes with the same ABI.
- The two `nano-bernd` packages (npm + JVM) **always share a version** — they're two hosts wrapping the same wasm blob. Never release one without the other.

The gateway version and the nano-bernd version are independent — bumping one does not require bumping the other.

## Prerequisites (one-time)

Each train has its own credentials, documented in place:

- **Gateway/ProcessOS**: `C8CTL_PLUGIN_REPO_TOKEN`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` — see the header comments in `.github/workflows/publish-{c8ctl,processos}-binaries.yml`.
- **npm nano-bernd**: `NPM_TOKEN` — see [`clients/nano-bernd/RELEASING.md`](clients/nano-bernd/RELEASING.md).
- **JVM nano-bernd**: `CENTRAL_USERNAME`, `CENTRAL_PASSWORD`, `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE` — see [`clients/nano-bernd-jvm/RELEASING.md`](clients/nano-bernd-jvm/RELEASING.md).

Verify all secrets are set at <https://github.com/nanobpm/nano-bpm/settings/secrets/actions> before your first release.

## Cutting a coordinated release

Do this from a clean `main` after all release-worthy PRs have merged.

### 1. Bump versions in a single PR

- **Gateway/ProcessOS**: bump the workspace/`server/Cargo.toml` (and `processos/Cargo.toml`) `version = "…"` fields.
- **nano-bernd npm**: bump `clients/nano-bernd/package.json` `"version"`, then run `npm install` in that directory so `package-lock.json` stays in sync.
- **nano-bernd JVM**: bump `clients/nano-bernd-jvm/pom.xml` `<version>`.

If the FFI ABI changed on this train, also update `EXPECTED_ABI_VERSION` in `clients/nano-bernd/src/index.ts` and `EmbeddedEngine.EXPECTED_ABI_VERSION` in `clients/nano-bernd-jvm/src/main/java/io/github/jwulf/nano/bernd/EmbeddedEngine.java`, and bump `ABI_VERSION` in `engine-core/scripts/emit-dist.mjs` + `engine-core/scripts/verify-wasm-ffi.mjs`. CI enforces the match.

Open a PR titled e.g. `chore(release): v0.14.0 + nano-bernd 0.2.0`, get it reviewed, merge.

### 2. Tag all three from the merge commit

```bash
git checkout main
git pull

# Choose your versions.
GATEWAY_TAG=v0.14.0
BERND_TAG_NPM=nano-bernd-npm-v0.2.0
BERND_TAG_JVM=nano-bernd-jvm-v0.2.0

git tag "$GATEWAY_TAG"
git tag "$BERND_TAG_NPM"
git tag "$BERND_TAG_JVM"
git push origin "$GATEWAY_TAG" "$BERND_TAG_NPM" "$BERND_TAG_JVM"
```

Push all three in a single `git push` — the three workflows run in parallel with no cross-dependencies.

### 3. Watch the workflows

- <https://github.com/nanobpm/nano-bpm/actions/workflows/publish-c8ctl-binaries.yml>
- <https://github.com/nanobpm/nano-bpm/actions/workflows/publish-processos-binaries.yml>
- <https://github.com/nanobpm/nano-bpm/actions/workflows/release-nano-bernd-npm.yml>
- <https://github.com/nanobpm/nano-bpm/actions/workflows/release-nano-bernd-jvm.yml>

Typical durations:
- npm nano-bernd: 3–5 min.
- JVM nano-bernd: 10–20 min (Central Portal validation dominates).
- Gateway + ProcessOS: 15–25 min (matrix cross-compilation).

### 4. Verify

- **Gateway binaries**: <https://github.com/jwulf/c8ctl-plugin-nano/releases> — assets attached to the new release. S3 mirror at `s3://sitapati-storage/nanobpm-gateway/<tag>/`.
- **ProcessOS binaries**: <https://github.com/nanobpm/nano-bpm/releases> — assets on the tag. S3 at `s3://sitapati-storage/processos/<tag>/`.
- **npm**: `npm view @nanobpm/nano-bernd version` → should be the new version. (Provenance is not published — it requires a public source repo, and nano-bpm is private.)
- **Maven Central**: <https://central.sonatype.com> shows the new version immediately; <https://search.maven.org/artifact/io.github.jwulf/nano-bernd> propagates within ~30 min. Test with `mvn dependency:get -Dartifact=io.github.jwulf:nano-bernd:0.2.0`.

## Releasing only one train

You don't have to cut all three every time.

- Engine bugfix that affects the gateway only → tag `v*` only.
- Host-side fix in `EmbeddedEngine` (either language) → bump nano-bernd patch, tag **both** `nano-bernd-{npm,jvm}-v*` (they must stay in sync).
- FFI-only change with no host update → bump nano-bernd minor, tag all three (gateway also embeds the FFI code path via the console feature).

## If something goes wrong

- **npm publish fails after tag**: fix the underlying issue on `main`, delete the failed tag locally + on origin (`git push --delete origin nano-bernd-npm-v0.2.0`), then re-tag from the fixed commit. `npm publish` refuses to overwrite an already-published version; if it partially succeeded, cut a `.1` patch instead.
- **Maven Central deployment stuck at validation**: check the portal at <https://central.sonatype.com/publishing/deployments>. Common failures are missing GPG signature or a namespace not yet verified. The Central Portal keeps failed deployments — you can inspect them, drop them, and re-run the workflow.
- **Tag pushed to wrong commit**: delete on origin, re-tag correctly. Both workflows are idempotent up to the actual `publish`/`deploy` step, which is guarded by a `tag == version` check.
