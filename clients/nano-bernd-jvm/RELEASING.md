# Releasing `io.github.jwulf:nano-bernd`

This document is the one-time-setup + per-release checklist for publishing the JVM `nano-bernd` artefact to [Maven Central](https://central.sonatype.com) via the Sonatype Central Portal (the OSSRH replacement).

The npm counterpart (`@nanobpm/nano-bernd`) has its own release path; this doc covers only the JVM artefact.

## One-time setup

You need to do all of these once. The workflow (`.github/workflows/release-nano-bernd-jvm.yml`) assumes they're all in place.

### 1. Register the namespace

- Sign in at <https://central.sonatype.com> with the GitHub identity that owns this repository.
- Under **Namespaces**, add `io.github.jwulf`. Because it matches your GitHub username, it is auto-verified — no manual TXT record needed.

### 2. Generate a User Token

- In the Central Portal, go to **View Account → Generate User Token**.
- You'll get a **token username** and a **token password**. Save them; the password is shown only once.

### 3. Create a signing key

```bash
gpg --gen-key                             # accept the defaults; use josh@magikcraft.io
gpg --list-secret-keys --keyid-format=long
# copy the long key id (e.g. ABCDEF0123456789)

# Publish the public key so Central can verify signatures.
gpg --keyserver keys.openpgp.org --send-keys ABCDEF0123456789
gpg --keyserver keyserver.ubuntu.com --send-keys ABCDEF0123456789
```

### 4. Add repository secrets

In <https://github.com/jwulf/nano-bpm/settings/secrets/actions> add four secrets:

| Name              | Value                                                            |
|-------------------|------------------------------------------------------------------|
| `CENTRAL_USERNAME`| Token username from step 2                                        |
| `CENTRAL_PASSWORD`| Token password from step 2                                        |
| `GPG_PRIVATE_KEY` | `gpg --armor --export-secret-keys ABCDEF0123456789` (whole block) |
| `GPG_PASSPHRASE`  | The passphrase you set in step 3                                  |

## Cutting a release

1. Update `clients/nano-bernd-jvm/pom.xml` `<version>` to the new release version (e.g. `0.2.0`).
2. Update `CHANGELOG.md` (once we have one).
3. Merge to `main`.
4. Tag from `main`:
   ```bash
   git tag nano-bernd-jvm-v0.2.0
   git push origin nano-bernd-jvm-v0.2.0
   ```
5. The `release-nano-bernd-jvm` workflow runs automatically. It will:
   - Rebuild `nano_engine.wasm` from source (Rust → wasm32 → wasm-opt).
   - Sync it into the module's resources.
   - Verify the tag matches the pom version.
   - Build source + javadoc jars.
   - Sign every artefact with the release GPG key.
   - Upload via the `central-publishing-maven-plugin` and wait until the deployment is validated + published (usually 5–15 minutes).
6. Confirm the release on <https://central.sonatype.com> and, once it propagates (~10–30 min), on <https://search.maven.org>.

## Manual dry-run (local)

```bash
cd clients/nano-bernd-jvm
export CENTRAL_USERNAME=... CENTRAL_PASSWORD=... GPG_PASSPHRASE=...
mvn -Prelease deploy
```

Requires the same env vars the workflow uses, plus your local gpg-agent unlocked with the signing key.

## Version policy

`nano-bernd` (JVM) and `@nanobpm/nano-bernd` (npm) share a version stream keyed to the FFI ABI they wrap:
- **Major**: ABI break.
- **Minor**: additive ABI change (ABI v1 → v2 is why they're `0.2.0`).
- **Patch**: host-side fixes with the same ABI.
