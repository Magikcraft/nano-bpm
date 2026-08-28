# ADR 0064 — Server crate decomposition (compile-time-driven workspace split)

Status: Proposed
Date: 2026-08-29
Relates to: ADR 0016 (Falcon protocol), ADR 0034 (console observe/studio profiles — the `console` feature this split must preserve)
Repo: Magikcraft/nano-bpm (`server/`)

## Context

`server/` is a single binary-only crate — no `lib.rs` — of ~103,000 lines across 50 files:

| Chunk | Lines | Notes |
|---|---|---|
| `console/` (20 files) | ~35,300 (34%) | feature-gated behind `console` |
| `main.rs` | 34,259 (33%) | ~12,500 of those lines are inline `#[cfg(test)]`; one test module (`clustered_startup_tests`) is 8,832 lines |
| raft cluster (`raft`, `raft_logstore`, `raft_net`, `peer`, `cluster`, `placement`, `falcon`, `remote_sink`) | ~12,200 | the only openraft consumers |
| storage (`seglog`, `journal`, `varstore`, `varspill`, `coldspill`, `readstore`, `sqlite_space`) | ~8,200 | the only rusqlite consumers |
| engine glue + misc (13 files) | ~13,500 | |

One crate means one compilation unit for type-checking: any edit anywhere re-typechecks all
103k lines, and heavy dependencies (openraft, rusqlite, prometheus) cannot be cached behind
stable crate boundaries. The `deploy-fast` profile (thin LTO, 16 CGUs, incremental) mitigates
link cost but not single-crate type-check cost. Other codebases (e.g. Bun's recent refactor)
have addressed the same wall by decomposing the monolith into a workspace of smaller crates.

A full audit of the module dependency graph (code edges only; doc-link references excluded)
shows the layering is already **acyclic and hub-and-spoke**:

```
L0 util        metrics, memory, backpressure, recovery_throttle, drain_guard, runtime_config
L1 storage     sqlite_space → varspill → varstore; seglog, journal, readstore, remote_sink
L2 runtime     deepthi → {journal, backpressure}; partition → {deepthi, cluster, journal}
L3 raft        raft → {deepthi, journal, raft_logstore, raft_net}; raft_net → peer; peer
L4 falcon      falcon handlers → {journal, metrics, ServerImpl}
L5 console     console/* → {readstore, backpressure, metrics, ServerImpl}
L6 binary      main.rs — ServerImpl god-object + ~70 *_impl handlers + stub_impls (generated)
```

Storage and raft have **no back-references into `main.rs`** and narrow external dependency
sets — they extract cleanly. The hard knots:

1. **`ServerImpl` god-object** (`main.rs:1425`, ~50 fields, a 10,572-line impl block).
   `falcon.rs` calls 26 distinct methods on it; `console/mod.rs` and `console/generated_api.rs`
   consume it; the generated `stub_impls.rs` (35 trait impls, from `scripts/gen-stub-server.py`)
   hardcodes `use crate::ServerImpl;`. Falcon handlers and console cannot leave the binary
   until `ServerImpl` does.
2. **Console circularity**: `ServerImpl` holds a `#[cfg(feature = "console")] trace_store:
   Arc<console::trace::TraceStore>` field while `console/mod.rs` imports `crate::ServerImpl`.
3. **Falcon straddles two layers**: `peer`/`raft_net` (L3) need only the wire frames
   (`ClientFrame`/`ServerFrame`); the handlers (L4) need `ServerImpl`.
4. **Private root items consumed by child modules** (legal only within one crate):
   `raft_enabled()`, `DEFAULT_REQUEST_TIMEOUT_MS`, `json_to_value`/`value_to_json`, `PeerAddr`,
   `recovery_counts`, plus `pub(crate)` fields in console submodules.
5. **`query.rs`** is a leaf but has 291 use sites inside `main.rs` — it travels with the binary.

Build-system constraints that shape the answer:

- `[patch.crates-io]` (the vendored openraft correctness fix) and `[profile.*]` (release with
  `panic = "unwind"`, `release-test`, `deploy-fast`) apply **only at the workspace root**.
- `server/target` paths are hardcoded in `scripts/cross-build.sh`, `scripts/build-tagged.sh`,
  `load-testing/scripts/build-server.sh`, and `publish-c8ctl-binaries.yml`.
- `server/tests/*.rs` spawn the compiled binary via `env!("CARGO_BIN_EXE_nanobpm-gateway-rest-server")`,
  which requires the bin target and its integration tests to stay in the same package.
- `publish-c8ctl-binaries.yml` greps the release version from `server/Cargo.toml` — it must
  remain a real package manifest, not a virtual workspace manifest.
- `server/.config/nextest.toml` is resolved relative to the workspace root.

## Decision

Decompose `server/` into a **cargo workspace rooted at `server/`**, keeping the existing
binary package (name `nanobpm-gateway-rest-server`, its `src/`, its tests, its build.rs, and
its manifest version) in place, with new library members under `server/crates/`. Rooting the
workspace at `server/` keeps `server/target`, `server/Cargo.lock`, `server/.config/nextest.toml`,
the `[patch]` section, and the profiles exactly where every Makefile target, script, and CI job
already expects them — no build-orchestration churn.

Target layout:

```
server/
  Cargo.toml            # [workspace] + existing bin package (name/version unchanged)
  src/                  # binary: main.rs, ServerImpl, handlers, query, response_contract,
                        # stub_impls, falcon handlers, consumers
  crates/
    nano-server-storage/    # metrics, memory, sqlite_space, varstore, varspill,
                            # coldspill, seglog, journal, readstore, remote_sink
    nano-server-runtime/    # backpressure, drain_guard, recovery_throttle,
                            # runtime_config, placement, cluster, deepthi, partition
    nano-falcon-protocol/   # ClientFrame/ServerFrame wire types only
    nano-server-raft/       # raft, raft_logstore, raft_net, peer
    nano-server-console/    # console/* — requires the ServerImpl seam (below)
```

Migration is **phased**, each phase behavior-preserving and independently mergeable:

- **Phase 1 — workspace + `nano-server-storage`.** Mechanical file move; `pub(crate)` → `pub`
  promotion where the binary consumes items; the bin crate re-exports moved modules at its root
  (`pub(crate) use nano_server_storage::journal;` etc.) so `crate::journal::…` paths in
  untouched files keep resolving. Build entry points updated so clippy/nextest cover the whole
  workspace (`--workspace`).
- **Phase 2 — `nano-server-runtime`, `nano-falcon-protocol`, `nano-server-raft`.** Same
  pattern. `falcon.rs` splits along its existing seam: wire frames to the protocol crate,
  the `ServerImpl`-driven dispatcher stays in the binary.
- **Phase 3 — `nano-server-console`.** Requires untying the `ServerImpl` knot: pull
  `TraceStore` into a small shared crate (breaks the cycle), then either (a) move `ServerImpl`
  + `stub_impls.rs` into a `nano-server-core` crate both the binary and console depend on —
  teaching `scripts/gen-stub-server.py` the new import path — or (b) define a trait seam over
  the ~15 methods console actually calls. Default: (a), less churn, matches how the generated
  stubs already work.
- **Phase 4 — test relocation (optional).** `clustered_startup_tests` +
  `subscription_placement_tests` (~9,800 lines, 29% of `main.rs`) drive `pub(crate)` internals
  via `build_server_in_memory`; once `ServerImpl` lives in a library crate they can move to
  that crate's `tests/` directory.

End state takes the always-compiled binary from ~103k to ~55k lines; with console extracted
(on for CI and dev builds) the hot edit loop drops to ~20k.

## Consequences

- **Dev-loop builds benefit most**: `cargo check`/clippy/nextest iteration and CI lint time.
  Fat-LTO `codegen-units = 1` release builds still serialize at link time by design; the
  release artifact build is not the target.
- Editing the binary no longer recompiles storage/raft; their dependency stacks (rusqlite,
  openraft, prometheus) cache behind stable crate boundaries.
- CI (`ci.yml` server job) and Makefile switch to `--workspace` for clippy/nextest so member
  crates are actually linted and tested. `nextest.toml` test-name filters (e.g. the
  `serial-heavy` group's `seglog::tests`) must track moved test binaries.
- Visibility promotion (`pub(crate)` → `pub`) widens the effective API surface of moved
  modules; the workspace boundary makes `warnings = "deny"` surface every missed edge as a
  hard error, which acts as the migration checklist.
- One lockfile already covers the workspace (`server/Cargo.lock`); sibling crates
  (engine-core, read-model, …) keep their own lockfiles and are unaffected.
- Each phase is a separate PR: red/green discipline means the full existing suite (nextest +
  the three e2e binary harnesses) must pass on the first run, per project policy.

## Open questions

- Phase 3 seam choice: shared `nano-server-core` crate vs. trait seam for console's
  `ServerImpl` usage — decided when Phase 3 is scoped.
- Is test relocation (Phase 4) worth the visibility promotion it requires, or do the inline
  test modules stay in the binary where `pub(crate)` access is free?
- Does `console-observe` (ADR 0034) need any feature-graph reshaping once console is its own
  crate, or does a feature on the binary that enables a feature on `nano-server-console`
  suffice?
