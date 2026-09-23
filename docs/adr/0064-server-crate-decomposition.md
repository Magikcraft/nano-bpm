# ADR 0064 — Server crate decomposition (compile-time-driven workspace split)

Status: Proposed
Date: 2026-08-29
Relates to: ADR 0016 (Falcon protocol), ADR 0034 (console observe/studio profiles — the `console` feature this split must preserve)
Repo: nanobpm/nano-bpm (`server/`)

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
    nano-trace-store/       # TraceStore (shared leaf crate; breaks the console↔core cycle)
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
- **Phase 3 — `nano-server-console` (done, #1050).** Untied the `ServerImpl` knot in two
  steps: first pulled `TraceStore` into the shared `nano-trace-store` leaf crate (breaks the
  cycle), then took option (b) — a `ConsoleServer` trait seam over the ~9 methods console
  actually calls — rather than (a)'s shared `nano-server-core`. Option (b) avoided dragging the
  console DTO/mapping surface into a core crate to satisfy the `generated_api.rs` orphan-rule
  knot (see Open questions); the 14 generated `impl apis::* for ServerImpl` blocks stay in the
  binary (`server/src/console_api.rs`). Shared leaves `nano-server-net` and `nano-version-stamp`
  were split out to remove the last binary↔console couplings.
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
  `ServerImpl` usage.
  - **Step 1 (done, #1050): `TraceStore` extracted into the `nano-trace-store` leaf
    crate**, breaking the `ServerImpl.trace_store: Arc<console::trace::TraceStore>` ↔
    `console → crate::ServerImpl` import cycle (knot #2 of the audit). Both the binary and
    a future console crate can now depend on it; the console module aliases it back
    (`pub use nano_trace_store as trace;`) so intra-console `trace::…` paths are unchanged.
  - **Step 2 (done, #1050): the console extracted into `nano-server-console` behind a
    `ConsoleServer` trait seam — option (b).** Rather than move `ServerImpl` (and, per the
    orphan-rule knot below, its Api-trait impls plus the DTO/mapping logic they call) into a
    shared core crate, the console now depends on the binary only through the object-safe
    `ConsoleServer` trait (`nano_server_console::ConsoleServer`), an `Arc<dyn ConsoleServer>`
    router state, and `&dyn ConsoleServer` handler params. The trait exposes the ~9 methods the
    console actually needs (`store`, `trace_store`, `cluster_topology`, `sla_mode` /
    `switch_sla_mode`, `raft_enabled`, `recovery_counts`, `raft_partition_metrics`,
    `instance_job_overlay`) and deliberately hides binary-only types (`Partitions`,
    `DeepthiHandle`, the raft registry). The `generated_api.rs` orphan-rule knot (below) is
    resolved by keeping the 14 `impl apis::* for ServerImpl` blocks in the binary
    (`server/src/console_api.rs`), where they call into `nano_server_console::…` and add one
    `impl ConsoleServer for ServerImpl`. Two shared leaf helpers were also extracted to break
    remaining coupling: `nano-server-net` (`PeerAddr` + `NoDelayListener`, needed unconditionally
    by the binary and by the console's `ConnectInfo<PeerAddr>`) and `nano-version-stamp` (the
    `NANOBPM_VERSION` build-script derivation, shared by both build scripts so the two can never
    drift), and `json_to_value` / `RecoveryCounts` moved to `nanobpmn-read-model` /
    `nano-server-runtime` respectively.
  - **The `generated_api.rs` orphan-rule knot (resolved by option (b) above).**
    `server/src/console_api.rs` (was `console/generated_api.rs`) holds **14 `impl apis::* for
    ServerImpl` blocks** (the generated `nanobpm-console-api` Api traits). Rust's orphan rule
    requires an `impl ForeignTrait for ForeignType` to live in the crate that defines the trait
    (`nanobpm-console-api`, generated — not editable) **or** the type (`ServerImpl`). Under
    option (a) — `ServerImpl` in `nano-server-core`, `nano-server-console` depends on core —
    these 14 impls **must** live in core, but their calls into console logic would then make
    **core depend on console**, re-forming a cycle; option (a) would therefore have to drag the
    console DTO/mapping surface into core too. Option (b)'s trait seam keeps that surface in
    `nano-server-console` and the impls in the binary, so no cycle forms.
- Test relocation: the inline `#[cfg(test)]` modules moved **with** their code into
  `nano-server-console`; the visibility promotion this required (module-private `pub(super)`
  items became `pub`, the console's now-public API surface) was mechanical and the workspace
  boundary's `-D warnings` caught every leak, so relocation was worth it.
- `console-observe` (ADR 0034) needs no feature-graph reshaping: the crate carries a matching
  `console-observe` feature (swapping the embedded `console/dist` → `console/dist-observe`
  RustEmbed folder), and the binary's `console-observe = ["console",
  "nano-server-console/console-observe"]` simply forwards it — a feature on the binary that
  enables a feature on `nano-server-console` suffices.
