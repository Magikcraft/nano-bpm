//! Out-of-process delegation to the `@nanobpm/urban` CLI (`urban`).
//!
//! Per ADRs 0052/0053/0054 (epic #514, "dry out the Nano host") the console is
//! migrating from its embedded Rust + TS codegen to spawning the shared
//! `@nanobpm/urban` toolkit out-of-process (`urban gen` / `urban run`): the
//! manifest is the contract and `@nanobpm/urban` is the one interpreter +
//! deriver, so hosts (CLI, IDE, console) are interchangeable front-ends over
//! the same library rather than parallel re-implementations of it.
//!
//! This module is the **single host-side seam** for locating and probing that
//! binary. Keeping resolution in one place is what lets the later delegation
//! steps (spawn `urban gen`, delete the embedded codegen) rewire call sites
//! without each one growing its own copy of the lookup.
//!
//! ## Coordination seam — issue #520
//!
//! [`find_urban`] is the one place the console resolves the `urban` binary
//! (ownership agreed on #522: #522 owns the single resolver + the
//! `urbanAvailable` status field; #520 injects the pack lookup here and consumes
//! `urbanAvailable` in the frontend). Issue #520 delivers `urban` as a
//! first-party marketplace App pack (`nano-ide-app-urban`), lazy-installed under
//! `<workspace>/extensions/<pkg>/node_modules/.bin/urban`. That pack-relative
//! lookup plugs in **here**, at the marked `#520 seam`, between the
//! explicit-env override and the `PATH` probe — making the resolution order
//! `NANOBPMN_URBAN_BIN` → pack → `PATH` (today, before that seam is filled, it
//! is simply `NANOBPMN_URBAN_BIN` → `PATH`). Do not add a second resolver
//! elsewhere — extend this one.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// Name of the `urban` executable, platform-adjusted (npm installs a `.cmd`
/// shim on Windows).
const URBAN_EXE: &str = if cfg!(windows) { "urban.cmd" } else { "urban" };

/// The pack-relative `urban` binary path: `<extensions>/nanobpm__nano-ide-app-urban/
/// node_modules/.bin/urban`. This is the Studio-managed acquisition path — the
/// pack is lazy-installed on first Urban use (#520) — and is preferred over an
/// ambient `PATH` install so a Studio-pinned `urban` wins. Returns the
/// candidate path unconditionally (existence is checked by [`resolve_urban`]).
///
/// The pack directory is resolved through [`super::extensions::pack_install_dir`]
/// from the canonical [`super::extensions::URBAN_PACK_PKG`] name, so the install
/// target (the installer) and the lookup target (here) are guaranteed to be the
/// same path — one spelling, no drift. Falls back to the bare extensions root if
/// the pack name ever fails validation (it will not: it is a compile-time const).
fn pack_urban_bin() -> PathBuf {
    super::extensions::pack_install_dir(super::extensions::URBAN_PACK_PKG)
        .unwrap_or_else(super::extensions::extensions_root)
        .join("node_modules")
        .join(".bin")
        .join(URBAN_EXE)
}

/// The project-local `urban` binary path: `<project>/node_modules/.bin/urban`.
/// This is the toolkit an Urban app declares in its own `package.json` and that
/// a plain `npm install` (or the console's post-apply refresh) materialises —
/// the exact version the app was authored/tested against. Returns the candidate
/// path unconditionally (existence is checked by [`resolve_urban`]).
///
/// The nano server process's own `PATH` does not include a project's local
/// `node_modules/.bin` (only an npm script's environment does), so without an
/// explicit probe here a project-local install is invisible to the host — the
/// bug behind #776.
fn project_urban_bin(project_dir: &Path) -> PathBuf {
    project_dir
        .join("node_modules")
        .join(".bin")
        .join(URBAN_EXE)
}

/// Locates the `urban` CLI binary, mirroring [`super::workers::find_deno`].
/// The resolution order is `NANOBPMN_URBAN_BIN` (explicit override) → the
/// first-party marketplace pack ([`pack_urban_bin`]) → `PATH`.
///
/// This is the **host-global** lookup, used for the `urbanAvailable` capability
/// flag and pack-install decisions. Project-scoped call sites (`urban gen` /
/// `urban derive` for a specific app) should prefer [`find_urban_for`], which
/// also consults the project's own `node_modules/.bin/urban`.
///
/// Returns `None` when no binary is found; callers then report
/// `urbanAvailable: false` so the Studio can prompt the user to install the
/// pack. The order is deliberate (agreed on #520/#522): an explicit override
/// always wins, the first-party pack is preferred over an ambient `PATH`
/// install so a Studio-pinned `urban` wins, and there is no `npx` fallback —
/// the pack is the real-machine acquisition path.
///
/// The actual matching is delegated to the pure [`resolve_urban`] so it can be
/// unit-tested without mutating the global process environment (which is UB
/// under parallel `cargo test`).
pub(crate) fn find_urban() -> Option<PathBuf> {
    let bin_override = std::env::var("NANOBPMN_URBAN_BIN").ok();
    let pack = pack_urban_bin();
    let path = std::env::var_os("PATH");
    resolve_urban(
        bin_override.as_deref(),
        Some(pack.as_path()),
        None,
        path.as_deref(),
    )
}

/// Project-scoped variant of [`find_urban`] that also consults the project's own
/// `<project_dir>/node_modules/.bin/urban`, ranked between the marketplace pack
/// and `PATH`. Resolution order:
///
///   `NANOBPMN_URBAN_BIN` → marketplace pack → `<project>/node_modules/.bin/urban` → `PATH`
///
/// The project-local install (the version the app declares in its `package.json`
/// and that its post-apply refresh `npm install`s) wins over a bare ambient
/// `PATH` install — "run the version the app was authored/tested against" — but
/// still yields to an explicit override and the Studio-pinned pack, preserving
/// existing precedence (#776).
pub(crate) fn find_urban_for(project_dir: &Path) -> Option<PathBuf> {
    let bin_override = std::env::var("NANOBPMN_URBAN_BIN").ok();
    let pack = pack_urban_bin();
    let local = project_urban_bin(project_dir);
    let path = std::env::var_os("PATH");
    resolve_urban(
        bin_override.as_deref(),
        Some(pack.as_path()),
        Some(local.as_path()),
        path.as_deref(),
    )
}

/// Pure core of [`find_urban`] / [`find_urban_for`]: given the
/// `NANOBPMN_URBAN_BIN` override, the marketplace-pack bin candidate, the
/// optional project-local bin candidate, and the `PATH` value, applies the
/// `env → pack → project-local → PATH` resolution. Kept free of any global-env
/// reads so it is deterministic and testable in parallel.
fn resolve_urban(
    bin_override: Option<&str>,
    pack_bin: Option<&Path>,
    local_bin: Option<&Path>,
    path: Option<&OsStr>,
) -> Option<PathBuf> {
    if let Some(p) = bin_override
        && !p.is_empty()
    {
        let pb = PathBuf::from(p);
        if pb.is_file() {
            return Some(pb);
        }
    }
    // #520 seam: the marketplace pack's `<pack>/node_modules/.bin/urban`, tried
    // before the project-local install and `PATH` so a Studio-pinned pack wins.
    if let Some(pack) = pack_bin
        && pack.is_file()
    {
        return Some(pack.to_path_buf());
    }
    // #776: the project's own `<project>/node_modules/.bin/urban` — the version
    // the app declares — preferred over a bare ambient `PATH` install. Absent
    // for host-global lookups ([`find_urban`]), present for project-scoped ones.
    if let Some(local) = local_bin
        && local.is_file()
    {
        return Some(local.to_path_buf());
    }
    if let Some(path) = path {
        for dir in std::env::split_paths(path) {
            let cand = dir.join(URBAN_EXE);
            if cand.is_file() {
                return Some(cand);
            }
        }
    }
    None
}

/// Whether the `urban` CLI is available on this host — surfaced as
/// `urbanAvailable` next to `denoAvailable`/`nodeAvailable` in the project
/// status JSON so the Studio can gate urban-app affordances (mirrors
/// [`super::workers::WorkerSupervisor::deno_available`]). Presence only; the
/// binary is not executed. Host-global (override/pack/`PATH`); a project-local
/// install is not counted here — see [`urban_available_for`].
pub(crate) fn urban_available() -> bool {
    find_urban().is_some()
}

/// Project-scoped counterpart of [`urban_available`]: whether an `urban` binary
/// resolves for `project_dir`, including the project's own
/// `node_modules/.bin/urban` (#776). Used to gate project codegen delegation so
/// an app whose toolkit lives only in its local `node_modules` still gens.
pub(crate) fn urban_available_for(project_dir: &Path) -> bool {
    find_urban_for(project_dir).is_some()
}

/// Whether a resolved `urban` binary supports **model derivation** — the
/// `urban derive [--stdout]` subcommand + the `urban gen --no-models` flag
/// (nano-ide#92, ADR 0045/0048). This is a **capability probe, not a version
/// check** (agreed on #522): probing `urban --help` survives forks, backports
/// and out-of-band installs, and keeps correctness console-owned rather than
/// coupled to parsing `urbanVersion` (#529, kept for telemetry only).
///
/// The delegation call sites (`derive_models`/`generate_models` →
/// `urban derive --stdout`; `regenerate_domain_types` → `urban gen --no-models`)
/// gate on this so an older toolkit that predates derivation falls back to the
/// console's embedded Deno driver / bare `urban gen` — additive + non-regressing,
/// exactly like #530. Derived from the memoised [`urban_help_text`] read, so the
/// binary is spawned at most once regardless of how many capabilities we gate on.
pub(crate) async fn urban_supports_derive(urban: &Path) -> bool {
    urban_help_text(urban)
        .await
        .as_deref()
        .map(help_indicates_derive)
        .unwrap_or(false)
}

/// Whether a resolved `urban` binary carries the **`urban data` op gateway** —
/// the ADR-0053 shared datasource seam (#522 slice b). A **capability probe, not
/// a version check** (same rationale as [`urban_supports_derive`]): probing
/// `urban --help` survives forks, backports and out-of-band installs.
///
/// The `run_data_op` seam gates on this so an Urban app whose resolved toolkit
/// predates the `data` op falls back to the console's embedded `data-cli.ts`
/// instead of failing — additive + non-regressing, exactly like the derive gate.
/// Without it, an older `urban` found via `PATH`/project-local install would be
/// routed to `urban data`, fail, and never reach the still-working embedded
/// fallback. Derived from the memoised [`urban_help_text`] read.
pub(crate) async fn urban_supports_data(urban: &Path) -> bool {
    urban_help_text(urban)
        .await
        .as_deref()
        .map(help_indicates_data)
        .unwrap_or(false)
}

/// The `urban --help` text for a resolved binary (stdout + stderr concatenated,
/// `NO_COLOR`), memoised per binary path. This is the **single** place a toolkit
/// is spawned to probe its capabilities: every `urban_supports_*` gate derives
/// its answer from this one cached read via a pure `help_indicates_*` predicate,
/// so a given binary's help is read once and shared across every capability gate
/// — one cached read, no per-capability cache to drift out of sync.
///
/// The cache is populated by a check-then-insert, **not** single-flight: several
/// *concurrent first* calls for the same path can each spawn `urban --help` once
/// before the entry lands (a bounded, benign duplication — help output is
/// deterministic, so every racer computes the same text and the last insert
/// wins). Every call after the cache is warm reuses the stored text without
/// spawning. Memoisation is safe because a binary's help output is stable for the
/// process lifetime (a pack upgrade installs a new path, or the server restarts).
/// `None` means the binary could not be run.
async fn urban_help_text(urban: &Path) -> Option<String> {
    use std::sync::{Mutex, OnceLock};
    static CACHE: OnceLock<Mutex<std::collections::HashMap<PathBuf, Option<String>>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    // Recover from a poisoned mutex rather than propagating the panic: the lock
    // only guards a tiny insert/get (no user code runs under it), so a poisoned
    // guard carries a valid map — matching this module's poison-recovery pattern
    // keeps the capability gate robust instead of taking down urban delegation.
    if let Some(hit) = cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(urban)
        .cloned()
    {
        return hit;
    }
    let help = tokio::process::Command::new(urban)
        .arg("--help")
        .env("NO_COLOR", "1")
        .kill_on_drop(true)
        .output()
        .await
        .ok()
        .map(|o| {
            let mut help = String::from_utf8_lossy(&o.stdout).into_owned();
            help.push_str(&String::from_utf8_lossy(&o.stderr));
            help
        });
    cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(urban.to_path_buf(), help.clone());
    help
}

/// Pure predicate over `urban --help` text: does this toolkit expose model
/// derivation? Requires **both** capabilities this gate fronts — the
/// `gen --no-models` flag AND the `derive` subcommand (its `--stdout` flag or
/// usage line) — since a single \[`urban_supports_derive`\] gate guards call
/// sites that use each, and the two ship as a unit (nano-ide#92). An OR could
/// mis-detect a toolkit exposing only one and then invoke the other, unsupported.
/// A bare `"derive"` substring is deliberately NOT a marker: the pre-derivation
/// help already contains "derive" in its `gen` description (`urban gen … derive
/// artifacts (migrations, worker-io)`), so it would false-positive on every old
/// toolkit.
fn help_indicates_derive(help: &str) -> bool {
    help.contains("--no-models") && (help.contains("--stdout") || help.contains("urban derive"))
}

/// Pure predicate over `urban --help` text: does this toolkit expose the shared
/// `urban data` op gateway (#522 slice b, ADR 0053)? Matches the `urban data`
/// usage/subcommand marker — the same "subcommand appears in help" shape as the
/// `urban derive` half of [`help_indicates_derive`]. A pre-`data` toolkit lists
/// only `gen`/`run`, so this stays false and routing falls back to the embedded
/// gateway.
fn help_indicates_data(help: &str) -> bool {
    help.contains("urban data")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_urban_prefers_explicit_override() {
        // A real override file is returned; a non-existent one is ignored.
        let dir = std::env::temp_dir().join(format!("nbpm-urban-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let fake = dir.join(URBAN_EXE);
        std::fs::write(&fake, b"#!/bin/sh\n").unwrap();

        // Override wins outright, without consulting the pack or PATH.
        assert_eq!(
            resolve_urban(Some(fake.to_str().unwrap()), None, None, None),
            Some(fake.clone())
        );
        // A bogus override falls through (here: to an empty pack/PATH → None),
        // and never returns the bogus path itself.
        assert_eq!(
            resolve_urban(Some("/nonexistent/definitely/not/urban"), None, None, None),
            None
        );
        // An empty override is treated as unset.
        assert_eq!(resolve_urban(Some(""), None, None, None), None);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_urban_prefers_pack_over_path() {
        // With no override, an installed pack bin is preferred over a PATH hit,
        // and a non-existent pack candidate is skipped in favour of PATH.
        let base = std::env::temp_dir().join(format!("nbpm-urban-pack-{}", std::process::id()));
        let pack_dir = base.join("pack");
        let path_dir = base.join("path");
        std::fs::create_dir_all(&pack_dir).unwrap();
        std::fs::create_dir_all(&path_dir).unwrap();
        let pack_bin = pack_dir.join(URBAN_EXE);
        let path_bin = path_dir.join(URBAN_EXE);
        std::fs::write(&pack_bin, b"#!/bin/sh\n").unwrap();
        std::fs::write(&path_bin, b"#!/bin/sh\n").unwrap();
        let path = std::env::join_paths([path_dir.as_os_str()]).unwrap();

        // Pack present → pack wins over PATH.
        assert_eq!(
            resolve_urban(None, Some(pack_bin.as_path()), None, Some(path.as_os_str())),
            Some(pack_bin.clone())
        );
        // Pack candidate does not exist → fall through to PATH.
        let missing_pack = pack_dir.join("does-not-exist").join(URBAN_EXE);
        assert_eq!(
            resolve_urban(
                None,
                Some(missing_pack.as_path()),
                None,
                Some(path.as_os_str())
            ),
            Some(path_bin)
        );

        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn resolve_urban_prefers_project_local_over_path() {
        // #776: with no override and no pack, a project-local
        // `node_modules/.bin/urban` is discovered and wins over a PATH install.
        let base = std::env::temp_dir().join(format!("nbpm-urban-local-{}", std::process::id()));
        let local_dir = base.join("local");
        let path_dir = base.join("path");
        std::fs::create_dir_all(&local_dir).unwrap();
        std::fs::create_dir_all(&path_dir).unwrap();
        let local_bin = local_dir.join(URBAN_EXE);
        let path_bin = path_dir.join(URBAN_EXE);
        std::fs::write(&local_bin, b"#!/bin/sh\n").unwrap();
        std::fs::write(&path_bin, b"#!/bin/sh\n").unwrap();
        let path = std::env::join_paths([path_dir.as_os_str()]).unwrap();

        // Project-local present → wins over PATH.
        assert_eq!(
            resolve_urban(
                None,
                None,
                Some(local_bin.as_path()),
                Some(path.as_os_str())
            ),
            Some(local_bin.clone())
        );
        // Project-local candidate absent → fall through to PATH.
        let missing_local = local_dir.join("does-not-exist").join(URBAN_EXE);
        assert_eq!(
            resolve_urban(
                None,
                None,
                Some(missing_local.as_path()),
                Some(path.as_os_str())
            ),
            Some(path_bin)
        );

        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn resolve_urban_prefers_pack_over_project_local() {
        // The Studio-pinned pack still wins over a project-local install, so an
        // explicit pack acquisition is authoritative (#520 precedence preserved).
        let base = std::env::temp_dir().join(format!("nbpm-urban-pl-{}", std::process::id()));
        let pack_dir = base.join("pack");
        let local_dir = base.join("local");
        std::fs::create_dir_all(&pack_dir).unwrap();
        std::fs::create_dir_all(&local_dir).unwrap();
        let pack_bin = pack_dir.join(URBAN_EXE);
        let local_bin = local_dir.join(URBAN_EXE);
        std::fs::write(&pack_bin, b"#!/bin/sh\n").unwrap();
        std::fs::write(&local_bin, b"#!/bin/sh\n").unwrap();

        assert_eq!(
            resolve_urban(
                None,
                Some(pack_bin.as_path()),
                Some(local_bin.as_path()),
                None
            ),
            Some(pack_bin)
        );

        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn resolve_urban_falls_through_to_path() {
        // With no override, a `urban` binary on PATH is discovered.
        let dir = std::env::temp_dir().join(format!("nbpm-urban-path-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let bin = dir.join(URBAN_EXE);
        std::fs::write(&bin, b"#!/bin/sh\n").unwrap();

        let path = std::env::join_paths([dir.as_os_str()]).unwrap();
        assert_eq!(
            resolve_urban(None, None, None, Some(path.as_os_str())),
            Some(bin)
        );
        // Nothing on PATH, no override, no pack, no project-local → not available.
        assert_eq!(resolve_urban(None, None, None, None), None);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn help_indicates_derive_discriminates_new_from_old_toolkit() {
        // The pre-derivation toolkit: `derive` appears only inside the `gen`
        // description, so a bare `contains("derive")` would false-positive.
        let old_help = "\
urban — build and run Urban apps (nano.app.json)
  urban gen [--check]               derive artifacts (migrations, worker-io)
  urban run                         materialize + serve the app";
        assert!(
            !help_indicates_derive(old_help),
            "old help must not be read as derivation-capable"
        );

        // The derivation-capable toolkit (nano-ide#92) lists the `derive`
        // subcommand + the `--no-models`/`--stdout` flags.
        let new_help = "\
urban — build and run Urban apps (nano.app.json)
  urban gen [--check] [--no-models]   derive artifacts (migrations, worker-io)
  urban derive [--check|--stdout]     derive executable BPMN from workflows";
        assert!(help_indicates_derive(new_help));
        // Both capabilities must be present: the `gen --no-models` flag AND a
        // `derive`-subcommand marker (`--stdout` or the `urban derive` usage
        // line). A partial help exposing only ONE is NOT derivation-capable — the
        // gate fronts both, so an OR would let the console invoke an unsupported
        // flag/subcommand.
        assert!(!help_indicates_derive("  urban derive [--stdout]"));
        assert!(!help_indicates_derive("gen [--no-models]"));
        // Both markers together ⇒ capable.
        assert!(help_indicates_derive(
            "gen [--no-models]\nurban derive [--stdout]"
        ));
    }

    #[test]
    fn help_indicates_data_discriminates_new_from_old_toolkit() {
        // A pre-`data` toolkit lists only `gen`/`run` — no `urban data` gateway,
        // so routing must fall back to the embedded `data-cli.ts`.
        let old_help = "\
urban — build and run Urban apps (nano.app.json)
  urban gen [--check]               derive artifacts (migrations, worker-io)
  urban run                         materialize + serve the app";
        assert!(
            !help_indicates_data(old_help),
            "old help must not be read as data-op-capable"
        );

        // The `data`-capable toolkit (#522 slice b) lists the `urban data` op
        // gateway subcommand.
        let new_help = "\
urban — build and run Urban apps (nano.app.json)
  urban gen [--check]               derive artifacts (migrations, worker-io)
  urban data                        run a datasource op through the gateway";
        assert!(help_indicates_data(new_help));
    }
}
