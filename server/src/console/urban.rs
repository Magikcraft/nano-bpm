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

/// Locates the `urban` CLI binary, mirroring [`super::workers::find_deno`].
/// The resolution order is `NANOBPMN_URBAN_BIN` (explicit override) → the
/// first-party marketplace pack ([`pack_urban_bin`]) → `PATH`.
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
        path.as_deref(),
    )
}

/// Pure core of [`find_urban`]: given the `NANOBPMN_URBAN_BIN` override, the
/// marketplace-pack bin candidate, and the `PATH` value, applies the
/// `env → pack → PATH` resolution. Kept free of any global-env reads so it is
/// deterministic and testable in parallel.
fn resolve_urban(
    bin_override: Option<&str>,
    pack_bin: Option<&Path>,
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
    // before `PATH` so a Studio-pinned pack install wins over an ambient one.
    if let Some(pack) = pack_bin
        && pack.is_file()
    {
        return Some(pack.to_path_buf());
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
/// binary is not executed.
pub(crate) fn urban_available() -> bool {
    find_urban().is_some()
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
/// exactly like #530. The result is memoised per binary path since a given
/// binary's help output is stable for the process lifetime (a pack upgrade
/// installs a new path, or the server restarts).
pub(crate) async fn urban_supports_derive(urban: &Path) -> bool {
    use std::sync::{Mutex, OnceLock};
    static CACHE: OnceLock<Mutex<std::collections::HashMap<PathBuf, bool>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    if let Some(hit) = cache.lock().unwrap().get(urban).copied() {
        return hit;
    }
    let supported = tokio::process::Command::new(urban)
        .arg("--help")
        .env("NO_COLOR", "1")
        .kill_on_drop(true)
        .output()
        .await
        .ok()
        .map(|o| {
            let mut help = String::from_utf8_lossy(&o.stdout).into_owned();
            help.push_str(&String::from_utf8_lossy(&o.stderr));
            help_indicates_derive(&help)
        })
        .unwrap_or(false);
    cache.lock().unwrap().insert(urban.to_path_buf(), supported);
    supported
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
            resolve_urban(Some(fake.to_str().unwrap()), None, None),
            Some(fake.clone())
        );
        // A bogus override falls through (here: to an empty pack/PATH → None),
        // and never returns the bogus path itself.
        assert_eq!(
            resolve_urban(Some("/nonexistent/definitely/not/urban"), None, None),
            None
        );
        // An empty override is treated as unset.
        assert_eq!(resolve_urban(Some(""), None, None), None);

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
            resolve_urban(None, Some(pack_bin.as_path()), Some(path.as_os_str())),
            Some(pack_bin.clone())
        );
        // Pack candidate does not exist → fall through to PATH.
        let missing_pack = pack_dir.join("does-not-exist").join(URBAN_EXE);
        assert_eq!(
            resolve_urban(None, Some(missing_pack.as_path()), Some(path.as_os_str())),
            Some(path_bin)
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
        assert_eq!(resolve_urban(None, None, Some(path.as_os_str())), Some(bin));
        // Nothing on PATH, no override, no pack → not available.
        assert_eq!(resolve_urban(None, None, None), None);

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
}
