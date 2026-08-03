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

/// The pack-relative `urban` binary path: `<extensions>/nano-ide-app-urban/
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
}
