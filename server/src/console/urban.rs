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
//! explicit-env override and the `PATH` probe — resolution order is
//! `NANOBPMN_URBAN_BIN` → pack → `PATH`. Do not add a second resolver
//! elsewhere — extend this one.

use std::path::PathBuf;

/// Name of the `urban` executable, platform-adjusted (npm installs a `.cmd`
/// shim on Windows).
const URBAN_EXE: &str = if cfg!(windows) { "urban.cmd" } else { "urban" };

/// Locates the `urban` CLI binary, mirroring [`super::workers::find_deno`]:
/// `NANOBPMN_URBAN_BIN` (explicit override), then the #520 marketplace-pack bin,
/// then `PATH`.
///
/// Returns `None` when no binary is found; callers then report
/// `urbanAvailable: false` so the Studio can prompt the user to install the
/// pack. The resolution order is deliberately `env → pack → PATH` (agreed on
/// #520/#522): an explicit override always wins, the first-party pack is
/// preferred over an ambient `PATH` install, and there is no `npx` fallback —
/// the pack is the real-machine acquisition path.
pub(crate) fn find_urban() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("NANOBPMN_URBAN_BIN")
        && !p.is_empty()
    {
        let pb = PathBuf::from(p);
        if pb.is_file() {
            return Some(pb);
        }
    }
    // #520 seam: resolve `<workspace>/extensions/<pkg>/node_modules/.bin/urban`
    // here, before falling through to PATH. Owned by #520.
    if let Ok(path) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path) {
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
    fn find_urban_honors_explicit_bin_env() {
        // A non-existent override is ignored; a real file is returned.
        let dir = std::env::temp_dir().join(format!("nbpm-urban-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let fake = dir.join(URBAN_EXE);
        std::fs::write(&fake, b"#!/bin/sh\n").unwrap();
        // SAFETY: single-threaded test; scoped env mutation on a var unique to
        // this test (mirrors workers::find_node_honors_explicit_bin_env).
        unsafe { std::env::set_var("NANOBPMN_URBAN_BIN", &fake) };
        assert_eq!(find_urban(), Some(fake));
        assert!(urban_available());
        unsafe { std::env::set_var("NANOBPMN_URBAN_BIN", "/nonexistent/definitely/not/urban") };
        // Falls through to PATH; just assert it doesn't return the bogus path.
        assert_ne!(
            find_urban(),
            Some(PathBuf::from("/nonexistent/definitely/not/urban"))
        );
        unsafe { std::env::remove_var("NANOBPMN_URBAN_BIN") };
        std::fs::remove_dir_all(&dir).ok();
    }
}
