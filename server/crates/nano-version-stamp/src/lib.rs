//! Shared build-script helper that stamps a crate's reported gateway version.
//!
//! The version is the single source of truth for a release: CI exports
//! `NANOBPM_VERSION` from the pushed tag; locally we fall back to `git describe`,
//! then the compiling crate's `Cargo.toml` version. A leading `v` is stripped so
//! `v1.2.3` -> `1.2.3`.
//!
//! Both the gateway binary and the extracted `nano-server-console` crate call
//! [`emit`] from their `build.rs`, so the derivation lives in exactly one place
//! and their `env!("NANOBPM_VERSION")` values can never drift (ADR 0064).

use std::env;
use std::path::Path;
use std::process::Command;

/// Emits the `cargo:` directives that stamp `NANOBPM_VERSION` for the compiling
/// crate and watch the git state the fallback derivation reads. Call this once
/// from a crate's `build.rs` `main`.
pub fn emit() {
    println!("cargo:rerun-if-env-changed=NANOBPM_VERSION");
    // Emitting *any* `rerun-if-*` opts this build script out of Cargo's default
    // "rerun when any package file changes" heuristic, so unless we say otherwise
    // Cargo caches the stamped version across `git pull`/`checkout`. That is how a
    // binary built from fresh `main` can still report an old commit (observed on a
    // cross-build whose reported gateway version lagged the code by weeks). Watch
    // the git state that `git describe` reads so the stamp re-derives on every HEAD
    // movement: `logs/HEAD` is appended on each commit/checkout/reset, and `HEAD`
    // itself changes on branch switch. `git rev-parse --git-path` resolves these to
    // the real files even with packed refs or a linked worktree (where `.git` is a
    // file). When git is unavailable (release tarball), no paths are emitted and the
    // explicit `NANOBPM_VERSION` env — already watched above — governs.
    for git_ref in ["logs/HEAD", "HEAD"] {
        if let Some(path) = git_path(git_ref).filter(|p| Path::new(p).exists()) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    let version = resolve_version();
    println!("cargo:rustc-env=NANOBPM_VERSION={version}");
}

/// Absolute path to a file inside the git dir (e.g. `HEAD`, `logs/HEAD`), resolved
/// via `git rev-parse --git-path` so it is correct for packed refs and linked
/// worktrees. `None` when git is absent or the command fails.
fn git_path(rel: &str) -> Option<String> {
    let manifest = env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let out = Command::new("git")
        .args(["rev-parse", "--git-path", rel])
        .current_dir(&manifest)
        .output()
        .ok()
        .filter(|out| out.status.success())?;
    let path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!path.is_empty()).then_some(path)
}

/// Resolves the reported version from `NANOBPM_VERSION`, then `git describe`,
/// then the compiling crate's `CARGO_PKG_VERSION`, stripping a leading `v`.
pub fn resolve_version() -> String {
    if let Ok(v) = env::var("NANOBPM_VERSION") {
        let v = v.trim().trim_start_matches('v');
        if !v.is_empty() {
            return v.to_string();
        }
    }
    let manifest = env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let git = Command::new("git")
        .args(["describe", "--tags", "--always", "--dirty"])
        .current_dir(&manifest)
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| {
            String::from_utf8_lossy(&out.stdout)
                .trim()
                .trim_start_matches('v')
                .to_string()
        })
        .filter(|v| !v.is_empty());
    if let Some(v) = git {
        return v;
    }
    env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string())
}
