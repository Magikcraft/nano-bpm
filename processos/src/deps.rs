//! External-tool preflight.
//!
//! ProcessOS shells out to two optional CLI tools that are **not** bundled: the
//! llama.cpp `llama-server` (which powers the local LLM sidecars used for
//! investigations and chat) and **Deno** (which the Nano engine ProcessOS
//! supervises uses to run its embedded job workers). When either is absent the
//! failure otherwise surfaces deep inside a spawn as a cryptic
//! "No such file or directory". This module detects both up front so the Console
//! can tell the operator exactly *what* is missing, *why* it's needed, and link
//! them to the official, OS-aware install instructions.

use serde::Serialize;
use std::process::Command;

/// OS-aware install instructions (cover macOS/Linux/Windows package managers).
const DENO_INSTALL_URL: &str = "https://docs.deno.com/runtime/getting_started/installation/";
const LLAMA_INSTALL_URL: &str = "https://github.com/ggml-org/llama.cpp/blob/master/docs/install.md";

/// One external tool ProcessOS depends on, and whether it is available.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Dependency {
    /// Stable identifier (`deno` | `llama`).
    pub id: &'static str,
    /// Human-facing name.
    pub name: &'static str,
    /// What ProcessOS needs the tool for.
    pub purpose: &'static str,
    /// The binary ProcessOS will invoke (resolved from settings/env where set).
    pub bin: String,
    /// Whether the binary is present and runnable.
    pub present: bool,
    /// First line of `<bin> --version`, when present.
    pub version: Option<String>,
    /// Official, OS-aware install instructions.
    pub install_url: &'static str,
    /// A one-line, actionable hint shown when the tool is missing.
    pub hint: String,
}

/// Probe `<bin> --version`. Returns `Some(first non-empty output line)` when the
/// binary ran (presence), or `None` when it could not be spawned (missing).
///
/// Presence is "did it spawn", *not* the exit code — some tools print their
/// version and exit non-zero. `deno` prints to stdout; `llama-server` to stderr,
/// so both streams are considered.
fn probe(bin: &str) -> Option<String> {
    let out = Command::new(bin).arg("--version").output().ok()?;
    let stream: &[u8] = if out.stdout.is_empty() {
        &out.stderr
    } else {
        &out.stdout
    };
    let line = String::from_utf8_lossy(stream)
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("")
        .to_string();
    Some(line)
}

/// Resolve the Deno binary the supervised Nano engine would use, mirroring that
/// engine's own resolution order: `NANOBPMN_DENO_BIN`, then `deno` on `PATH`,
/// then the default `~/.deno/bin/deno` install location.
fn check_deno() -> Dependency {
    let mut candidates: Vec<String> = Vec::new();
    if let Ok(b) = std::env::var("NANOBPMN_DENO_BIN") {
        let b = b.trim().to_string();
        if !b.is_empty() {
            candidates.push(b);
        }
    }
    candidates.push("deno".to_string());
    if let Ok(home) = std::env::var("HOME") {
        if !home.is_empty() {
            candidates.push(format!("{home}/.deno/bin/deno"));
        }
    }

    let mut bin = candidates[0].clone();
    let mut version = None;
    for cand in &candidates {
        if let Some(v) = probe(cand) {
            bin = cand.clone();
            version = if v.is_empty() { None } else { Some(v) };
            break;
        }
    }
    // When nothing resolved, report the most user-meaningful name.
    let present = version.is_some() || probe(&bin).is_some();

    Dependency {
        id: "deno",
        name: "Deno",
        purpose: "Running Nano's embedded job workers — the Nano engine ProcessOS supervises spawns sandboxed Deno worker processes.",
        bin,
        present,
        version,
        install_url: DENO_INSTALL_URL,
        hint: if present {
            String::new()
        } else {
            "Deno was not found. Install it (see the link) so `deno` is on PATH, or set NANOBPMN_DENO_BIN to its path. Until then, embedded Nano workers cannot run."
                .to_string()
        },
    }
}

/// Resolve the `llama-server` binary ProcessOS supervises: the operator-configured
/// `llama_bin` from settings, else `llama-server` on `PATH`.
fn check_llama(llama_bin: Option<&str>) -> Dependency {
    let bin = llama_bin
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("llama-server")
        .to_string();
    let probed = probe(&bin);
    let present = probed.is_some();
    let version = probed.filter(|s| !s.is_empty());

    Dependency {
        id: "llama",
        name: "llama.cpp (llama-server)",
        purpose: "Running the local LLM sidecars that power investigations and chat.",
        bin,
        present,
        version,
        install_url: LLAMA_INSTALL_URL,
        hint: if present {
            String::new()
        } else {
            "`llama-server` was not found. Install llama.cpp (see the link) so it is on PATH, or set the llama binary path in Settings. Until then, local model sidecars cannot start."
                .to_string()
        },
    }
}

/// Check every external dependency. `llama_bin` is the operator-configured path
/// from settings (or `None` to use `llama-server` on `PATH`).
pub fn check(llama_bin: Option<&str>) -> Vec<Dependency> {
    vec![check_deno(), check_llama(llama_bin)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_resolves_a_present_binary() {
        // Every platform we target ships a shell; `sh` reliably exists on PATH.
        // (We only assert spawnability, not a particular version string.)
        assert!(Command::new("sh").arg("-c").arg("exit 0").output().is_ok());
    }

    #[test]
    fn probe_reports_missing_binary_as_none() {
        assert!(probe("definitely-not-a-real-binary-xyzzy-42").is_none());
    }

    #[test]
    fn check_returns_both_dependencies_with_install_links() {
        let deps = check(None);
        assert_eq!(deps.len(), 2);
        let deno = deps.iter().find(|d| d.id == "deno").expect("deno entry");
        let llama = deps.iter().find(|d| d.id == "llama").expect("llama entry");
        assert!(deno.install_url.starts_with("https://"));
        assert!(llama.install_url.starts_with("https://"));
        // A missing tool always carries an actionable hint; a present one does not.
        assert_eq!(deno.present, deno.hint.is_empty());
        assert_eq!(llama.present, llama.hint.is_empty());
    }

    #[test]
    fn check_llama_honours_configured_binary() {
        let dep = check_llama(Some("/no/such/llama-server"));
        assert_eq!(dep.bin, "/no/such/llama-server");
        assert!(!dep.present);
        assert!(!dep.hint.is_empty());
    }
}
