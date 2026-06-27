//! Stamps the gateway binary's reported version. Release CI exports
//! `NANOBPM_VERSION` from the pushed tag (the single source of truth for a
//! release); locally we fall back to `git describe`, then the crate version in
//! `Cargo.toml`. A leading `v` is stripped so `v1.2.3` -> `1.2.3`.

use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=NANOBPM_VERSION");
    let version = resolve_version();
    println!("cargo:rustc-env=NANOBPM_VERSION={version}");
}

fn resolve_version() -> String {
    if let Ok(v) = env::var("NANOBPM_VERSION") {
        let v = v.trim().trim_start_matches('v');
        if !v.is_empty() {
            return v.to_string();
        }
    }
    let manifest = env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let git = std::process::Command::new("git")
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
