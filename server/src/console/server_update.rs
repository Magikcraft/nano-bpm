//! Server self-update status for the console (`GET /console/api/server/update`).
//!
//! The nano server binary is distributed and managed by an external launcher
//! (the `c8ctl-plugin-nano` c8ctl plugin), not by a plain `npm i -g`. The
//! launcher tells us, via environment markers, how the running binary was
//! provisioned so we can decide whether an in-place self-update is possible:
//!
//! - `NANOBPMN_LAUNCHER`: identifier of the launcher (e.g. `c8ctl-plugin-nano`)
//! - `NANOBPMN_BINARY_SOURCE`: provenance — `managed-npm`, `managed-download`,
//!   `configured`, `flag`, `repo-release`, or `repo-debug`
//! - `NANOBPMN_UPDATE_CHANNEL`: where to resolve "latest" — `npm` or `download`
//! - `NANOBPMN_UPDATE_PKG`: npm package name (npm channel only)
//! - `PROCESSOS_DOWNLOAD_URL`: base URL of the download channel (download channel only)
//!
//! All markers are optional. When absent the binary is treated as self-managed
//! (`installMethod: unknown`, `canSelfUpdate: false`) and the update nag is
//! suppressed — so a maintainer running a local checkout, a `--binary` override,
//! or any repo build is never nagged to "update", and we never attempt to
//! overwrite a binary the launcher does not own.
//!
//! Everything here is best-effort and offline-soft: a failed version lookup
//! degrades to `latest: null`, never an error.

use serde::Serialize;

/// The running server version, baked in at build time (see `build.rs`).
const CURRENT_VERSION: &str = env!("NANOBPM_VERSION");

/// Timeout for the download-channel `version.json` probe. Kept short so the
/// endpoint stays snappy even when the network is black-holed.
const DOWNLOAD_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(4);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerUpdateStatus {
    /// Installed version in the update channel's version space (what `latest`
    /// is compared against and shown next to it). For the npm channel this is
    /// the launcher/plugin version; otherwise the running server build.
    pub current: String,
    /// The actual running server binary build (`NANOBPM_VERSION`). Kept distinct
    /// from `current` because the npm update unit (the plugin) versions in a
    /// different space than the server's own git-describe build.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest: Option<String>,
    pub update_available: bool,
    pub can_self_update: bool,
    pub install_method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub launcher: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_hint: Option<String>,
}

/// Normalize the `NANOBPMN_BINARY_SOURCE` marker to a known install method.
/// Anything unrecognized or absent maps to `"unknown"` (self-managed).
fn install_method(binary_source: Option<&str>) -> &'static str {
    match binary_source.map(str::trim) {
        Some("managed-npm") => "managed-npm",
        Some("managed-download") => "managed-download",
        Some("configured") => "configured",
        Some("flag") => "flag",
        Some("repo-release") => "repo-release",
        Some("repo-debug") => "repo-debug",
        _ => "unknown",
    }
}

/// Only launcher-managed binaries can be self-updated in place; every
/// self-managed / dev provenance (or an absent marker) cannot.
fn can_self_update(method: &str) -> bool {
    matches!(method, "managed-npm" | "managed-download")
}

/// Method-specific guidance shown in the UI.
fn update_hint(method: &str, launcher: Option<&str>) -> Option<String> {
    match method {
        "managed-npm" | "managed-download" => {
            // Both managed channels update through the launcher's own command.
            let _ = launcher;
            Some("Run `c8ctl nano update` to install the latest server.".to_string())
        }
        "configured" => Some(
            "This server uses a launcher-configured binary path; update your local \
             build (or clear the configured path) to change versions."
                .to_string(),
        ),
        "flag" => Some(
            "This server was started from an explicit --binary path; update that \
             binary to change versions."
                .to_string(),
        ),
        "repo-release" | "repo-debug" => Some(
            "This server runs from a local repository build; rebuild your checkout \
             to change versions."
                .to_string(),
        ),
        _ => None,
    }
}

/// Compare two dotted numeric versions, returning true when `latest` is strictly
/// greater than `current`. Pre-release / build suffixes (e.g. `-alpha.1`,
/// `+sha`) on a component are ignored for the numeric compare; if neither side
/// parses as numbers at all, falls back to a conservative "differs" check that
/// never reports an upgrade unless the numeric core actually increased.
fn semver_gt(latest: &str, current: &str) -> bool {
    fn core(v: &str) -> Vec<u64> {
        v.trim()
            .trim_start_matches('v')
            .split(['-', '+'])
            .next()
            .unwrap_or("")
            .split('.')
            .map(|p| p.parse::<u64>().unwrap_or(0))
            .collect()
    }
    let (l, c) = (core(latest), core(current));
    let n = l.len().max(c.len());
    for i in 0..n {
        let lv = l.get(i).copied().unwrap_or(0);
        let cv = c.get(i).copied().unwrap_or(0);
        if lv != cv {
            return lv > cv;
        }
    }
    false
}

/// Pure status assembly, separated from IO for testing. `current` is the
/// installed version in the update channel's version space (the comparison
/// basis); `server_version` is the actual running build. The nag
/// (`update_available`) fires only for a self-updatable install whose `latest`
/// is strictly newer than `current`.
fn assemble(
    current: &str,
    server_version: &str,
    method: &str,
    channel: Option<String>,
    launcher: Option<String>,
    latest: Option<String>,
) -> ServerUpdateStatus {
    let can_self_update = can_self_update(method);
    let update_available = can_self_update
        && latest
            .as_deref()
            .map(|l| semver_gt(l, current))
            .unwrap_or(false);
    let update_hint = update_hint(method, launcher.as_deref());
    ServerUpdateStatus {
        current: current.to_string(),
        // Only surface the running build separately when it differs from the
        // channel-space `current` (i.e. an npm-managed install).
        server_version: (server_version != current).then(|| server_version.to_string()),
        latest,
        update_available,
        can_self_update,
        install_method: method.to_string(),
        channel,
        launcher,
        update_hint,
    }
}

/// Resolve the latest version for the download channel by fetching
/// `<base>/version.json` (`{ "version": "x.y.z", ... }`). Offline-soft.
async fn resolve_download_latest(base: &str) -> Option<String> {
    let url = format!("{}/version.json", base.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .timeout(DOWNLOAD_PROBE_TIMEOUT)
        .build()
        .ok()?;
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body = resp.text().await.ok()?;
    let v: serde_json::Value = serde_json::from_str(&body).ok()?;
    v.get("version")
        .and_then(|x| x.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Resolve the latest version for the npm channel via `npm view <pkg> version
/// --prefer-online`. Offline-soft; requires the launcher to have named the
/// managed platform package via `NANOBPMN_UPDATE_PKG`.
fn resolve_npm_latest(pkg: &str) -> Option<String> {
    let npm = super::extensions::find_program("npm")?;
    let out = std::process::Command::new(&npm)
        .args(["view", pkg, "version", "--prefer-online", "--silent"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let v = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if v.is_empty() { None } else { Some(v) }
}

/// Build the current server-update status from environment markers, resolving
/// the latest version best-effort. Only attempts a version lookup when the
/// binary is launcher-managed (self-managed binaries suppress the nag anyway,
/// so there is nothing to gain from probing).
pub async fn status() -> ServerUpdateStatus {
    let launcher = std::env::var("NANOBPMN_LAUNCHER")
        .ok()
        .filter(|s| !s.is_empty());
    let binary_source = std::env::var("NANOBPMN_BINARY_SOURCE").ok();
    let channel = std::env::var("NANOBPMN_UPDATE_CHANNEL")
        .ok()
        .filter(|s| !s.is_empty());
    let launcher_version = std::env::var("NANOBPMN_LAUNCHER_VERSION")
        .ok()
        .filter(|s| !s.is_empty());
    let method = install_method(binary_source.as_deref());

    let latest = if can_self_update(method) {
        match channel.as_deref() {
            Some("download") => match std::env::var("PROCESSOS_DOWNLOAD_URL") {
                Ok(base) if !base.is_empty() => resolve_download_latest(&base).await,
                _ => None,
            },
            Some("npm") => match std::env::var("NANOBPMN_UPDATE_PKG") {
                Ok(pkg) if !pkg.is_empty() => {
                    tokio::task::spawn_blocking(move || resolve_npm_latest(&pkg))
                        .await
                        .ok()
                        .flatten()
                }
                _ => None,
            },
            _ => None,
        }
    } else {
        None
    };

    // Comparison basis, in the channel's version space:
    //  - npm channel: the launcher/plugin version (same space as `latest`, which
    //    is `npm view <plugin>`). The plugin is the update unit; its platform
    //    binary ships pinned to it.
    //  - download channel: the running server build, since the download
    //    `version.json` is written in the server's own version space.
    let current = match channel.as_deref() {
        Some("npm") => launcher_version
            .clone()
            .unwrap_or_else(|| CURRENT_VERSION.to_string()),
        _ => CURRENT_VERSION.to_string(),
    };

    assemble(&current, CURRENT_VERSION, method, channel, launcher, latest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_sources_can_self_update() {
        assert!(can_self_update(install_method(Some("managed-npm"))));
        assert!(can_self_update(install_method(Some("managed-download"))));
    }

    #[test]
    fn self_managed_and_unknown_cannot_self_update() {
        for src in ["configured", "flag", "repo-release", "repo-debug"] {
            assert!(!can_self_update(install_method(Some(src))), "{src}");
        }
        // Absent / unrecognized marker => unknown => not self-updatable.
        assert_eq!(install_method(None), "unknown");
        assert_eq!(install_method(Some("weird")), "unknown");
        assert!(!can_self_update("unknown"));
    }

    #[test]
    fn semver_compare() {
        assert!(semver_gt("1.0.1", "1.0.0"));
        assert!(semver_gt("1.2.0", "1.1.9"));
        assert!(semver_gt("2.0.0", "1.9.9"));
        assert!(semver_gt("v1.0.1", "1.0.0"));
        assert!(!semver_gt("1.0.0", "1.0.0"));
        assert!(!semver_gt("1.0.0", "1.0.1"));
        // Pre-release suffix on the numeric core is ignored for the compare.
        assert!(!semver_gt("1.0.0-alpha.1", "1.0.0"));
        assert!(semver_gt("1.0.1-alpha.1", "1.0.0"));
    }

    #[test]
    fn self_managed_never_nags_even_with_newer_latest() {
        // A repo build with a "newer" latest available must NOT report an update.
        let s = assemble(
            "1.0.0",
            "1.0.0",
            install_method(Some("repo-release")),
            Some("npm".into()),
            Some("c8ctl-plugin-nano".into()),
            Some("9.9.9".into()),
        );
        assert!(!s.can_self_update);
        assert!(!s.update_available);
        assert_eq!(s.install_method, "repo-release");
    }

    #[test]
    fn managed_nags_only_when_newer() {
        let newer = assemble(
            "1.0.0",
            "1.0.0",
            install_method(Some("managed-npm")),
            Some("npm".into()),
            Some("c8ctl-plugin-nano".into()),
            Some("1.0.1".into()),
        );
        assert!(newer.can_self_update);
        assert!(newer.update_available);
        assert!(newer.update_hint.is_some());

        let same = assemble(
            "1.0.1",
            "1.0.1",
            install_method(Some("managed-npm")),
            None,
            None,
            Some("1.0.1".into()),
        );
        assert!(same.can_self_update);
        assert!(!same.update_available);
    }

    #[test]
    fn managed_without_resolved_latest_does_not_nag() {
        let s = assemble(
            "1.0.0",
            "1.0.0",
            install_method(Some("managed-download")),
            Some("download".into()),
            Some("c8ctl-plugin-nano".into()),
            None,
        );
        assert!(s.can_self_update);
        assert!(!s.update_available);
        assert!(s.latest.is_none());
    }

    #[test]
    fn npm_channel_compares_in_launcher_version_space() {
        // The npm update unit (plugin) versions independently of the server
        // build. `current` is the plugin version (e.g. 0.2.0); `latest` from
        // `npm view <plugin>` is the same space; the raw server build (e.g.
        // 0.0.7) must NOT be the comparison basis, and is surfaced separately.
        let s = assemble(
            "0.2.0",             // current: plugin version (channel space)
            "0.0.7-65-gdeadbee", // server_version: the running build
            install_method(Some("managed-npm")),
            Some("npm".into()),
            Some("c8ctl-plugin-nano".into()),
            Some("0.2.1".into()), // latest: plugin version
        );
        assert!(s.update_available, "0.2.1 > 0.2.0 in plugin space");
        assert_eq!(s.current, "0.2.0");
        assert_eq!(s.server_version.as_deref(), Some("0.0.7-65-gdeadbee"));

        // Equal plugin versions => no nag even though the server build string
        // looks numerically "older" than latest.
        let same = assemble(
            "0.2.1",
            "0.0.7-65-gdeadbee",
            install_method(Some("managed-npm")),
            Some("npm".into()),
            Some("c8ctl-plugin-nano".into()),
            Some("0.2.1".into()),
        );
        assert!(!same.update_available);
    }
}
