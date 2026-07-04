//! RAD **extension system** (ADR 0007) — the pluggable backbone for polyglot
//! authoring (ADR 0008) and GUI app projects (ADR 0009).
//!
//! An extension is an npm package named `nano-ide-ext-*` (`nano-ide-lang-*`
//! and `nano-ide-app-*` are the two specialisations) carrying a single
//! `nano-ide.ext.json` manifest. The host reads the manifest as **declared
//! data** — nothing is `eval`'d — and uses it to drive three seams:
//!
//! * the editor grammar map (which Monaco language a file extension gets);
//! * the project scaffolder (which starter templates exist, see [`super::projects`]);
//! * the run/compile supervisor (which on-machine toolchain runs/compiles a project).
//!
//! First-party packs (`deno`, `rust`, `deno-gui`) ship **built in** so the
//! console works offline with zero installs and existing Deno projects are
//! unchanged. Third-party packs install into `<workspace>/extensions/<pkg>/`.
//!
//! Toolchain commands run on the user's machine, so the default is allowlist +
//! consent ([`TrustStore`]): per-extension *approve always* plus a global
//! *yolo* mode, both off by default.

use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::workspace;

/// Which IDE seams a pack drives.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtKind {
    /// A language pack: file types/grammar + a toolchain (`nano-ide-lang-*`).
    Lang,
    /// An output/runtime pack: project templates + compile/run profile
    /// (`nano-ide-app-*`).
    App,
    /// A complete example app shipped under `appDir`, copied into a new
    /// project (`nano-ide-example-*`).
    Example,
    /// A console colour-theme pack: one or more themes declared in the
    /// manifest as design-token values (`nano-ide-theme-*`). Pure data — no
    /// toolchain, no code.
    Theme,
}

/// One console colour theme a `kind: "theme"` pack contributes. `tokens` maps
/// the console's design-token vocabulary (see console/src/theme/themes.ts
/// TOKEN_KEYS — "app", "panel", "accent", …) to CSS colours; unknown keys are
/// ignored client-side, missing keys fall back to the base `appearance`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThemeSpec {
    /// Stable id, unique across packs (e.g. "nord-dark").
    pub id: String,
    /// Human-facing name shown in the theme picker.
    pub label: String,
    /// Base palette the tokens override: "light" or "dark".
    pub appearance: String,
    /// Design-token name -> CSS colour.
    #[serde(default)]
    pub tokens: std::collections::BTreeMap<String, String>,
}

/// Editor profile for one file extension.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileType {
    /// File extension including the dot, e.g. `.rs`.
    pub ext: String,
    /// Monaco language id used for highlighting; the editor lazy-loads it only
    /// when a matching file is opened.
    pub monaco_lang: String,
}

/// On-machine toolchain the supervisor drives. Commands run on the user's
/// machine and are gated by [`TrustStore`].
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Toolchain {
    /// Probe argv proving the toolchain is installed (e.g. `cargo --version`).
    /// Empty for the built-in Deno pack (handled internally).
    #[serde(default)]
    pub detect: Vec<String>,
    /// Shell-style argv to run the project (cwd = project dir). Empty => use the
    /// built-in Deno runner.
    #[serde(default)]
    pub run: Vec<String>,
    /// Shell-style argv to compile the project. Empty => Deno compile.
    #[serde(default)]
    pub compile: Vec<String>,
    /// Cross-compile target triples this toolchain offers.
    #[serde(default)]
    pub targets: Vec<String>,
    /// Official, OS-aware install instructions for this toolchain, surfaced in the
    /// IDE config panel when the `detect` probe fails. Empty for the built-in Deno
    /// pack (whose runtime is reported separately as a first-class dependency).
    #[serde(default)]
    pub install_url: Option<String>,
    /// One-line, actionable hint shown when the toolchain is missing.
    #[serde(default)]
    pub install_hint: Option<String>,
}

/// A configuration field a pack contributes to the IDE config panel. Read-only
/// for now (surfaced for visibility); packs declare the knobs they honour so the
/// panel can grow without console changes. `value` is resolved from the named
/// environment variable when `env` is set.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigField {
    /// Stable key within the pack.
    pub key: String,
    /// Human-facing label.
    pub label: String,
    /// What the field controls.
    #[serde(default)]
    pub description: Option<String>,
    /// Environment variable this field reads its current value from, if any.
    #[serde(default)]
    pub env: Option<String>,
    /// Documented default when unset.
    #[serde(default)]
    pub default: Option<String>,
}

/// A scaffold template a pack contributes.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemplateSpec {
    pub id: String,
    pub label: String,
}

/// The `nano-ide.ext.json` manifest, read as data.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtManifest {
    pub id: String,
    pub kind: ExtKind,
    pub display_name: String,
    #[serde(default)]
    pub file_types: Vec<FileType>,
    #[serde(default)]
    pub templates: Vec<TemplateSpec>,
    #[serde(default)]
    pub toolchain: Toolchain,
    /// example packs: lang pack ids required to build/run this example.
    #[serde(default)]
    pub requires: Vec<String>,
    /// example packs: subdir holding the ready-to-copy project.
    #[serde(default)]
    pub app_dir: Option<String>,
    /// example packs: one-line description for the picker.
    #[serde(default)]
    pub summary: Option<String>,
    /// Whether this pack is bundled in the binary (cannot be removed).
    #[serde(default)]
    pub builtin: bool,
    /// Config fields this pack contributes to the IDE config panel (read-only).
    #[serde(default)]
    pub config_fields: Vec<ConfigField>,
    /// Console colour themes this pack contributes (theme packs).
    #[serde(default)]
    pub themes: Vec<ThemeSpec>,
}

/// The built-in first-party packs — always available, offline, unremovable.
/// Deno is the legacy lang+app runtime (empty toolchain => internal Deno path).
pub fn builtin_extensions() -> Vec<ExtManifest> {
    vec![
        ExtManifest {
            id: "deno".into(),
            kind: ExtKind::Lang,
            display_name: "Deno (TypeScript)".into(),
            file_types: vec![
                FileType {
                    ext: ".ts".into(),
                    monaco_lang: "typescript".into(),
                },
                FileType {
                    ext: ".js".into(),
                    monaco_lang: "javascript".into(),
                },
            ],
            templates: vec![],
            toolchain: Toolchain::default(),
            requires: vec![],
            app_dir: None,
            summary: None,
            builtin: true,
            config_fields: vec![ConfigField {
                key: "denoBin".into(),
                label: "Deno binary".into(),
                description: Some(
                    "Path to the Deno runtime used to run embedded job workers. Auto-resolved from PATH / ~/.deno/bin when unset.".into(),
                ),
                env: Some("NANOBPMN_DENO_BIN".into()),
                default: Some("deno (on PATH)".into()),
            }],
            themes: vec![],
        },
        ExtManifest {
            id: "rust".into(),
            kind: ExtKind::Lang,
            display_name: "Rust".into(),
            file_types: vec![FileType {
                ext: ".rs".into(),
                monaco_lang: "rust".into(),
            }],
            templates: vec![TemplateSpec {
                id: "rust-throughput".into(),
                label: "Throughput (Rust) — native pipelined falcon A/B".into(),
            }],
            toolchain: Toolchain {
                detect: vec!["cargo".into(), "--version".into()],
                run: vec!["cargo".into(), "run".into(), "--release".into()],
                compile: vec!["cargo".into(), "build".into(), "--release".into()],
                targets: vec![],
                install_url: Some("https://www.rust-lang.org/tools/install".into()),
                install_hint: Some(
                    "`cargo` was not found. Install the Rust toolchain (see the link) so `cargo` is on PATH. Until then, Rust projects cannot run or compile.".into(),
                ),
            },
            requires: vec![],
            app_dir: None,
            summary: None,
            builtin: true,
            config_fields: vec![],
            themes: vec![],
        },
        ExtManifest {
            id: "deno-gui".into(),
            kind: ExtKind::App,
            display_name: "Deno GUI app".into(),
            file_types: vec![],
            templates: vec![TemplateSpec {
                id: "gui-starter".into(),
                label: "GUI app — served UI binary (Deno.serve)".into(),
            }],
            toolchain: Toolchain::default(),
            requires: vec![],
            app_dir: None,
            summary: None,
            builtin: true,
            config_fields: vec![],
            themes: vec![],
        },
    ]
}

/// `<workspace>/extensions` — installed third-party packs + trust store.
pub fn extensions_root() -> PathBuf {
    match std::env::var("NANOBPMN_EXTENSIONS_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => workspace::workspace_dir().join("extensions"),
    }
}

fn manifest_name() -> &'static str {
    "nano-ide.ext.json"
}

/// Every extension the console knows: built-ins plus installed packs (built-ins
/// take precedence on id collision).
pub fn all_extensions() -> Vec<ExtManifest> {
    let mut out = builtin_extensions();
    let seen: BTreeSet<String> = out.iter().map(|e| e.id.clone()).collect();
    if let Ok(rd) = std::fs::read_dir(extensions_root()) {
        for entry in rd.flatten() {
            let mf = entry.path().join(manifest_name());
            if let Ok(txt) = std::fs::read_to_string(&mf)
                && let Ok(m) = serde_json::from_str::<ExtManifest>(&txt)
                && !seen.contains(&m.id)
            {
                out.push(m);
            }
        }
    }
    out
}

/// Resolve the lang pack for a project's `lang` id (default "deno").
pub fn lang_pack(id: &str) -> Option<ExtManifest> {
    all_extensions()
        .into_iter()
        .find(|e| e.kind == ExtKind::Lang && e.id == id)
}

/// Locate the on-disk source dir for a scaffold template contributed by an
/// installed pack: `templates/<template_id>` for lang/app packs, or the
/// example's `appDir`. Returns (manifest, dir) so the scaffolder can copy it.
pub fn template_source(template_id: &str) -> Option<(ExtManifest, PathBuf)> {
    let rd = std::fs::read_dir(extensions_root()).ok()?;
    for entry in rd.flatten() {
        let base = entry.path();
        let txt = std::fs::read_to_string(base.join(manifest_name())).ok()?;
        let m: ExtManifest = match serde_json::from_str(&txt) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if m.kind == ExtKind::Example && m.id == template_id {
            let dir = base.join(m.app_dir.clone().unwrap_or_else(|| "app".into()));
            if dir.is_dir() {
                return Some((m, dir));
            }
        }
        if m.templates.iter().any(|t| t.id == template_id) {
            let dir = base.join("templates").join(template_id);
            if dir.is_dir() {
                return Some((m, dir));
            }
        }
    }
    None
}

/// Recursively copy a pack template dir into a project dir.
pub fn copy_tree(src: &PathBuf, dst: &PathBuf) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_tree(&from, &to)?;
        } else {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Trust store — allowlist + consent (ADR 0007)
// ---------------------------------------------------------------------------

/// Persisted consent: a global yolo bypass plus per-extension approve-always.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrustStore {
    #[serde(default)]
    pub yolo: bool,
    #[serde(default)]
    pub approved: BTreeSet<String>,
}

fn trust_path() -> PathBuf {
    extensions_root().join("trust.json")
}

pub fn load_trust() -> TrustStore {
    std::fs::read_to_string(trust_path())
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

pub fn save_trust(t: &TrustStore) -> std::io::Result<()> {
    std::fs::create_dir_all(extensions_root())?;
    let json = serde_json::to_string_pretty(t).unwrap_or_else(|_| "{}".into());
    std::fs::write(trust_path(), format!("{json}\n"))
}

/// Whether a pack's toolchain commands may run without prompting.
pub fn is_trusted(id: &str) -> bool {
    let t = load_trust();
    t.yolo || t.approved.contains(id) || builtin_ids().contains(id)
}

fn builtin_ids() -> BTreeSet<String> {
    builtin_extensions().into_iter().map(|e| e.id).collect()
}

// ---------------------------------------------------------------------------
// Install / remove
// ---------------------------------------------------------------------------

fn safe_pkg_dir(pkg: &str) -> Option<PathBuf> {
    // npm package -> safe dir name (`@scope/name` -> `scope__name`).
    let flat = pkg.trim_start_matches('@').replace('/', "__");
    if flat.is_empty()
        || flat.contains("..")
        || !flat
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return None;
    }
    Some(extensions_root().join(flat))
}

/// Install a `nano-ide-ext-*` package from npm via `npm pack` + extract. The
/// package must carry a `nano-ide.ext.json` manifest. Returns its parsed
/// manifest. Best-effort: requires `npm` on PATH.
pub fn install_from_npm(pkg: &str) -> Result<ExtManifest, String> {
    let dir = safe_pkg_dir(pkg).ok_or("invalid package name")?;
    // Clean install: clear any prior copy so a re-install (i.e. an update to a
    // newer npm version) never leaves stale files from the old version behind.
    if dir.exists() {
        let _ = std::fs::remove_dir_all(&dir);
    }
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    let npm = find_program("npm").ok_or("npm not found on PATH")?;
    let out = std::process::Command::new(&npm)
        .args(["pack", pkg, "--silent"])
        .current_dir(&dir)
        .output()
        .map_err(|e| format!("npm pack: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "npm pack failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    let tgz = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let tar = find_program("tar").ok_or("tar not found")?;
    let st = std::process::Command::new(&tar)
        .args(["xzf", &tgz, "--strip-components=1"])
        .current_dir(&dir)
        .status()
        .map_err(|e| format!("tar: {e}"))?;
    if !st.success() {
        return Err("tar extract failed".into());
    }
    let _ = std::fs::remove_file(dir.join(&tgz));
    let mf = dir.join(manifest_name());
    let txt =
        std::fs::read_to_string(&mf).map_err(|_| "package has no nano-ide.ext.json".to_string())?;
    let m: ExtManifest = serde_json::from_str(&txt).map_err(|e| format!("bad manifest: {e}"))?;
    Ok(m)
}

/// Remove an installed (non-builtin) extension by package name.
pub fn remove(pkg: &str) -> Result<(), String> {
    let dir = safe_pkg_dir(pkg).ok_or("invalid package name")?;
    if !dir.is_dir() {
        return Err("not installed".into());
    }
    std::fs::remove_dir_all(dir).map_err(|e| format!("remove: {e}"))
}

/// The version of an installed pack, read from its bundled `package.json` (the
/// `npm pack` tarball carries it). `None` when the pack isn't installed or has
/// no readable version — used to tell whether a newer npm release is available.
pub fn installed_version(pkg: &str) -> Option<String> {
    let dir = safe_pkg_dir(pkg)?;
    let txt = std::fs::read_to_string(dir.join("package.json")).ok()?;
    let v: serde_json::Value = serde_json::from_str(&txt).ok()?;
    v.get("version")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
}

// ---------------------------------------------------------------------------
// Marketplace — discover packs on npm by the `nano-ide-ext` keyword
// ---------------------------------------------------------------------------

/// The discovery keyword every published pack carries. The marketplace lists
/// every npm package tagged with it; categories come from `nano-ide-{lang,app,
/// example}`.
pub const MARKETPLACE_KEYWORD: &str = "nano-ide-ext";

/// One npm package surfaced in the marketplace.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketEntry {
    pub name: String,
    pub version: String,
    pub description: String,
    /// "lang" | "app" | "example" | "other", from keywords.
    pub category: String,
    pub installed: bool,
    /// The locally-installed version, when this pack is installed (else `None`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub installed_version: Option<String>,
    /// True when the pack is installed and its version differs from the latest
    /// on npm — i.e. an update can be pulled.
    pub update_available: bool,
}

/// Browse npm for packs tagged `nano-ide-ext`. Shells out to `npm search`
/// (npm is already required for install). Best-effort; empty on offline/error.
pub fn marketplace() -> Result<Vec<MarketEntry>, String> {
    let npm = find_program("npm").ok_or("npm not found on PATH")?;
    let out = std::process::Command::new(&npm)
        .args([
            "search",
            &format!("keywords:{MARKETPLACE_KEYWORD}"),
            "--json",
        ])
        .output()
        .map_err(|e| format!("npm search: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "npm search failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    let raw: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).map_err(|e| format!("parse search: {e}"))?;
    let mut entries: Vec<MarketEntry> = raw
        .into_iter()
        .map(|p| {
            let kws: Vec<String> = p["keywords"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|k| k.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let category = if kws.iter().any(|k| k == "nano-ide-lang") {
                "lang"
            } else if kws.iter().any(|k| k == "nano-ide-app") {
                "app"
            } else if kws.iter().any(|k| k == "nano-ide-example") {
                "example"
            } else if kws.iter().any(|k| k == "nano-ide-theme") {
                "theme"
            } else {
                "other"
            };
            let name = p["name"].as_str().unwrap_or_default().to_string();
            let latest = p["version"].as_str().unwrap_or_default().to_string();
            let inst_ver = installed_version(&name);
            let installed =
                inst_ver.is_some() || safe_pkg_dir(&name).map(|d| d.is_dir()).unwrap_or(false);
            // Flag an update only when we can read the installed version and it
            // differs from the latest published one.
            let update_available = inst_ver
                .as_deref()
                .map(|iv| !iv.is_empty() && !latest.is_empty() && iv != latest)
                .unwrap_or(false);
            MarketEntry {
                installed,
                version: latest,
                description: p["description"].as_str().unwrap_or_default().to_string(),
                category: category.to_string(),
                installed_version: inst_ver,
                update_available,
                name,
            }
        })
        .collect();
    entries.sort_by(|a, b| a.category.cmp(&b.category).then(a.name.cmp(&b.name)));
    Ok(entries)
}

/// Find a program on PATH (and the Cargo bin dir for Rust). Mirrors
/// [`super::workers::find_deno`]'s resolution order.
pub fn find_program(name: &str) -> Option<PathBuf> {
    if let Ok(path) = std::env::var("PATH") {
        for d in std::env::split_paths(&path) {
            let c = d.join(name);
            if c.is_file() {
                return Some(c);
            }
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        let c = PathBuf::from(home).join(".cargo").join("bin").join(name);
        if c.is_file() {
            return Some(c);
        }
    }
    None
}

/// Whether a pack's toolchain is installed (detect probe). Built-in/empty => true.
pub fn toolchain_available(m: &ExtManifest) -> bool {
    match m.toolchain.detect.first() {
        Some(bin) => find_program(bin).is_some(),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_cover_deno_rust_gui() {
        let ids: BTreeSet<_> = builtin_extensions().into_iter().map(|e| e.id).collect();
        assert!(ids.contains("deno") && ids.contains("rust") && ids.contains("deno-gui"));
    }

    #[test]
    fn builtins_are_always_trusted() {
        assert!(is_trusted("rust"));
        assert!(is_trusted("deno"));
    }

    #[test]
    fn lang_lookup() {
        assert_eq!(lang_pack("rust").unwrap().display_name, "Rust");
        assert!(lang_pack("deno-gui").is_none());
    }

    #[test]
    fn pkg_dir_flattens_scope() {
        let p = safe_pkg_dir("@nanobpm/nano-ide-lang-rust").unwrap();
        assert!(p.ends_with("nanobpm__nano-ide-lang-rust"));
        assert!(safe_pkg_dir("../evil").is_none());
    }

    #[test]
    fn manifest_round_trips() {
        let m = &builtin_extensions()[1];
        let s = serde_json::to_string(m).unwrap();
        let back: ExtManifest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.toolchain.run, vec!["cargo", "run", "--release"]);
    }

    #[test]
    fn installed_version_reads_package_json() {
        // Point the extensions root at a unique temp dir and drop a pack with a
        // package.json, then confirm installed_version reads its version.
        let root = std::env::temp_dir().join(format!("nano-ext-test-{}", std::process::id()));
        let pkg = "@nanobpm/nano-ide-lang-rust";
        // SAFETY: test-local env set; other tests in this module don't depend on
        // the extensions-root *value* (only on path suffixes / builtins).
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let dir = safe_pkg_dir(pkg).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("package.json"), r#"{"version":"1.0.0"}"#).unwrap();

        assert_eq!(installed_version(pkg).as_deref(), Some("1.0.0"));
        assert_eq!(installed_version("@nanobpm/not-installed"), None);
        // The update-available rule: installed version differs from latest.
        assert_ne!(installed_version(pkg).as_deref(), Some("1.1.0"));

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }
}
