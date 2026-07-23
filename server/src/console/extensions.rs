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

/// One completion item a pack offers for its language (see [`LangIntellisense`]).
/// The console has a real language service only for TS/JS; other languages get
/// this curated, SDK-derived data instead. Read as opaque data and forwarded to
/// the console verbatim.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletionSpec {
    /// Text shown in the completion list.
    pub label: String,
    /// Monaco `CompletionItemKind` name (e.g. "method", "struct"); the console
    /// maps it. Defaults to "value" client-side when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Text inserted on accept. Defaults to `label`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub insert_text: Option<String>,
    /// When true, `insertText` is a Monaco snippet (`${1:name}` placeholders).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<bool>,
    /// Short right-aligned signature/type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Markdown documentation shown in the details flyout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
}

/// A hover card shown when the pointer rests on `symbol` (whole-word match).
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HoverSpec {
    pub symbol: String,
    /// Markdown rendered in the hover card.
    pub contents: String,
}

/// One parameter within a [`SignatureSpec`].
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignatureParam {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
}

/// One function/method signature surfaced by signature help.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignatureSpec {
    /// Identifier that, when followed by `(`, triggers this help.
    pub trigger: String,
    /// Full signature line shown in the popup.
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    #[serde(default)]
    pub parameters: Vec<SignatureParam>,
}

/// IntelliSense data a lang pack ships for one Monaco language. The console
/// registers one provider per `monacoLang` and feeds it every pack's entries —
/// no in-browser language server required. Read as data, forwarded verbatim.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LangIntellisense {
    /// Monaco language id these entries apply to (e.g. "csharp", "rust").
    pub monaco_lang: String,
    /// Extra characters that reopen the completion popup (e.g. ["."]).
    #[serde(default)]
    pub trigger_characters: Vec<String>,
    #[serde(default)]
    pub completions: Vec<CompletionSpec>,
    #[serde(default)]
    pub hovers: Vec<HoverSpec>,
    #[serde(default)]
    pub signatures: Vec<SignatureSpec>,
}

/// One named way to run/compile the same project — used by packs whose
/// example is a matrix (e.g. `example-java-throughput` has four
/// transport/profile combos over one Java source). The Console offers
/// these in a Run/Target dropdown; the picked id is persisted per project.
///
/// **Env merging:** `env` extends (and, on key conflict, overrides) the
/// project-level environment when the config is spawned.
///
/// **Trust:** each config's `run`/`compile` argv is snapshotted into the
/// project on scaffold — trust is granted per scaffolding pack id, same
/// model as the flat `run`/`compile`.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunConfig {
    /// Stable id, unique within a pack (e.g. `"stock-rest"`).
    pub id: String,
    /// Human label shown in the picker (e.g. `"Camunda 8 · REST"`).
    pub label: String,
    /// If true and no `activeRunConfig` is set on the project, this one wins.
    /// At most one per pack should be flagged; extras are ignored deterministically
    /// (first one wins in pack order).
    #[serde(default)]
    pub default: bool,
    /// Shell-style argv to run this config. Empty falls back to the toolchain's
    /// top-level `run`.
    #[serde(default)]
    pub run: Vec<String>,
    /// Shell-style argv to compile this config. Empty falls back to the
    /// toolchain's top-level `compile`.
    #[serde(default)]
    pub compile: Vec<String>,
    /// Extra environment variables set on spawn — overrides project env on key
    /// conflict.
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
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
    /// built-in Deno runner. Serves as a fallback when the active run config's
    /// `run` argv is empty (per [`RunConfig`] semantics).
    #[serde(default)]
    pub run: Vec<String>,
    /// Shell-style argv to compile the project. Empty => Deno compile. Serves
    /// as a fallback when the active run config's `compile` argv is empty.
    #[serde(default)]
    pub compile: Vec<String>,
    /// Cross-compile target triples this toolchain offers.
    #[serde(default)]
    pub targets: Vec<String>,
    /// Named run configurations (see [`RunConfig`]). When present, the Console
    /// surfaces them in a Run/Target dropdown and the supervisor prefers them
    /// over the top-level `run`/`compile`.
    #[serde(default)]
    pub run_configs: Vec<RunConfig>,
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
    /// Optional pack icon: an inline SVG XML string (preferred) or a data:/http:
    /// URL. Lang packs supply this so the Console can badge project cards with a
    /// language icon. Built-in packs embed a small brand glyph below.
    #[serde(default)]
    pub icon: Option<String>,
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
    /// SDK-derived Monaco IntelliSense (completions/hovers/signatures) this pack
    /// contributes, one entry per `monacoLang`. Optional; forwarded to the
    /// console which registers providers per language.
    #[serde(default)]
    pub intellisense: Vec<LangIntellisense>,
    /// Component element templates this pack contributes (ADR 0033 §4): a list
    /// of pack-relative paths to Zeebe element-template JSON files (each holding
    /// a single template or an array). The host reads + parses them and forwards
    /// the resolved templates to the console, which merges them **under** the
    /// project's own components (project wins on an id collision) to drive the
    /// BPMN palette — the installable-component-library / Delphi-VCL axis.
    #[serde(default)]
    pub components: Vec<String>,
}

/// Built-in language-pack icons: theme-robust lettermark tiles (a brand-coloured
/// rounded square with a white glyph) rendered as an `<img>` on project cards.
/// A colored tile stays legible on both the light and dark console themes (an
/// `<img>`-loaded SVG can't inherit `currentColor`). Published packs may ship
/// their own richer SVG via `nano-ide.ext.json`'s `icon`.
const ICON_DENO: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect width="24" height="24" rx="4" fill="#3178C6"/><text x="12" y="16.5" font-family="Helvetica,Arial,sans-serif" font-size="10" font-weight="700" fill="#fff" text-anchor="middle">TS</text></svg>"##;

/// The built-in first-party packs — always available, offline, unremovable.
/// Deno is the legacy lang+app runtime (empty toolchain => internal Deno path).
pub fn builtin_extensions() -> Vec<ExtManifest> {
    vec![
        ExtManifest {
            id: "deno".into(),
            kind: ExtKind::Lang,
            display_name: "Deno (TypeScript)".into(),
            icon: Some(ICON_DENO.into()),
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
                    "Path to the Deno runtime, used only to compile a project to a standalone binary (`deno compile`). Auto-resolved from PATH / ~/.deno/bin when unset.".into(),
                ),
                env: Some("NANOBPMN_DENO_BIN".into()),
                default: Some("deno (on PATH)".into()),
            }],
            themes: vec![],
            intellisense: vec![],
            components: vec![],
        },
        ExtManifest {
            id: "deno-gui".into(),
            kind: ExtKind::App,
            display_name: "Deno GUI app".into(),
            icon: None,
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
            intellisense: vec![],
            components: vec![],
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

/// Resolve any installed extension (lang/app/example/theme) by manifest id.
/// Used for the trust check on a project's snapshotted toolchain (approving
/// `embedded-jvm` covers Run/Compile on projects it scaffolded).
pub fn find_ext(id: &str) -> Option<ExtManifest> {
    all_extensions().into_iter().find(|e| e.id == id)
}

/// Best-effort version of the pack whose manifest id is `ext_id`, read from
/// its bundled `package.json`. Returns `None` for built-in packs (no npm
/// tarball) or when the pack dir isn't found. Used as the `scaffoldedFrom.
/// version` breadcrumb on a project — never as a run-time gate.
pub fn pack_version(ext_id: &str) -> Option<String> {
    let rd = std::fs::read_dir(extensions_root()).ok()?;
    for entry in rd.flatten() {
        let base = entry.path();
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        if m.id != ext_id {
            continue;
        }
        let pkg = std::fs::read_to_string(base.join("package.json")).ok()?;
        let v: serde_json::Value = serde_json::from_str(&pkg).ok()?;
        return v
            .get("version")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from);
    }
    None
}

/// Reads the element templates contributed by the installed pack `ext_id` (ADR
/// 0033 §4): resolves each of its manifest `components` paths against the pack
/// dir, parses the JSON (a single template or an array), and returns every
/// element-template-looking entry (a string `id` + a non-empty `appliesTo`).
/// Path-escaping paths, missing files, and malformed JSON are skipped so a bad
/// pack never blanks the palette. Built-in packs (no on-disk dir) yield nothing.
pub fn pack_component_templates(ext_id: &str) -> Vec<serde_json::Value> {
    let Ok(rd) = std::fs::read_dir(extensions_root()) else {
        return vec![];
    };
    for entry in rd.flatten() {
        let base = entry.path();
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        if m.id != ext_id {
            continue;
        }
        let mut out = Vec::new();
        for rel in &m.components {
            let Some(path) = safe_pack_path(&base, rel) else {
                continue;
            };
            let Ok(body) = std::fs::read_to_string(&path) else {
                continue;
            };
            let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body) else {
                continue;
            };
            match parsed {
                serde_json::Value::Array(items) => {
                    out.extend(items.into_iter().filter(is_element_template));
                }
                v if is_element_template(&v) => out.push(v),
                _ => {}
            }
        }
        return out;
    }
    vec![]
}

/// Whether a JSON value looks like a Zeebe element template: a string `id` and a
/// non-empty `appliesTo` array. Loose on purpose — the console's
/// `elementTemplates.set()` runs the authoritative schema validation; this only
/// screens obviously-unrelated JSON so one stray file can't poison the set.
fn is_element_template(v: &serde_json::Value) -> bool {
    v.get("id").and_then(|x| x.as_str()).is_some()
        && v.get("appliesTo")
            .and_then(|x| x.as_array())
            .is_some_and(|a| !a.is_empty())
}

/// Joins a pack-relative path to the pack `base`, rejecting absolute paths and
/// any `..`/root/prefix component so a manifest can't read files outside its own
/// dir (the same containment rule the project file API enforces).
fn safe_pack_path(base: &std::path::Path, rel: &str) -> Option<PathBuf> {
    let candidate = std::path::Path::new(rel);
    if candidate
        .components()
        .all(|c| matches!(c, std::path::Component::Normal(_)))
    {
        Some(base.join(candidate))
    } else {
        None
    }
}

/// Locate the on-disk source dir for a scaffold template contributed by an
/// installed pack: `templates/<template_id>` for lang/app packs, or the
/// example's `appDir`. Returns (manifest, dir) so the scaffolder can copy it.
pub fn template_source(template_id: &str) -> Option<(ExtManifest, PathBuf)> {
    let rd = std::fs::read_dir(extensions_root()).ok()?;
    for entry in rd.flatten() {
        let base = entry.path();
        // The extensions root holds more than pack dirs — the trust store
        // (trust.json), OS litter (.DS_Store), a mid-install tarball. Skip
        // anything without a readable manifest instead of aborting the scan:
        // a `?` here let the FIRST such entry hide every installed template
        // (the picker still offered them via the tolerant all_extensions(),
        // but creation silently fell back to the built-in Deno starter).
        let txt = match std::fs::read_to_string(base.join(manifest_name())) {
            Ok(t) => t,
            Err(_) => continue,
        };
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

/// Uninstall a non-builtin extension. Accepts either the npm package name
/// (e.g. `@nanobpm/nano-ide-lang-rust`) or the manifest id (`rust`) — the
/// latter is what the Console's Extensions overview payload carries, so
/// the UI's remove button uses it. Returns `Err("not installed")` if
/// neither form resolves to an installed pack directory.
pub fn remove(pkg: &str) -> Result<(), String> {
    // Accept either the npm package name (e.g. `@nanobpm/nano-ide-lang-rust`)
    // or the manifest id (`rust`). The Console's Extensions view only knows
    // the manifest id from the overview payload, so we resolve id → dir by
    // scanning installed packs when the direct lookup misses.
    let dir = safe_pkg_dir(pkg)
        .filter(|d| d.is_dir())
        .or_else(|| pack_dir_by_manifest_id(pkg))
        .ok_or_else(|| "not installed".to_string())?;
    std::fs::remove_dir_all(dir).map_err(|e| format!("remove: {e}"))
}

/// Best-effort reverse-lookup: manifest id → installed pack directory. Used
/// so callers holding only a manifest id (like the Console UI) can uninstall
/// without also carrying the pack's npm package name.
fn pack_dir_by_manifest_id(ext_id: &str) -> Option<PathBuf> {
    let rd = std::fs::read_dir(extensions_root()).ok()?;
    for entry in rd.flatten() {
        let base = entry.path();
        if !base.is_dir() {
            continue;
        }
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        if m.id == ext_id {
            return Some(base);
        }
    }
    None
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
    // npm's search index lags publication by minutes to hours. For any pack
    // the user has installed, hit `npm view <name> version --prefer-online`
    // to get the actual published latest — otherwise a freshly-published fix
    // won't surface an "Update" affordance in the console for a long time.
    // Bounded by the installed-pack count so this stays cheap.
    refresh_installed_latest(&npm, &mut entries);
    Ok(entries)
}

/// For each installed entry, overlay the current `latest` via `npm view` and
/// recompute `update_available`. `npm view --prefer-online` bypasses the local
/// metadata cache and hits registry.npmjs.org directly. Failures are ignored
/// (the search result stands).
fn refresh_installed_latest(npm: &std::path::Path, entries: &mut [MarketEntry]) {
    use std::sync::{Arc, Mutex};
    use std::thread;
    let updates: Arc<Mutex<Vec<(usize, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let handles: Vec<_> = entries
        .iter()
        .enumerate()
        .filter(|(_, e)| e.installed)
        .map(|(idx, e)| {
            let name = e.name.clone();
            let npm = npm.to_path_buf();
            let updates = Arc::clone(&updates);
            thread::spawn(move || {
                let out = std::process::Command::new(&npm)
                    .args(["view", &name, "version", "--prefer-online", "--silent"])
                    .output();
                if let Ok(o) = out
                    && o.status.success()
                {
                    let v = String::from_utf8_lossy(&o.stdout).trim().to_string();
                    if !v.is_empty() {
                        updates.lock().unwrap().push((idx, v));
                    }
                }
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }
    for (idx, latest) in updates.lock().unwrap().drain(..) {
        let e = &mut entries[idx];
        e.version = latest;
        e.update_available = e
            .installed_version
            .as_deref()
            .map(|iv| !iv.is_empty() && !e.version.is_empty() && iv != e.version)
            .unwrap_or(false);
    }
}

/// Find a program on PATH (plus the usual per-user tool bin dirs). Mirrors
/// [`super::workers::find_deno`]'s resolution order.
///
/// A detached server (e.g. spawned by systemd / at boot) often inherits a
/// minimal PATH like `/usr/local/sbin:…:/bin` that omits the per-user bin dirs
/// where tool installers drop binaries — notably `~/.local/bin` (the astral
/// `uv` installer) and `~/.cargo/bin` (rustup). We fall back to those so a
/// pack's toolchain (`uv`, `cargo`, …) is found for both detection and
/// execution without the operator having to symlink or patch PATH.
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
        let home = PathBuf::from(home);
        for sub in [
            home.join(".local").join("bin").join(name),
            home.join(".cargo").join("bin").join(name),
        ] {
            if sub.is_file() {
                return Some(sub);
            }
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

    /// Tests that mutate `NANOBPMN_EXTENSIONS_DIR` (a process-global env var)
    /// must serialize on this mutex — cargo test runs them in parallel by
    /// default, so two concurrent tests would race on the env var and read
    /// each other's temp dirs. Use `let _guard = ENV_LOCK.lock().unwrap();`
    /// at the top of any such test.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn builtins_cover_deno_and_gui() {
        // Rust (and Java) are pack-provided, not built-in — installing the
        // nano-ide lang pack is what adds them. Only the Deno runtime ships in-box.
        let ids: BTreeSet<_> = builtin_extensions().into_iter().map(|e| e.id).collect();
        assert!(ids.contains("deno") && ids.contains("deno-gui"));
        assert!(
            !ids.contains("rust"),
            "rust is now pack-provided, not built-in"
        );
    }

    #[test]
    fn builtins_are_always_trusted() {
        assert!(is_trusted("deno"));
        assert!(is_trusted("deno-gui"));
    }

    #[test]
    fn lang_lookup() {
        // Deno is the only built-in lang pack; rust/java come from installed packs.
        assert_eq!(lang_pack("deno").unwrap().display_name, "Deno (TypeScript)");
        assert!(lang_pack("rust").is_none());
    }

    #[test]
    fn pkg_dir_flattens_scope() {
        let p = safe_pkg_dir("@nanobpm/nano-ide-lang-rust").unwrap();
        assert!(p.ends_with("nanobpm__nano-ide-lang-rust"));
        assert!(safe_pkg_dir("../evil").is_none());
    }

    #[test]
    fn pack_component_templates_reads_declared_files() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-comp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let pack = root.join("nanobpm__nano-ide-components-hvac");
        std::fs::create_dir_all(pack.join("components")).unwrap();
        // A manifest declaring component files: one a single template, one an
        // array of templates. A third declared path escapes the pack dir and one
        // more is missing — both must be skipped, not abort the read.
        std::fs::write(
            pack.join(manifest_name()),
            r#"{
              "id": "components-hvac",
              "kind": "app",
              "displayName": "HVAC Components",
              "components": [
                "components/read.json",
                "components/pair.json",
                "../escape.json",
                "components/missing.json"
              ]
            }"#,
        )
        .unwrap();
        std::fs::write(
            pack.join("components/read.json"),
            r#"{ "id": "hvac.read", "name": "Read", "appliesTo": ["bpmn:Task"] }"#,
        )
        .unwrap();
        std::fs::write(
            pack.join("components/pair.json"),
            r#"[
              { "id": "hvac.a", "appliesTo": ["bpmn:Task"] },
              { "id": "hvac.b", "appliesTo": ["bpmn:Task"] },
              { "id": "not-a-template" }
            ]"#,
        )
        .unwrap();
        std::fs::write(
            root.join("escape.json"),
            r#"{"id":"evil","appliesTo":["x"]}"#,
        )
        .unwrap();

        // SAFETY: test-local env set, serialized on ENV_LOCK; no other test in
        // this module depends on the extensions-root value.
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let comps = pack_component_templates("components-hvac");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        let _ = std::fs::remove_dir_all(&root);

        let ids: BTreeSet<_> = comps
            .iter()
            .filter_map(|c| c.get("id").and_then(|x| x.as_str()).map(String::from))
            .collect();
        // The single template + both valid array entries — but not the id-less
        // array entry, the path-escaping file, or the missing file.
        assert_eq!(
            ids,
            ["hvac.a", "hvac.b", "hvac.read"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        // Built-in packs (no on-disk dir) contribute nothing.
        assert!(pack_component_templates("deno").is_empty());
    }

    #[test]
    fn find_program_falls_back_to_local_bin() {
        let _guard = ENV_LOCK.lock().unwrap();
        let home = std::env::temp_dir().join(format!("nano-fp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        // A tool an installer dropped in ~/.local/bin, absent from PATH — mirrors
        // uv installed by the astral script under a detached server's minimal PATH.
        let local_bin = home.join(".local").join("bin");
        std::fs::create_dir_all(&local_bin).unwrap();
        let tool = format!("nano-fake-uv-{}", std::process::id());
        std::fs::write(local_bin.join(&tool), b"#!/bin/sh\n").unwrap();

        // SAFETY: test-local env, serialized on ENV_LOCK; restored below.
        let saved_home = std::env::var_os("HOME");
        let saved_path = std::env::var_os("PATH");
        unsafe {
            std::env::set_var("HOME", &home);
            // A PATH that does *not* contain the tool.
            std::env::set_var("PATH", home.join("nowhere"));
        }
        let found = find_program(&tool);
        unsafe {
            match saved_home {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
            match saved_path {
                Some(v) => std::env::set_var("PATH", v),
                None => std::env::remove_var("PATH"),
            }
        }
        let _ = std::fs::remove_dir_all(&home);

        assert_eq!(found.as_deref(), Some(local_bin.join(&tool).as_path()));
    }

    #[test]
    fn manifest_round_trips() {
        let m = &builtin_extensions()[0];
        assert_eq!(m.id, "deno");
        let s = serde_json::to_string(m).unwrap();
        let back: ExtManifest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.id, "deno");
        assert_eq!(back.display_name, "Deno (TypeScript)");
    }

    #[test]
    fn installed_version_reads_package_json() {
        let _guard = ENV_LOCK.lock().unwrap();
        // Point the extensions root at a unique temp dir and drop a pack with a
        // package.json, then confirm installed_version reads its version.
        let root = std::env::temp_dir().join(format!("nano-ext-ver-{}", std::process::id()));
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

    #[test]
    fn remove_resolves_manifest_id_when_npm_name_missing() {
        let _guard = ENV_LOCK.lock().unwrap();
        // Console UI has only the manifest id (e.g. "throughput-jvm") from the
        // overview payload, so remove() must accept it and reverse-lookup the
        // pack dir via its bundled nano-ide.ext.json — otherwise the button
        // 400s with "not installed" for every non-builtin pack.
        let root = std::env::temp_dir().join(format!("nano-ext-remove-{}", std::process::id()));
        let pkg = "@nanobpm/nano-ide-example-throughput-demo";
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let dir = safe_pkg_dir(pkg).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(manifest_name()),
            r#"{"id":"throughput-demo","kind":"example","displayName":"x"}"#,
        )
        .unwrap();

        assert!(dir.is_dir());
        // Pass the manifest id (not the npm package name).
        remove("throughput-demo").unwrap();
        assert!(!dir.exists(), "pack dir should be gone after remove");

        // Idempotent: second call reports not-installed rather than crashing.
        assert!(remove("throughput-demo").is_err());

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }
}
