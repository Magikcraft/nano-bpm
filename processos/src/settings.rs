//! **Operator settings** — the LLM connection and the Python interpreter, made
//! configurable from the console instead of only via environment variables.
//!
//! These were previously env-only (`PROCESSOS_LLM_*`, `PROCESSOS_PYTHON`), which is
//! awkward for a consultant driving the console: they'd have to relaunch the server to
//! point at a different model or interpreter. This module persists the operator's chosen
//! values to a JSON file in the user's config directory and layers them **over** the
//! environment defaults at request time:
//!
//! ```text
//! provider/base_url/model/api_key/...  =  built-in default
//!                                          → PROCESSOS_LLM_* env
//!                                          → persisted settings (this file)
//!                                          → per-request override
//! ```
//!
//! Each later layer overrides the earlier when present, so the console is authoritative
//! over the environment while a one-off request body can still override everything.
//!
//! Persistence lives at `${PROCESSOS_CONFIG_DIR:-${XDG_CONFIG_HOME:-$HOME/.config}}/processos/settings.json`.
//! It is a *trusted-operator* file (it may hold an API key in plaintext, like a `.npmrc`
//! or `~/.aws/credentials`); it is created with `0600` permissions where the platform
//! supports it.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::harness::LlmOverride;
use crate::pyrunner::PyConfig;

/// The persisted, operator-editable settings. Every field is optional: an unset field
/// means "fall back to the environment / built-in default". Stored camelCase so the JSON
/// on disk matches the console's wire format exactly.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Settings {
    /// LLM provider label (`openai` / `anthropic` / aliases). See [`crate::harness`].
    pub llm_provider: Option<String>,
    /// OpenAI-compatible / Anthropic base URL.
    pub llm_base_url: Option<String>,
    /// Model id (required before any investigation/hypothesis call can run).
    pub llm_model: Option<String>,
    /// API key (optional; local models usually need none). Held in plaintext.
    pub llm_api_key: Option<String>,
    /// Max completion tokens.
    pub llm_max_tokens: Option<u32>,
    /// Sampling temperature.
    pub llm_temperature: Option<f32>,
    /// Python interpreter for the analysis escape hatch (point at a venv with
    /// pandas/duckdb/scipy for full power).
    pub python_bin: Option<String>,
}

impl Settings {
    /// Project the persisted settings into a per-request-style [`LlmOverride`] so they can
    /// be layered onto [`crate::harness::LlmConfig`] with the existing precedence logic.
    pub fn as_llm_override(&self) -> LlmOverride {
        LlmOverride {
            provider: self.llm_provider.clone(),
            base_url: self.llm_base_url.clone(),
            model: self.llm_model.clone(),
            api_key: self.llm_api_key.clone(),
            max_tokens: self.llm_max_tokens,
            temperature: self.llm_temperature,
        }
    }

    /// Build the effective [`PyConfig`]: start from the environment (timeout / output
    /// caps) and override the interpreter with the operator's chosen one when set.
    pub fn py_config(&self) -> PyConfig {
        let mut cfg = PyConfig::from_env();
        if let Some(bin) = self.python_bin.as_deref().filter(|s| !s.trim().is_empty()) {
            cfg.python_bin = bin.to_string();
        }
        cfg
    }

    /// Apply a partial update. For each field, `None` leaves it unchanged, `Some("")`
    /// (or `Some(0)`) clears it back to the environment default, and any other value sets
    /// it. This lets the console blank a field to "use the env default" and send only the
    /// fields it actually changed (notably the secret API key).
    fn apply(&mut self, patch: SettingsPatch) {
        if let Some(v) = patch.llm_provider {
            self.llm_provider = non_empty(v);
        }
        if let Some(v) = patch.llm_base_url {
            self.llm_base_url = non_empty(v);
        }
        if let Some(v) = patch.llm_model {
            self.llm_model = non_empty(v);
        }
        if let Some(v) = patch.llm_api_key {
            self.llm_api_key = non_empty(v);
        }
        if let Some(v) = patch.llm_max_tokens {
            self.llm_max_tokens = (v != 0).then_some(v);
        }
        if let Some(v) = patch.llm_temperature {
            self.llm_temperature = (v >= 0.0).then_some(v);
        }
        if let Some(v) = patch.python_bin {
            self.python_bin = non_empty(v);
        }
    }
}

fn non_empty(s: String) -> Option<String> {
    let t = s.trim();
    (!t.is_empty()).then(|| t.to_string())
}

/// A partial update sent by the console. Absent fields are left untouched; see
/// [`Settings::apply`] for the per-field clear/set semantics.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SettingsPatch {
    pub llm_provider: Option<String>,
    pub llm_base_url: Option<String>,
    pub llm_model: Option<String>,
    pub llm_api_key: Option<String>,
    pub llm_max_tokens: Option<u32>,
    pub llm_temperature: Option<f32>,
    pub python_bin: Option<String>,
}

/// A redacted view safe to return over the API and show in the console: the API key is
/// never echoed back, only whether one is set.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsView {
    pub llm_provider: Option<String>,
    pub llm_base_url: Option<String>,
    pub llm_model: Option<String>,
    /// Whether an API key is stored (the key itself is never returned).
    pub llm_api_key_set: bool,
    pub llm_max_tokens: Option<u32>,
    pub llm_temperature: Option<f32>,
    pub python_bin: Option<String>,
    /// Absolute path of the backing file, so the operator knows where it persists.
    pub path: String,
}

impl SettingsView {
    fn of(s: &Settings, path: &std::path::Path) -> Self {
        Self {
            llm_provider: s.llm_provider.clone(),
            llm_base_url: s.llm_base_url.clone(),
            llm_model: s.llm_model.clone(),
            llm_api_key_set: s.llm_api_key.as_deref().is_some_and(|k| !k.is_empty()),
            llm_max_tokens: s.llm_max_tokens,
            llm_temperature: s.llm_temperature,
            python_bin: s.python_bin.clone(),
            path: path.display().to_string(),
        }
    }
}

/// Thread-safe, file-backed settings store. Cloneable (shares the inner lock) so it can
/// live in the axum `AppState`.
#[derive(Clone)]
pub struct SettingsStore {
    inner: Arc<RwLock<Settings>>,
    path: Arc<PathBuf>,
}

impl SettingsStore {
    /// Open the store, loading the persisted file if present. A missing or malformed file
    /// degrades to empty settings (env defaults) rather than failing the server boot.
    pub fn open() -> Self {
        let path = settings_path();
        let settings = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str::<Settings>(&s).ok())
            .unwrap_or_default();
        Self {
            inner: Arc::new(RwLock::new(settings)),
            path: Arc::new(path),
        }
    }

    /// A snapshot of the current settings (cheap clone).
    pub fn snapshot(&self) -> Settings {
        self.inner.read().expect("settings lock").clone()
    }

    /// The redacted view for the API/console.
    pub fn view(&self) -> SettingsView {
        SettingsView::of(&self.inner.read().expect("settings lock"), &self.path)
    }

    /// Apply a patch, persist it, and return the new redacted view. Persistence failures
    /// are surfaced so the console can report them.
    pub fn update(&self, patch: SettingsPatch) -> Result<SettingsView, String> {
        let view = {
            let mut g = self.inner.write().expect("settings lock");
            g.apply(patch);
            self.persist(&g)?;
            SettingsView::of(&g, &self.path)
        };
        Ok(view)
    }

    fn persist(&self, s: &Settings) -> Result<(), String> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)
                .map_err(|e| format!("create {}: {e}", dir.display()))?;
        }
        let json = serde_json::to_string_pretty(s).map_err(|e| e.to_string())?;
        std::fs::write(&*self.path, json)
            .map_err(|e| format!("write {}: {e}", self.path.display()))?;
        restrict_permissions(&self.path);
        Ok(())
    }
}

/// Resolve the settings file path. Honours `PROCESSOS_CONFIG_DIR` (mainly for tests),
/// then `XDG_CONFIG_HOME`, then `$HOME/.config`, falling back to the current directory.
pub fn settings_path() -> PathBuf {
    if let Some(dir) = std::env::var_os("PROCESSOS_CONFIG_DIR").filter(|s| !s.is_empty()) {
        return PathBuf::from(dir).join("settings.json");
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("processos").join("settings.json")
}

#[cfg(unix)]
fn restrict_permissions(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &std::path::Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_in(dir: &std::path::Path) -> SettingsStore {
        std::env::set_var("PROCESSOS_CONFIG_DIR", dir);
        SettingsStore::open()
    }

    #[test]
    fn patch_sets_clears_and_keeps() {
        let tmp = std::env::temp_dir().join(format!("processos-settings-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let store = store_in(&tmp);

        // Set a model + key.
        store
            .update(SettingsPatch {
                llm_model: Some("gemma".into()),
                llm_api_key: Some("secret".into()),
                ..Default::default()
            })
            .unwrap();
        let v = store.view();
        assert_eq!(v.llm_model.as_deref(), Some("gemma"));
        assert!(v.llm_api_key_set, "key should be set");

        // Absent fields are untouched; empty string clears.
        store
            .update(SettingsPatch {
                llm_model: Some(String::new()), // clear
                ..Default::default()            // key untouched
            })
            .unwrap();
        let v = store.view();
        assert_eq!(v.llm_model, None, "empty string clears model");
        assert!(v.llm_api_key_set, "absent key field leaves it set");

        // The override projection carries the key through (server-side, not the view).
        let ovr = store.snapshot().as_llm_override();
        assert_eq!(ovr.api_key.as_deref(), Some("secret"));

        // Reopening reads the persisted file.
        let reopened = store_in(&tmp);
        assert!(reopened.view().llm_api_key_set);

        let _ = std::fs::remove_dir_all(&tmp);
        std::env::remove_var("PROCESSOS_CONFIG_DIR");
    }

    #[test]
    fn view_never_echoes_the_key() {
        let json = serde_json::to_string(&store_view_with_key()).unwrap();
        assert!(!json.contains("secret"), "view leaked key: {json}");
    }

    fn store_view_with_key() -> SettingsView {
        let s = Settings {
            llm_api_key: Some("secret".into()),
            ..Default::default()
        };
        SettingsView::of(&s, std::path::Path::new("/tmp/x"))
    }
}
