//! **Operator settings** — named LLM connection *profiles* plus the Python interpreter,
//! editable from the console instead of only via environment variables.
//!
//! A consultant works across several models/providers (a local llama.cpp at
//! `localhost:8888`, a cloud Anthropic key, a teammate's vLLM, …). Rather than a single
//! flat config, settings hold a list of **[`LlmProfile`]s** and an *active* one; the
//! console can add/edit/delete profiles, switch the active one, and even query an
//! endpoint's `/models` route to pick the model id. The active profile is layered **over**
//! the environment at request time:
//!
//! ```text
//! provider/base_url/model/api_key/...  =  built-in default
//!                                          → PROCESSOS_LLM_* env
//!                                          → active profile (this file)
//!                                          → per-request override
//! ```
//!
//! Persistence lives at `${PROCESSOS_CONFIG_DIR:-${XDG_CONFIG_HOME:-$HOME/.config}}/processos/settings.json`,
//! created `0600` where supported (it may hold an API key, like `~/.aws/credentials`).
//! A legacy single-LLM file (pre-profiles) is migrated into one `default` profile on load.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::harness::{LlmConfig, LlmOverride};
use crate::pyrunner::PyConfig;

/// A named LLM connection. Every connection field is optional: an unset field falls back
/// to the environment / built-in default for that provider.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LlmProfile {
    /// Stable identifier (slug); referenced by `active_profile`.
    pub id: String,
    /// Human-friendly display name.
    pub name: String,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub temperature: Option<f32>,
}

impl LlmProfile {
    /// Project this profile into a per-request-style [`LlmOverride`] for layering onto
    /// [`LlmConfig`].
    pub fn as_llm_override(&self) -> LlmOverride {
        LlmOverride {
            provider: self.provider.clone(),
            base_url: self.base_url.clone(),
            model: self.model.clone(),
            api_key: self.api_key.clone(),
            max_tokens: self.max_tokens,
            temperature: self.temperature,
        }
    }

    /// Apply a partial update. Per field: `None` keeps it, `Some("")`/`Some(0)`/negative
    /// clears it to the env default, any other value sets it. (`name` cannot be cleared.)
    fn apply(&mut self, patch: ProfilePatch) {
        if let Some(v) = patch.name.and_then(non_empty) {
            self.name = v;
        }
        if let Some(v) = patch.provider {
            self.provider = non_empty(v);
        }
        if let Some(v) = patch.base_url {
            self.base_url = non_empty(v);
        }
        if let Some(v) = patch.model {
            self.model = non_empty(v);
        }
        if let Some(v) = patch.api_key {
            self.api_key = non_empty(v);
        }
        if let Some(v) = patch.max_tokens {
            self.max_tokens = (v != 0).then_some(v);
        }
        if let Some(v) = patch.temperature {
            self.temperature = (v >= 0.0).then_some(v);
        }
    }
}

fn non_empty(s: String) -> Option<String> {
    let t = s.trim();
    (!t.is_empty()).then(|| t.to_string())
}

/// The persisted settings: the profile list, the active profile id, and the (global)
/// Python interpreter for the analysis escape hatch.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    #[serde(default)]
    pub profiles: Vec<LlmProfile>,
    #[serde(default)]
    pub active_profile: Option<String>,
    #[serde(default)]
    pub python_bin: Option<String>,
}

impl Settings {
    /// The active profile: the one named by `active_profile`, else the first, else none.
    pub fn active(&self) -> Option<&LlmProfile> {
        match &self.active_profile {
            Some(id) => self
                .profiles
                .iter()
                .find(|p| &p.id == id)
                .or_else(|| self.profiles.first()),
            None => self.profiles.first(),
        }
    }

    /// The active profile's override (empty if there are no profiles).
    pub fn as_llm_override(&self) -> LlmOverride {
        self.active()
            .map(LlmProfile::as_llm_override)
            .unwrap_or_default()
    }

    /// Build the effective [`PyConfig`]: env (timeouts/caps) with the operator's chosen
    /// interpreter when set.
    pub fn py_config(&self) -> PyConfig {
        let mut cfg = PyConfig::from_env();
        if let Some(bin) = self.python_bin.as_deref().filter(|s| !s.trim().is_empty()) {
            cfg.python_bin = bin.to_string();
        }
        cfg
    }

    fn profile_mut(&mut self, id: &str) -> Option<&mut LlmProfile> {
        self.profiles.iter_mut().find(|p| p.id == id)
    }

    /// Allocate a unique slug id from a display name.
    fn fresh_id(&self, name: &str) -> String {
        let base: String = name
            .trim()
            .to_ascii_lowercase()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect::<String>()
            .trim_matches('-')
            .to_string();
        let base = if base.is_empty() {
            "profile".to_string()
        } else {
            base
        };
        if !self.profiles.iter().any(|p| p.id == base) {
            return base;
        }
        (2..)
            .map(|n| format!("{base}-{n}"))
            .find(|c| !self.profiles.iter().any(|p| &p.id == c))
            .unwrap()
    }
}

/// The seeded default when no settings file exists: a local llama.cpp profile pointing at
/// `localhost:8888/v1`, ready for the operator to fetch + pick a model.
fn seeded() -> Settings {
    Settings {
        profiles: vec![LlmProfile {
            id: "local".to_string(),
            name: "Local (llama.cpp)".to_string(),
            provider: Some("openai".to_string()),
            base_url: Some("http://localhost:8888/v1".to_string()),
            model: None,
            api_key: None,
            max_tokens: Some(8192),
            temperature: Some(0.2),
        }],
        active_profile: Some("local".to_string()),
        python_bin: None,
    }
}

/// Partial update for a single profile (the `name` plus connection fields).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProfilePatch {
    pub name: Option<String>,
    pub provider: Option<String>,
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
}

/// Partial update for the global (non-profile) settings.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GlobalsPatch {
    pub active_profile: Option<String>,
    pub python_bin: Option<String>,
}

/// An ad-hoc endpoint descriptor for listing models — either a saved profile (by id, so
/// its stored key is used) and/or inline connection overrides (so the console can probe
/// an endpoint it is still editing).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ProbeRequest {
    pub profile_id: Option<String>,
    pub provider: Option<String>,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
}

/// A redacted profile safe to return over the API: the API key itself is never echoed,
/// only whether one is set.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileView {
    pub id: String,
    pub name: String,
    pub provider: Option<String>,
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub api_key_set: bool,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
}

impl ProfileView {
    fn of(p: &LlmProfile) -> Self {
        Self {
            id: p.id.clone(),
            name: p.name.clone(),
            provider: p.provider.clone(),
            base_url: p.base_url.clone(),
            model: p.model.clone(),
            api_key_set: p.api_key.as_deref().is_some_and(|k| !k.is_empty()),
            max_tokens: p.max_tokens,
            temperature: p.temperature,
        }
    }
}

/// The full redacted settings view returned to the console.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsView {
    pub profiles: Vec<ProfileView>,
    pub active_profile: Option<String>,
    pub python_bin: Option<String>,
    pub path: String,
}

impl SettingsView {
    fn of(s: &Settings, path: &std::path::Path) -> Self {
        Self {
            profiles: s.profiles.iter().map(ProfileView::of).collect(),
            active_profile: s.active().map(|p| p.id.clone()),
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
    /// Open the store, loading + migrating the persisted file if present, else seeding a
    /// default local profile. A malformed file degrades to the seeded default.
    pub fn open() -> Self {
        let path = settings_path();
        let settings = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| migrate(&s))
            .unwrap_or_else(seeded);
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

    /// Update the globals (active profile + python interpreter), persist, return the view.
    pub fn update_globals(&self, patch: GlobalsPatch) -> Result<SettingsView, String> {
        let mut g = self.inner.write().expect("settings lock");
        if let Some(id) = patch.active_profile {
            let id = id.trim();
            if id.is_empty() {
                g.active_profile = None;
            } else if g.profiles.iter().any(|p| p.id == id) {
                g.active_profile = Some(id.to_string());
            } else {
                return Err(format!("no such profile: {id}"));
            }
        }
        if let Some(v) = patch.python_bin {
            g.python_bin = non_empty(v);
        }
        self.persist(&g)?;
        Ok(SettingsView::of(&g, &self.path))
    }

    /// Create a new profile (optionally seeded from a patch); make it active if it is the
    /// first. Returns the new profile id plus the full view.
    pub fn add_profile(&self, patch: ProfilePatch) -> Result<(String, SettingsView), String> {
        let mut g = self.inner.write().expect("settings lock");
        let name = patch
            .name
            .clone()
            .and_then(non_empty)
            .unwrap_or_else(|| "New profile".to_string());
        let id = g.fresh_id(&name);
        let mut profile = LlmProfile {
            id: id.clone(),
            name,
            provider: None,
            base_url: None,
            model: None,
            api_key: None,
            max_tokens: None,
            temperature: None,
        };
        profile.apply(ProfilePatch {
            name: None,
            ..patch
        });
        g.profiles.push(profile);
        if g.active_profile.is_none() {
            g.active_profile = Some(id.clone());
        }
        self.persist(&g)?;
        Ok((id, SettingsView::of(&g, &self.path)))
    }

    /// Apply a patch to an existing profile, persist, return the view.
    pub fn update_profile(&self, id: &str, patch: ProfilePatch) -> Result<SettingsView, String> {
        let mut g = self.inner.write().expect("settings lock");
        let p = g
            .profile_mut(id)
            .ok_or_else(|| format!("no such profile: {id}"))?;
        p.apply(patch);
        self.persist(&g)?;
        Ok(SettingsView::of(&g, &self.path))
    }

    /// Delete a profile; if it was active, the active falls back to the first remaining.
    pub fn delete_profile(&self, id: &str) -> Result<SettingsView, String> {
        let mut g = self.inner.write().expect("settings lock");
        let before = g.profiles.len();
        g.profiles.retain(|p| p.id != id);
        if g.profiles.len() == before {
            return Err(format!("no such profile: {id}"));
        }
        if g.active_profile.as_deref() == Some(id) {
            g.active_profile = g.profiles.first().map(|p| p.id.clone());
        }
        self.persist(&g)?;
        Ok(SettingsView::of(&g, &self.path))
    }

    /// Resolve the [`LlmConfig`] to use for a model-listing probe: start from the env, layer
    /// the saved profile (if `profile_id` is given) so its stored key is used, then layer
    /// the inline overrides (so the console can probe an endpoint it is still editing).
    pub fn probe_config(&self, req: &ProbeRequest) -> Result<LlmConfig, String> {
        let g = self.inner.read().expect("settings lock");
        let mut cfg = LlmConfig::from_env();
        if let Some(id) = req.profile_id.as_deref() {
            let p = g
                .profiles
                .iter()
                .find(|p| p.id == id)
                .ok_or_else(|| format!("no such profile: {id}"))?;
            cfg = cfg.with_override(&p.as_llm_override());
        }
        cfg = cfg.with_override(&LlmOverride {
            provider: req.provider.clone(),
            base_url: req.base_url.clone(),
            api_key: req.api_key.clone(),
            ..Default::default()
        });
        Ok(cfg)
    }

    fn persist(&self, s: &Settings) -> Result<(), String> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        }
        let json = serde_json::to_string_pretty(s).map_err(|e| e.to_string())?;
        std::fs::write(&*self.path, json)
            .map_err(|e| format!("write {}: {e}", self.path.display()))?;
        restrict_permissions(&self.path);
        Ok(())
    }
}

/// Parse a stored settings file, migrating the legacy single-LLM shape (flat `llm*`
/// fields) into one `default` profile. Returns `None` if the JSON is unusable.
fn migrate(s: &str) -> Option<Settings> {
    #[derive(Deserialize, Default)]
    #[serde(rename_all = "camelCase", default)]
    struct Stored {
        profiles: Vec<LlmProfile>,
        active_profile: Option<String>,
        python_bin: Option<String>,
        // legacy flat fields (pre-profiles)
        llm_provider: Option<String>,
        llm_base_url: Option<String>,
        llm_model: Option<String>,
        llm_api_key: Option<String>,
        llm_max_tokens: Option<u32>,
        llm_temperature: Option<f32>,
    }
    let st: Stored = serde_json::from_str(s).ok()?;
    let mut settings = Settings {
        profiles: st.profiles,
        active_profile: st.active_profile,
        python_bin: st.python_bin,
    };
    if settings.profiles.is_empty() {
        let has_legacy = st.llm_provider.is_some()
            || st.llm_base_url.is_some()
            || st.llm_model.is_some()
            || st.llm_api_key.is_some()
            || st.llm_max_tokens.is_some()
            || st.llm_temperature.is_some();
        if has_legacy {
            settings.profiles.push(LlmProfile {
                id: "default".to_string(),
                name: "Default".to_string(),
                provider: st.llm_provider,
                base_url: st.llm_base_url,
                model: st.llm_model,
                api_key: st.llm_api_key,
                max_tokens: st.llm_max_tokens,
                temperature: st.llm_temperature,
            });
            settings
                .active_profile
                .get_or_insert_with(|| "default".to_string());
        } else {
            // Empty/unknown file → fall back to the seeded local profile.
            return Some(seeded());
        }
    }
    Some(settings)
}

/// Resolve the ProcessOS config directory. Honours `PROCESSOS_CONFIG_DIR` (mainly for
/// tests), then `XDG_CONFIG_HOME`/processos, then `$HOME/.config`/processos, falling back
/// to `./processos`.
pub fn config_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("PROCESSOS_CONFIG_DIR").filter(|s| !s.is_empty()) {
        return PathBuf::from(dir);
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("processos")
}

/// Resolve the settings file path (the `settings.json` inside [`config_dir`]).
pub fn settings_path() -> PathBuf {
    config_dir().join("settings.json")
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
    fn ships_with_a_local_profile() {
        let tmp = std::env::temp_dir().join(format!("processos-set-seed-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let store = store_in(&tmp);
        let v = store.view();
        assert_eq!(v.profiles.len(), 1);
        assert_eq!(v.active_profile.as_deref(), Some("local"));
        assert_eq!(
            v.profiles[0].base_url.as_deref(),
            Some("http://localhost:8888/v1")
        );
        let _ = std::fs::remove_dir_all(&tmp);
        std::env::remove_var("PROCESSOS_CONFIG_DIR");
    }

    #[test]
    fn add_switch_and_redact_profiles() {
        let tmp = std::env::temp_dir().join(format!("processos-set-multi-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let store = store_in(&tmp);

        let (id, _) = store
            .add_profile(ProfilePatch {
                name: Some("Cloud".into()),
                provider: Some("anthropic".into()),
                model: Some("claude".into()),
                api_key: Some("sk-secret".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(id, "cloud");

        // Switch active to the new profile and confirm the override follows it.
        store
            .update_globals(GlobalsPatch {
                active_profile: Some(id.clone()),
                python_bin: None,
            })
            .unwrap();
        let ovr = store.snapshot().as_llm_override();
        assert_eq!(ovr.model.as_deref(), Some("claude"));
        assert_eq!(ovr.api_key.as_deref(), Some("sk-secret"));

        // The view redacts the key but flags it set.
        let v = store.view();
        let cloud = v.profiles.iter().find(|p| p.id == "cloud").unwrap();
        assert!(cloud.api_key_set);
        assert!(!serde_json::to_string(&v).unwrap().contains("sk-secret"));

        // Switching to an unknown profile is rejected.
        assert!(store
            .update_globals(GlobalsPatch {
                active_profile: Some("nope".into()),
                python_bin: None
            })
            .is_err());

        // Delete falls back the active to the first remaining.
        store.delete_profile("cloud").unwrap();
        assert_eq!(store.view().active_profile.as_deref(), Some("local"));

        let _ = std::fs::remove_dir_all(&tmp);
        std::env::remove_var("PROCESSOS_CONFIG_DIR");
    }

    #[test]
    fn migrates_legacy_flat_file() {
        let tmp = std::env::temp_dir().join(format!("processos-set-legacy-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(
            tmp.join("settings.json"),
            r#"{"llmProvider":"openai","llmBaseUrl":"http://x/v1","llmModel":"m","llmApiKey":"k","pythonBin":"/p"}"#,
        )
        .unwrap();
        let store = store_in(&tmp);
        let v = store.view();
        assert_eq!(v.profiles.len(), 1);
        assert_eq!(v.profiles[0].id, "default");
        assert_eq!(v.profiles[0].model.as_deref(), Some("m"));
        assert_eq!(v.python_bin.as_deref(), Some("/p"));
        assert!(v.profiles[0].api_key_set);
        let _ = std::fs::remove_dir_all(&tmp);
        std::env::remove_var("PROCESSOS_CONFIG_DIR");
    }
}
