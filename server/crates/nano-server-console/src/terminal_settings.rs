//! Persistent, console-managed enablement of the integrated terminal (#500).
//!
//! A terminal is arbitrary code execution, so its gate stays server-side. The
//! effective state is resolved from two inputs, with the env var only ever able
//! to *lock it off* — never silently the only control (that was the #496 trap,
//! where a default-off env var manifested as a bare "shell exited"):
//!
//! 1. `NANO_CONSOLE_TERMINAL` **explicitly disabled** (`0`/`false`/`no`/`off`)
//!    is a **hard lock** — the console cannot enable the terminal. This is for
//!    locked-down / shared deployments.
//! 2. Otherwise the **console setting** governs, persisted to
//!    `<NANOBPMN_DATA_DIR>/console-settings.json` so it survives a restart.
//!    Default off; the operator turns it on in the console. The first-boot
//!    default is *on* only when the env var is **explicitly enabled**
//!    (`1`/`true`/`yes`/`on`), preserving the pre-#500 `NANO_CONSOLE_TERMINAL=1`
//!    behaviour until the operator toggles it.
//!
//! The pure resolver + file IO are unit-tested here; `pty::terminal_enabled()`
//! consults the process-global [`Gate`] initialised at boot by [`init`].

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

/// File under the data dir holding console-managed settings. A JSON object so
/// future settings can share it; we only touch the `terminalEnabled` key.
const SETTINGS_FILE: &str = "console-settings.json";
const KEY: &str = "terminalEnabled";

/// Tri-state of the `NANO_CONSOLE_TERMINAL` env var.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum EnvState {
    ExplicitOn,
    ExplicitOff,
    Unset,
}

/// Parse the env var. Truthy/falsey spellings are case- and whitespace-
/// insensitive; anything else (including unset/empty/garbage) is `Unset`, which
/// means "let the console decide".
fn env_state(raw: Option<&str>) -> EnvState {
    match raw.map(|r| r.trim().to_ascii_lowercase()) {
        Some(v) if matches!(v.as_str(), "1" | "true" | "yes" | "on") => EnvState::ExplicitOn,
        Some(v) if matches!(v.as_str(), "0" | "false" | "no" | "off") => EnvState::ExplicitOff,
        _ => EnvState::Unset,
    }
}

/// How the effective value was decided, surfaced to the console UI.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Source {
    /// Env var explicitly disabled → locked off.
    EnvLocked,
    /// A persisted console toggle.
    Console,
    /// No persisted value; env var explicitly enabled seeded the default-on.
    EnvDefault,
    /// No persisted value, no env → default off.
    Default,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::EnvLocked => "env-locked",
            Source::Console => "console",
            Source::EnvDefault => "env-default",
            Source::Default => "default",
        }
    }
}

/// Pure resolution of the boot state from the env tri-state and any persisted
/// console value. Returns `(enabled, source, locked_off)`.
fn resolve(env: EnvState, persisted: Option<bool>) -> (bool, Source, bool) {
    match env {
        // Hard lock: console cannot override.
        EnvState::ExplicitOff => (false, Source::EnvLocked, true),
        EnvState::ExplicitOn => match persisted {
            Some(v) => (v, Source::Console, false),
            None => (true, Source::EnvDefault, false),
        },
        EnvState::Unset => match persisted {
            Some(v) => (v, Source::Console, false),
            None => (false, Source::Default, false),
        },
    }
}

/// Live, in-memory gate. Reads are lock-free; the rare toggle updates the atomic
/// and best-effort persists to disk.
pub struct Gate {
    enabled: AtomicBool,
    locked_off: bool,
    source: std::sync::Mutex<Source>,
    /// Persistence target, or `None` when no data dir is configured (in-memory
    /// mode) — the setting then lives for the process lifetime only.
    path: Option<PathBuf>,
}

/// Returned by [`Gate::set`] when the env var has locked the terminal off.
pub struct Locked;

impl Gate {
    /// The effective enablement the pty gate must honour.
    pub fn effective(&self) -> bool {
        !self.locked_off && self.enabled.load(Ordering::Relaxed)
    }

    /// `true` when `NANO_CONSOLE_TERMINAL` explicitly disabled the terminal, so
    /// the console must not offer to enable it.
    pub fn locked_off(&self) -> bool {
        self.locked_off
    }

    pub fn source(&self) -> Source {
        *self.source.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Apply a console toggle. `Err(Locked)` when env-locked; otherwise updates
    /// the live value and persists it (best effort — a failed write is logged
    /// but does not fail the request, matching other console-settings writes).
    pub fn set(&self, on: bool) -> Result<(), Locked> {
        if self.locked_off {
            return Err(Locked);
        }
        self.enabled.store(on, Ordering::Relaxed);
        *self.source.lock().unwrap_or_else(|e| e.into_inner()) = Source::Console;
        if let Some(path) = &self.path
            && let Err(e) = save(path, on)
        {
            eprintln!("console: could not persist terminal setting to {path:?}: {e}");
        }
        Ok(())
    }
}

/// Load the persisted `terminalEnabled` value, if the file exists and parses.
fn load(path: &Path) -> Option<bool> {
    let text = std::fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    value.get(KEY)?.as_bool()
}

/// Persist `terminalEnabled`, preserving any other keys already in the file.
fn save(path: &Path, on: bool) -> std::io::Result<()> {
    let mut root = std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    root.insert(KEY.to_string(), serde_json::Value::Bool(on));
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_string_pretty(&serde_json::Value::Object(root))
        .unwrap_or_else(|_| "{}".to_string());
    std::fs::write(path, format!("{body}\n"))
}

static GATE: OnceLock<Gate> = OnceLock::new();

/// Initialise the process-global gate from the env var and an optional data dir.
/// Idempotent: a second call is ignored (first wins).
pub fn init(data_dir: Option<&Path>) {
    let env_raw = std::env::var("NANO_CONSOLE_TERMINAL").ok();
    let path = data_dir.map(|d| d.join(SETTINGS_FILE));
    let persisted = path.as_deref().and_then(load);
    let (enabled, source, locked_off) = resolve(env_state(env_raw.as_deref()), persisted);
    let _ = GATE.set(Gate {
        enabled: AtomicBool::new(enabled),
        locked_off,
        source: std::sync::Mutex::new(source),
        path,
    });
}

/// Convenience boot entry point: resolves the data dir from `NANOBPMN_DATA_DIR`.
pub fn init_from_env() {
    let data_dir = std::env::var("NANOBPMN_DATA_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from);
    init(data_dir.as_deref());
}

/// The live gate, or `None` before [`init`] (treated as default-off).
pub fn gate() -> Option<&'static Gate> {
    GATE.get()
}

/// JSON status for the console. `local` is whether *this caller* is on the
/// machine (loopback) and could therefore actually use the terminal.
pub fn status_json(local: bool) -> serde_json::Value {
    let g = gate();
    serde_json::json!({
        "enabled": g.map(Gate::effective).unwrap_or(false),
        "locked": g.map(Gate::locked_off).unwrap_or(false),
        "local": local,
        "source": g.map(|g| g.source().as_str()).unwrap_or("default"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_state_spellings() {
        for s in ["1", "true", "TRUE", "yes", "On", "  on  "] {
            assert_eq!(env_state(Some(s)), EnvState::ExplicitOn, "{s:?}");
        }
        for s in ["0", "false", "NO", "off", "  off "] {
            assert_eq!(env_state(Some(s)), EnvState::ExplicitOff, "{s:?}");
        }
        for s in ["", " ", "2", "enabled", "maybe"] {
            assert_eq!(env_state(Some(s)), EnvState::Unset, "{s:?}");
        }
        assert_eq!(env_state(None), EnvState::Unset);
    }

    #[test]
    fn env_explicit_off_hard_locks_regardless_of_persisted() {
        // The whole point: an operator who disabled it via env cannot be
        // overridden by a stale/persisted console value.
        for persisted in [None, Some(true), Some(false)] {
            let (enabled, source, locked) = resolve(EnvState::ExplicitOff, persisted);
            assert!(!enabled);
            assert!(locked);
            assert_eq!(source, Source::EnvLocked);
        }
    }

    #[test]
    fn console_governs_when_not_locked() {
        // Unset env: default off, console can turn it on.
        assert_eq!(
            resolve(EnvState::Unset, None),
            (false, Source::Default, false)
        );
        assert_eq!(
            resolve(EnvState::Unset, Some(true)),
            (true, Source::Console, false)
        );
        assert_eq!(
            resolve(EnvState::Unset, Some(false)),
            (false, Source::Console, false)
        );
    }

    #[test]
    fn env_explicit_on_seeds_default_but_console_wins_once_set() {
        // Back-compat: NANO_CONSOLE_TERMINAL=1 with no persisted value → on.
        assert_eq!(
            resolve(EnvState::ExplicitOn, None),
            (true, Source::EnvDefault, false)
        );
        // Once toggled off in the console, that persists over the env default.
        assert_eq!(
            resolve(EnvState::ExplicitOn, Some(false)),
            (false, Source::Console, false)
        );
    }

    #[test]
    fn persistence_round_trips_and_preserves_other_keys() {
        let dir = std::env::temp_dir().join(format!("nano-term-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(SETTINGS_FILE);
        // Pre-seed an unrelated key to prove save() preserves it.
        std::fs::write(&path, r#"{"theme":"dark"}"#).unwrap();

        assert_eq!(load(&path), None); // no terminal key yet
        save(&path, true).unwrap();
        assert_eq!(load(&path), Some(true));

        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("\"theme\""), "other keys must survive: {raw}");

        save(&path, false).unwrap();
        assert_eq!(load(&path), Some(false));

        std::fs::remove_dir_all(&dir).ok();
    }
}
