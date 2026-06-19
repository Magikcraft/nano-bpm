//! Filesystem workspace for the console — the user's authoring **source of
//! truth** (BPMN models today; worker code later), deliberately kept **separate
//! from the engine's data dir** so that deleting cluster data (`NANOBPMN_DATA_DIR`)
//! leaves models and workers intact.
//!
//! Layout under the workspace root:
//! ```text
//! <workspace>/models/<name>.bpmn
//! ```
//! The root defaults to `./nanobpm-workspace` (relative to the server's cwd) and
//! is overridden by `NANOBPMN_WORKSPACE_DIR`. Unlike the engine data dir — which
//! forbids creating a deep path so a typo fails fast — the workspace is explicit
//! user content, so it is created on demand.

use std::path::PathBuf;
use std::time::UNIX_EPOCH;

/// Root of the console workspace. `NANOBPMN_WORKSPACE_DIR` overrides the default
/// `./nanobpm-workspace`.
pub fn workspace_dir() -> PathBuf {
    match std::env::var("NANOBPMN_WORKSPACE_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => PathBuf::from("nanobpm-workspace"),
    }
}

/// Directory holding `*.bpmn` model files.
pub fn models_dir() -> PathBuf {
    workspace_dir().join("models")
}

/// Ensures the models directory exists and returns it.
pub fn ensure_models_dir() -> std::io::Result<PathBuf> {
    let dir = models_dir();
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// Whether `name` is a safe, flat model identifier: it can never escape the
/// models directory (no path separators, no `..`) and stays a tidy filename.
pub fn is_safe_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name != "."
        && !name.contains("..")
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

/// The on-disk path for model `name` (`<models>/<name>.bpmn`), or `None` when
/// the name is unsafe.
pub fn model_path(name: &str) -> Option<PathBuf> {
    is_safe_name(name).then(|| models_dir().join(format!("{name}.bpmn")))
}

/// A model file's `(modified_ms, size_bytes)`, defaulting to `(0, 0)` when the
/// metadata or mtime is unavailable.
pub fn file_meta(path: &std::path::Path) -> (u64, u64) {
    let Ok(meta) = std::fs::metadata(path) else {
        return (0, 0);
    };
    let modified_ms = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    (modified_ms, meta.len())
}

/// Lists the `*.bpmn` model file stems in the workspace, ensuring the directory
/// exists first. Returns an empty list (not an error) when the directory is
/// freshly created and empty.
pub fn list_model_names() -> std::io::Result<Vec<String>> {
    let dir = ensure_models_dir()?;
    let mut names = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("bpmn")
            && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
            && is_safe_name(stem)
        {
            names.push(stem.to_string());
        }
    }
    names.sort();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_path_traversal_and_separators() {
        assert!(!is_safe_name(""));
        assert!(!is_safe_name(".."));
        assert!(!is_safe_name("../etc/passwd"));
        assert!(!is_safe_name("a/b"));
        assert!(!is_safe_name("a\\b"));
        assert!(!is_safe_name("foo..bar"));
        assert!(model_path("../secret").is_none());
    }

    #[test]
    fn accepts_tidy_names() {
        assert!(is_safe_name("order"));
        assert!(is_safe_name("order-v2"));
        assert!(is_safe_name("order_2.final"));
        let p = model_path("order").unwrap();
        assert!(p.ends_with("order.bpmn"));
    }
}
