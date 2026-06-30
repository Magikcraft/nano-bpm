//! **Workspaces** — the consultant's folder structure of customer/deployment engagements.
//!
//! ProcessOS started single-tenant: one production `target` engine, one `own`
//! engine, one flat pile of experiments. A consultant, though, works *many*
//! workspaces (one per customer/deployment), each with *many* processes to optimize
//! — and not always against a live engine: often the material is a captured dataset
//! of traces handed over for analysis. This module gives ProcessOS that shape:
//!
//! ```text
//! <root>/                          PROCESSOS_WORKSPACES_DIR
//!   <workspace-slug>/
//!     workspace.json               { displayName, notes? }
//!     <process-slug>/
//!       process.json               { displayName, targetUrl?, dataset?, objective?, notes? }
//!       model.bpmn                  optional process model (BPMN XML), rendered in the console
//!       traces/                     optional captured trace dataset (instance JSON)
//! ```
//!
//! A **process** binds to a [`TraceSource`]: a live customer/deployment Nano
//! (`targetUrl`) **or** a loaded trace dataset (`dataset` path, defaulting to a
//! `traces/` folder beside `process.json`). Everything downstream — Insights today,
//! cockpit/prompts/pilot per-process later — operates *within a selected process*.
//!
//! The tree is **discovered by scanning** (so a consultant can curate folders by
//! hand and have them appear) **and** mutable via the API (create workspaces /
//! processes). Persistence mirrors the dependency-light, path-traversal-safe pattern
//! used by [`crate::conversation`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::contracts::NanoClient;
use crate::dataset::{DatasetSource, TraceSource};

/// On-disk workspace metadata (`workspace.json`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceConfig {
    #[serde(default)]
    pub display_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

/// On-disk process metadata (`process.json`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessConfig {
    #[serde(default)]
    pub display_name: String,
    /// A live workspace/deployment Nano base URL (the analysis target). When set, the
    /// process reads live; when unset, it falls back to a dataset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_url: Option<String>,
    /// A captured trace dataset directory (absolute, or relative to `process.json`).
    /// When unset, a `traces/` folder beside `process.json` is used if it exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset: Option<String>,
    /// Free-form optimization objective for this process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

/// A workspace plus its slug (the directory name).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Workspace {
    pub slug: String,
    #[serde(flatten)]
    pub config: WorkspaceConfig,
    pub process_count: usize,
}

/// A process plus its slug and a resolved description of how it is bound.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Process {
    pub slug: String,
    #[serde(flatten)]
    pub config: ProcessConfig,
    /// `live` (targetUrl set), `dataset` (a usable trace folder), or `unbound`.
    pub binding: String,
    /// For a dataset binding, the resolved dataset directory; otherwise `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_path: Option<String>,
    /// Whether a `model.bpmn` sits beside `process.json` (rendered in the console).
    pub has_model: bool,
}

/// The workspace root. Cheap to clone (just a `PathBuf`); all reads scan the disk so
/// hand-curated folders show up without a restart.
#[derive(Clone)]
pub struct WorkspaceCatalog {
    root: PathBuf,
}

impl WorkspaceCatalog {
    /// Open (creating if needed) a workspace rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        if let Err(e) = std::fs::create_dir_all(&root) {
            tracing::warn!(root = %root.display(), error = %e, "workspace: root create failed");
        }
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    // --- workspaces --------------------------------------------------------

    /// List all customers (directories under the root holding a `workspace.json`,
    /// or any directory — a hand-made folder without metadata still appears).
    pub fn list_workspaces(&self) -> Vec<Workspace> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return out;
        };
        for entry in entries.flatten() {
            if !entry.path().is_dir() {
                continue;
            }
            let Some(slug) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if !is_safe_slug(&slug) {
                continue;
            }
            if let Some(c) = self.get_workspace(&slug) {
                out.push(c);
            }
        }
        out.sort_by(|a, b| a.slug.cmp(&b.slug));
        out
    }

    pub fn get_workspace(&self, slug: &str) -> Option<Workspace> {
        let dir = self.workspace_dir(slug)?;
        if !dir.is_dir() {
            return None;
        }
        let config = read_json(&dir.join("workspace.json")).unwrap_or_else(|| WorkspaceConfig {
            display_name: slug.to_string(),
            notes: None,
        });
        Some(Workspace {
            slug: slug.to_string(),
            process_count: self.list_processes(slug).len(),
            config,
        })
    }

    /// Create a workspace from a display name (slug derived from it). Idempotent: an
    /// existing slug has its metadata refreshed rather than erroring.
    pub fn create_workspace(
        &self,
        display_name: &str,
        notes: Option<String>,
    ) -> Result<Workspace, String> {
        let slug = slugify(display_name);
        if slug.is_empty() {
            return Err("workspace name produced an empty slug".into());
        }
        let dir = self.root.join(&slug);
        std::fs::create_dir_all(&dir).map_err(|e| format!("create workspace dir: {e}"))?;
        let config = WorkspaceConfig {
            display_name: display_name.trim().to_string(),
            notes,
        };
        write_json(&dir.join("workspace.json"), &config)?;
        self.get_workspace(&slug)
            .ok_or_else(|| "workspace vanished after create".into())
    }

    // --- processes ---------------------------------------------------------

    pub fn list_processes(&self, workspace: &str) -> Vec<Process> {
        let mut out = Vec::new();
        let Some(cdir) = self.workspace_dir(workspace) else {
            return out;
        };
        let Ok(entries) = std::fs::read_dir(&cdir) else {
            return out;
        };
        for entry in entries.flatten() {
            if !entry.path().is_dir() {
                continue;
            }
            let Some(slug) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if !is_safe_slug(&slug) {
                continue;
            }
            if let Some(p) = self.get_process(workspace, &slug) {
                out.push(p);
            }
        }
        out.sort_by(|a, b| a.slug.cmp(&b.slug));
        out
    }

    pub fn get_process(&self, workspace: &str, process: &str) -> Option<Process> {
        let dir = self.process_dir(workspace, process)?;
        if !dir.is_dir() {
            return None;
        }
        let config: ProcessConfig =
            read_json(&dir.join("process.json")).unwrap_or_else(|| ProcessConfig {
                display_name: process.to_string(),
                ..Default::default()
            });
        let (binding, dataset_path) = self.binding_of(&dir, &config);
        Some(Process {
            slug: process.to_string(),
            config,
            binding,
            dataset_path,
            has_model: dir.join("model.bpmn").is_file(),
        })
    }

    /// Create (or refresh) a process under a workspace.
    pub fn create_process(
        &self,
        workspace: &str,
        display_name: &str,
        config: ProcessConfig,
    ) -> Result<Process, String> {
        let cdir = self
            .workspace_dir(workspace)
            .ok_or_else(|| "invalid workspace slug".to_string())?;
        if !cdir.is_dir() {
            return Err(format!("no such workspace: {workspace}"));
        }
        let slug = slugify(display_name);
        if slug.is_empty() {
            return Err("process name produced an empty slug".into());
        }
        let dir = cdir.join(&slug);
        std::fs::create_dir_all(&dir).map_err(|e| format!("create process dir: {e}"))?;
        let mut config = config;
        if config.display_name.trim().is_empty() {
            config.display_name = display_name.trim().to_string();
        }
        write_json(&dir.join("process.json"), &config)?;
        self.get_process(workspace, &slug)
            .ok_or_else(|| "process vanished after create".into())
    }

    /// Replace a process's configuration (binding, objective, notes).
    pub fn update_process(
        &self,
        workspace: &str,
        process: &str,
        config: ProcessConfig,
    ) -> Result<Process, String> {
        let dir = self
            .process_dir(workspace, process)
            .ok_or_else(|| "invalid slug".to_string())?;
        if !dir.is_dir() {
            return Err(format!("no such process: {workspace}/{process}"));
        }
        write_json(&dir.join("process.json"), &config)?;
        self.get_process(workspace, process)
            .ok_or_else(|| "process vanished after update".into())
    }

    /// Resolve the [`TraceSource`] a process reads from: a live gateway when
    /// `targetUrl` is set, otherwise a dataset loaded from disk.
    pub fn resolve_source(&self, workspace: &str, process: &str) -> Result<TraceSource, String> {
        let p = self
            .get_process(workspace, process)
            .ok_or_else(|| format!("no such process: {workspace}/{process}"))?;
        if let Some(url) = p.config.target_url.as_deref().filter(|s| !s.is_empty()) {
            return Ok(TraceSource::Live(NanoClient::new(url)));
        }
        if let Some(path) = p.dataset_path {
            let ds = DatasetSource::open(&path)?;
            return Ok(TraceSource::Dataset(Arc::new(ds)));
        }
        Err(format!(
            "process {workspace}/{process} is unbound (no targetUrl and no trace dataset)"
        ))
    }

    // --- process model (BPMN) ----------------------------------------------

    /// Read the process's `model.bpmn` (BPMN XML), if present.
    pub fn read_model(&self, workspace: &str, process: &str) -> Option<String> {
        let dir = self.process_dir(workspace, process)?;
        std::fs::read_to_string(dir.join("model.bpmn")).ok()
    }

    /// Write (or replace) the process's `model.bpmn`.
    pub fn write_model(&self, workspace: &str, process: &str, xml: &str) -> Result<(), String> {
        let dir = self
            .process_dir(workspace, process)
            .ok_or_else(|| "invalid slug".to_string())?;
        if !dir.is_dir() {
            return Err(format!("no such process: {workspace}/{process}"));
        }
        std::fs::write(dir.join("model.bpmn"), xml).map_err(|e| format!("write model.bpmn: {e}"))
    }

    /// The `traces/` dataset directory beside a process's `process.json` (created on
    /// demand), used when loading captured traces into a process.
    pub fn process_traces_dir(&self, workspace: &str, process: &str) -> Result<PathBuf, String> {
        let dir = self
            .process_dir(workspace, process)
            .ok_or_else(|| "invalid slug".to_string())?;
        if !dir.is_dir() {
            return Err(format!("no such process: {workspace}/{process}"));
        }
        let traces = dir.join("traces");
        std::fs::create_dir_all(&traces).map_err(|e| format!("create traces dir: {e}"))?;
        Ok(traces)
    }

    /// Classify how a process is bound and resolve its dataset directory.
    fn binding_of(&self, dir: &Path, config: &ProcessConfig) -> (String, Option<String>) {
        if config.target_url.as_deref().is_some_and(|s| !s.is_empty()) {
            return ("live".into(), None);
        }
        // Explicit dataset path (absolute or relative to process.json), else a
        // `traces/` folder beside it.
        let candidate = match config.dataset.as_deref().filter(|s| !s.is_empty()) {
            Some(d) => {
                let pd = Path::new(d);
                if pd.is_absolute() {
                    pd.to_path_buf()
                } else {
                    dir.join(pd)
                }
            }
            None => dir.join("traces"),
        };
        if candidate.is_dir() {
            (
                "dataset".into(),
                Some(candidate.to_string_lossy().to_string()),
            )
        } else {
            ("unbound".into(), None)
        }
    }

    fn workspace_dir(&self, slug: &str) -> Option<PathBuf> {
        is_safe_slug(slug).then(|| self.root.join(slug))
    }

    fn process_dir(&self, workspace: &str, process: &str) -> Option<PathBuf> {
        if !is_safe_slug(workspace) || !is_safe_slug(process) {
            return None;
        }
        Some(self.root.join(workspace).join(process))
    }
}

/// A slug is a single path segment of `[a-z0-9-_]` — it can never traverse out of
/// the workspace root.
fn is_safe_slug(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

/// Derive a filesystem-safe slug from a display name.
fn slugify(name: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for c in name.trim().chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            prev_dash = false;
        } else if !prev_dash && !out.is_empty() {
            out.push('-');
            prev_dash = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    out.truncate(128);
    out
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|e| format!("serialize: {e}"))?;
    std::fs::write(path, bytes).map_err(|e| format!("write {}: {e}", path.display()))
}

/// Resolve the workspaces root from `PROCESSOS_WORKSPACES_DIR`, defaulting to a
/// `workspaces/` folder under the data dir.
pub fn root_from_env(data_dir: &Path) -> PathBuf {
    std::env::var("PROCESSOS_WORKSPACES_DIR")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| data_dir.join("workspaces"))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn tmp() -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!(
            "processos-ws-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            n
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn slugify_is_safe_and_tidy() {
        assert_eq!(slugify("Acme Corp."), "acme-corp");
        assert_eq!(slugify("  Order  Fulfilment!! "), "order-fulfilment");
        assert_eq!(slugify("../../etc"), "etc");
        assert!(slugify("***").is_empty());
        assert!(is_safe_slug(&slugify("Acme Corp.")));
        assert!(!is_safe_slug("../escape"));
        assert!(!is_safe_slug("Has Space"));
    }

    #[test]
    fn create_list_and_get_round_trip() {
        let root = tmp();
        let ws = WorkspaceCatalog::open(&root);

        let c = ws
            .create_workspace("Acme Corp", Some("priority account".into()))
            .unwrap();
        assert_eq!(c.slug, "acme-corp");
        assert_eq!(c.config.display_name, "Acme Corp");
        assert_eq!(c.process_count, 0);

        let p = ws
            .create_process(
                "acme-corp",
                "Order Fulfilment",
                ProcessConfig {
                    objective: Some("cut p99".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(p.slug, "order-fulfilment");
        assert_eq!(p.binding, "unbound");

        // appears in listings, count reflects the new process
        let customers = ws.list_workspaces();
        assert_eq!(customers.len(), 1);
        assert_eq!(customers[0].process_count, 1);
        assert_eq!(ws.list_processes("acme-corp").len(), 1);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn live_binding_resolves_to_a_nano_client() {
        let root = tmp();
        let ws = WorkspaceCatalog::open(&root);
        ws.create_workspace("C", None).unwrap();
        ws.create_process(
            "c",
            "P",
            ProcessConfig {
                target_url: Some("http://localhost:8080".into()),
                ..Default::default()
            },
        )
        .unwrap();

        let p = ws.get_process("c", "p").unwrap();
        assert_eq!(p.binding, "live");
        let src = ws.resolve_source("c", "p").unwrap();
        assert!(matches!(src, TraceSource::Live(_)));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn dataset_binding_resolves_from_a_traces_folder() {
        let root = tmp();
        let ws = WorkspaceCatalog::open(&root);
        ws.create_workspace("C", None).unwrap();
        let p = ws
            .create_process("c", "P", ProcessConfig::default())
            .unwrap();
        assert_eq!(p.binding, "unbound");

        // drop a traces/ dataset beside process.json
        let traces = root.join("c").join("p").join("traces");
        std::fs::create_dir_all(&traces).unwrap();
        std::fs::write(
            traces.join("1.json"),
            r#"{"instanceKey":"1","processId":"order","outcome":"completed","startedAt":1,"elements":[],"incidents":[]}"#,
        )
        .unwrap();

        let p = ws.get_process("c", "p").unwrap();
        assert_eq!(p.binding, "dataset");
        assert!(p.dataset_path.is_some());

        let src = ws.resolve_source("c", "p").unwrap();
        assert!(matches!(src, TraceSource::Dataset(_)));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn unsafe_slugs_never_escape_the_root() {
        let root = tmp();
        let ws = WorkspaceCatalog::open(&root);
        assert!(ws.workspace_dir("../escape").is_none());
        assert!(ws.process_dir("ok", "../escape").is_none());
        assert!(ws.get_workspace("../escape").is_none());
        assert!(ws.resolve_source("../x", "../y").is_err());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn hand_made_folder_without_metadata_still_appears() {
        let root = tmp();
        std::fs::create_dir_all(root.join("manual-workspace").join("manual-process")).unwrap();
        let ws = WorkspaceCatalog::open(&root);
        let customers = ws.list_workspaces();
        assert_eq!(customers.len(), 1);
        assert_eq!(customers[0].slug, "manual-workspace");
        // display name falls back to the slug
        assert_eq!(customers[0].config.display_name, "manual-workspace");
        assert_eq!(ws.list_processes("manual-workspace").len(), 1);
        std::fs::remove_dir_all(&root).ok();
    }
}
