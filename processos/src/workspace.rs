//! **Workspaces** — the consultant's folder structure of customer engagements.
//!
//! ProcessOS started single-tenant: one production `target` engine, one `own`
//! engine, one flat pile of experiments. A consultant, though, works *many*
//! customers, each with *many* processes to optimize — and not always against a
//! live engine: often the material is a captured dataset of traces handed over for
//! analysis. This module gives ProcessOS that shape:
//!
//! ```text
//! <root>/                          PROCESSOS_WORKSPACES_DIR
//!   <customer-slug>/
//!     customer.json                { displayName, notes? }
//!     <process-slug>/
//!       process.json               { displayName, targetUrl?, dataset?, objective?, notes? }
//!       traces/                     optional captured trace dataset (instance JSON)
//! ```
//!
//! A **process** binds to a [`TraceSource`]: a live customer/deployment Nano
//! (`targetUrl`) **or** a loaded trace dataset (`dataset` path, defaulting to a
//! `traces/` folder beside `process.json`). Everything downstream — Insights today,
//! cockpit/prompts/pilot per-process later — operates *within a selected process*.
//!
//! The tree is **discovered by scanning** (so a consultant can curate folders by
//! hand and have them appear) **and** mutable via the API (create customers /
//! processes). Persistence mirrors the dependency-light, path-traversal-safe pattern
//! used by [`crate::conversation`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::contracts::NanoClient;
use crate::dataset::{DatasetSource, TraceSource};

/// On-disk customer metadata (`customer.json`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomerConfig {
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
    /// A live customer/deployment Nano base URL (the analysis target). When set, the
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

/// A customer plus its slug (the directory name).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Customer {
    pub slug: String,
    #[serde(flatten)]
    pub config: CustomerConfig,
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
}

/// The workspace root. Cheap to clone (just a `PathBuf`); all reads scan the disk so
/// hand-curated folders show up without a restart.
#[derive(Clone)]
pub struct Workspace {
    root: PathBuf,
}

impl Workspace {
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

    // --- customers ---------------------------------------------------------

    /// List all customers (directories under the root holding a `customer.json`,
    /// or any directory — a hand-made folder without metadata still appears).
    pub fn list_customers(&self) -> Vec<Customer> {
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
            if let Some(c) = self.get_customer(&slug) {
                out.push(c);
            }
        }
        out.sort_by(|a, b| a.slug.cmp(&b.slug));
        out
    }

    pub fn get_customer(&self, slug: &str) -> Option<Customer> {
        let dir = self.customer_dir(slug)?;
        if !dir.is_dir() {
            return None;
        }
        let config = read_json(&dir.join("customer.json")).unwrap_or_else(|| CustomerConfig {
            display_name: slug.to_string(),
            notes: None,
        });
        Some(Customer {
            slug: slug.to_string(),
            process_count: self.list_processes(slug).len(),
            config,
        })
    }

    /// Create a customer from a display name (slug derived from it). Idempotent: an
    /// existing slug has its metadata refreshed rather than erroring.
    pub fn create_customer(&self, display_name: &str, notes: Option<String>) -> Result<Customer, String> {
        let slug = slugify(display_name);
        if slug.is_empty() {
            return Err("customer name produced an empty slug".into());
        }
        let dir = self.root.join(&slug);
        std::fs::create_dir_all(&dir).map_err(|e| format!("create customer dir: {e}"))?;
        let config = CustomerConfig {
            display_name: display_name.trim().to_string(),
            notes,
        };
        write_json(&dir.join("customer.json"), &config)?;
        self.get_customer(&slug)
            .ok_or_else(|| "customer vanished after create".into())
    }

    // --- processes ---------------------------------------------------------

    pub fn list_processes(&self, customer: &str) -> Vec<Process> {
        let mut out = Vec::new();
        let Some(cdir) = self.customer_dir(customer) else {
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
            if let Some(p) = self.get_process(customer, &slug) {
                out.push(p);
            }
        }
        out.sort_by(|a, b| a.slug.cmp(&b.slug));
        out
    }

    pub fn get_process(&self, customer: &str, process: &str) -> Option<Process> {
        let dir = self.process_dir(customer, process)?;
        if !dir.is_dir() {
            return None;
        }
        let config: ProcessConfig = read_json(&dir.join("process.json")).unwrap_or_else(|| {
            ProcessConfig {
                display_name: process.to_string(),
                ..Default::default()
            }
        });
        let (binding, dataset_path) = self.binding_of(&dir, &config);
        Some(Process {
            slug: process.to_string(),
            config,
            binding,
            dataset_path,
        })
    }

    /// Create (or refresh) a process under a customer.
    pub fn create_process(
        &self,
        customer: &str,
        display_name: &str,
        config: ProcessConfig,
    ) -> Result<Process, String> {
        let cdir = self
            .customer_dir(customer)
            .ok_or_else(|| "invalid customer slug".to_string())?;
        if !cdir.is_dir() {
            return Err(format!("no such customer: {customer}"));
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
        self.get_process(customer, &slug)
            .ok_or_else(|| "process vanished after create".into())
    }

    /// Replace a process's configuration (binding, objective, notes).
    pub fn update_process(
        &self,
        customer: &str,
        process: &str,
        config: ProcessConfig,
    ) -> Result<Process, String> {
        let dir = self
            .process_dir(customer, process)
            .ok_or_else(|| "invalid slug".to_string())?;
        if !dir.is_dir() {
            return Err(format!("no such process: {customer}/{process}"));
        }
        write_json(&dir.join("process.json"), &config)?;
        self.get_process(customer, process)
            .ok_or_else(|| "process vanished after update".into())
    }

    /// Resolve the [`TraceSource`] a process reads from: a live gateway when
    /// `targetUrl` is set, otherwise a dataset loaded from disk.
    pub fn resolve_source(&self, customer: &str, process: &str) -> Result<TraceSource, String> {
        let p = self
            .get_process(customer, process)
            .ok_or_else(|| format!("no such process: {customer}/{process}"))?;
        if let Some(url) = p.config.target_url.as_deref().filter(|s| !s.is_empty()) {
            return Ok(TraceSource::Live(NanoClient::new(url)));
        }
        if let Some(path) = p.dataset_path {
            let ds = DatasetSource::open(&path)?;
            return Ok(TraceSource::Dataset(Arc::new(ds)));
        }
        Err(format!(
            "process {customer}/{process} is unbound (no targetUrl and no trace dataset)"
        ))
    }

    // --- internals ---------------------------------------------------------

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

    fn customer_dir(&self, slug: &str) -> Option<PathBuf> {
        is_safe_slug(slug).then(|| self.root.join(slug))
    }

    fn process_dir(&self, customer: &str, process: &str) -> Option<PathBuf> {
        if !is_safe_slug(customer) || !is_safe_slug(process) {
            return None;
        }
        Some(self.root.join(customer).join(process))
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
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

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
        let ws = Workspace::open(&root);

        let c = ws
            .create_customer("Acme Corp", Some("priority account".into()))
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
        let customers = ws.list_customers();
        assert_eq!(customers.len(), 1);
        assert_eq!(customers[0].process_count, 1);
        assert_eq!(ws.list_processes("acme-corp").len(), 1);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn live_binding_resolves_to_a_nano_client() {
        let root = tmp();
        let ws = Workspace::open(&root);
        ws.create_customer("C", None).unwrap();
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
        let ws = Workspace::open(&root);
        ws.create_customer("C", None).unwrap();
        let p = ws.create_process("c", "P", ProcessConfig::default()).unwrap();
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
        let ws = Workspace::open(&root);
        assert!(ws.customer_dir("../escape").is_none());
        assert!(ws.process_dir("ok", "../escape").is_none());
        assert!(ws.get_customer("../escape").is_none());
        assert!(ws.resolve_source("../x", "../y").is_err());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn hand_made_folder_without_metadata_still_appears() {
        let root = tmp();
        std::fs::create_dir_all(root.join("manual-customer").join("manual-process")).unwrap();
        let ws = Workspace::open(&root);
        let customers = ws.list_customers();
        assert_eq!(customers.len(), 1);
        assert_eq!(customers[0].slug, "manual-customer");
        // display name falls back to the slug
        assert_eq!(customers[0].config.display_name, "manual-customer");
        assert_eq!(ws.list_processes("manual-customer").len(), 1);
        std::fs::remove_dir_all(&root).ok();
    }
}
