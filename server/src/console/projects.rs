//! RAD **project** model for the console — the unit of authoring in the Nano
//! "Rapid Application Development" environment.
//!
//! A project is a **self-contained directory** under the projects root that is
//! itself a runnable Deno application:
//!
//! ```text
//! <projectsRoot>/<project>/
//!   nanobpm.project.json      project config (name, deploy target, platforms)
//!   deno.json                 import map (@nanobpm/worker, @lib/) + start task
//!   main.ts                   entrypoint: deploys processes + starts workers
//!   resources/
//!     processes/  *.bpmn       deployed to the engine on run
//!     decisions/  *.dmn        authored + bundled (engine does not execute DMN)
//!     forms/      *.form       authored + bundled (engine does not execute forms)
//!   workers/<name>/{worker.ts, deno.json}
//!   lib/                       shared TS/JS importable via @lib/
//!   .nanobpm/worker-sdk.ts     embedded Deno worker SDK (materialised on disk)
//!   .deno-cache/               DENO_DIR for dependency caching
//! ```
//!
//! "Run" spawns `deno run main.ts`; "compile" runs `deno compile` (optionally
//! cross-compiling for selected targets); "export" zips the whole project. All
//! of this is feature-gated behind `console` and never touches the engine data
//! dir.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, Notify, broadcast};

use super::{worker_export, workers, workspace};

const WORKER_SDK_TS: &str = include_str!("worker_sdk.ts");
const METRIC_PREFIX: &str = "@@NBPM_METRIC@@";
const STATUS_PREFIX: &str = "@@NBPM_STATUS@@";
const LOG_RING_CAP: usize = 1000;

/// The config file name at the root of every project.
pub const CONFIG_FILE: &str = "nanobpm.project.json";

/// Deno `--target` triples we support for cross-compilation.
pub const PLATFORMS: &[&str] = &[
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-pc-windows-msvc",
];

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Filesystem model
// ---------------------------------------------------------------------------

/// Root holding every project directory. `NANOBPMN_PROJECTS_DIR` overrides the
/// default `<workspace>/projects`.
pub fn projects_root() -> PathBuf {
    match std::env::var("NANOBPMN_PROJECTS_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => workspace::workspace_dir().join("projects"),
    }
}

/// Ensures the projects root exists and returns it.
pub fn ensure_projects_root() -> std::io::Result<PathBuf> {
    let dir = projects_root();
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

/// The directory for project `name`, or `None` when the name is unsafe.
pub fn project_dir(name: &str) -> Option<PathBuf> {
    workspace::is_safe_name(name).then(|| projects_root().join(name))
}

/// Resolves a project-relative path (which may contain `/` separators) to an
/// absolute path **guaranteed to stay inside the project**. Rejects absolute
/// paths, `..` components, and empty segments.
pub fn safe_project_path(name: &str, rel: &str) -> Option<PathBuf> {
    let base = project_dir(name)?;
    let rel = rel.trim_start_matches('/');
    if rel.is_empty() {
        return Some(base);
    }
    let mut out = base.clone();
    for seg in rel.split('/') {
        if seg.is_empty() || seg == "." || seg == ".." || seg.len() > 128 {
            return None;
        }
        // Keep names tidy and traversal-safe (the worker/lib rule).
        if !seg
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        {
            return None;
        }
        out.push(seg);
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// Project config (nanobpm.project.json)
// ---------------------------------------------------------------------------

/// Persisted, user-editable project configuration.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectConfig {
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// Gateway base URL the app deploys to and dials the command stream on. The
    /// REST API lives at `<deployTarget>/v2`. Default: `http://localhost:8080`.
    #[serde(default = "default_deploy_target")]
    pub deploy_target: String,
    /// The entrypoint module run on "Run"/"Compile". Default `main.ts`.
    #[serde(default = "default_main")]
    pub main: String,
    /// Cross-compilation targets selected for export (Deno `--target` triples).
    #[serde(default)]
    pub platforms: Vec<String>,
    #[serde(default)]
    pub created_ms: u64,
    #[serde(default)]
    pub updated_ms: u64,
}

fn default_deploy_target() -> String {
    "http://localhost:8080".to_string()
}

fn default_main() -> String {
    "main.ts".to_string()
}

impl ProjectConfig {
    fn new(name: &str, description: &str) -> Self {
        let ts = now_ms();
        ProjectConfig {
            name: name.to_string(),
            description: description.to_string(),
            deploy_target: default_deploy_target(),
            main: default_main(),
            platforms: vec![host_target().to_string()],
            created_ms: ts,
            updated_ms: ts,
        }
    }
}

/// Best-effort guess at the host's Deno target triple, used as the default
/// selected platform for a new project.
fn host_target() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => "aarch64-apple-darwin",
        ("macos", _) => "x86_64-apple-darwin",
        ("windows", _) => "x86_64-pc-windows-msvc",
        ("linux", "aarch64") => "aarch64-unknown-linux-gnu",
        _ => "x86_64-unknown-linux-gnu",
    }
}

/// Reads `<project>/nanobpm.project.json`, falling back to a minimal default
/// when the file is missing or malformed (so a hand-created dir still opens).
pub fn read_config(name: &str) -> Option<ProjectConfig> {
    let dir = project_dir(name)?;
    if !dir.is_dir() {
        return None;
    }
    let path = dir.join(CONFIG_FILE);
    let cfg = std::fs::read_to_string(&path)
        .ok()
        .and_then(|t| serde_json::from_str::<ProjectConfig>(&t).ok())
        .unwrap_or_else(|| ProjectConfig::new(name, ""));
    Some(ProjectConfig {
        name: name.to_string(),
        ..cfg
    })
}

/// Writes the project config back to disk (pretty-printed).
pub fn write_config(name: &str, cfg: &ProjectConfig) -> std::io::Result<()> {
    let dir = project_dir(name).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid project name")
    })?;
    std::fs::create_dir_all(&dir)?;
    let json = serde_json::to_string_pretty(cfg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(dir.join(CONFIG_FILE), format!("{json}\n"))
}

// ---------------------------------------------------------------------------
// Scaffolding
// ---------------------------------------------------------------------------

/// Default project `deno.json`: import map for the SDK + shared library, and a
/// `start` task that runs the entrypoint with the right permissions.
const PROJECT_DENO_JSON: &str = r#"{
  "imports": {
    "@nanobpm/worker": "./.nanobpm/worker-sdk.ts",
    "@lib/": "./lib/"
  },
  "tasks": {
    "start": "deno run --allow-net --allow-read --allow-write --allow-env main.ts"
  }
}
"#;

/// The generated entrypoint. It is runtime-dynamic: it discovers BPMN processes
/// and workers on disk, so dropping a file into the project is all it takes.
const MAIN_TS: &str = r#"// Generated entrypoint for your Nano application.
//
// On start it (1) deploys every BPMN process in resources/processes/ to the
// engine, then (2) starts every worker in workers/. Add your own startup logic
// at the bottom. You can edit this file freely.

const BASE_URL = (Deno.env.get("NANOBPMN_BASE_URL") ?? "http://localhost:8080").replace(/\/+$/, "");

async function deployProcesses(): Promise<void> {
  const dir = "resources/processes";
  let entries: Deno.DirEntry[] = [];
  try {
    for await (const e of Deno.readDir(dir)) entries.push(e);
  } catch {
    return; // no processes folder yet
  }
  const form = new FormData();
  let count = 0;
  for (const e of entries) {
    if (!e.isFile || !e.name.endsWith(".bpmn")) continue;
    const xml = await Deno.readTextFile(`${dir}/${e.name}`);
    form.append("resources", new Blob([xml], { type: "text/xml" }), e.name);
    count++;
  }
  if (count === 0) return;
  const res = await fetch(`${BASE_URL}/v2/deployments`, { method: "POST", body: form });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(`deployment failed: ${res.status} ${detail}`);
  }
  console.log(`deployed ${count} process(es) to ${BASE_URL}/v2`);
}

async function startWorkers(): Promise<void> {
  let names: string[] = [];
  try {
    for await (const e of Deno.readDir("workers")) {
      if (e.isDirectory) names.push(e.name);
    }
  } catch {
    return; // no workers folder yet
  }
  names.sort();
  for (const name of names) {
    try {
      Deno.env.set("NANOBPMN_BASE_URL", BASE_URL);
      Deno.env.set("NANOBPMN_WORKER_NAME", name);
      await import(`./workers/${name}/worker.ts`);
      console.log(`started worker: ${name}`);
    } catch (err) {
      console.error(`worker ${name} failed to start: ${err}`);
    }
  }
}

await deployProcesses();
await startWorkers();

// ---- Your application logic below ----
console.log("application running.");
"#;

fn readme_md(name: &str) -> String {
    format!(
        "# {name}\n\n\
A Nano BPM application created with the RAD environment.\n\n\
## Layout\n\n\
- `resources/processes/` — BPMN processes, deployed to the engine on run.\n\
- `resources/decisions/` — DMN decisions (authoring/bundling; the engine does not execute DMN).\n\
- `resources/forms/` — forms (authoring/bundling; the engine does not execute forms).\n\
- `workers/<name>/` — Deno job workers.\n\
- `lib/` — shared TS/JS, importable via `@lib/`.\n\
- `main.ts` — entrypoint that deploys processes and starts workers.\n\n\
## Run\n\n\
```sh\ndeno task start\n```\n"
    )
}

/// A starter BPMN process so a fresh project has something to deploy.
fn starter_process(name: &str) -> String {
    let pid = format!("{name}-process");
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="defs-{pid}" targetNamespace="http://nanobpm">
  <bpmn:process id="{pid}" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="task" />
    <bpmn:serviceTask id="task" name="Do work">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="do-work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="task" targetRef="end" />
    <bpmn:endEvent id="end" />
  </bpmn:process>
</bpmn:definitions>
"#
    )
}

const STARTER_WORKER_TS: &str = r#"import { defineWorker } from "@nanobpm/worker";

// Handles the "do-work" job type from the starter process. Return output
// variables to complete the job; throw or call job.fail(...) to fail it.
defineWorker({
  type: "do-work",
  maxParallelJobs: 10,
  async handle(job) {
    console.log(`handling job ${job.jobKey} for instance ${job.processInstanceKey}`);
    return { handledBy: "do-work" };
  },
});
"#;

const WORKER_DENO_JSON: &str = r#"{
  "imports": {
    "@nanobpm/worker": "../../.nanobpm/worker-sdk.ts",
    "@lib/": "../../lib/"
  }
}
"#;

/// Materialises the embedded worker SDK into `<project>/.nanobpm/worker-sdk.ts`,
/// overwriting any prior copy so project code imports the current version.
pub fn ensure_project_sdk(name: &str) -> std::io::Result<()> {
    let dir = project_dir(name).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid project name")
    })?;
    let nano = dir.join(".nanobpm");
    std::fs::create_dir_all(&nano)?;
    std::fs::write(nano.join("worker-sdk.ts"), WORKER_SDK_TS)
}

/// Scaffolds a brand-new project directory. Fails if it already exists.
pub fn create_project(name: &str, description: &str) -> Result<ProjectConfig, String> {
    let dir = project_dir(name).ok_or("invalid project name")?;
    if dir.exists() {
        return Err("a project with that name already exists".into());
    }
    let mk = |p: PathBuf| std::fs::create_dir_all(&p).map_err(|e| format!("create {p:?}: {e}"));
    mk(dir.join("resources").join("processes"))?;
    mk(dir.join("resources").join("decisions"))?;
    mk(dir.join("resources").join("forms"))?;
    mk(dir.join("lib"))?;
    mk(dir.join(".nanobpm"))?;
    let starter_worker = dir.join("workers").join("do-work");
    mk(starter_worker.clone())?;

    let w = |p: PathBuf, body: &str| std::fs::write(&p, body).map_err(|e| format!("write {p:?}: {e}"));
    w(dir.join("deno.json"), PROJECT_DENO_JSON)?;
    w(dir.join("main.ts"), MAIN_TS)?;
    w(dir.join("README.md"), &readme_md(name))?;
    w(dir.join(".nanobpm").join("worker-sdk.ts"), WORKER_SDK_TS)?;
    w(
        dir.join("resources").join("processes").join(format!("{name}.bpmn")),
        &starter_process(name),
    )?;
    w(starter_worker.join("worker.ts"), STARTER_WORKER_TS)?;
    w(starter_worker.join("deno.json"), WORKER_DENO_JSON)?;

    let cfg = ProjectConfig::new(name, description);
    write_config(name, &cfg).map_err(|e| format!("write config: {e}"))?;
    Ok(cfg)
}

/// Deletes a project directory and everything in it.
pub fn delete_project(name: &str) -> std::io::Result<()> {
    let dir = project_dir(name).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid project name")
    })?;
    std::fs::remove_dir_all(dir)
}

// ---------------------------------------------------------------------------
// Listing / tiles
// ---------------------------------------------------------------------------

/// Tile-level summary of a project for the manager grid.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectSummary {
    pub name: String,
    pub description: String,
    pub deploy_target: String,
    pub updated_ms: u64,
    pub processes: usize,
    pub decisions: usize,
    pub forms: usize,
    pub workers: usize,
    pub running: bool,
}

fn count_ext(dir: &Path, ext: &str) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .filter(|e| e.path().extension().map(|x| x == ext).unwrap_or(false))
                .count()
        })
        .unwrap_or(0)
}

fn count_dirs(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| rd.flatten().filter(|e| e.path().is_dir()).count())
        .unwrap_or(0)
}

/// Lists every project (each subdirectory of the projects root). `running` is
/// left `false` here; the handler fills it from the supervisor.
pub fn list_projects() -> std::io::Result<Vec<ProjectSummary>> {
    let root = ensure_projects_root()?;
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&root)?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.starts_with('.') || !workspace::is_safe_name(name) {
            continue;
        }
        let cfg = read_config(name).unwrap_or_else(|| ProjectConfig::new(name, ""));
        let res = path.join("resources");
        out.push(ProjectSummary {
            name: name.to_string(),
            description: cfg.description,
            deploy_target: cfg.deploy_target,
            updated_ms: cfg.updated_ms,
            processes: count_ext(&res.join("processes"), "bpmn"),
            decisions: count_ext(&res.join("decisions"), "dmn"),
            forms: count_ext(&res.join("forms"), "form"),
            workers: count_dirs(&path.join("workers")),
            running: false,
        });
    }
    out.sort_by(|a, b| b.updated_ms.cmp(&a.updated_ms).then(a.name.cmp(&b.name)));
    Ok(out)
}

// ---------------------------------------------------------------------------
// File tree
// ---------------------------------------------------------------------------

/// A node in the project file tree.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileNode {
    /// Display name (the last path segment).
    pub name: String,
    /// Project-relative path (`/`-separated).
    pub path: String,
    /// `"dir"` or `"file"`.
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<FileNode>>,
}

/// Hidden / generated entries never shown in the browser.
fn is_hidden(name: &str) -> bool {
    name.starts_with('.') || name == "node_modules"
}

fn read_tree(dir: &Path, rel: &str) -> Vec<FileNode> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map(|rd| rd.flatten().collect())
        .unwrap_or_default();
    entries.sort_by_key(|e| {
        let p = e.path();
        // Directories first, then alphabetical.
        (!p.is_dir(), e.file_name().to_string_lossy().to_lowercase())
    });
    let mut out = Vec::new();
    for entry in entries {
        let name = entry.file_name().to_string_lossy().to_string();
        if is_hidden(&name) {
            continue;
        }
        let child_rel = if rel.is_empty() {
            name.clone()
        } else {
            format!("{rel}/{name}")
        };
        let path = entry.path();
        if path.is_dir() {
            out.push(FileNode {
                children: Some(read_tree(&path, &child_rel)),
                name,
                path: child_rel,
                kind: "dir",
            });
        } else {
            out.push(FileNode {
                name,
                path: child_rel,
                kind: "file",
                children: None,
            });
        }
    }
    out
}

/// Builds the recursive file tree for a project (excluding hidden/generated
/// entries), or `None` when the project does not exist.
pub fn file_tree(name: &str) -> Option<Vec<FileNode>> {
    let dir = project_dir(name)?;
    if !dir.is_dir() {
        return None;
    }
    Some(read_tree(&dir, ""))
}

// ---------------------------------------------------------------------------
// Export — zip the whole project
// ---------------------------------------------------------------------------

/// Walks the project collecting `(zip-relative-path, bytes)` for every file,
/// skipping caches and the compiled `dist/` output unless asked.
fn collect_files(dir: &Path, prefix: &str, include_dist: bool, out: &mut Vec<(String, Vec<u8>)>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in rd.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name == ".deno-cache" || name == "node_modules" {
            continue;
        }
        if name == "dist" && !include_dist {
            continue;
        }
        let path = entry.path();
        let zip_path = format!("{prefix}/{name}");
        if path.is_dir() {
            collect_files(&path, &zip_path, include_dist, out);
        } else if let Ok(bytes) = std::fs::read(&path) {
            out.push((zip_path, bytes));
        }
    }
}

/// Zips an entire project into a downloadable, runnable archive (nested under
/// `<name>/`). Compiled binaries under `dist/` are included when `include_dist`.
pub fn export_zip(name: &str, include_dist: bool) -> Result<Vec<u8>, String> {
    let dir = project_dir(name).ok_or("invalid project name")?;
    if !dir.is_dir() {
        return Err("no such project".into());
    }
    // Refresh the SDK so the exported app imports the current version.
    let _ = ensure_project_sdk(name);
    let mut entries = Vec::new();
    collect_files(&dir, name, include_dist, &mut entries);
    if entries.is_empty() {
        return Err("project is empty".into());
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(worker_export::build_stored_zip(&entries))
}

/// The download filename for a project export.
pub fn export_filename(name: &str) -> String {
    format!("{name}.zip")
}

// ---------------------------------------------------------------------------
// Run / compile supervisor
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Phase {
    Stopped,
    Starting,
    Running,
    Crashed,
}

/// A single log line from a project's run/compile output.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogLine {
    pub ts_ms: u64,
    /// `out` (stdout), `err` (stderr), or `sys` (supervisor notes).
    pub stream: String,
    pub text: String,
}

/// Runtime view of one project's run state.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunStateDto {
    status: Phase,
    pid: Option<u32>,
    started_at_ms: Option<u64>,
    last_error: Option<String>,
    /// Whether a compile is currently in progress.
    compiling: bool,
}

struct ProjectInner {
    phase: Mutex<Phase>,
    last_error: Mutex<Option<String>>,
    started_at_ms: Mutex<Option<u64>>,
    pid: AtomicU32,
    desired_running: AtomicBool,
    compiling: AtomicBool,
    stop: Notify,
    logs_tx: broadcast::Sender<LogLine>,
    log_ring: Mutex<VecDeque<LogLine>>,
}

impl ProjectInner {
    fn new() -> Arc<Self> {
        let (logs_tx, _) = broadcast::channel(256);
        Arc::new(Self {
            phase: Mutex::new(Phase::Stopped),
            last_error: Mutex::new(None),
            started_at_ms: Mutex::new(None),
            pid: AtomicU32::new(0),
            desired_running: AtomicBool::new(false),
            compiling: AtomicBool::new(false),
            stop: Notify::new(),
            logs_tx,
            log_ring: Mutex::new(VecDeque::with_capacity(LOG_RING_CAP)),
        })
    }

    async fn push_log(&self, stream: &str, text: String) {
        let line = LogLine {
            ts_ms: now_ms(),
            stream: stream.to_string(),
            text,
        };
        {
            let mut ring = self.log_ring.lock().await;
            if ring.len() >= LOG_RING_CAP {
                ring.pop_front();
            }
            ring.push_back(line.clone());
        }
        let _ = self.logs_tx.send(line);
    }

    async fn dto(&self) -> RunStateDto {
        let pid = self.pid.load(Ordering::Relaxed);
        RunStateDto {
            status: *self.phase.lock().await,
            pid: (pid != 0).then_some(pid),
            started_at_ms: *self.started_at_ms.lock().await,
            last_error: self.last_error.lock().await.clone(),
            compiling: self.compiling.load(Ordering::Relaxed),
        }
    }
}

/// Process-global supervisor for project run/compile, keyed by project name.
pub struct ProjectSupervisor {
    projects: Mutex<HashMap<String, Arc<ProjectInner>>>,
}

static SUPERVISOR: OnceLock<ProjectSupervisor> = OnceLock::new();

pub fn supervisor() -> &'static ProjectSupervisor {
    SUPERVISOR.get_or_init(|| ProjectSupervisor {
        projects: Mutex::new(HashMap::new()),
    })
}

impl ProjectSupervisor {
    pub fn deno_available(&self) -> bool {
        workers::find_deno().is_some()
    }

    async fn entry(&self, name: &str) -> Arc<ProjectInner> {
        let mut map = self.projects.lock().await;
        map.entry(name.to_string())
            .or_insert_with(ProjectInner::new)
            .clone()
    }

    /// Run state for one project (defaults to stopped if unknown).
    pub async fn run_state(&self, name: &str) -> RunStateDto {
        self.entry(name).await.dto().await
    }

    pub async fn is_running(&self, name: &str) -> bool {
        let inner = self.entry(name).await;
        matches!(*inner.phase.lock().await, Phase::Starting | Phase::Running)
    }

    pub async fn log_history(&self, name: &str) -> Vec<LogLine> {
        let inner = self.entry(name).await;
        inner.log_ring.lock().await.iter().cloned().collect()
    }

    pub async fn subscribe(&self, name: &str) -> broadcast::Receiver<LogLine> {
        self.entry(name).await.logs_tx.subscribe()
    }

    /// Resolves the base URL the app should deploy to: the project's configured
    /// deploy target, defaulting to the live gateway when empty/localhost.
    fn base_url(cfg: &ProjectConfig) -> String {
        let t = cfg.deploy_target.trim().trim_end_matches('/');
        if t.is_empty() {
            format!("http://127.0.0.1:{}", workers::gateway_port())
        } else {
            t.to_string()
        }
    }

    /// Spawns `deno run main.ts` for a project. Idempotent if already running.
    pub async fn run(&self, name: &str) -> Result<(), String> {
        let dir = project_dir(name).ok_or("invalid project name")?;
        if !dir.is_dir() {
            return Err("no such project".into());
        }
        let cfg = read_config(name).ok_or("no such project")?;
        let deno = workers::find_deno().ok_or_else(|| {
            "Deno runtime not found. Install Deno (https://deno.com) or set NANOBPMN_DENO_BIN.".to_string()
        })?;

        let inner = self.entry(name).await;
        if matches!(*inner.phase.lock().await, Phase::Starting | Phase::Running) {
            return Ok(());
        }
        *inner.phase.lock().await = Phase::Starting;
        let _ = ensure_project_sdk(name);

        let entry = dir.join(&cfg.main);
        if !entry.is_file() {
            *inner.phase.lock().await = Phase::Stopped;
            return Err(format!("entrypoint {} not found", cfg.main));
        }
        let cache = dir.join(".deno-cache");
        let _ = std::fs::create_dir_all(&cache);
        let base_url = Self::base_url(&cfg);

        let mut cmd = Command::new(&deno);
        cmd.current_dir(&dir)
            .arg("run")
            .arg("--no-prompt")
            .arg("--allow-net")
            .arg(format!("--allow-read={}", dir.display()))
            .arg(format!("--allow-write={}", cache.display()))
            .arg("--allow-env")
            .arg(&cfg.main)
            .env("DENO_DIR", &cache)
            .env("NO_COLOR", "1")
            .env("NANOBPMN_BASE_URL", &base_url)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                *inner.phase.lock().await = Phase::Stopped;
                return Err(format!("failed to spawn deno: {e}"));
            }
        };
        let pid = child.id().unwrap_or(0);
        inner.pid.store(pid, Ordering::Relaxed);
        inner.desired_running.store(true, Ordering::Relaxed);
        *inner.phase.lock().await = Phase::Running;
        *inner.started_at_ms.lock().await = Some(now_ms());
        *inner.last_error.lock().await = None;
        inner
            .push_log("sys", format!("running {} (pid {pid}) -> {base_url}", cfg.main))
            .await;

        if let Some(stdout) = child.stdout.take() {
            let inner = inner.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if line.starts_with(METRIC_PREFIX) {
                        continue; // per-worker metric telemetry — not shown
                    } else if let Some(rest) = line.strip_prefix(STATUS_PREFIX) {
                        let msg = serde_json::from_str::<serde_json::Value>(rest)
                            .ok()
                            .map(|v| {
                                format!(
                                    "{}: {}",
                                    v.get("state").and_then(|s| s.as_str()).unwrap_or("status"),
                                    v.get("message").and_then(|s| s.as_str()).unwrap_or("")
                                )
                            })
                            .unwrap_or_else(|| rest.to_string());
                        inner.push_log("sys", msg).await;
                    } else {
                        inner.push_log("out", line).await;
                    }
                }
            });
        }
        if let Some(stderr) = child.stderr.take() {
            let inner = inner.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    inner.push_log("err", line).await;
                }
            });
        }

        let inner = inner.clone();
        tokio::spawn(async move {
            let status = tokio::select! {
                _ = inner.stop.notified() => {
                    let _ = child.start_kill();
                    inner.push_log("sys", "stop requested; terminating".into()).await;
                    child.wait().await.ok()
                }
                st = child.wait() => st.ok(),
            };
            inner.pid.store(0, Ordering::Relaxed);
            let desired = inner.desired_running.load(Ordering::Relaxed);
            let code = status.and_then(|s| s.code());
            if desired {
                *inner.phase.lock().await = Phase::Crashed;
                inner.desired_running.store(false, Ordering::Relaxed);
                if code.unwrap_or(0) != 0 {
                    *inner.last_error.lock().await = Some(format!("exited with code {code:?}"));
                }
                inner
                    .push_log("sys", format!("application exited (code {code:?})"))
                    .await;
            } else {
                *inner.phase.lock().await = Phase::Stopped;
                inner.push_log("sys", "application stopped".into()).await;
            }
        });

        Ok(())
    }

    /// Stops a running project. No-op if not running.
    pub async fn stop(&self, name: &str) -> Result<(), String> {
        let inner = self.entry(name).await;
        if matches!(*inner.phase.lock().await, Phase::Stopped | Phase::Crashed) {
            return Ok(());
        }
        inner.desired_running.store(false, Ordering::Relaxed);
        inner.stop.notify_waiters();
        Ok(())
    }

    /// Compiles the project into `dist/` — once for the host, or once per
    /// requested target triple for cross-compilation. Streams progress to the
    /// project log channel and resolves when all builds finish.
    pub async fn compile(&self, name: &str, targets: &[String]) -> Result<Vec<String>, String> {
        let dir = project_dir(name).ok_or("invalid project name")?;
        if !dir.is_dir() {
            return Err("no such project".into());
        }
        let cfg = read_config(name).ok_or("no such project")?;
        let deno = workers::find_deno().ok_or_else(|| {
            "Deno runtime not found. Install Deno (https://deno.com) or set NANOBPMN_DENO_BIN.".to_string()
        })?;
        let inner = self.entry(name).await;
        if inner.compiling.swap(true, Ordering::Relaxed) {
            return Err("a compile is already in progress".into());
        }

        let dist = dir.join("dist");
        let _ = std::fs::create_dir_all(&dist);
        let cache = dir.join(".deno-cache");
        let _ = std::fs::create_dir_all(&cache);

        // Empty target list => compile for the host only.
        let mut effective: Vec<Option<String>> = Vec::new();
        if targets.is_empty() {
            effective.push(None);
        } else {
            let allowed: BTreeSet<&str> = PLATFORMS.iter().copied().collect();
            for t in targets {
                if !allowed.contains(t.as_str()) {
                    inner.compiling.store(false, Ordering::Relaxed);
                    return Err(format!("unsupported target: {t}"));
                }
                effective.push(Some(t.clone()));
            }
        }

        let mut produced = Vec::new();
        let mut failure: Option<String> = None;
        for target in &effective {
            let out_name = match target {
                Some(t) => format!("{name}-{t}"),
                None => name.to_string(),
            };
            let mut out_path = dist.join(&out_name);
            if matches!(target.as_deref(), Some(t) if t.contains("windows")) {
                out_path.set_extension("exe");
            }
            inner
                .push_log(
                    "sys",
                    match target {
                        Some(t) => format!("compiling for {t}..."),
                        None => "compiling for host...".to_string(),
                    },
                )
                .await;

            let mut cmd = Command::new(&deno);
            cmd.current_dir(&dir)
                .arg("compile")
                .arg("--allow-net")
                .arg("--allow-read")
                .arg("--allow-write")
                .arg("--allow-env")
                .arg("--no-prompt");
            if let Some(t) = target {
                cmd.arg("--target").arg(t);
            }
            cmd.arg("--output")
                .arg(&out_path)
                .arg(&cfg.main)
                .env("DENO_DIR", &cache)
                .env("NO_COLOR", "1")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());

            let child = cmd.spawn().map_err(|e| format!("failed to spawn deno: {e}"));
            let mut child = match child {
                Ok(c) => c,
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            };
            if let Some(stdout) = child.stdout.take() {
                let inner = inner.clone();
                tokio::spawn(async move {
                    let mut lines = BufReader::new(stdout).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        inner.push_log("out", line).await;
                    }
                });
            }
            if let Some(stderr) = child.stderr.take() {
                let inner = inner.clone();
                tokio::spawn(async move {
                    let mut lines = BufReader::new(stderr).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        inner.push_log("err", line).await;
                    }
                });
            }
            match child.wait().await {
                Ok(st) if st.success() => {
                    let rel = format!("dist/{}", out_path.file_name().unwrap().to_string_lossy());
                    inner.push_log("sys", format!("built {rel}")).await;
                    produced.push(rel);
                }
                Ok(st) => {
                    failure = Some(format!("deno compile failed (code {:?})", st.code()));
                    break;
                }
                Err(e) => {
                    failure = Some(format!("deno compile error: {e}"));
                    break;
                }
            }
        }

        inner.compiling.store(false, Ordering::Relaxed);
        match failure {
            Some(e) => {
                inner.push_log("sys", format!("compile failed: {e}")).await;
                Err(e)
            }
            None => {
                inner
                    .push_log("sys", format!("compile complete ({} artifact(s))", produced.len()))
                    .await;
                Ok(produced)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AOrd};
    use std::sync::{Mutex, MutexGuard, OnceLock};

    /// Serializes tests that mutate the process-global `NANOBPMN_PROJECTS_DIR`.
    fn lock() -> MutexGuard<'static, ()> {
        static L: OnceLock<Mutex<()>> = OnceLock::new();
        L.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn temp_root() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "nano-proj-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, AOrd::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        unsafe {
            std::env::set_var("NANOBPMN_PROJECTS_DIR", &p);
        }
        p
    }

    #[test]
    fn safe_project_path_rejects_traversal() {
        let _g = lock();
        let _root = temp_root();
        assert!(safe_project_path("app", "../etc/passwd").is_none());
        assert!(safe_project_path("app", "/abs").is_some()); // leading / stripped
        assert!(safe_project_path("app", "a/../b").is_none());
        assert!(safe_project_path("app", "resources/processes/x.bpmn").is_some());
        assert!(safe_project_path("../bad", "x").is_none());
    }

    #[test]
    fn create_scaffolds_a_runnable_project() {
        let _g = lock();
        let root = temp_root();
        let cfg = create_project("demo", "a demo").expect("create");
        assert_eq!(cfg.name, "demo");
        assert_eq!(cfg.deploy_target, "http://localhost:8080");
        let dir = root.join("demo");
        assert!(dir.join("main.ts").is_file());
        assert!(dir.join("deno.json").is_file());
        assert!(dir.join(CONFIG_FILE).is_file());
        assert!(dir.join(".nanobpm/worker-sdk.ts").is_file());
        assert!(dir.join("resources/processes/demo.bpmn").is_file());
        assert!(dir.join("workers/do-work/worker.ts").is_file());
        // Idempotency guard.
        assert!(create_project("demo", "").is_err());
    }

    #[test]
    fn lists_projects_with_counts() {
        let _g = lock();
        let _root = temp_root();
        create_project("alpha", "").unwrap();
        let list = list_projects().unwrap();
        let alpha = list.iter().find(|p| p.name == "alpha").unwrap();
        assert_eq!(alpha.processes, 1);
        assert_eq!(alpha.workers, 1);
    }

    #[test]
    fn file_tree_hides_dotdirs_and_nests() {
        let _g = lock();
        let _root = temp_root();
        create_project("tree", "").unwrap();
        let nodes = file_tree("tree").unwrap();
        assert!(nodes.iter().all(|n| !n.name.starts_with('.')));
        let resources = nodes.iter().find(|n| n.name == "resources").unwrap();
        assert_eq!(resources.kind, "dir");
        assert!(resources.children.is_some());
    }

    #[test]
    fn export_zip_bundles_the_project() {
        let _g = lock();
        let _root = temp_root();
        create_project("ziptest", "").unwrap();
        let zip = export_zip("ziptest", false).expect("zip");
        // Local file header signature.
        assert_eq!(&zip[0..4], &[0x50, 0x4b, 0x03, 0x04]);
        // Nested under the project name.
        let blob = String::from_utf8_lossy(&zip);
        assert!(blob.contains("ziptest/main.ts"));
    }

    #[test]
    fn config_roundtrips() {
        let _g = lock();
        let _root = temp_root();
        let mut cfg = create_project("cfg", "").unwrap();
        cfg.deploy_target = "http://example.test:9999".into();
        cfg.platforms = vec!["aarch64-apple-darwin".into()];
        write_config("cfg", &cfg).unwrap();
        let back = read_config("cfg").unwrap();
        assert_eq!(back.deploy_target, "http://example.test:9999");
        assert_eq!(back.platforms, vec!["aarch64-apple-darwin".to_string()]);
    }
}
