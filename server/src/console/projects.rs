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
        "rust-throughput",
        "Throughput (Rust) — native pipelined falcon (where stream beats REST)",
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

fn default_lang() -> String {
    "deno".to_string()
}

fn default_app() -> String {
    "console".to_string()
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
// Rust throughput template (lang pack: rust) — native pipelined producer/worker
// ---------------------------------------------------------------------------

const RUST_CARGO_TOML: &str = r#"[package]
name = "throughput-rust"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "throughput-rust"
path = "src/main.rs"

[dependencies]
reqwest = { version = "0.12", default-features = false, features = ["json", "multipart"] }
tokio = { version = "1", features = ["full"] }
serde_json = "1"

[profile.release]
opt-level = 3
"#;

const RUST_THROUGHPUT_MAIN: &str = r#"// Throughput (Rust) — pipelined producer + concurrent drainer against the engine.
//
// Unlike the JS demos (single-socket, await-per-create), a native producer
// pipelines creates concurrently across pooled connections, while drainers
// activate and complete jobs concurrently so the process actually runs end to
// end. A live per-second line shows creates/s and completes/s as it works.
// cargo run --release.  Tunables (env): PROD_CONNS, WORKER_CONNS, DURATION_SECS.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let base = std::env::var("NANOBPMN_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into());
    let base = base.trim_end_matches('/').to_string();
    let pid = std::env::var("PID").unwrap_or_else(|_| "throughput-demo".into());
    let job_type = std::env::var("JOB_TYPE").unwrap_or_else(|_| "tick".into());
    let conns: usize = env("PROD_CONNS", 256);
    let workers: usize = env("WORKER_CONNS", 64);
    let secs: u64 = env("DURATION_SECS", 15) as u64;
    let client = reqwest::Client::builder().pool_max_idle_per_host(usize::MAX).build().unwrap();

    println!("deploying throughput.bpmn -> {base}");
    match deploy(&client, &base).await {
        Ok(true) => println!("deployed process '{pid}' (job type '{job_type}')"),
        Ok(false) => eprintln!("warning: deploy returned non-success — is the gateway at {base}?"),
        Err(e) => { eprintln!("deploy failed: {e}"); return; }
    }
    println!("running {secs}s with {conns} producer + {workers} drainer connections...");

    let created = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicU64::new(0));
    let create_url = format!("{base}/v2/process-instances");
    let activate_url = format!("{base}/v2/jobs/activation");
    let body = serde_json::json!({ "processDefinitionId": pid, "awaitCompletion": false }).to_string();
    let activate = serde_json::json!({ "type": job_type, "maxJobsToActivate": 100, "timeout": 30000 }).to_string();
    let t0 = Instant::now();
    let dur = Duration::from_secs(secs);
    let mut tasks = Vec::new();

    // Live progress: one line per second with the per-second deltas, so you can
    // watch the run rather than waiting for a single summary at the end.
    {
        let (created, done, failed) = (created.clone(), done.clone(), failed.clone());
        tasks.push(tokio::spawn(async move {
            let (mut pc, mut pd) = (0u64, 0u64);
            while t0.elapsed() < dur {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let (c, d, f) = (created.load(Ordering::Relaxed), done.load(Ordering::Relaxed), failed.load(Ordering::Relaxed));
                println!("t={:>2}s  created {c} (+{}/s)  completed {d} (+{}/s)  errors {f}", t0.elapsed().as_secs(), c - pc, d - pd);
                pc = c; pd = d;
            }
        }));
    }

    // Producers: pipeline awaitCompletion:false creates across pooled connections.
    for _ in 0..conns {
        let (c, u, b, n, fe) = (client.clone(), create_url.clone(), body.clone(), created.clone(), failed.clone());
        tasks.push(tokio::spawn(async move {
            while t0.elapsed() < dur {
                match c.post(&u).header("content-type", "application/json").body(b.clone()).send().await {
                    Ok(r) if r.status().is_success() => { n.fetch_add(1, Ordering::Relaxed); }
                    _ => { fe.fetch_add(1, Ordering::Relaxed); }
                }
            }
        }));
    }

    // Drainers: activate up to 100 jobs, then complete the batch concurrently —
    // sequential completion would bottleneck the drain far below the create rate.
    for _ in 0..workers {
        let (c, base, n, au, req) = (client.clone(), base.clone(), done.clone(), activate_url.clone(), activate.clone());
        tasks.push(tokio::spawn(async move {
            while t0.elapsed() < dur {
                let Ok(r) = c.post(&au).header("content-type", "application/json").body(req.clone()).send().await else { continue };
                let j: serde_json::Value = r.json().await.unwrap_or_default();
                let Some(jobs) = j.get("jobs").and_then(|v| v.as_array()) else { continue };
                let mut batch = Vec::new();
                for job in jobs {
                    if let Some(k) = job.get("jobKey").and_then(|v| v.as_str()) {
                        let (c, base, k) = (c.clone(), base.clone(), k.to_string());
                        batch.push(tokio::spawn(async move {
                            c.post(format!("{base}/v2/jobs/{k}/completion"))
                                .header("content-type", "application/json").body("{}").send().await
                                .map(|r| r.status().is_success()).unwrap_or(false)
                        }));
                    }
                }
                for b in batch { if let Ok(true) = b.await { n.fetch_add(1, Ordering::Relaxed); } }
            }
        }));
    }

    for t in tasks { let _ = t.await; }
    let (c, d, f) = (created.load(Ordering::Relaxed), done.load(Ordering::Relaxed), failed.load(Ordering::Relaxed));
    let s = secs.max(1);
    println!("=== {c} created (~{}/s), {d} completed (~{}/s), {f} errors over {secs}s ===", c / s, d / s);
}

fn env(k: &str, def: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(def)
}

async fn deploy(client: &reqwest::Client, base: &str) -> Result<bool, String> {
    let xml = include_str!("../resources/processes/throughput.bpmn");
    let form = reqwest::multipart::Form::new().part(
        "resources",
        reqwest::multipart::Part::text(xml).file_name("throughput.bpmn").mime_str("text/xml").map_err(|e| e.to_string())?,
    );
    let r = client.post(format!("{base}/v2/deployments")).multipart(form).send().await.map_err(|e| e.to_string())?;
    Ok(r.status().is_success())
}
"#;

fn rust_throughput_readme() -> String {
    "# Throughput (Rust)\n\n\
A native producer that pipelines creates across pooled connections while \
drainers activate and complete jobs concurrently, so the process runs end to \
end. A live per-second line streams creates/s and completes/s to this console \
as it runs. cargo runs it: press **Run** (needs the Rust toolchain installed; \
the IDE detects `cargo`).\n\n\
## Tunables (env)\n\n\
`PROD_CONNS` (producer connections, default 256), `WORKER_CONNS` (drainer \
connections, default 64), `DURATION_SECS` (default 15).\n\n\
## A/B (native, clean engine, async durability)\n\n\
| Metric | REST | Falcon |\n\
|---|---|---|\n\
| Peak instances/sec | ~20k | **~32k** |\n\
| Engine memory | high | **~4x lower** |\n\n\
In JS the SDK awaits one create at a time so REST wins; a native pipelining \
producer flips it. Swap the reqwest loop for the stream client for the headline.\n"
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
        "starter" | "throughput" | "throughput-stream" | "rust-throughput" | "gui-starter"
    );
    if !is_builtin_template && let Some((m, src)) = super::extensions::template_source(template) {
        mk(dir.clone())?;
        super::extensions::copy_tree(&src, &dir).map_err(|e| format!("copy pack template: {e}"))?;
        let cfg_lang = m
            .requires
            .first()
            .cloned()
            .unwrap_or_else(|| "deno".to_string());
        // Best-effort detection of the "main" entrypoint the console
        // surfaces in the workspace toolbar. Ordered from most specific
        // to least; falls back to main.ts to match the Deno default.
        let candidates: &[&str] = &[
            "src/main.rs",
            "pom.xml",
            "microservice/pom.xml",
            "app/pom.xml",
            "main.ts",
        ];
        let cfg_main = candidates
            .iter()
            .find(|c| dir.join(c).exists())
            .map(|c| c.to_string())
            .unwrap_or_else(|| "main.ts".to_string());
        let mut cfg = ProjectConfig::new(name, description);
        cfg.lang = cfg_lang;
        cfg.app = "console".to_string();
        cfg.main = cfg_main;
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

    let mut cfg_lang = "deno".to_string();
    let mut cfg_app = "console";
    let mut cfg_main = "main.ts".to_string();

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
    } else if template == "rust-throughput" {
        mk(dir.join("src"))?;
        w(dir.join("Cargo.toml"), RUST_CARGO_TOML)?;
        w(dir.join("src").join("main.rs"), RUST_THROUGHPUT_MAIN)?;
        w(dir.join("README.md"), &rust_throughput_readme())?;
        w(
            dir.join("resources")
                .join("processes")
                .join("throughput.bpmn"),
            DEMO_PROCESS_BPMN,
        )?;
        cfg_lang = "rust".to_string();
        cfg_main = "src/main.rs".to_string();
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
        // Polyglot: a non-Deno lang pack drives its own on-machine toolchain
        // (ADR 0007/0008). Gated by the trust store.
        if cfg.lang != "deno" {
            return self.run_toolchain(name, &cfg, &dir).await;
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

    /// Runs a non-Deno lang pack via its declared toolchain (ADR 0007/0008).
    /// Gated by the extension trust store; the toolchain binary must be on PATH.
    async fn run_toolchain(
        &self,
        name: &str,
        cfg: &ProjectConfig,
        dir: &Path,
    ) -> Result<(), String> {
        let pack = super::extensions::lang_pack(&cfg.lang)
            .ok_or_else(|| format!("unknown language pack '{}'", cfg.lang))?;
        let argv = pack.toolchain.run.clone();
        if argv.is_empty() {
            return Err(format!("lang pack '{}' has no run command", cfg.lang));
        }
        if !super::extensions::is_trusted(&pack.id) {
            return Err(format!(
                "extension '{}' is not approved to run toolchain commands; approve it (or enable yolo) in Extensions",
                pack.id
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
        let mut cmd = Command::new(&bin);
        cmd.current_dir(&dir)
            .args(&argv[1..])
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
        if cfg.lang != "deno" {
            return self.compile_toolchain(name, &cfg, &dir).await;
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

    /// Compiles a non-Deno lang pack via its toolchain (e.g. `cargo build
    /// --release`), streaming output. Host target only; cross-compile is a pack
    /// concern. Gated by the trust store.
    async fn compile_toolchain(
        &self,
        name: &str,
        cfg: &ProjectConfig,
        dir: &Path,
    ) -> Result<Vec<String>, String> {
        let pack = super::extensions::lang_pack(&cfg.lang)
            .ok_or_else(|| format!("unknown language pack '{}'", cfg.lang))?;
        let argv = pack.toolchain.compile.clone();
        if argv.is_empty() {
            return Err(format!("lang pack '{}' has no compile command", cfg.lang));
        }
        if !super::extensions::is_trusted(&pack.id) {
            return Err(format!(
                "extension '{}' is not approved; approve it in Extensions",
                pack.id
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

    #[test]
    fn rust_template_sets_lang_and_cargo_files() {
        let _g = lock();
        let root = temp_root();
        let cfg = create_project("rdemo", "", "rust-throughput").expect("create");
        assert_eq!(cfg.lang, "rust");
        assert_eq!(cfg.main, "src/main.rs");
        let dir = root.join("rdemo");
        assert!(dir.join("Cargo.toml").is_file());
        assert!(dir.join("src/main.rs").is_file());
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

    #[test]
    fn create_scaffolds_a_runnable_project() {
        let _g = lock();
        let root = temp_root();
        let cfg = create_project("demo", "a demo", "starter").expect("create");
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
