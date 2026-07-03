//! Console configuration panel backend.
//!
//! Serves two read-only config surfaces the console renders under a cog in the
//! left rail:
//!
//! * **Server config** ([`server_config`]) — the operator-facing runtime knobs.
//!   Basic mode shows a single control for the SLA mode (preserve latency ⇄
//!   preserve admission; see ADR 0013); advanced mode lists every
//!   `NANOBPMN_*` environment parameter with its category, documented default,
//!   and current value. Parameters are **read-only for now** (set on startup via
//!   the environment) — the panel surfaces them for visibility.
//!
//! * **IDE config** ([`ide_config`]) — the language-pack toolchains and the
//!   external system dependencies they need. Any missing dependency (e.g. Deno
//!   for embedded workers, or `cargo` for the Rust pack) is surfaced with
//!   OS-aware install guidance, mirroring what ProcessOS does — so a user whose
//!   Deno is not installed is told exactly what to install and why. Language
//!   packs can extend this panel with their own config fields
//!   ([`extensions::ConfigField`]).

use std::process::Command;

use serde::Serialize;

use super::extensions;
use crate::backpressure::{SlaMode, parse_sla_mode};

// ---------------------------------------------------------------------------
// Server config
// ---------------------------------------------------------------------------

/// A single environment-driven server parameter, surfaced read-only.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvParam {
    /// The `NANOBPMN_*` environment variable name.
    key: &'static str,
    /// Grouping shown in the advanced view.
    category: &'static str,
    /// Human-facing label.
    label: &'static str,
    /// What the parameter controls.
    description: &'static str,
    /// Documented default when unset.
    default: &'static str,
    /// The value currently set in the process environment, if any.
    value: Option<String>,
}

/// Static registry of the server parameters worth surfacing, grouped by concern.
/// `(key, category, label, default, description)`.
const PARAMS: &[(&str, &str, &str, &str, &str)] = &[
    // --- Admission / SLA -----------------------------------------------------
    (
        "NANOBPMN_SLA_MODE",
        "Admission & SLA",
        "SLA mode",
        "latency",
        "Behaviour at the saturation ceiling: preserve end-to-end latency (shed admission) or preserve admission (accept latency). See ADR 0013.",
    ),
    (
        "NANOBPMN_ADMISSION_MAX_BACKLOG",
        "Admission & SLA",
        "Max active backlog",
        "off",
        "Active-instance backlog above which creates are shed (latency mode). Off by default.",
    ),
    (
        "NANOBPMN_ADMISSION_MAX_CREATE_QUEUE",
        "Admission & SLA",
        "Max create queue",
        "adaptive",
        "Create-queue depth rail — a memory-safety guard active in every SLA mode.",
    ),
    (
        "NANOBPMN_BACKPRESSURE_MAX_INFLIGHT",
        "Admission & SLA",
        "Max in-flight (AIMD ceiling)",
        "adaptive",
        "Upper bound the adaptive concurrency limiter will open to before shedding.",
    ),
    (
        "NANOBPMN_STREAM_SUBMISSION_WINDOW",
        "Admission & SLA",
        "Stream submission window",
        "auto",
        "Per-connection falcon submission-credit window (create intake metering).",
    ),
    // --- Memory & spill ------------------------------------------------------
    (
        "NANOBPMN_MEM_WATERMARK",
        "Memory & spill",
        "Memory watermark mode",
        "adaptive",
        "Resident-memory rail governing when create admission backs off. `adaptive` sizes from host RAM.",
    ),
    (
        "NANOBPMN_MEM_WATERMARK_MB",
        "Memory & spill",
        "Memory watermark (MB)",
        "(adaptive)",
        "Explicit resident-memory watermark in MB, overriding the adaptive sizing.",
    ),
    (
        "NANOBPMN_VAR_SPILL",
        "Memory & spill",
        "Variable spill mode",
        "adaptive",
        "Spill live process variables to disk under RAM pressure. `adaptive` is the default.",
    ),
    (
        "NANOBPMN_VAR_SPILL_MB",
        "Memory & spill",
        "Variable spill watermark (MB)",
        "(adaptive)",
        "Resident-variable budget before spilling begins, when not adaptive.",
    ),
    (
        "NANOBPMN_COLD_SPILL",
        "Memory & spill",
        "Cold spill mode",
        "adaptive",
        "Spill cold/idle instance state to disk under pressure.",
    ),
    (
        "NANOBPMN_PIPELINE_BYTES",
        "Memory & spill",
        "Pipeline bytes rail",
        "adaptive",
        "In-flight create payload-bytes watermark — a memory-safety rail active in every SLA mode.",
    ),
    (
        "NANOBPMN_IDLE_PURGE_MS",
        "Memory & spill",
        "Idle purge interval (ms)",
        "5000",
        "Idle tick that shrinks hot-state maps and returns freed arenas to the OS. 0 disables.",
    ),
    // --- Durability & journal ------------------------------------------------
    (
        "NANOBPMN_DURABILITY",
        "Durability & journal",
        "Durability tier",
        "default",
        "Write-path durability tier (see ADR 0003).",
    ),
    (
        "NANOBPMN_JOURNAL",
        "Durability & journal",
        "Journal mode",
        "segmented",
        "Journal implementation. `segmented` gives bounded-disk compaction.",
    ),
    (
        "NANOBPMN_JOURNAL_SEGMENT_BYTES",
        "Durability & journal",
        "Journal segment size (bytes)",
        "(default)",
        "Target size of each journal segment before rotation/compaction.",
    ),
    (
        "NANOBPMN_LEAN_SNAPSHOT",
        "Durability & journal",
        "Lean snapshots",
        "1",
        "Control-only snapshots backed by the durable var-store (Fix 1b / ADR 0012).",
    ),
    (
        "NANOBPMN_SNAPSHOT_INTERVAL_MS",
        "Durability & journal",
        "Snapshot interval (ms)",
        "(default)",
        "How often the engine snapshots control state for compaction/recovery.",
    ),
    (
        "NANOBPMN_VARSTORE_WAL_CHECKPOINT_SECS",
        "Durability & journal",
        "Var-store WAL checkpoint (s)",
        "30",
        "Interval for truncating the durable var-store WAL so its disk footprint stays bounded (ADR 0013). 0 disables.",
    ),
    // --- Exporter & read model ----------------------------------------------
    (
        "NANOBPMN_EXPORTER_QUEUE",
        "Exporter & read model",
        "Exporter queue mode",
        "adaptive",
        "Bounds the read-model exporter backlog with per-shard soft backpressure.",
    ),
    (
        "NANOBPMN_HISTORY_RETENTION",
        "Exporter & read model",
        "History retention mode",
        "adaptive",
        "Disk-pressure driven read-model retention. `adaptive` prunes under disk pressure.",
    ),
    (
        "NANOBPMN_HISTORY_RETENTION_MB",
        "Exporter & read model",
        "History retention budget (MB)",
        "(adaptive)",
        "Explicit read-model disk budget in MB before pruning, when not adaptive.",
    ),
    (
        "NANOBPMN_READ_DB",
        "Exporter & read model",
        "Read-model database path",
        "(data dir)",
        "Location of the read-model projection store.",
    ),
    // --- Cluster & Raft ------------------------------------------------------
    (
        "NANOBPMN_NODE_ID",
        "Cluster & Raft",
        "Node id",
        "0",
        "This node's identity within the cluster.",
    ),
    (
        "NANOBPMN_NODES",
        "Cluster & Raft",
        "Cluster members",
        "(single-node)",
        "Comma-separated peer addresses forming the cluster.",
    ),
    (
        "NANOBPMN_PARTITIONS",
        "Cluster & Raft",
        "Partition count",
        "1",
        "Number of engine partitions the keyspace is sharded into.",
    ),
    (
        "NANOBPMN_RF",
        "Cluster & Raft",
        "Replication factor",
        "1",
        "How many replicas hold each partition.",
    ),
    (
        "NANOBPMN_RAFT",
        "Cluster & Raft",
        "Raft replication",
        "off",
        "Per-partition Raft replication (experimental).",
    ),
    // --- Paths & runtime -----------------------------------------------------
    (
        "NANOBPMN_DATA_DIR",
        "Paths & runtime",
        "Data directory",
        "./.data",
        "Root directory for the journal, snapshots, read model, and var stores.",
    ),
    (
        "NANOBPMN_WORKSPACE_DIR",
        "Paths & runtime",
        "Workspace directory",
        "(console default)",
        "Where console projects, workers, and installed extensions live.",
    ),
    (
        "NANOBPMN_DENO_BIN",
        "Paths & runtime",
        "Deno binary",
        "deno (on PATH)",
        "Deno runtime used to run embedded job workers. Auto-resolved when unset.",
    ),
];

/// `GET /console/api/config/server` — SLA mode + the full env-parameter registry.
pub fn server_config_json() -> serde_json::Value {
    let raw = std::env::var("NANOBPMN_SLA_MODE").ok();
    let mode = parse_sla_mode(raw.as_deref());
    let params: Vec<EnvParam> = PARAMS
        .iter()
        .map(|(key, category, label, default, description)| EnvParam {
            key,
            category,
            label,
            description,
            default,
            value: std::env::var(key).ok().filter(|v| !v.trim().is_empty()),
        })
        .collect();

    serde_json::json!({
        "slaMode": {
            "current": mode.as_str(),
            "description": mode.describe(),
            "source": if raw.is_some() { "NANOBPMN_SLA_MODE" } else { "default" },
            "options": [
                {
                    "id": SlaMode::Latency.as_str(),
                    "label": "Reject admission",
                    "tagline": "Preserve end-to-end latency",
                    "description": SlaMode::Latency.describe(),
                },
                {
                    "id": SlaMode::Admission.as_str(),
                    "label": "Accept latency",
                    "tagline": "Preserve admission",
                    "description": SlaMode::Admission.describe(),
                },
            ],
        },
        // Read-only for now: parameters are set on startup via the environment.
        "readOnly": true,
        "params": params,
    })
}

// ---------------------------------------------------------------------------
// IDE config: toolchain dependencies + language-pack config
// ---------------------------------------------------------------------------

const DENO_INSTALL_URL: &str = "https://docs.deno.com/runtime/getting_started/installation/";

/// One external toolchain the IDE needs, and whether it is available — the
/// deps-preflight shape ProcessOS uses so missing tools come with guidance.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Dependency {
    id: String,
    name: String,
    purpose: String,
    /// The binary the console will invoke (resolved from env/PATH where set).
    bin: String,
    present: bool,
    version: Option<String>,
    install_url: String,
    /// Actionable hint shown when the tool is missing (empty when present).
    hint: String,
}

/// Probe `<bin> --version`; `Some(first non-empty line)` when it spawned.
/// Presence is "did it spawn", not the exit code (some tools exit non-zero).
fn probe(bin: &str) -> Option<String> {
    let out = Command::new(bin).arg("--version").output().ok()?;
    let stream: &[u8] = if out.stdout.is_empty() {
        &out.stderr
    } else {
        &out.stdout
    };
    let line = String::from_utf8_lossy(stream)
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("")
        .to_string();
    Some(line)
}

/// The Deno runtime dependency — the one a user most often lacks. Mirrors the
/// engine's resolution order (`NANOBPMN_DENO_BIN`, PATH, `~/.deno/bin/deno`).
fn check_deno() -> Dependency {
    let bin = super::workers::find_deno()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "deno".to_string());
    let version = probe(&bin).filter(|s| !s.is_empty());
    let present = super::workers::supervisor().deno_available();
    Dependency {
        id: "deno".into(),
        name: "Deno".into(),
        purpose:
            "Running Nano's embedded job workers — each enabled worker runs as a sandboxed Deno subprocess."
                .into(),
        bin,
        present,
        version,
        install_url: DENO_INSTALL_URL.into(),
        hint: if present {
            String::new()
        } else {
            "Deno was not found. Install it (see the link) so `deno` is on PATH, or set NANOBPMN_DENO_BIN to its path. Until then, embedded Nano workers cannot run."
                .into()
        },
    }
}

/// Toolchain dependency derived from a language pack's `detect` probe (e.g.
/// `cargo` for the Rust pack). Built-in packs with an empty toolchain (Deno) are
/// reported via [`check_deno`] instead.
fn check_pack_toolchain(m: &extensions::ExtManifest) -> Option<Dependency> {
    let bin = m.toolchain.detect.first()?.clone();
    let version = probe(&bin).filter(|s| !s.is_empty());
    let present = extensions::find_program(&bin).is_some();
    let install_url = m
        .toolchain
        .install_url
        .clone()
        .unwrap_or_else(|| "https://www.google.com/search?q=install+".to_string() + &bin);
    let hint = if present {
        String::new()
    } else {
        m.toolchain.install_hint.clone().unwrap_or_else(|| {
            format!(
                "`{bin}` was not found. Install the {} toolchain (see the link) so `{bin}` is on PATH.",
                m.display_name
            )
        })
    };
    Some(Dependency {
        id: format!("toolchain:{}", m.id),
        name: format!("{} toolchain ({bin})", m.display_name),
        purpose: format!("Running and compiling {} projects.", m.display_name),
        bin,
        present,
        version,
        install_url,
        hint,
    })
}

/// A language pack as shown in the IDE config panel.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LangPackConfig {
    id: String,
    display_name: String,
    builtin: bool,
    /// The probe argv (empty for the internal Deno path).
    detect: Vec<String>,
    /// Whether the pack's toolchain is usable on this machine.
    available: bool,
    /// Config fields the pack contributes, resolved against the environment.
    config_fields: Vec<ResolvedConfigField>,
}

/// A pack config field with its current value resolved from the environment.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResolvedConfigField {
    key: String,
    label: String,
    description: Option<String>,
    env: Option<String>,
    default: Option<String>,
    value: Option<String>,
}

/// `GET /console/api/config/ide` — toolchain dependencies + language packs.
pub fn ide_config_json() -> serde_json::Value {
    let packs = extensions::all_extensions();

    // Missing-dependency guidance: the Deno runtime plus each lang pack's
    // toolchain probe. This is the surface that tells a user their Deno (or
    // cargo, etc.) is missing and how to install it.
    let mut deps = vec![check_deno()];
    for m in &packs {
        if matches!(m.kind, extensions::ExtKind::Lang)
            && let Some(d) = check_pack_toolchain(m)
        {
            deps.push(d);
        }
    }

    let lang_packs: Vec<LangPackConfig> = packs
        .iter()
        .filter(|m| matches!(m.kind, extensions::ExtKind::Lang))
        .map(|m| LangPackConfig {
            id: m.id.clone(),
            display_name: m.display_name.clone(),
            builtin: m.builtin,
            detect: m.toolchain.detect.clone(),
            available: extensions::toolchain_available(m),
            config_fields: m
                .config_fields
                .iter()
                .map(|f| ResolvedConfigField {
                    key: f.key.clone(),
                    label: f.label.clone(),
                    description: f.description.clone(),
                    env: f.env.clone(),
                    default: f.default.clone(),
                    value: f
                        .env
                        .as_deref()
                        .and_then(|e| std::env::var(e).ok())
                        .filter(|v| !v.trim().is_empty()),
                })
                .collect(),
        })
        .collect();

    serde_json::json!({
        "dependencies": deps,
        "langPacks": lang_packs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_reports_sla_mode_and_params() {
        let v = server_config_json();
        assert!(v["slaMode"]["current"].is_string());
        assert_eq!(v["slaMode"]["options"].as_array().unwrap().len(), 2);
        let params = v["params"].as_array().unwrap();
        assert!(params.iter().any(|p| p["key"] == "NANOBPMN_SLA_MODE"));
        assert!(params.iter().any(|p| p["key"] == "NANOBPMN_VARSTORE_WAL_CHECKPOINT_SECS"));
        assert_eq!(v["readOnly"], true);
    }

    #[test]
    fn ide_config_surfaces_deno_dependency_with_guidance() {
        let v = ide_config_json();
        let deps = v["dependencies"].as_array().unwrap();
        let deno = deps.iter().find(|d| d["id"] == "deno").expect("deno dep");
        assert!(deno["installUrl"].as_str().unwrap().starts_with("https://"));
        // A missing tool always carries an actionable hint; a present one does not.
        assert_eq!(deno["present"].as_bool().unwrap(), deno["hint"].as_str().unwrap().is_empty());
    }

    #[test]
    fn ide_config_lists_lang_packs_with_toolchains() {
        let v = ide_config_json();
        let packs = v["langPacks"].as_array().unwrap();
        assert!(packs.iter().any(|p| p["id"] == "deno"));
        let rust = packs.iter().find(|p| p["id"] == "rust").expect("rust pack");
        assert_eq!(rust["detect"][0], "cargo");
    }

    #[test]
    fn missing_pack_toolchain_carries_install_url() {
        // The Rust pack declares an install URL used when cargo is absent.
        let rust = extensions::lang_pack("rust").unwrap();
        let dep = check_pack_toolchain(&rust).unwrap();
        assert!(dep.install_url.starts_with("https://"));
    }
}
