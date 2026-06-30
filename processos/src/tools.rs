//! The cockpit **tool library** — a data-driven, file-backed surface of investigation tools.
//!
//! A baked-in tool is Rust ([`crate::investigate`]); a *custom* tool is a [`ToolDef`]: a named,
//! parameterised wrapper over one of three executors, authored by the operator and offered to the
//! investigator LLM alongside the built-ins. See ADR-0010.
//!
//! - [`ToolBackend::Python`] — `template` is Python source, run through the existing
//!   [`crate::pyrunner`] sandbox with the named arguments injected as a `params` dict (and
//!   `{{name}}` placeholders interpolated).
//! - [`ToolBackend::Sql`] — `template` is a single read-only DuckDB `SELECT`/`WITH`; arguments are
//!   rendered as **safe SQL literals** into `{{name}}` placeholders and run through
//!   `Analysis::query` (which enforces single-statement / read-only).
//! - [`ToolBackend::Subprocess`] — `command` is a fixed **argv array** (no shell); only declared
//!   params interpolate into argv elements (each as one argument) and the whole argument object is
//!   also delivered as JSON on stdin.
//!
//! The pure rendering helpers ([`ToolDef::render_python`] / [`render_sql`](ToolDef::render_sql) /
//! [`build_argv`](ToolDef::build_argv) / [`stdin_json`](ToolDef::stdin_json)) live here and are
//! unit-tested; the actual execution (which needs the dataset / Python env / model) is performed by
//! the toolbox in [`crate::investigate`]. Definitions persist to `<config_dir>/tools.json`,
//! mirroring [`crate::personas`] / [`crate::pairings`].

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The baked-in tool names. Custom tools may not shadow these (and these are surfaced read-only in
/// the catalog so operators can see — and clone — the full surface).
pub const BUILTIN_NAMES: &[&str] = &[
    "query_traces",
    "discover_flow",
    "run_python",
    "read_model",
    "read_model_xml",
    "analyze_model",
    "validate_model",
    "edit_model",
    "simulate",
    "compare_variants",
    "conformance_check",
    "delegate",
];

/// How a custom tool runs when the LLM calls it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ToolBackend {
    /// Arbitrary Python over the exported trace CSVs (via [`crate::pyrunner`]).
    #[default]
    Python,
    /// A single read-only DuckDB `SELECT`/`WITH` over the trace tables.
    Sql,
    /// An external program invoked as a fixed argv (no shell).
    Subprocess,
}

/// One custom tool definition.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolDef {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub backend: ToolBackend,
    /// JSON-schema `object` describing the tool's arguments (what the LLM fills in).
    #[serde(default = "default_parameters")]
    pub parameters: Value,
    /// Python source (`python`) or DuckDB SQL (`sql`). Unused for `subprocess`.
    #[serde(default)]
    pub template: String,
    /// Subprocess argv (program + args, with `{{name}}` placeholders). Unused otherwise.
    #[serde(default)]
    pub command: Vec<String>,
    /// Subprocess wall-clock budget in ms (clamped 100..120000; default 20000).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// Free-form tags for browsing / categorising (e.g. language, domain).
    #[serde(default)]
    pub tags: Vec<String>,
    /// Whether this tool is offered to investigations at all. Off by default for safety.
    #[serde(default)]
    pub enabled: bool,
    /// True for the read-only baked-in catalog entries (cannot be edited/deleted/executed here).
    #[serde(default)]
    pub builtin: bool,
}

fn default_parameters() -> Value {
    json!({ "type": "object", "properties": {} })
}

impl Default for ToolDef {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            description: String::new(),
            backend: ToolBackend::default(),
            parameters: default_parameters(),
            template: String::new(),
            command: Vec::new(),
            timeout_ms: None,
            tags: Vec::new(),
            enabled: false,
            builtin: false,
        }
    }
}

impl ToolDef {
    /// The wall-clock budget for a subprocess tool, clamped to a sane range.
    pub fn timeout(&self) -> std::time::Duration {
        let ms = self.timeout_ms.unwrap_or(20_000).clamp(100, 120_000);
        std::time::Duration::from_millis(ms)
    }

    /// Build the Python program: a `params` dict seeded from the arguments, then `{{name}}`
    /// placeholders interpolated into the template body, then the author's code.
    pub fn render_python(&self, args: &Value) -> String {
        let params = serde_json::to_string(args).unwrap_or_else(|_| "{}".into());
        let body = interpolate(&self.template, args, ToolBackend::Python);
        format!("import json\nparams = json.loads({})\n{}", py_str(&params), body)
    }

    /// Render the SQL by substituting `{{name}}` placeholders with **safe SQL literals**. The
    /// result still passes through `Analysis::query`'s read-only single-statement guard.
    pub fn render_sql(&self, args: &Value) -> String {
        interpolate(&self.template, args, ToolBackend::Sql)
    }

    /// Build the subprocess argv: each element has its `{{name}}` placeholders replaced by the
    /// argument's string value. The program (`command[0]`) is never split; nothing hits a shell.
    pub fn build_argv(&self, args: &Value) -> Result<Vec<String>, String> {
        if self.command.is_empty() {
            return Err("subprocess tool has no command".into());
        }
        Ok(self
            .command
            .iter()
            .map(|part| interpolate(part, args, ToolBackend::Subprocess))
            .collect())
    }

    /// The argument object serialised for delivery on the subprocess's stdin.
    pub fn stdin_json(&self, args: &Value) -> String {
        serde_json::to_string(args).unwrap_or_else(|_| "{}".into())
    }

    /// Validate a definition for storage. Enforces a non-empty id/name, a name that does not
    /// shadow a built-in, and backend-specific content.
    fn validated(mut self) -> Result<Self, String> {
        self.id = self.id.trim().to_string();
        self.name = self.name.trim().to_string();
        if self.id.is_empty() {
            return Err("tool id must not be empty".into());
        }
        if self.name.is_empty() {
            self.name = self.id.clone();
        }
        if !is_valid_tool_name(&self.name) {
            return Err(
                "tool name must be a function-style identifier (letters, digits, underscore)".into(),
            );
        }
        if BUILTIN_NAMES.contains(&self.name.as_str()) {
            return Err(format!("'{}' is a built-in tool name", self.name));
        }
        if !self.parameters.is_object() {
            return Err("parameters must be a JSON schema object".into());
        }
        match self.backend {
            ToolBackend::Python | ToolBackend::Sql if self.template.trim().is_empty() => {
                return Err("this tool needs a non-empty template".into());
            }
            ToolBackend::Subprocess if self.command.iter().all(|c| c.trim().is_empty()) => {
                return Err("a subprocess tool needs a command (argv)".into());
            }
            _ => {}
        }
        self.builtin = false;
        Ok(self)
    }
}

/// A function-style tool name the LLM tool API accepts.
fn is_valid_tool_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && name.chars().next().map(|c| c.is_ascii_alphabetic()).unwrap_or(false)
}

/// Replace `{{name}}` placeholders in `tpl` with the corresponding argument, rendered for the
/// given backend (SQL literal for SQL, plain string otherwise). Unknown placeholders become empty.
fn interpolate(tpl: &str, args: &Value, backend: ToolBackend) -> String {
    let mut out = String::with_capacity(tpl.len());
    let bytes = tpl.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            if let Some(end) = tpl[i + 2..].find("}}") {
                let key = tpl[i + 2..i + 2 + end].trim();
                let val = args.get(key).unwrap_or(&Value::Null);
                out.push_str(&render_value(val, backend));
                i = i + 2 + end + 2;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn render_value(v: &Value, backend: ToolBackend) -> String {
    match backend {
        ToolBackend::Sql => sql_literal(v),
        _ => match v {
            Value::Null => String::new(),
            Value::String(s) => s.clone(),
            other => other.to_string(),
        },
    }
}

/// Render a JSON value as a safe DuckDB SQL literal. Strings are single-quote escaped; numbers and
/// booleans pass through; null becomes `NULL`. Containers are escaped as their JSON text string.
fn sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

/// Quote a Rust string as a Python string literal (single-quoted, escaped).
fn py_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// The read-only catalog of baked-in tools, surfaced so operators can see the full surface and
/// clone an entry as a starting point. These are display-only — they are executed in Rust, never
/// through the store.
pub fn builtin_catalog() -> Vec<ToolDef> {
    const ENTRIES: &[(&str, &str, &str)] = &[
        ("query_traces", "Run a read-only DuckDB SQL query over the captured trace dataset.", "always"),
        ("discover_flow", "Mine the directly-follows graph the trace implies (model-independent).", "always"),
        ("run_python", "Run model-authored Python over the dataset's flattened tables.", "Python enabled"),
        ("read_model", "Structural view of the bound BPMN model (nodes, kinds, reachability).", "model present"),
        ("read_model_xml", "Raw BPMN XML of the model (or one phase of an orchestrator).", "model present"),
        ("analyze_model", "Deterministic static checks over the model structure.", "model present"),
        ("validate_model", "Lint/validate a candidate BPMN model before simulating.", "model present"),
        ("edit_model", "Author a candidate BPMN variant via validated structured operations.", "model present"),
        ("simulate", "Replay recorded instances against a forked variant (Alternate Reality Engine).", "recorded inputs"),
        ("compare_variants", "Compare two model variants over the recorded inputs.", "recorded inputs"),
        ("conformance_check", "Check the trace against the model for conformance.", "model present"),
        ("delegate", "Hand a self-contained research task to an isolated subagent.", "subagent paired"),
    ];
    ENTRIES
        .iter()
        .map(|(name, desc, gate)| ToolDef {
            id: format!("builtin:{name}"),
            name: (*name).to_string(),
            description: (*desc).to_string(),
            backend: ToolBackend::Python,
            parameters: default_parameters(),
            template: String::new(),
            command: Vec::new(),
            timeout_ms: None,
            tags: vec!["built-in".into(), format!("gate:{gate}")],
            enabled: true,
            builtin: true,
        })
        .collect()
}

/// A file-backed, sorted-by-id custom-tool library.
pub struct ToolStore {
    path: PathBuf,
    defs: RwLock<BTreeMap<String, ToolDef>>,
}

impl ToolStore {
    /// Open the store at `path`, loading any persisted custom tools. Ships empty.
    pub fn open(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let mut defs = BTreeMap::new();
        if let Ok(body) = std::fs::read_to_string(&path) {
            if let Ok(saved) = serde_json::from_str::<Vec<ToolDef>>(&body) {
                for mut d in saved {
                    d.builtin = false;
                    defs.insert(d.id.clone(), d);
                }
            }
        }
        Self {
            path,
            defs: RwLock::new(defs),
        }
    }

    /// All custom tools, ordered by id.
    pub fn list(&self) -> Vec<ToolDef> {
        self.defs
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// All currently-enabled custom tools (what an investigation may offer the LLM).
    pub fn enabled(&self) -> Vec<ToolDef> {
        self.defs
            .read()
            .map(|m| m.values().filter(|d| d.enabled).cloned().collect())
            .unwrap_or_default()
    }

    /// Create or update a custom tool by id.
    pub fn upsert(&self, def: ToolDef) -> Result<ToolDef, String> {
        let def = def.validated()?;
        if let Ok(mut m) = self.defs.write() {
            m.insert(def.id.clone(), def.clone());
        }
        self.persist();
        Ok(def)
    }

    /// Import several definitions at once (used by the import bundle). Invalid entries are skipped
    /// and reported; valid ones are upserted. Returns the count imported and any errors.
    pub fn import_many(&self, defs: Vec<ToolDef>) -> (usize, Vec<String>) {
        let mut imported = 0usize;
        let mut errors = Vec::new();
        for d in defs {
            let id = d.id.clone();
            match d.validated() {
                Ok(valid) => {
                    if let Ok(mut m) = self.defs.write() {
                        m.insert(valid.id.clone(), valid);
                        imported += 1;
                    }
                }
                Err(e) => errors.push(format!("{id}: {e}")),
            }
        }
        if imported > 0 {
            self.persist();
        }
        (imported, errors)
    }

    /// Remove a custom tool. Refuses an unknown id.
    pub fn delete(&self, id: &str) -> Result<(), String> {
        {
            let mut m = self
                .defs
                .write()
                .map_err(|_| "tool store lock poisoned".to_string())?;
            if m.remove(id).is_none() {
                return Err(format!("no such tool: {id}"));
            }
        }
        self.persist();
        Ok(())
    }

    fn persist(&self) {
        let saved: Vec<ToolDef> = self
            .defs
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match serde_json::to_string_pretty(&saved) {
            Ok(body) => {
                if let Err(e) = std::fs::write(&self.path, body) {
                    tracing::warn!(path = %self.path.display(), error = %e, "tools: persist failed");
                }
            }
            Err(e) => tracing::warn!(error = %e, "tools: serialize failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        std::env::temp_dir().join(format!("tools-test-{}.json", std::process::id()))
    }

    #[test]
    fn validation_rejects_builtin_names_and_empty_bodies() {
        let store = ToolStore::open(tmp());
        let shadow = ToolDef {
            id: "x".into(),
            name: "query_traces".into(),
            backend: ToolBackend::Sql,
            template: "SELECT 1".into(),
            ..Default::default()
        };
        assert!(store.upsert(shadow).is_err());
        let empty_sql = ToolDef {
            id: "y".into(),
            name: "y_tool".into(),
            backend: ToolBackend::Sql,
            template: "   ".into(),
            ..Default::default()
        };
        assert!(store.upsert(empty_sql).is_err());
        let no_cmd = ToolDef {
            id: "z".into(),
            name: "z_tool".into(),
            backend: ToolBackend::Subprocess,
            ..Default::default()
        };
        assert!(store.upsert(no_cmd).is_err());
    }

    #[test]
    fn sql_interpolation_escapes_string_literals() {
        let def = ToolDef {
            backend: ToolBackend::Sql,
            template: "SELECT * FROM jobs WHERE job_type = {{jt}} AND n > {{n}}".into(),
            ..Default::default()
        };
        let sql = def.render_sql(&json!({"jt": "o'brien", "n": 5}));
        assert_eq!(
            sql,
            "SELECT * FROM jobs WHERE job_type = 'o''brien' AND n > 5"
        );
    }

    #[test]
    fn python_render_injects_params_dict() {
        let def = ToolDef {
            backend: ToolBackend::Python,
            template: "print(params['k'])".into(),
            ..Default::default()
        };
        let code = def.render_python(&json!({"k": "hi"}));
        assert!(code.contains("params = json.loads("));
        assert!(code.contains("print(params['k'])"));
    }

    #[test]
    fn subprocess_argv_interpolates_each_element_singly() {
        let def = ToolDef {
            backend: ToolBackend::Subprocess,
            command: vec!["node".into(), "check.js".into(), "--id={{id}}".into()],
            ..Default::default()
        };
        let argv = def.build_argv(&json!({"id": "A B"})).unwrap();
        assert_eq!(argv, vec!["node", "check.js", "--id=A B"]);
    }

    #[test]
    fn upsert_list_and_delete_round_trip() {
        let path = tmp();
        let _ = std::fs::remove_file(&path);
        let store = ToolStore::open(&path);
        let def = ToolDef {
            id: "weekend-rate".into(),
            name: "weekend_rate".into(),
            description: "share of jobs on a weekend".into(),
            backend: ToolBackend::Sql,
            template: "SELECT count(*) FROM jobs".into(),
            enabled: true,
            ..Default::default()
        };
        store.upsert(def).expect("upsert ok");
        assert_eq!(store.list().len(), 1);
        assert_eq!(store.enabled().len(), 1);

        let store2 = ToolStore::open(&path);
        assert_eq!(store2.list().len(), 1);
        store2.delete("weekend-rate").expect("delete ok");
        assert!(store2.list().is_empty());
        let _ = std::fs::remove_file(&path);
    }
}
