//! The cockpit **persona library** — selectable *system* prompts for the interactive chat.
//!
//! A persona is the standing instruction that frames every turn of a chat session: it sets
//! the droid's lens and discipline (what it optimises for, how it reports), distinct from the
//! per-message [`crate::chat_prompts`] templates (what the operator asks right now) and the
//! [`crate::harness::prompts`] system prompts (which steer *hypothesis* generation).
//!
//! The built-in **Performance Analyst** persona is the default and mirrors
//! [`crate::investigate::CHAT_SYSTEM`]. Operators can author their own personas; those are
//! persisted to the user's config dir (`<config_dir>/personas.json`) so they follow the user
//! across workspaces. The persona is baked into a session's system message on its first turn,
//! so a session's persona is fixed once the conversation has started.
//!
//! No secrets live here, so (unlike settings.json) the file uses default permissions.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

/// The id of the built-in default persona (used when a request names none).
pub const DEFAULT_PERSONA_ID: &str = "performance-analyst";

/// A selectable chat persona (a standing system prompt).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Persona {
    pub id: String,
    #[serde(default)]
    pub name: String,
    /// A one-line description shown under the persona's name in the picker/library.
    #[serde(default)]
    pub summary: String,
    /// The system prompt this persona installs at the head of a chat session.
    pub system: String,
    /// True for seeded built-ins (cannot be deleted).
    #[serde(default)]
    pub builtin: bool,
    /// True for the persona used when a request names none.
    #[serde(default)]
    pub default: bool,
}

/// A file-backed, sorted-by-id persona library.
pub struct PersonaStore {
    path: PathBuf,
    personas: RwLock<BTreeMap<String, Persona>>,
}

impl PersonaStore {
    /// Open the store at `path`, seeding built-ins and merging any persisted personas.
    pub fn open(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let mut personas = builtins();
        if let Ok(body) = std::fs::read_to_string(&path) {
            if let Ok(saved) = serde_json::from_str::<Vec<Persona>>(&body) {
                for mut p in saved {
                    // Persisted personas are never built-in or default (both are server-owned).
                    p.builtin = false;
                    p.default = false;
                    personas.insert(p.id.clone(), p);
                }
            }
        }
        Self {
            path,
            personas: RwLock::new(personas),
        }
    }

    /// All personas, ordered by id.
    pub fn list(&self) -> Vec<Persona> {
        self.personas
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Fetch one persona by id.
    pub fn get(&self, id: &str) -> Option<Persona> {
        self.personas.read().ok().and_then(|m| m.get(id).cloned())
    }

    /// Resolve a requested persona id to the `(id, system_prompt)` to use this turn. An unknown
    /// or absent id falls back to the default persona (never fails — chat must always run).
    pub fn resolve(&self, id: Option<&str>) -> (String, String) {
        let want = id.map(str::trim).filter(|s| !s.is_empty());
        if let Some(p) = want.and_then(|id| self.get(id)) {
            return (p.id, p.system);
        }
        self.get(DEFAULT_PERSONA_ID)
            .map(|p| (p.id, p.system))
            .unwrap_or_else(|| {
                (
                    DEFAULT_PERSONA_ID.to_string(),
                    crate::investigate::CHAT_SYSTEM.to_string(),
                )
            })
    }

    /// Author or update a persona (create or update by id). Validates a non-empty id and
    /// system text; the `builtin`/`default` flags are server-controlled.
    pub fn upsert(&self, mut p: Persona) -> Result<Persona, String> {
        p.id = p.id.trim().to_string();
        if p.id.is_empty() {
            return Err("persona id must not be empty".to_string());
        }
        if p.system.trim().is_empty() {
            return Err("persona system prompt must not be empty".to_string());
        }
        if p.name.trim().is_empty() {
            p.name = p.id.clone();
        }
        if let Ok(mut m) = self.personas.write() {
            // Preserve existing server-owned flags; never let a caller mint them.
            let existing = m.get(&p.id);
            p.builtin = existing.map(|e| e.builtin).unwrap_or(false);
            p.default = existing.map(|e| e.default).unwrap_or(false);
            m.insert(p.id.clone(), p.clone());
        }
        self.persist();
        Ok(p)
    }

    /// Remove a persona. Refuses to delete a built-in or an unknown id.
    pub fn delete(&self, id: &str) -> Result<(), String> {
        {
            let mut m = self
                .personas
                .write()
                .map_err(|_| "persona store lock poisoned".to_string())?;
            match m.get(id) {
                None => return Err(format!("no such persona: {id}")),
                Some(p) if p.builtin => {
                    return Err(format!("cannot delete built-in persona: {id}"))
                }
                Some(_) => {
                    m.remove(id);
                }
            }
        }
        self.persist();
        Ok(())
    }

    /// Persist the non-built-in personas to disk (best-effort).
    fn persist(&self) {
        let saved: Vec<Persona> = self
            .personas
            .read()
            .map(|m| m.values().filter(|p| !p.builtin).cloned().collect())
            .unwrap_or_default();
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match serde_json::to_string_pretty(&saved) {
            Ok(body) => {
                if let Err(e) = std::fs::write(&self.path, body) {
                    tracing::warn!(path = %self.path.display(), error = %e, "personas: persist failed");
                }
            }
            Err(e) => tracing::warn!(error = %e, "personas: serialize failed"),
        }
    }
}

/// The seeded built-in personas. **Performance Analyst** is the default and reuses the
/// canonical [`crate::investigate::CHAT_SYSTEM`]; the others reframe the lens (reliability,
/// capacity) while keeping the same query-the-data discipline.
fn builtins() -> BTreeMap<String, Persona> {
    let seed = [
        Persona {
            id: DEFAULT_PERSONA_ID.into(),
            name: "Performance Analyst".into(),
            summary: "Profiles the process openly and localises the dominant pathology with \
                      effect sizes and held-out replication."
                .into(),
            system: crate::investigate::CHAT_SYSTEM.to_string(),
            builtin: true,
            default: true,
        },
        Persona {
            id: "sre-incident".into(),
            name: "SRE / Incident Responder".into(),
            summary: "Triages reliability: where failures and queue blow-ups concentrate, blast \
                      radius, and the fastest mitigation."
                .into(),
            system: "\
You are a site-reliability engineer triaging a captured BPMN trace dataset with an operator. \
Treat this like an incident: find what is failing or degrading, scope its blast radius, and \
get to the fastest credible mitigation. Answer by actually querying the data — never guess or \
invent numbers. Your primary tool is query_traces, which runs read-only DuckDB SQL over the \
dataset. If a run_python tool is offered, use it only AFTER SQL has localised a candidate, for \
analysis SQL cannot express.\n\
\n\
Investigate openly. Don't assume where the problem is: profile failures and incidents, queue \
and latency tails, and how they move over time, then let the evidence point you to the worst \
offender. For each suspected issue establish WHEN it started, WHAT is affected (job types, \
elements), and HOW BADLY (effect size + how many instances/jobs). Replicate any time-localised \
pattern on a held-out slice before trusting it.\n\
\n\
Style: lead with the headline (what's wrong, how bad, since when), then the evidence with \
concrete figures, then a concrete mitigation and what you'd watch to confirm it worked. Write \
clear prose for a human in a chat — do NOT emit JSON."
                .into(),
            builtin: true,
            default: false,
        },
        Persona {
            id: "capacity-planner".into(),
            name: "Capacity Planner".into(),
            summary: "Looks at throughput, utilisation and headroom: where the bottleneck is and \
                      what scaling would buy."
                .into(),
            system: "\
You are a capacity planner analysing a captured BPMN trace dataset with an operator. Your lens \
is throughput, utilisation, and headroom: where the process is bottlenecked, how close it runs \
to saturation, and what scaling a resource would actually buy. Answer by querying the data — \
never guess or invent numbers. Your primary tool is query_traces, which runs read-only DuckDB \
SQL over the dataset. If a run_python tool is offered, use it only AFTER SQL has localised a \
candidate, for analysis SQL cannot express (queueing/utilisation modelling, distribution \
fitting).\n\
\n\
Investigate openly. Don't assume where the constraint is: profile arrival/throughput rates, \
where time is spent across job types, and how utilisation and queueing move over time, then \
let the evidence identify the binding constraint. Quantify the bottleneck with an effect size \
and sample size, compare peak against off-peak baselines, and estimate the headroom a change \
would buy rather than asserting it. Replicate any time-localised pattern on a held-out slice \
before trusting it.\n\
\n\
Style: clear prose citing the figures you measured — the constraint, how saturated it is, and \
the quantified headroom of any recommendation, with its uncertainty. Do NOT emit JSON."
                .into(),
            builtin: true,
            default: false,
        },
        Persona {
            id: "process-architect".into(),
            name: "Process Architect".into(),
            summary: "Reviews the BPMN model's structure for soundness and anti-patterns, then \
                      intersects structural risk with the trace data."
                .into(),
            system: "\
You are a process architect reviewing a BPMN process with an operator. Your lens is the MODEL \
itself — its structure, control flow, and resilience — independent of runtime data, and then how \
that structure intersects with what actually happened at runtime.\n\
\n\
Start from the model. Use read_model to get the distilled structural graph (nodes, kinds, flows, \
reachability, gateway roles, service-task job types) and analyze_model to get deterministic static \
findings (missing end events, unreachable or dead-end nodes, exclusive gateways without a default \
flow, unguarded service tasks, parallel-join deadlock hazards, exclusive joins of parallel paths, \
rework loops). These tools work with ZERO trace data — a clean design-time review is valid on its \
own. If no model is available, say so plainly.\n\
\n\
Then, when traces exist, intersect structure with runtime. The model's flow-node `id` and a \
service task's `job_type` are the SAME join keys used in the trace tables (jobs.element_id / \
jobs.job_type, incidents.element_id). So a structural risk can be confirmed or prioritised against \
reality: e.g. is an unguarded service task the one raising incidents? does a rework loop actually \
re-execute often? is a flagged gateway on the hot path? Query the data with query_traces \
(read-only DuckDB SQL) to check — never guess or invent numbers.\n\
\n\
Advise on soundness (can the flow get stuck, deadlock, or strand tokens?), anti-patterns, and \
resilience (error/timeout boundaries, retries, idempotency of risky tasks). For each finding give \
the structural reason, the runtime evidence if any, and a concrete model change. Style: clear \
prose for a human in a chat — do NOT emit JSON."
                .into(),
            builtin: true,
            default: false,
        },
        Persona {
            id: "conformance-miner".into(),
            name: "Conformance Miner".into(),
            summary: "Mines the process the traces actually imply and diffs it against the \
      designed model — where reality and design diverge."
.into(),
            system: "\
You are a process-mining analyst working with an operator. Your job is to compare the process \
people DESIGNED with the process they actually RUN, and explain exactly where the two diverge.\n\
\n\
Work in three moves. (1) Mine reality: call discover_flow to get the directly-follows graph the \
trace implies — the task nodes actually executed and the task-to-task transitions between them, \
with frequencies. (2) Confront the design: call conformance_check to replay that behaviour against \
the BPMN model. It returns a transition-fitness score plus the concrete divergences — \
nonconformant transitions (a path the model forbids), undocumented tasks (executed but not in the \
model), unused model transitions (designed but never taken), and start/end deviations. (3) \
Explain and quantify: for each material divergence, say what it is, how often it happens (use the \
counts), and the most likely cause — a missing/short-circuited path, an out-of-model task, dead \
design, or a linearised parallel branch (cross-branch directly-follows edges are capture \
artefacts, not real ordering — call those out rather than treating them as violations).\n\
\n\
You can go deeper with the other tools: read_model / analyze_model to understand the intended \
structure behind a deviation, and query_traces (read-only DuckDB) to characterise WHO deviates and \
what it costs (do the nonconformant instances fail more, queue longer, take longer end-to-end?). \
Tasks are the join key: a mined node id is jobs.element_id and equals the model's task id. Never \
invent numbers — every claim comes from a tool result.\n\
\n\
Style: lead with the headline (fitness, and the single biggest divergence), then the ranked \
divergences with their frequencies and your causal read, then what you'd change — fix the model to \
match reality, or fix the process to match the model. Clear prose for a human; do NOT emit JSON."
.into(),
            builtin: true,
            default: false,
        },
    ];
    seed.into_iter().map(|p| (p.id.clone(), p)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("processos-personas-{}-{}.json", std::process::id(), n))
    }

    #[test]
    fn default_persona_present_and_protected() {
        let path = tmp();
        let store = PersonaStore::open(&path);
        let def = store.get(DEFAULT_PERSONA_ID).unwrap();
        assert!(def.builtin && def.default);
        assert_eq!(def.system, crate::investigate::CHAT_SYSTEM);
        assert!(store.delete(DEFAULT_PERSONA_ID).is_err());
        assert!(store.delete("missing").is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn resolve_falls_back_to_default() {
        let path = tmp();
        let store = PersonaStore::open(&path);
        let (id, sys) = store.resolve(Some("does-not-exist"));
        assert_eq!(id, DEFAULT_PERSONA_ID);
        assert_eq!(sys, crate::investigate::CHAT_SYSTEM);
        let (id2, _) = store.resolve(None);
        assert_eq!(id2, DEFAULT_PERSONA_ID);
        let (id3, _) = store.resolve(Some("sre-incident"));
        assert_eq!(id3, "sre-incident");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn authored_persona_persists_and_cannot_forge_flags() {
        let path = tmp();
        {
            let store = PersonaStore::open(&path);
            store
                .upsert(Persona {
                    id: "mine".into(),
                    name: String::new(),
                    summary: String::new(),
                    system: "You are a contrarian reviewer.".into(),
                    builtin: true, // attempt to forge — should be cleared
                    default: true, // ditto
                })
                .unwrap();
        }
        let store = PersonaStore::open(&path);
        let mine = store.get("mine").unwrap();
        assert_eq!(mine.name, "mine"); // defaulted to id
        assert!(!mine.builtin && !mine.default);
        assert!(store.delete("mine").is_ok());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn upsert_validates() {
        let path = tmp();
        let store = PersonaStore::open(&path);
        assert!(store
            .upsert(Persona {
                id: "  ".into(),
                name: String::new(),
                summary: String::new(),
                system: "x".into(),
                builtin: false,
                default: false,
            })
            .is_err());
        assert!(store
            .upsert(Persona {
                id: "blank".into(),
                name: String::new(),
                summary: String::new(),
                system: "   ".into(),
                builtin: false,
                default: false,
            })
            .is_err());
        let _ = std::fs::remove_file(&path);
    }
}
