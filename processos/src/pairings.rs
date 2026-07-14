//! The cockpit **LLM-pairing library** — named, first-class "Pair AI" configurations.
//!
//! A *pairing* bundles everything needed to run a second model alongside the primary in one of
//! four collaboration [`PairMode`]s, so the operator picks a saved pairing instead of assembling
//! a mode + model + prompt ad-hoc every turn:
//!
//! - the **mode** (review / monitor / delegate / speculate),
//! - the **secondary** LLM profile (the second model),
//! - an optional **primary** LLM profile (set it for a full "team"; leave it blank to keep using
//!   whatever primary is currently active),
//! - the **system prompt** for the second model — stored inline so a pairing is self-contained
//!   (empty means "use this mode's default persona prompt"),
//! - and, for [`PairMode::Subagent`], the delegation tuning (`maxRounds` / `digestCap`).
//!
//! Pairings are persisted to `<config_dir>/pairings.json` (no secrets live here — they only
//! reference profile ids — so the file uses default permissions) and so follow the user across
//! workspaces, exactly like [`crate::personas`].

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

/// How a pairing's secondary model collaborates with the primary. Pair/Monitor/Subagent mirror
/// the three partner roles the chat turn supports (Pair AI reviewer, loop monitor, delegation
/// subagent); Speculator pairs the models at the **inference engine** level instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PairMode {
    /// A reviewer that runs *after* the primary each turn and pressure-tests its answer.
    #[default]
    Pair,
    /// A supervisor that watches the primary's live transcript and steers it out of loops.
    Monitor,
    /// A worker the primary can DELEGATE self-contained research tasks to (the `delegate` tool).
    Subagent,
    /// The sparring-partner slot becomes a **speculative-decoding draft model**: the primary's
    /// `llama-server` loads the secondary profile's GGUF via `--model-draft` and uses it to
    /// propose tokens the primary verifies. Unlike the chat-level modes, the secondary never runs
    /// as its own sidecar and contributes no prose — it only accelerates the primary. The same
    /// model may be named on both sides ("loaded twice", self-speculation). Both sides must be
    /// local sidecar GGUFs with compatible tokenizers/vocab; the API validates and refuses
    /// incompatible configs (see `gguf::speculator_compatible`).
    Speculator,
    /// A small local sidecar that writes **model IR under GBNF grammar constraint**: the primary
    /// gets a `draft_ir` tool; calling it fires ONE grammar-constrained completion on the secondary
    /// (temperature ~0, the emitted `ir.gbnf` loaded as the `grammar` field) that returns a
    /// well-formed IR document, which the primary reviews and deploys via `write_model_ir`. This is
    /// the *app-level* sibling of [`PairMode::Speculator`]: the secondary contributes no prose and
    /// runs no tool loop — the grammar is a "leveller" that lets a tiny model emit syntactically
    /// valid IR as reliably as a large one. No `maxRounds` / `digestCap` / draft-token tuning apply.
    Drafter,
}

/// One saved Pair AI configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pairing {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub mode: PairMode,
    /// The saved LLM profile the *second* model runs on (required).
    #[serde(default)]
    pub secondary_profile_id: String,
    /// An optional primary LLM profile. When set, selecting this pairing also switches the active
    /// primary to it (a full "team"); when absent, the current/default primary is kept.
    #[serde(default)]
    pub primary_profile_id: Option<String>,
    /// The inline system prompt for the second model. Empty means "use this mode's default
    /// persona prompt" so the pairing still runs without authoring a prompt.
    #[serde(default)]
    pub system: String,
    /// Subagent only: tool-loop budget per delegated task (server clamps 1..30; default 8).
    #[serde(default)]
    pub max_rounds: Option<usize>,
    /// Subagent only: max chars of digest fed back to the primary (server clamps 500..20000).
    #[serde(default)]
    pub digest_cap: Option<usize>,
    /// Speculator only: `--draft-max` — most tokens drafted per step (llama-server default when
    /// unset; server clamps 1..64).
    #[serde(default)]
    pub draft_max: Option<u32>,
    /// Speculator only: `--draft-min` — fewest tokens drafted per step (server clamps 0..16).
    #[serde(default)]
    pub draft_min: Option<u32>,
    /// Optional allowlist of tool names the PRIMARY model may use under this pairing. `None` =
    /// the full available surface. Lets a pairing scope the primary's tools as well as the team.
    #[serde(default)]
    pub primary_tools: Option<Vec<String>>,
    /// Optional allowlist of tool names the SECONDARY (paired) model may use. `None` = full surface.
    #[serde(default)]
    pub secondary_tools: Option<Vec<String>>,
    /// True for seeded built-ins (cannot be deleted/edited). Reserved; the library ships empty.
    #[serde(default)]
    pub builtin: bool,
}

/// A file-backed, sorted-by-id pairing library.
pub struct PairingStore {
    path: PathBuf,
    pairings: RwLock<BTreeMap<String, Pairing>>,
}

impl PairingStore {
    /// Open the store at `path`, loading any persisted pairings. The library ships empty: the
    /// operator authors their own (the whole point — first-class named configs).
    pub fn open(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let mut pairings = BTreeMap::new();
        if let Ok(body) = std::fs::read_to_string(&path) {
            if let Ok(saved) = serde_json::from_str::<Vec<Pairing>>(&body) {
                for mut p in saved {
                    p.builtin = false; // the builtin flag is server-owned
                    pairings.insert(p.id.clone(), p);
                }
            }
        }
        Self {
            path,
            pairings: RwLock::new(pairings),
        }
    }

    /// All pairings, ordered by id.
    pub fn list(&self) -> Vec<Pairing> {
        self.pairings
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Author or update a pairing (create or update by id). Validates a non-empty id, name and
    /// secondary profile; an empty `system` is allowed (the mode default is used at run time).
    pub fn upsert(&self, mut p: Pairing) -> Result<Pairing, String> {
        p.id = p.id.trim().to_string();
        if p.id.is_empty() {
            return Err("pairing id must not be empty".to_string());
        }
        if p.secondary_profile_id.trim().is_empty() {
            return Err("a pairing must name a second-model profile".to_string());
        }
        if p.name.trim().is_empty() {
            p.name = p.id.clone();
        }
        // Normalise an empty/blank primary override to None so "use current primary" round-trips.
        if p.primary_profile_id.as_deref().map(str::trim) == Some("") {
            p.primary_profile_id = None;
        }
        // Clamp the speculator draft tuning into llama-server-sane ranges (mirrors the
        // subagent maxRounds/digestCap clamping done server-side).
        p.draft_max = p.draft_max.map(|v| v.clamp(1, 64));
        p.draft_min = p.draft_min.map(|v| v.clamp(0, 16));
        if let Ok(mut m) = self.pairings.write() {
            let existing = m.get(&p.id);
            // A caller may never mint the server-owned builtin flag.
            p.builtin = existing.map(|e| e.builtin).unwrap_or(false);
            if existing.map(|e| e.builtin).unwrap_or(false) {
                return Err(format!("cannot edit built-in pairing: {}", p.id));
            }
            m.insert(p.id.clone(), p.clone());
        }
        self.persist();
        Ok(p)
    }

    /// Remove a pairing. Refuses to delete a built-in or an unknown id.
    pub fn delete(&self, id: &str) -> Result<(), String> {
        {
            let mut m = self
                .pairings
                .write()
                .map_err(|_| "pairing store lock poisoned".to_string())?;
            match m.get(id) {
                None => return Err(format!("no such pairing: {id}")),
                Some(p) if p.builtin => {
                    return Err(format!("cannot delete built-in pairing: {id}"))
                }
                Some(_) => {
                    m.remove(id);
                }
            }
        }
        self.persist();
        Ok(())
    }

    /// Persist the non-built-in pairings to disk (best-effort).
    fn persist(&self) {
        let saved: Vec<Pairing> = self
            .pairings
            .read()
            .map(|m| m.values().filter(|p| !p.builtin).cloned().collect())
            .unwrap_or_default();
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match serde_json::to_string_pretty(&saved) {
            Ok(body) => {
                if let Err(e) = std::fs::write(&self.path, body) {
                    tracing::warn!(path = %self.path.display(), error = %e, "pairings: persist failed");
                }
            }
            Err(e) => tracing::warn!(error = %e, "pairings: serialize failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        std::env::temp_dir().join(format!("pairings-test-{}.json", std::process::id()))
    }

    #[test]
    fn upsert_requires_id_and_secondary_profile() {
        let store = PairingStore::open(tmp());
        let bad = Pairing {
            id: "  ".into(),
            name: "x".into(),
            mode: PairMode::Pair,
            secondary_profile_id: "p".into(),
            primary_profile_id: None,
            system: String::new(),
            max_rounds: None,
            digest_cap: None,
            draft_max: None,
            draft_min: None,
            primary_tools: None,
            secondary_tools: None,
            builtin: false,
        };
        assert!(store.upsert(bad).is_err());
        let no_model = Pairing {
            id: "team".into(),
            name: "Team".into(),
            mode: PairMode::Monitor,
            secondary_profile_id: "  ".into(),
            primary_profile_id: None,
            system: String::new(),
            max_rounds: None,
            digest_cap: None,
            draft_max: None,
            draft_min: None,
            primary_tools: None,
            secondary_tools: None,
            builtin: false,
        };
        assert!(store.upsert(no_model).is_err());
    }

    #[test]
    fn upsert_list_get_and_delete_round_trip() {
        let path = tmp();
        let _ = std::fs::remove_file(&path);
        let store = PairingStore::open(&path);
        let p = Pairing {
            id: "skeptic-pair".into(),
            name: "Skeptic Pair".into(),
            mode: PairMode::Pair,
            secondary_profile_id: "qwen3-4b-local".into(),
            primary_profile_id: Some("  ".into()), // blank → normalised to None
            system: "Pressure-test the answer.".into(),
            max_rounds: None,
            digest_cap: None,
            draft_max: None,
            draft_min: None,
            primary_tools: None,
            secondary_tools: None,
            builtin: false,
        };
        let saved = store.upsert(p).expect("upsert ok");
        assert!(saved.primary_profile_id.is_none());
        assert_eq!(store.list().len(), 1);
        let got = store
            .list()
            .into_iter()
            .find(|x| x.id == "skeptic-pair")
            .unwrap();
        assert_eq!(got.name, "Skeptic Pair");

        // Reopen from disk to prove persistence.
        let store2 = PairingStore::open(&path);
        let reloaded = store2
            .list()
            .into_iter()
            .find(|x| x.id == "skeptic-pair")
            .unwrap();
        assert_eq!(reloaded.system, "Pressure-test the answer.");

        store2.delete("skeptic-pair").expect("delete ok");
        assert!(store2.list().into_iter().all(|x| x.id != "skeptic-pair"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn drafter_pairing_round_trips_and_needs_secondary() {
        let path =
            std::env::temp_dir().join(format!("pairings-drafter-test-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let store = PairingStore::open(&path);
        // A drafter must still name a second (IR-writer) model.
        let no_secondary = Pairing {
            id: "ir-drafter".into(),
            name: "IR Drafter".into(),
            mode: PairMode::Drafter,
            secondary_profile_id: "  ".into(),
            primary_profile_id: None,
            system: String::new(),
            max_rounds: None,
            digest_cap: None,
            draft_max: None,
            draft_min: None,
            primary_tools: None,
            secondary_tools: None,
            builtin: false,
        };
        assert!(store.upsert(no_secondary).is_err());
        // A well-formed drafter saves and round-trips its mode through serde (lowercase tag).
        let ok = Pairing {
            id: "ir-drafter".into(),
            name: "IR Drafter".into(),
            mode: PairMode::Drafter,
            secondary_profile_id: "qwen3-1_7b-local".into(),
            primary_profile_id: None,
            system: String::new(),
            max_rounds: None,
            digest_cap: None,
            draft_max: None,
            draft_min: None,
            primary_tools: None,
            secondary_tools: None,
            builtin: false,
        };
        store.upsert(ok).expect("drafter upsert ok");
        let store2 = PairingStore::open(&path);
        let reloaded = store2
            .list()
            .into_iter()
            .find(|x| x.id == "ir-drafter")
            .expect("drafter persisted");
        assert_eq!(reloaded.mode, PairMode::Drafter);
        let _ = std::fs::remove_file(&path);
    }
}
