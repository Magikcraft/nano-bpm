//! The cockpit **chat prompt library** — reusable operator message templates.
//!
//! Distinct from the [`crate::harness::prompts`] *system*-prompt library (which steers
//! hypothesis generation), these are **user-message templates** the operator loads into
//! the chat compose box and sends to the droid — e.g. "find the worst queue tail and
//! localise it to a time window". New prompts the operator authors are persisted to the
//! user's config dir (`<config_dir>/chat-prompts.json`) so they follow the user across
//! workspaces, alongside `settings.json`.
//!
//! No secrets live here, so (unlike settings.json) the file uses default permissions.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

/// A reusable chat prompt the operator can load into the compose box.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatPrompt {
    pub id: String,
    #[serde(default)]
    pub name: String,
    /// The message text loaded into the compose box.
    pub text: String,
    /// True for seeded built-ins (cannot be deleted).
    #[serde(default)]
    pub builtin: bool,
}

/// A file-backed, sorted-by-id chat-prompt library.
pub struct ChatPromptStore {
    path: PathBuf,
    prompts: RwLock<BTreeMap<String, ChatPrompt>>,
}

impl ChatPromptStore {
    /// Open the store at `path`, seeding built-ins and merging any persisted prompts.
    pub fn open(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let mut prompts = builtins();
        if let Ok(body) = std::fs::read_to_string(&path) {
            if let Ok(saved) = serde_json::from_str::<Vec<ChatPrompt>>(&body) {
                for mut p in saved {
                    // Persisted prompts are never built-in (built-ins are seeded fresh);
                    // a saved id matching a built-in id overrides nothing important.
                    p.builtin = false;
                    prompts.insert(p.id.clone(), p);
                }
            }
        }
        Self {
            path,
            prompts: RwLock::new(prompts),
        }
    }

    /// All prompts, ordered by id.
    pub fn list(&self) -> Vec<ChatPrompt> {
        self.prompts
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Author or update a prompt (create or update by id). Validates a non-empty id and
    /// text; the `builtin` flag is server-controlled.
    pub fn upsert(&self, mut p: ChatPrompt) -> Result<ChatPrompt, String> {
        p.id = p.id.trim().to_string();
        if p.id.is_empty() {
            return Err("prompt id must not be empty".to_string());
        }
        if p.text.trim().is_empty() {
            return Err("prompt text must not be empty".to_string());
        }
        if p.name.trim().is_empty() {
            p.name = p.id.clone();
        }
        if let Ok(mut m) = self.prompts.write() {
            // Preserve an existing built-in flag; never let a caller mint one.
            p.builtin = m.get(&p.id).map(|e| e.builtin).unwrap_or(false);
            m.insert(p.id.clone(), p.clone());
        }
        self.persist();
        Ok(p)
    }

    /// Remove a prompt. Refuses to delete a built-in or an unknown id.
    pub fn delete(&self, id: &str) -> Result<(), String> {
        {
            let mut m = self
                .prompts
                .write()
                .map_err(|_| "prompt store lock poisoned".to_string())?;
            match m.get(id) {
                None => return Err(format!("no such prompt: {id}")),
                Some(p) if p.builtin => {
                    return Err(format!("cannot delete built-in prompt: {id}"))
                }
                Some(_) => {
                    m.remove(id);
                }
            }
        }
        self.persist();
        Ok(())
    }

    /// Persist the non-built-in prompts to disk (best-effort).
    fn persist(&self) {
        let saved: Vec<ChatPrompt> = self
            .prompts
            .read()
            .map(|m| m.values().filter(|p| !p.builtin).cloned().collect())
            .unwrap_or_default();
        if let Some(parent) = self.path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match serde_json::to_string_pretty(&saved) {
            Ok(body) => {
                if let Err(e) = std::fs::write(&self.path, body) {
                    tracing::warn!(path = %self.path.display(), error = %e, "chat-prompts: persist failed");
                }
            }
            Err(e) => tracing::warn!(error = %e, "chat-prompts: serialize failed"),
        }
    }
}

/// The seeded built-in chat prompts.
fn builtins() -> BTreeMap<String, ChatPrompt> {
    let seed = [
        ChatPrompt {
            id: "open".into(),
            name: "Open investigation".into(),
            text: "Investigate this dataset with fresh eyes. Profile the process end to \
                   end — volumes, latency, where time is spent, failures, and how any of \
                   these move over time — and surface the single most important problem \
                   you can find, with evidence. Don't assume where it is; let the data \
                   tell you. Then say what you'd recommend and why."
                .into(),
            builtin: true,
        },
        ChatPrompt {
            id: "worker-swap".into(),
            name: "Worker swap hypothesis".into(),
            text: "Test one specific hypothesis: the dominant latency comes from a single \
                   job type saturating during a recurring peak, and adding workers to that \
                   job type during that window would cut the tail. Identify the job type \
                   and the window from the data, quantify the tail (e.g. p99 queue) against \
                   its off-peak baseline with sample sizes, and estimate what headroom a \
                   larger worker pool would buy. If the data doesn't support the \
                   hypothesis, say so."
                .into(),
            builtin: true,
        },
        ChatPrompt {
            id: "temporal".into(),
            name: "Look for a pattern over time".into(),
            text: "Does anything about this process change with time of day or day of \
                   week? Compare the relevant metric across hour and day-of-week buckets, \
                   report effect sizes and sample sizes, and replicate any pattern you \
                   find on a held-out slice before trusting it."
                .into(),
            builtin: true,
        },
        ChatPrompt {
            id: "failures".into(),
            name: "Investigate failures & incidents".into(),
            text: "Where are failures and incidents concentrated? Break them down by job \
                   type and element, with counts and rates, and propose the most likely \
                   cause."
                .into(),
            builtin: true,
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
        std::env::temp_dir().join(format!(
            "processos-chatprompts-{}-{}.json",
            std::process::id(),
            n
        ))
    }

    #[test]
    fn seeded_builtins_present_and_protected() {
        let path = tmp();
        let store = ChatPromptStore::open(&path);
        assert!(store.list().len() >= 3);
        assert!(store.delete("open").is_err());
        assert!(store.delete("missing").is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn authored_prompt_persists_across_reopen() {
        let path = tmp();
        {
            let store = ChatPromptStore::open(&path);
            store
                .upsert(ChatPrompt {
                    id: "mine".into(),
                    name: String::new(),
                    text: "Show me retries by element.".into(),
                    builtin: true, // attempt to forge — should be cleared
                })
                .unwrap();
        }
        let store = ChatPromptStore::open(&path);
        let mine = store.list().into_iter().find(|p| p.id == "mine").unwrap();
        assert_eq!(mine.name, "mine"); // defaulted to id
        assert!(!mine.builtin);
        assert!(store.delete("mine").is_ok());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn upsert_validates() {
        let path = tmp();
        let store = ChatPromptStore::open(&path);
        assert!(store
            .upsert(ChatPrompt {
                id: "  ".into(),
                name: String::new(),
                text: "x".into(),
                builtin: false,
            })
            .is_err());
        assert!(store
            .upsert(ChatPrompt {
                id: "blank".into(),
                name: String::new(),
                text: "   ".into(),
                builtin: false,
            })
            .is_err());
        let _ = std::fs::remove_file(&path);
    }
}
