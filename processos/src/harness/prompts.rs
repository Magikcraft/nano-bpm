//! The prompt library: import, select, and author the prompts that drive
//! hypothesis generation.
//!
//! Hypothesis generation has two experimental knobs. The **LLM** is already one —
//! every `/api/harness/hypothesize` call can override provider / model / base-url /
//! temperature (see [`super::llm::LlmOverride`]). The **prompt** is the other, and
//! until now it was a hard-coded constant. This module makes the system prompt a
//! first-class, named, authorable object so the experimental phase can vary the
//! *generator* (prompt × LLM) the same way it varies candidate process variants —
//! and, crucially, every prompt's proposals are still measured and ranked by the
//! SimRunner, so a "better prompt" is one whose candidates actually win.
//!
//! Scope: the **system prompt** is the authorable text. The user prompt is
//! generated from scenario data ([`super::hypothesize`]); the [`Prompt`] type is
//! intentionally extensible so a user-prompt template could join later without
//! breaking the contract. The library is in-memory, seeded with the built-in
//! default, importable from a directory on startup, and authorable over the API.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// The id of the built-in, undeletable default prompt.
pub const DEFAULT_ID: &str = "default";

/// A named, authorable prompt. `system` is the system prompt handed to the model.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Prompt {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// The system prompt text.
    pub system: String,
    /// True for the seeded default; built-in prompts cannot be deleted.
    #[serde(default)]
    pub builtin: bool,
}

/// An in-memory, sorted-by-id prompt library.
#[derive(Clone, Default)]
pub struct PromptLibrary {
    prompts: BTreeMap<String, Prompt>,
}

impl PromptLibrary {
    /// A library seeded with the built-in default system prompt.
    pub fn seeded(default_system: &str) -> Self {
        let mut lib = Self::default();
        lib.prompts.insert(
            DEFAULT_ID.to_string(),
            Prompt {
                id: DEFAULT_ID.to_string(),
                name: "Built-in default".to_string(),
                description: "The default worker-swap hypothesis system prompt.".to_string(),
                system: default_system.to_string(),
                builtin: true,
            },
        );
        lib
    }

    /// All prompts, ordered by id.
    pub fn list(&self) -> Vec<Prompt> {
        self.prompts.values().cloned().collect()
    }

    /// One prompt by id.
    pub fn get(&self, id: &str) -> Option<Prompt> {
        self.prompts.get(id).cloned()
    }

    /// Resolve the system prompt text for an id, if present.
    pub fn system_of(&self, id: &str) -> Option<String> {
        self.prompts.get(id).map(|p| p.system.clone())
    }

    /// Author or import a prompt (create or update by id). Validates a non-empty id
    /// and system text. The `builtin` flag is server-controlled: an upsert can never
    /// create or clear it, and an existing built-in keeps its protection.
    pub fn upsert(&mut self, mut p: Prompt) -> Result<Prompt, String> {
        p.id = p.id.trim().to_string();
        if p.id.is_empty() {
            return Err("prompt id must not be empty".to_string());
        }
        if p.system.trim().is_empty() {
            return Err("prompt system text must not be empty".to_string());
        }
        if p.name.trim().is_empty() {
            p.name = p.id.clone();
        }
        // Preserve the built-in protection of an existing entry; never let a caller
        // mint a new built-in.
        p.builtin = self.prompts.get(&p.id).map(|e| e.builtin).unwrap_or(false);
        self.prompts.insert(p.id.clone(), p.clone());
        Ok(p)
    }

    /// Remove a prompt. Refuses to delete a built-in or an unknown id.
    pub fn remove(&mut self, id: &str) -> Result<(), String> {
        match self.prompts.get(id) {
            None => Err(format!("no such prompt: {id}")),
            Some(p) if p.builtin => Err(format!("cannot delete built-in prompt: {id}")),
            Some(_) => {
                self.prompts.remove(id);
                Ok(())
            }
        }
    }

    /// Import prompts from a directory: each `*.json` is parsed as a full [`Prompt`];
    /// each `*.md` / `*.txt` becomes a prompt whose id and name are the file stem and
    /// whose system text is the file contents. Returns how many were imported. A
    /// missing directory is not an error (returns 0); a malformed file is skipped.
    pub fn import_dir(&mut self, dir: &Path) -> Result<usize, String> {
        if !dir.exists() {
            return Ok(0);
        }
        let entries =
            std::fs::read_dir(dir).map_err(|e| format!("read_dir {}: {e}", dir.display()))?;
        let mut imported = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
                continue;
            };
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let Ok(contents) = std::fs::read_to_string(&path) else {
                continue;
            };
            let prompt = match ext.to_ascii_lowercase().as_str() {
                "json" => match serde_json::from_str::<Prompt>(&contents) {
                    Ok(p) => p,
                    Err(_) => continue,
                },
                "md" | "txt" | "prompt" => Prompt {
                    id: stem.to_string(),
                    name: stem.to_string(),
                    description: format!("Imported from {}", path.display()),
                    system: contents,
                    builtin: false,
                },
                _ => continue,
            };
            if self.upsert(prompt).is_ok() {
                imported += 1;
            }
        }
        Ok(imported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lib() -> PromptLibrary {
        PromptLibrary::seeded("DEFAULT SYSTEM PROMPT")
    }

    #[test]
    fn seeded_has_protected_default() {
        let l = lib();
        let d = l.get(DEFAULT_ID).unwrap();
        assert!(d.builtin);
        assert_eq!(d.system, "DEFAULT SYSTEM PROMPT");
        assert_eq!(l.list().len(), 1);
    }

    #[test]
    fn upsert_creates_then_updates() {
        let mut l = lib();
        let p = l
            .upsert(Prompt {
                id: "concise".to_string(),
                name: String::new(),
                description: "be terse".to_string(),
                system: "Be very concise.".to_string(),
                builtin: false,
            })
            .unwrap();
        assert_eq!(p.name, "concise"); // name defaults to id
        assert!(!p.builtin);
        assert_eq!(l.list().len(), 2);
        // Update in place.
        l.upsert(Prompt {
            id: "concise".to_string(),
            name: "Concise".to_string(),
            description: String::new(),
            system: "Be extremely concise.".to_string(),
            builtin: false,
        })
        .unwrap();
        assert_eq!(l.get("concise").unwrap().system, "Be extremely concise.");
        assert_eq!(l.list().len(), 2);
    }

    #[test]
    fn upsert_validates_and_cannot_forge_builtin() {
        let mut l = lib();
        assert!(l
            .upsert(Prompt {
                id: "  ".to_string(),
                name: String::new(),
                description: String::new(),
                system: "x".to_string(),
                builtin: false,
            })
            .is_err());
        assert!(l
            .upsert(Prompt {
                id: "empty".to_string(),
                name: String::new(),
                description: String::new(),
                system: "   ".to_string(),
                builtin: false,
            })
            .is_err());
        // A caller cannot mint a built-in.
        let p = l
            .upsert(Prompt {
                id: "sneaky".to_string(),
                name: String::new(),
                description: String::new(),
                system: "x".to_string(),
                builtin: true,
            })
            .unwrap();
        assert!(!p.builtin);
    }

    #[test]
    fn remove_protects_builtin_and_unknown() {
        let mut l = lib();
        assert!(l.remove(DEFAULT_ID).is_err());
        assert!(l.remove("missing").is_err());
        l.upsert(Prompt {
            id: "tmp".to_string(),
            name: String::new(),
            description: String::new(),
            system: "x".to_string(),
            builtin: false,
        })
        .unwrap();
        assert!(l.remove("tmp").is_ok());
        assert!(l.get("tmp").is_none());
    }

    #[test]
    fn import_dir_loads_text_and_json_and_skips_junk() {
        let dir = std::env::temp_dir().join(format!("pos-prompts-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("terse.md"), "Answer tersely.").unwrap();
        std::fs::write(
            dir.join("rich.json"),
            r#"{"id":"rich","name":"Rich","system":"Explain in depth."}"#,
        )
        .unwrap();
        std::fs::write(dir.join("notes.csv"), "ignored,row").unwrap();
        std::fs::write(dir.join("broken.json"), "{ not json").unwrap();

        let mut l = lib();
        let n = l.import_dir(&dir).unwrap();
        assert_eq!(n, 2);
        assert_eq!(l.get("terse").unwrap().system, "Answer tersely.");
        assert_eq!(l.get("rich").unwrap().system, "Explain in depth.");
        assert!(l.get("notes").is_none());
        assert!(l.get("broken").is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_missing_dir_is_not_an_error() {
        let mut l = lib();
        let n = l
            .import_dir(Path::new("/no/such/processos/prompts/dir"))
            .unwrap();
        assert_eq!(n, 0);
    }
}
