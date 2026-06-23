//! Interactive cockpit chat sessions — durable, multi-turn droid conversations
//! bound to a workspace's trace dataset.
//!
//! The one-shot investigation ([`crate::investigate::run_investigation`]) answers a
//! fixed question and forgets everything. A *cockpit chat* instead lets the operator
//! converse with the droid over many turns, where each turn can issue SQL/Python tool
//! calls and the model keeps its full working memory. To make that real we persist the
//! **entire model transcript** ([`Msg`], including tool calls and their results) per
//! `(workspace, process)` key, so a turn resumes exactly where the last one left off —
//! even across a server restart.
//!
//! Storage mirrors [`crate::conversation::ConversationStore`] in spirit (one file per
//! key, sanitised so it can't escape the data dir, degrades to memory-only on a dir
//! failure) but stores a *whole session* (overwrite-on-save) rather than appending
//! single human-readable turns, because the model transcript is rewritten in full each
//! turn.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::agent::{Msg, ToolCall};

/// A persisted chat session: the full model transcript plus a last-updated stamp.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChatSession {
    #[serde(default)]
    pub messages: Vec<Msg>,
    #[serde(default)]
    pub updated: u64,
}

/// A file-backed chat-session store keyed by a sanitised `(workspace, process)` key.
pub struct ChatStore {
    dir: PathBuf,
    mem: RwLock<HashMap<String, ChatSession>>,
}

impl ChatStore {
    /// Open (creating if needed) a store rooted at `dir`. A dir-create failure degrades
    /// to memory-only rather than failing the server.
    pub fn open(dir: impl Into<PathBuf>) -> Self {
        let dir = dir.into();
        if let Err(e) = fs::create_dir_all(&dir) {
            tracing::warn!(dir = %dir.display(), error = %e, "chat store: dir create failed; memory-only");
        }
        Self {
            dir,
            mem: RwLock::new(HashMap::new()),
        }
    }

    /// Load a session's transcript (empty when none exists yet).
    pub fn load(&self, key: &str) -> Vec<Msg> {
        self.ensure_loaded(key);
        self.mem
            .read()
            .ok()
            .and_then(|m| m.get(key).map(|s| s.messages.clone()))
            .unwrap_or_default()
    }

    /// Replace a session's transcript with `messages` and persist it.
    pub fn save(&self, key: &str, messages: Vec<Msg>) {
        let session = ChatSession {
            messages,
            updated: now_ms(),
        };
        if let Ok(mut mem) = self.mem.write() {
            mem.insert(key.to_string(), session.clone());
        }
        if let Err(e) = self.persist(key, &session) {
            tracing::warn!(key = %key, error = %e, "chat store: persist failed");
        }
    }

    /// Clear a session (forget the conversation).
    pub fn clear(&self, key: &str) {
        if let Ok(mut mem) = self.mem.write() {
            mem.remove(key);
        }
        let _ = fs::remove_file(self.path_for(key));
    }

    fn ensure_loaded(&self, key: &str) {
        if self.mem.read().map(|m| m.contains_key(key)).unwrap_or(false) {
            return;
        }
        let loaded = self.load_from_disk(key).unwrap_or_default();
        if let Ok(mut mem) = self.mem.write() {
            mem.entry(key.to_string()).or_insert(loaded);
        }
    }

    fn load_from_disk(&self, key: &str) -> Option<ChatSession> {
        let body = fs::read_to_string(self.path_for(key)).ok()?;
        serde_json::from_str(&body).ok()
    }

    fn persist(&self, key: &str, session: &ChatSession) -> std::io::Result<()> {
        let body = serde_json::to_string(session)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        fs::write(self.path_for(key), body)
    }

    /// One file per `(workspace, process)`; the key is sanitised so it can never escape
    /// `dir`.
    fn path_for(&self, key: &str) -> PathBuf {
        let safe: String = key
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.dir.join(format!("chat-{safe}.json"))
    }
}

/// Build the store's per-`(workspace, process)` key.
pub fn session_key(workspace: &str, process: &str) -> String {
    let san = |s: &str| -> String {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    };
    format!("{}__{}", san(workspace), san(process))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---- View projection: transcript → operator-facing turns --------------------------

/// One tool call + result the droid ran while answering (the lab notebook line).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatStepView {
    pub tool: String,
    pub arguments: serde_json::Value,
    pub result: String,
}

/// One operator-facing turn in the chat.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatTurnView {
    /// `user` (the operator) or `droid` (the model).
    pub role: String,
    pub text: String,
    /// For droid turns, the tool calls it ran before replying.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<ChatStepView>,
}

/// Project a model transcript into operator-facing turns: each `User` message becomes a
/// user turn; the model's tool calls are paired with their `Tool` results into steps and
/// attached to the next droid turn (an `Assistant` message carrying final text). The
/// system message is intentionally omitted.
pub fn render_view(messages: &[Msg]) -> Vec<ChatTurnView> {
    let mut turns: Vec<ChatTurnView> = Vec::new();
    // Tool calls awaiting their results / a final answer, by call_id.
    let mut pending: Vec<ToolCall> = Vec::new();
    let mut steps: Vec<ChatStepView> = Vec::new();

    for msg in messages {
        match msg {
            Msg::System(_) => {}
            Msg::User(text) => {
                turns.push(ChatTurnView {
                    role: "user".into(),
                    text: text.clone(),
                    steps: Vec::new(),
                });
            }
            Msg::Assistant { text, tool_calls } => {
                if !tool_calls.is_empty() {
                    // A tool-requesting turn: remember the calls so the following Tool
                    // messages can be matched to them.
                    pending.extend(tool_calls.iter().cloned());
                } else {
                    // A final answer: drain accumulated steps onto this droid turn.
                    turns.push(ChatTurnView {
                        role: "droid".into(),
                        text: text.clone().unwrap_or_default(),
                        steps: std::mem::take(&mut steps),
                    });
                    pending.clear();
                }
            }
            Msg::Tool { call_id, content } => {
                if let Some(pos) = pending.iter().position(|c| c.id == *call_id) {
                    let call = pending.remove(pos);
                    steps.push(ChatStepView {
                        tool: call.name,
                        arguments: call.arguments,
                        result: content.clone(),
                    });
                } else {
                    steps.push(ChatStepView {
                        tool: "tool".into(),
                        arguments: serde_json::Value::Null,
                        result: content.clone(),
                    });
                }
            }
        }
    }
    turns
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tmp() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("processos-chat-{}-{}", now_ms(), n));
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn save_load_round_trips_and_survives_reopen() {
        let dir = tmp();
        let key = session_key("acme", "loan");
        {
            let store = ChatStore::open(&dir);
            store.save(
                &key,
                vec![Msg::System("s".into()), Msg::User("hi".into())],
            );
        }
        let store = ChatStore::open(&dir);
        let loaded = store.load(&key);
        assert_eq!(loaded.len(), 2);
        store.clear(&key);
        assert!(store.load(&key).is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn keys_cannot_escape_the_data_dir() {
        let dir = tmp();
        let store = ChatStore::open(&dir);
        store.save("../../etc/passwd", vec![Msg::User("x".into())]);
        let entries: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(entries.iter().all(|n| !n.contains('/')));
        assert!(entries.iter().any(|n| n.ends_with(".json")));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn render_view_pairs_tool_calls_with_results() {
        let transcript = vec![
            Msg::System("sys".into()),
            Msg::User("why slow?".into()),
            Msg::Assistant {
                text: None,
                tool_calls: vec![ToolCall {
                    id: "c1".into(),
                    name: "query_traces".into(),
                    arguments: json!({"sql": "SELECT 1"}),
                }],
            },
            Msg::Tool {
                call_id: "c1".into(),
                content: "1".into(),
            },
            Msg::Assistant {
                text: Some("It's the credit-check job.".into()),
                tool_calls: vec![],
            },
        ];
        let view = render_view(&transcript);
        assert_eq!(view.len(), 2);
        assert_eq!(view[0].role, "user");
        assert_eq!(view[0].text, "why slow?");
        assert_eq!(view[1].role, "droid");
        assert_eq!(view[1].text, "It's the credit-check job.");
        assert_eq!(view[1].steps.len(), 1);
        assert_eq!(view[1].steps[0].tool, "query_traces");
        assert_eq!(view[1].steps[0].result, "1");
    }
}
