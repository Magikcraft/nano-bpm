//! Psychological-trace export/import.
//!
//! A *psychological trace* is a single, self-contained zip that captures everything needed to
//! remotely **replay and debug** an investigation that ran on someone else's machine — without
//! shipping the dataset or the model weights (those are referenced by name only, for now):
//!
//! ```text
//! psych-trace-<slug>.zip
//!   manifest.json          provenance: schema, processos version, dataset/model names,
//!                          persona, the LLM profiles used (API keys redacted), counts
//!   session.json           the full [`ChatSession`] (transcript + per-turn model attribution)
//!                          so the recipient can reconstruct it verbatim as an Investigation
//!   transcript.debug.md    the human-readable debug transcript (reasoning timeline + tool calls)
//!   persona.md             the standing system prompt (persona) that drove the investigation
//!   debug-requests.json    the exact model request payloads of the most recent turn (if captured)
//! ```
//!
//! The recipient imports the zip; it lands as an `"imported"`-origin session rendered with a
//! special icon, ready for replay. API keys are **always** redacted on export — a trace is meant
//! to be Slacked around, so it must never carry a secret.

use std::io::{Cursor, Read, Write};

use serde::{Deserialize, Serialize};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

use crate::chat::ChatSession;
use crate::settings::LlmProfile;

/// Schema marker stamped into every manifest (and checked on import).
pub const SCHEMA: &str = "nano.processos.psych-trace/v1";

/// Replacement value for any non-empty API key on export.
const REDACTED: &str = "***redacted***";

/// Provenance + index for a shared trace. Dataset = `workspace`, model = `process`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceManifest {
    pub schema: String,
    pub processos_version: String,
    pub exported_at: u64,
    /// The dataset (corpus / workspace) the investigation ran against.
    pub workspace: String,
    /// The BPMN process model under investigation.
    pub process: String,
    pub session_id: String,
    pub session_name: String,
    /// Persona id bound to the session (its standing system prompt).
    pub persona: String,
    /// Distinct LLM model labels that drove a turn (insertion order).
    pub models: Vec<String>,
    pub turns: usize,
    /// Number of captured per-round debug request payloads (0 if none were live).
    pub debug_rounds: usize,
    /// The LLM profiles referenced by the session, **with API keys redacted**.
    pub llm_profiles: Vec<serde_json::Value>,
}

/// Project an [`LlmProfile`] to a JSON value with any non-empty `api_key` redacted, so a trace
/// can be shared freely without leaking a secret.
pub fn redact_profile(profile: &LlmProfile) -> serde_json::Value {
    let mut v = serde_json::to_value(profile).unwrap_or(serde_json::Value::Null);
    if let Some(obj) = v.as_object_mut() {
        if obj
            .get("apiKey")
            .and_then(|k| k.as_str())
            .is_some_and(|s| !s.is_empty())
        {
            obj.insert(
                "apiKey".to_string(),
                serde_json::Value::String(REDACTED.to_string()),
            );
        }
    }
    v
}

/// Bundle a manifest + session + transcripts into the shareable zip (in memory).
pub fn build_zip(
    manifest: &TraceManifest,
    session: &ChatSession,
    transcript_md: &str,
    persona_md: &str,
    debug_json: &serde_json::Value,
) -> Result<Vec<u8>, String> {
    let mut zw = ZipWriter::new(Cursor::new(Vec::new()));
    let opts = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);

    let manifest_bytes =
        serde_json::to_vec_pretty(manifest).map_err(|e| format!("serialise manifest: {e}"))?;
    let session_bytes =
        serde_json::to_vec_pretty(session).map_err(|e| format!("serialise session: {e}"))?;
    let debug_bytes =
        serde_json::to_vec_pretty(debug_json).map_err(|e| format!("serialise debug: {e}"))?;

    let files: [(&str, &[u8]); 5] = [
        ("manifest.json", &manifest_bytes),
        ("session.json", &session_bytes),
        ("transcript.debug.md", transcript_md.as_bytes()),
        ("persona.md", persona_md.as_bytes()),
        ("debug-requests.json", &debug_bytes),
    ];
    for (name, bytes) in files {
        zw.start_file(name, opts)
            .map_err(|e| format!("zip {name}: {e}"))?;
        zw.write_all(bytes)
            .map_err(|e| format!("write {name}: {e}"))?;
    }

    let cursor = zw.finish().map_err(|e| format!("finalise zip: {e}"))?;
    Ok(cursor.into_inner())
}

/// Parse a shared trace zip back into its manifest + session for import. Validates the schema
/// marker so an arbitrary uploaded zip is rejected with a clear message.
pub fn parse_zip(bytes: Vec<u8>) -> Result<(TraceManifest, ChatSession), String> {
    let mut archive =
        ZipArchive::new(Cursor::new(bytes)).map_err(|e| format!("not a valid zip: {e}"))?;

    let manifest: TraceManifest = read_json(&mut archive, "manifest.json")?;
    if !manifest.schema.starts_with("nano.processos.psych-trace") {
        return Err(format!("unrecognised trace schema: {}", manifest.schema));
    }
    let session: ChatSession = read_json(&mut archive, "session.json")?;
    Ok((manifest, session))
}

fn read_json<T: serde::de::DeserializeOwned>(
    archive: &mut ZipArchive<Cursor<Vec<u8>>>,
    name: &str,
) -> Result<T, String> {
    let mut entry = archive
        .by_name(name)
        .map_err(|_| format!("trace is missing {name}"))?;
    let mut text = String::new();
    entry
        .read_to_string(&mut text)
        .map_err(|e| format!("read {name}: {e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("bad {name}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chat::ChatSession;

    fn sample_profile() -> LlmProfile {
        LlmProfile {
            id: "p1".into(),
            name: "Qwen3".into(),
            provider: None,
            base_url: Some("http://127.0.0.1:18080".into()),
            model: Some("qwen3".into()),
            api_key: Some("sk-secret-value".into()),
            max_tokens: None,
            context_window: None,
            temperature: None,
            sidecar: true,
            model_file: None,
            sidecar_args: None,
            mtp: false,
            thinking_level: None,
            starred: false,
        }
    }

    #[test]
    fn redact_profile_hides_api_key_but_keeps_config() {
        let v = redact_profile(&sample_profile());
        assert_eq!(v["apiKey"], serde_json::json!("***redacted***"));
        assert_eq!(v["baseUrl"], serde_json::json!("http://127.0.0.1:18080"));
        assert_eq!(v["model"], serde_json::json!("qwen3"));
    }

    #[test]
    fn redact_profile_leaves_empty_key_untouched() {
        let mut p = sample_profile();
        p.api_key = None;
        let v = redact_profile(&p);
        assert!(v["apiKey"].is_null());
    }

    #[test]
    fn zip_round_trips_manifest_and_session() {
        let mut session = ChatSession {
            id: "s-original".into(),
            name: "North Wind Bank".into(),
            persona: "investigator".into(),
            models: vec!["qwen3".into()],
            origin: String::new(),
            ..Default::default()
        };
        session.imported_from = None;

        let manifest = TraceManifest {
            schema: SCHEMA.into(),
            processos_version: "0.1.0".into(),
            exported_at: 123,
            workspace: "meridian-trust".into(),
            process: "cdd-refresh".into(),
            session_id: session.id.clone(),
            session_name: session.name.clone(),
            persona: session.persona.clone(),
            models: session.models.clone(),
            turns: 4,
            debug_rounds: 2,
            llm_profiles: vec![redact_profile(&sample_profile())],
        };

        let bytes = build_zip(
            &manifest,
            &session,
            "# debug transcript",
            "persona prompt",
            &serde_json::json!([{"round": 1}]),
        )
        .expect("build");

        let (m2, s2) = parse_zip(bytes).expect("parse");
        assert_eq!(m2.workspace, "meridian-trust");
        assert_eq!(m2.process, "cdd-refresh");
        assert_eq!(
            m2.llm_profiles[0]["apiKey"],
            serde_json::json!("***redacted***")
        );
        assert_eq!(s2.id, "s-original");
        assert_eq!(s2.name, "North Wind Bank");
        assert_eq!(s2.models, vec!["qwen3".to_string()]);
    }

    #[test]
    fn parse_zip_rejects_non_zip() {
        assert!(parse_zip(b"not a zip".to_vec()).is_err());
    }

    #[test]
    fn parse_zip_rejects_foreign_schema() {
        let session = ChatSession::default();
        let manifest = TraceManifest {
            schema: "something.else/v9".into(),
            processos_version: "0.1.0".into(),
            exported_at: 0,
            workspace: "w".into(),
            process: "p".into(),
            session_id: String::new(),
            session_name: String::new(),
            persona: String::new(),
            models: vec![],
            turns: 0,
            debug_rounds: 0,
            llm_profiles: vec![],
        };
        let bytes = build_zip(&manifest, &session, "", "", &serde_json::json!(null)).unwrap();
        let err = parse_zip(bytes).unwrap_err();
        assert!(err.contains("unrecognised trace schema"), "{err}");
    }
}
