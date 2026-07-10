//! **Semantics Curator** — LLM-assisted structural annotation proposals.
//!
//! Slice 12 of the semantic-annotation pipeline (ADR 0002 spin-off). Slices
//! 9–11 gave the human authoring surfaces (flow editor, cluster editor,
//! renderer preview) for `SemanticAnnotations`. The Curator closes the
//! authoring loop by giving the human an LLM collaborator that proposes a
//! first draft on the *structural* axes — flows, clusters, roles — that the
//! human then accepts or rejects per item in the workbench's diff overlay.
//!
//! Cost and time annotations are **out of scope by design**: numbers need
//! real telemetry or explicit human estimates. Letting the LLM hallucinate
//! `p99Ms` values would undermine the whole cost/time-aware optimisation
//! loop that slices 5–7 built. This module actively strips `costs` and
//! `times` from anything the LLM emits, in case the model doesn't respect
//! the instruction.
//!
//! ## Shape
//!
//! One request = one axis = one JSON payload:
//!
//! * `Axis::Flows`    → `{"flows":    [ {id, kind, nodes: [id, …]} , … ]}`
//! * `Axis::Clusters` → `{"clusters": [ {id, nodes: [id, …], affinity} , … ]}`
//! * `Axis::Roles`    → `{"roles":    {elementId: role, …}}`
//!
//! The Curator is a one-shot LLM call ([`harness::llm::complete`]), not a
//! tool-loop agent. The workbench is naturally stateless per proposal, and
//! the axis subset is small enough that a single completion fits comfortably
//! in a model's context. Optional chat history is folded into the user
//! message so the human can iterate ("make the happy flow shorter", "drop
//! the notifications cluster").
//!
//! [`harness::llm::complete`]: crate::harness::llm::complete

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::layout::SemanticAnnotations;

/// The three structural annotation axes the Curator may propose.
///
/// Costs and times are deliberately absent — they are not axes the Curator
/// can propose, ever. See the module-level docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Axis {
    Flows,
    Clusters,
    Roles,
}

impl Axis {
    /// Parse from the wire form (`"flows"` | `"clusters"` | `"roles"`).
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "flows" => Ok(Axis::Flows),
            "clusters" => Ok(Axis::Clusters),
            "roles" => Ok(Axis::Roles),
            other => Err(format!(
                "unknown curator axis {other:?}: expected one of \"flows\", \"clusters\", \"roles\""
            )),
        }
    }

    /// The JSON key this axis emits at the top level of its proposal.
    pub fn key(self) -> &'static str {
        match self {
            Axis::Flows => "flows",
            Axis::Clusters => "clusters",
            Axis::Roles => "roles",
        }
    }

    /// The human-facing name used in prompts and error messages.
    pub fn label(self) -> &'static str {
        match self {
            Axis::Flows => "flows",
            Axis::Clusters => "clusters",
            Axis::Roles => "roles",
        }
    }
}

/// One turn of the freeform chat refinement conversation. `role` is the
/// OpenAI-style `"user"` | `"assistant"`; `content` is the message text.
///
/// The workbench holds the transcript; the server just folds it into the
/// user message on the next call.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChatTurn {
    pub role: String,
    pub content: String,
}

/// Build the `(system, user)` prompt pair for one Curator call.
///
/// The system prompt is the Semantics Curator persona (looked up by the
/// caller). The user message packs:
///
/// 1. Which axis is being requested (unambiguously — the persona is generic).
/// 2. The BPMN XML the LLM must ground itself in.
/// 3. The current annotations for context (so the LLM proposes *additions* or
///    *refinements* rather than rewriting the human's choices).
/// 4. Optional prior chat turns for iterative refinement ("make the happy
///    flow shorter").
/// 5. Optional freeform user message (the current turn's ask).
///
/// The BPMN XML is truncated at `MAX_XML_CHARS` — most workbench models are
/// well under this, but the cap protects the token budget against
/// pathological inputs.
pub fn build_user_prompt(
    axis: Axis,
    xml: &str,
    current: &SemanticAnnotations,
    history: &[ChatTurn],
    user_msg: Option<&str>,
) -> String {
    const MAX_XML_CHARS: usize = 24_000;
    let xml_snip = if xml.len() > MAX_XML_CHARS {
        format!(
            "{}\n\n… [truncated: showing first {MAX_XML_CHARS} of {} chars]",
            &xml[..MAX_XML_CHARS],
            xml.len()
        )
    } else {
        xml.to_string()
    };

    let mut buf = String::new();
    buf.push_str(&format!(
        "Propose annotations for the **{}** axis of this BPMN model.\n\n",
        axis.label()
    ));

    buf.push_str("--- BPMN XML ---\n");
    buf.push_str(&xml_snip);
    buf.push_str("\n--- END BPMN XML ---\n\n");

    // Current annotations (structural + numeric) — the LLM benefits from
    // seeing costs/times as CONTEXT even though it must not modify them, so it
    // can e.g. tell which nodes the human already considers "important".
    let current_json = serde_json::to_string_pretty(current).unwrap_or_else(|_| "{}".to_string());
    buf.push_str("--- CURRENT ANNOTATIONS (respect the operator's choices; propose additions or refinements) ---\n");
    buf.push_str(&current_json);
    buf.push_str("\n--- END CURRENT ANNOTATIONS ---\n\n");

    if !history.is_empty() {
        buf.push_str("--- PRIOR REFINEMENT CHAT (most recent last) ---\n");
        for turn in history {
            buf.push_str(&format!("{}: {}\n", turn.role, turn.content.trim()));
        }
        buf.push_str("--- END PRIOR REFINEMENT CHAT ---\n\n");
    }

    if let Some(msg) = user_msg.map(str::trim).filter(|m| !m.is_empty()) {
        buf.push_str("--- OPERATOR MESSAGE THIS TURN ---\n");
        buf.push_str(msg);
        buf.push_str("\n--- END OPERATOR MESSAGE ---\n\n");
    }

    buf.push_str(&format!(
        "Emit exactly one JSON object with the top-level key \"{}\" (and no other keys). \
         No prose, no markdown fences. If nothing meaningful to propose, emit the empty \
         container for this axis.",
        axis.key()
    ));

    buf
}

/// Parse the LLM's raw text into a validated per-axis proposal.
///
/// The returned `Value` is always the axis subset object (e.g.
/// `{"flows": [...]}`) — never anything else. Guarantees:
///
/// * `costs` and `times` keys are stripped even if the LLM emitted them.
/// * Only the requested axis's key is retained (LLM might over-emit).
/// * The axis subset shape validates against `SemanticAnnotations`
///   (round-trips through `serde_json::from_value::<SemanticAnnotations>`).
/// * Common formatting noise — ```json / ``` fences, leading prose — is
///   tolerated.
///
/// Errors carry a short human-readable reason; callers should surface them
/// in the chat pane and let the operator retry.
pub fn parse_proposal(axis: Axis, raw: &str) -> Result<Value, String> {
    let json_text = extract_json_object(raw)
        .ok_or_else(|| "LLM response did not contain a JSON object".to_string())?;

    let mut value: Value = serde_json::from_str(&json_text)
        .map_err(|e| format!("LLM response is not valid JSON: {e}"))?;

    let obj = value
        .as_object_mut()
        .ok_or_else(|| "LLM response was JSON, but not an object".to_string())?;

    // Strip explicitly-forbidden axes. If the LLM sneaks them in, we do NOT
    // pass them through — the sidecar contract is that costs/times are
    // human-only.
    obj.remove("costs");
    obj.remove("times");

    // Retain only the requested axis's key. Any other structural key the LLM
    // emitted is silently dropped: a proposal call is per-axis by contract,
    // and mixing axes would confuse the accept/reject overlay.
    let key = axis.key();
    let axis_value = obj
        .remove(key)
        .ok_or_else(|| format!("LLM response missing required top-level key \"{key}\""))?;

    // Validate shape by round-tripping through SemanticAnnotations. We build a
    // one-key envelope for the target axis; if it round-trips, the shape is
    // sound.
    let envelope = json!({ key: axis_value.clone() });
    let _: SemanticAnnotations = serde_json::from_value(envelope.clone())
        .map_err(|e| format!("proposal shape for axis \"{key}\" is invalid: {e}"))?;

    Ok(envelope)
}

/// Find the first balanced `{...}` JSON object in `raw`. Tolerates ```json
/// fences and leading/trailing prose.
///
/// Returns `None` if no top-level object is found. Does not attempt to
/// unescape strings — string tokens are treated opaquely with proper quote
/// tracking so a `}` inside a string doesn't close the object prematurely.
fn extract_json_object(raw: &str) -> Option<String> {
    let start = raw.find('{')?;
    let bytes = raw.as_bytes();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut escape = false;
    for (i, &b) in bytes.iter().enumerate().skip(start) {
        if in_str {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                in_str = false;
            }
            continue;
        }
        match b {
            b'"' => in_str = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(raw[start..=i].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_ann() -> SemanticAnnotations {
        SemanticAnnotations::default()
    }

    #[test]
    fn axis_parse_roundtrip() {
        assert_eq!(Axis::parse("flows").unwrap(), Axis::Flows);
        assert_eq!(Axis::parse("Clusters").unwrap(), Axis::Clusters);
        assert_eq!(Axis::parse("  ROLES  ").unwrap(), Axis::Roles);
        assert!(Axis::parse("costs").is_err());
        assert!(Axis::parse("times").is_err());
        assert!(Axis::parse("").is_err());
    }

    #[test]
    fn parses_bare_flows_json() {
        let raw = r#"{"flows": [{"id":"happy","kind":"primary","nodes":["Start_1","End_1"]}]}"#;
        let out = parse_proposal(Axis::Flows, raw).unwrap();
        assert_eq!(out["flows"][0]["id"], "happy");
    }

    #[test]
    fn parses_flows_with_fences_and_prose() {
        let raw =
            "Here is the proposal:\n\n```json\n{\"flows\": [{\"id\":\"h\",\"kind\":\"primary\",\
             \"nodes\":[]}]}\n```\nEnd.";
        let out = parse_proposal(Axis::Flows, raw).unwrap();
        assert_eq!(out["flows"][0]["kind"], "primary");
    }

    #[test]
    fn strips_costs_and_times_keys() {
        // LLM leaks cost/time despite the persona instruction — we MUST strip
        // them silently rather than passing them through to the workbench.
        let raw = r#"{
            "flows": [{"id":"h","kind":"primary","nodes":["A"]}],
            "costs": {"A": {"value": 42.0}},
            "times": {"A": {"p50Ms": 1000}}
        }"#;
        let out = parse_proposal(Axis::Flows, raw).unwrap();
        assert!(out.get("costs").is_none(), "costs must be stripped");
        assert!(out.get("times").is_none(), "times must be stripped");
        assert!(out.get("flows").is_some());
    }

    #[test]
    fn drops_other_axes_when_axis_is_clusters() {
        let raw = r#"{
            "flows": [{"id":"h","kind":"primary","nodes":[]}],
            "clusters": [{"id":"c","nodes":["A"],"affinity":0.5}],
            "roles": {"A":"decision"}
        }"#;
        let out = parse_proposal(Axis::Clusters, raw).unwrap();
        assert!(out.get("flows").is_none());
        assert!(out.get("roles").is_none());
        assert_eq!(out["clusters"][0]["id"], "c");
    }

    #[test]
    fn rejects_missing_target_axis() {
        let raw = r#"{"clusters": []}"#;
        let err = parse_proposal(Axis::Flows, raw).unwrap_err();
        assert!(err.contains("flows"));
    }

    #[test]
    fn rejects_invalid_shape() {
        // affinity is required to be a number, not a string
        let raw = r#"{"clusters": [{"id":"c","nodes":["A"],"affinity":"high"}]}"#;
        let err = parse_proposal(Axis::Clusters, raw).unwrap_err();
        assert!(err.contains("invalid"));
    }

    #[test]
    fn accepts_empty_container() {
        let out = parse_proposal(Axis::Roles, r#"{"roles": {}}"#).unwrap();
        assert!(out["roles"].as_object().unwrap().is_empty());
    }

    #[test]
    fn build_prompt_names_axis_and_includes_xml() {
        let ann = empty_ann();
        let prompt = build_user_prompt(Axis::Flows, "<bpmn:definitions/>", &ann, &[], None);
        assert!(prompt.contains("**flows** axis"));
        assert!(prompt.contains("<bpmn:definitions/>"));
        assert!(prompt.contains("\"flows\""));
    }

    #[test]
    fn build_prompt_folds_in_chat_history() {
        let ann = empty_ann();
        let hist = vec![
            ChatTurn {
                role: "user".into(),
                content: "keep the happy flow short".into(),
            },
            ChatTurn {
                role: "assistant".into(),
                content: "(previous JSON)".into(),
            },
        ];
        let prompt = build_user_prompt(Axis::Flows, "<x/>", &ann, &hist, Some("now drop End_1"));
        assert!(prompt.contains("PRIOR REFINEMENT CHAT"));
        assert!(prompt.contains("keep the happy flow short"));
        assert!(prompt.contains("OPERATOR MESSAGE THIS TURN"));
        assert!(prompt.contains("now drop End_1"));
    }

    #[test]
    fn build_prompt_truncates_giant_xml() {
        let ann = empty_ann();
        let giant = "x".repeat(30_000);
        let prompt = build_user_prompt(Axis::Roles, &giant, &ann, &[], None);
        assert!(prompt.contains("truncated"));
    }

    #[test]
    fn extract_json_handles_nested_and_strings() {
        // Inner `}` inside a string must NOT close the object early; nested
        // `{}` must be tracked. This isn't valid JSON (extra whitespace and
        // trailing prose are fine), but IS a balanced object.
        let raw = r#"prose {"k": "}}}", "n": {"a": 1}} tail"#;
        let out = extract_json_object(raw).unwrap();
        assert!(out.starts_with('{') && out.ends_with('}'));
        // And that balanced substring must in fact be valid JSON.
        let _: Value = serde_json::from_str(&out).unwrap();
    }
}
