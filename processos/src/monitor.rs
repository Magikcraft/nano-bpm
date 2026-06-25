//! Loop monitor: a second LLM that watches the primary investigation's live transcript and,
//! when it detects the primary going in *circles* — repeating reasoning, re-attacking an
//! unreachable path, oscillating between hypotheses, or thinking for many rounds without acting
//! — nudges it back on track. It writes into the SAME steer/cancel channels the human operator's
//! "Steer" and "Wrap it up" controls use, so it is just an *automated operator*: no agent-loop
//! changes are needed.
//!
//! Why a model and not more heuristics: the existing guards in [`crate::agent`]
//! (`is_runaway_repetition`, `runaway_tail`, the duplicate-tool-call check) catch *lexical*
//! loops — the same line or the same tool call repeated verbatim. They are blind to *semantic*
//! circling, where the agent re-states the same dead-end in fresh words or keeps probing a path
//! that the current constraints make unreachable. Judging that requires reading meaning, which is
//! what this monitor is for.
//!
//! Authority (operator default): the monitor prefers to STEER, and only escalates to a forced
//! wrap-up after it has spent its steer budget and the primary is *still* circling. False
//! negatives (missing a loop) are deliberately preferred to false positives (interrupting good
//! work): any parse ambiguity is treated as "no intervention".

use crate::agent::{AgentStep, Msg, OpenAiAgent, Turn};
use crate::harness::llm::LlmConfig;

/// How many lines the rendered transcript window keeps from the tail, and the per-message text
/// cap, so the monitor sees the *recent* trajectory without paying for the whole conversation.
const WINDOW_MAX_CHARS: usize = 6000;

/// The monitor's read on the primary's recent trajectory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorVerdict {
    /// True when the primary appears to be going in circles rather than making progress.
    pub circling: bool,
    /// A one-sentence justification (shown to the operator in the live feed).
    pub reason: String,
    /// A concrete, single-action instruction to break the loop (empty when not circling).
    pub steer: Option<String>,
}

impl MonitorVerdict {
    fn none() -> Self {
        MonitorVerdict {
            circling: false,
            reason: String::new(),
            steer: None,
        }
    }
}

/// What the policy decides to DO after seeing a verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonitorAction {
    /// Leave the primary alone.
    None,
    /// Inject a steering instruction into the running turn.
    Steer(String),
    /// Stop second-guessing it and force a graceful wrap-up.
    WrapUp(String),
}

/// Bounded escalation: steer up to `max_steers` times; if the primary is *still* circling after
/// the budget is spent, force a wrap-up. A non-circling verdict resets nothing destructive — it
/// just means "no action this round". The steer budget is intentionally small so the monitor can
/// never itself spam the conversation.
#[derive(Debug)]
pub struct MonitorPolicy {
    max_steers: usize,
    steers_issued: usize,
}

impl MonitorPolicy {
    pub fn new(max_steers: usize) -> Self {
        MonitorPolicy {
            max_steers,
            steers_issued: 0,
        }
    }

    /// Decide what to do given the latest verdict. Caller is expected to only call this once per
    /// monitor evaluation (i.e. once the primary has had a chance to react to a prior steer).
    pub fn observe(&mut self, verdict: &MonitorVerdict) -> MonitorAction {
        if !verdict.circling {
            return MonitorAction::None;
        }
        if self.steers_issued >= self.max_steers {
            // The nudges did not land — stop spending tokens on a loop and summarise.
            return MonitorAction::WrapUp(if verdict.reason.is_empty() {
                "loop monitor: primary still circling after its steer budget; wrapping up"
                    .to_string()
            } else {
                format!("loop monitor: still circling — {}", verdict.reason)
            });
        }
        self.steers_issued += 1;
        let steer = verdict.steer.clone().unwrap_or_else(|| {
            "You appear to be going in circles. Stop re-reasoning what you already know and do \
             exactly one concrete thing now: issue a single tool call, run a simulation, or give \
             your final answer."
                .to_string()
        });
        MonitorAction::Steer(steer)
    }

    #[cfg(test)]
    fn steers_issued(&self) -> usize {
        self.steers_issued
    }
}

/// Render a bounded, monitor-friendly view of the *tail* of the transcript: role-labelled lines
/// for the recent messages, with each message's text capped and the system/persona prompt
/// dropped (the monitor judges trajectory, not the standing instructions). Kept under
/// [`WINDOW_MAX_CHARS`] by taking from the end.
pub fn render_window(msgs: &[Msg]) -> String {
    let mut lines: Vec<String> = Vec::new();
    for m in msgs {
        match m {
            // The standing system/persona prompt is not part of the *trajectory* and would
            // dominate the window — skip it.
            Msg::System(_) => {}
            Msg::User(t) => lines.push(format!("USER: {}", clip(t, 1200))),
            Msg::Assistant { text, tool_calls } => {
                if let Some(t) = text {
                    if !t.trim().is_empty() {
                        lines.push(format!("ASSISTANT: {}", clip(t, 1500)));
                    }
                }
                for c in tool_calls {
                    lines.push(format!(
                        "ASSISTANT_TOOL_CALL: {} {}",
                        c.name,
                        clip(&c.arguments.to_string(), 400)
                    ));
                }
            }
            Msg::Tool { content, .. } => lines.push(format!("TOOL_RESULT: {}", clip(content, 600))),
        }
    }
    // Keep the most recent lines that fit in the budget (take from the end).
    let mut out: Vec<&String> = Vec::new();
    let mut total = 0usize;
    for line in lines.iter().rev() {
        let add = line.len() + 1;
        if total + add > WINDOW_MAX_CHARS && !out.is_empty() {
            break;
        }
        total += add;
        out.push(line);
    }
    out.reverse();
    out.into_iter().cloned().collect::<Vec<_>>().join("\n")
}

fn clip(s: &str, max: usize) -> String {
    let s = s.trim();
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

const MONITOR_SYSTEM: &str = "\
You are a loop monitor supervising another AI agent that is investigating a captured BPMN process \
dataset for a human operator. You do not investigate yourself — you watch the agent's recent \
transcript and judge whether it is making progress or going in circles, and if it is stuck, you \
hand it the single concrete next action that breaks the loop.\n\
\n\
Going in circles means any of: repeating the same reasoning or hypothesis without gathering new \
evidence; re-attempting an action that keeps failing the same way; oscillating between options \
without deciding; or spending several rounds 'thinking' without issuing a tool call, running a \
simulation, or giving an answer.\n\
\n\
The agent has escape hatches it often forgets — when it seems stuck on an UNREACHABLE path, the \
fix is usually to change a constraint, not to keep reasoning:\n\
- query_traces: run read-only DuckDB SQL over the trace data (the only way to 'run' SQL — writing \
SQL in prose does nothing).\n\
- simulate / compare_variants with mockWorkers: mock a job's OUTPUT, or mock a job FAILURE with \
\"throwError\":\"<CODE>\" to exercise an error/timeout boundary that is otherwise never reached.\n\
- edit_model: add or change tasks, gateways, and boundary events.\n\
- validate_model: fast static check of BPMN XML before deploying (surface errors early).\n\
\n\
Respond with ONLY a JSON object and nothing else:\n\
{\"circling\": true or false, \"reason\": \"<one sentence>\", \"steer\": \"<one short concrete \
instruction, or empty>\"}\n\
\n\
When circling is true, 'steer' must name the ONE concrete next action — e.g. \"Stop re-deriving \
the credit-check rate; you already have it. Run a simulation with a mockWorkers throwError for \
credit-check to exercise the error boundary, or give your final answer.\" Be decisive: prefer \
letting the agent act and iterate over more analysis. If it is making genuine progress, return \
circling=false and an empty steer.";

/// Ask the monitor model to classify the primary's recent trajectory. Never panics and never
/// returns a spurious intervention: on any transport or parse failure it yields a non-circling
/// verdict (the safe default — missing a loop beats interrupting good work).
pub async fn evaluate(cfg: &LlmConfig, persona_system: &str, window: &str) -> MonitorVerdict {
    if window.trim().is_empty() {
        return MonitorVerdict::none();
    }
    let agent = OpenAiAgent { cfg: cfg.clone() };
    let system = if persona_system.trim().is_empty() {
        MONITOR_SYSTEM
    } else {
        persona_system
    };
    let user = format!(
        "Here is the recent transcript of the agent you are monitoring. Judge whether it is going \
         in circles.\n\n=== RECENT TRANSCRIPT ===\n{window}\n=== END TRANSCRIPT ===\n\nReturn only \
         the JSON verdict."
    );
    let msgs = vec![Msg::System(system.to_string()), Msg::User(user)];
    match agent.step(&msgs, &[]).await {
        Ok(Turn::Final(text)) => parse_verdict(&text),
        // A monitor that calls tools or fails is treated as 'no signal'.
        Ok(Turn::ToolCalls(_)) | Err(_) => MonitorVerdict::none(),
    }
}

/// Parse a verdict from the model's text. Accepts a bare JSON object, one wrapped in ```json
/// fences, or one embedded in prose (the first balanced `{...}` is used). Any failure yields a
/// non-circling verdict so a malformed monitor reply can never interrupt the primary.
pub fn parse_verdict(text: &str) -> MonitorVerdict {
    let Some(json) = first_json_object(text) else {
        return MonitorVerdict::none();
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&json) else {
        return MonitorVerdict::none();
    };
    let circling = v.get("circling").and_then(|c| c.as_bool()).unwrap_or(false);
    let reason = v
        .get("reason")
        .and_then(|r| r.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let steer = v
        .get("steer")
        .and_then(|s| s.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    MonitorVerdict {
        circling,
        reason,
        steer,
    }
}

/// Extract the first balanced top-level `{...}` object from arbitrary text (skips ```json fences
/// and surrounding prose). String-literal aware so braces inside quoted values don't confuse the
/// brace counter.
fn first_json_object(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let start = text.find('{')?;
    let mut depth = 0usize;
    let mut in_str = false;
    let mut escaped = false;
    for i in start..bytes.len() {
        let c = bytes[i] as char;
        if in_str {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_str = false;
            }
            continue;
        }
        match c {
            '"' => in_str = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(text[start..=i].to_string());
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
    use crate::agent::ToolCall;
    use serde_json::json;

    #[test]
    fn parses_a_bare_json_verdict() {
        let v = parse_verdict(r#"{"circling": true, "reason": "stuck", "steer": "do X"}"#);
        assert!(v.circling);
        assert_eq!(v.reason, "stuck");
        assert_eq!(v.steer.as_deref(), Some("do X"));
    }

    #[test]
    fn parses_a_fenced_verdict_embedded_in_prose() {
        let text = "Here is my read:\n```json\n{\"circling\": false, \"reason\": \"progressing\", \"steer\": \"\"}\n```\nthanks";
        let v = parse_verdict(text);
        assert!(!v.circling);
        assert_eq!(v.reason, "progressing");
        assert_eq!(v.steer, None);
    }

    #[test]
    fn braces_inside_strings_do_not_break_parsing() {
        let v = parse_verdict(
            r#"{"circling": true, "reason": "tried {a:1}", "steer": "run query_traces"}"#,
        );
        assert!(v.circling);
        assert_eq!(v.steer.as_deref(), Some("run query_traces"));
    }

    #[test]
    fn malformed_reply_is_treated_as_no_intervention() {
        assert_eq!(
            parse_verdict("I think it's looping but I'm not sure"),
            MonitorVerdict::none()
        );
        assert_eq!(parse_verdict(""), MonitorVerdict::none());
        assert_eq!(parse_verdict("{not json at all"), MonitorVerdict::none());
    }

    #[test]
    fn policy_steers_then_escalates_to_wrapup() {
        let mut p = MonitorPolicy::new(2);
        let circling = MonitorVerdict {
            circling: true,
            reason: "loop".into(),
            steer: Some("act now".into()),
        };
        // First two circling verdicts produce steers …
        assert_eq!(p.observe(&circling), MonitorAction::Steer("act now".into()));
        assert_eq!(p.observe(&circling), MonitorAction::Steer("act now".into()));
        assert_eq!(p.steers_issued(), 2);
        // … the third (budget spent, still circling) escalates to wrap-up.
        match p.observe(&circling) {
            MonitorAction::WrapUp(_) => {}
            other => panic!("expected wrap-up, got {other:?}"),
        }
    }

    #[test]
    fn policy_does_nothing_when_not_circling() {
        let mut p = MonitorPolicy::new(2);
        let ok = MonitorVerdict::none();
        assert_eq!(p.observe(&ok), MonitorAction::None);
        assert_eq!(p.steers_issued(), 0);
    }

    #[test]
    fn policy_falls_back_to_a_generic_steer_when_none_supplied() {
        let mut p = MonitorPolicy::new(1);
        let circling = MonitorVerdict {
            circling: true,
            reason: "loop".into(),
            steer: None,
        };
        match p.observe(&circling) {
            MonitorAction::Steer(s) => assert!(s.contains("going in circles")),
            other => panic!("expected a steer, got {other:?}"),
        }
    }

    #[test]
    fn window_drops_system_and_keeps_recent_tail() {
        let msgs = vec![
            Msg::System("standing persona prompt".into()),
            Msg::User("investigate the loan process".into()),
            Msg::Assistant {
                text: Some("Let me think about credit-check".into()),
                tool_calls: vec![ToolCall {
                    id: "c1".into(),
                    name: "query_traces".into(),
                    arguments: json!({"sql": "SELECT 1"}),
                }],
            },
            Msg::Tool {
                call_id: "c1".into(),
                content: "1 row".into(),
            },
        ];
        let w = render_window(&msgs);
        assert!(
            !w.contains("standing persona prompt"),
            "system prompt must be dropped: {w}"
        );
        assert!(w.contains("USER: investigate the loan process"));
        assert!(w.contains("ASSISTANT_TOOL_CALL: query_traces"));
        assert!(w.contains("TOOL_RESULT: 1 row"));
    }

    #[test]
    fn window_clips_overlong_messages() {
        let big = "x".repeat(5000);
        let msgs = vec![Msg::User(big)];
        let w = render_window(&msgs);
        assert!(
            w.len() < 2000,
            "overlong message must be clipped: {} chars",
            w.len()
        );
        assert!(w.ends_with('…'));
    }
}
