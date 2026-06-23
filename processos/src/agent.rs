//! **Agentic tool-calling loop** — the substrate that lets the cockpit droid *act*
//! on the data, not just be prompted with a summary.
//!
//! [`run_agent`] is provider-agnostic orchestration: it offers the model a set of
//! [`ToolSpec`]s, and whenever the model emits tool calls it dispatches them to a
//! [`ToolBox`], feeds the results back, and loops until the model returns a final
//! answer (or a round budget is hit). The transport is abstracted behind
//! [`AgentStep`] so the loop is unit-testable with a deterministic mock — no network.
//!
//! The concrete transport here speaks the **OpenAI chat-completions** tool-calling
//! wire shape (what a local `llama.cpp`/vLLM/Ollama server and OpenAI itself all
//! accept); Anthropic tool-use can slot in as a second `AgentStep` impl later.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::harness::llm::LlmConfig;

/// A tool the model may call.
#[derive(Debug, Clone, Serialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    /// JSON-Schema object describing the call arguments.
    pub parameters: Value,
}

/// One tool invocation the model requested.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// What the model decided on a step.
pub enum Turn {
    /// The model wants to run these tools before answering.
    ToolCalls(Vec<ToolCall>),
    /// The model produced its final answer.
    Final(String),
}

/// A neutral conversation message the transport serialises per provider.
///
/// `Serialize`/`Deserialize` let a multi-turn chat persist the *full* transcript
/// (including tool calls and their results) so the model keeps its working memory
/// across operator messages and server restarts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Msg {
    System(String),
    User(String),
    /// An assistant turn that requested tools (text optional).
    Assistant {
        text: Option<String>,
        tool_calls: Vec<ToolCall>,
    },
    /// The result of one tool call, fed back to the model.
    Tool { call_id: String, content: String },
}

/// A set of callable tools.
pub trait ToolBox {
    fn specs(&self) -> Vec<ToolSpec>;
    /// Execute a tool by name; the returned string is fed back verbatim to the model.
    fn call(&self, name: &str, args: &Value) -> Result<String, String>;
}

/// A streamed fragment of one model step, surfaced live to the operator.
pub enum Delta {
    /// A chunk of the model's chain-of-thought (`reasoning_content`).
    Reasoning(String),
    /// A chunk of the model's user-facing answer (`content`).
    Answer(String),
}

/// An event emitted by the streaming agent loop, for live cockpit feedback.
pub enum AgentEvent {
    /// A new round of the tool-calling loop began.
    Round(usize),
    /// The exact request body about to be sent to the model this round (for the debug view).
    Request { round: usize, body: Value },
    /// A chunk of the droid's thinking.
    Reasoning(String),
    /// A chunk of the droid's answer prose.
    Answer(String),
    /// The droid asked to run a tool (emitted before execution).
    ToolCall { tool: String, arguments: Value },
    /// A tool finished; `result` is the raw string fed back to the model.
    ToolResult { tool: String, result: String },
}

/// The model transport: one request/response step given the running messages.
#[allow(async_fn_in_trait)]
pub trait AgentStep {
    async fn step(&self, msgs: &[Msg], tools: &[ToolSpec]) -> Result<Turn, String>;

    /// The exact request body this transport would send for `msgs`/`tools`, surfaced to the
    /// cockpit's per-session debug view so the operator can see *everything* the model receives
    /// (system prompt, tool specs, full transcript) — not just their latest message. The default
    /// is `None` (mock transports make no HTTP request and have nothing to show).
    fn debug_body(&self, _msgs: &[Msg], _tools: &[ToolSpec]) -> Option<Value> {
        None
    }

    /// Streaming variant: same contract as [`AgentStep::step`], but `on_delta` is invoked with
    /// each token fragment as it arrives so the caller can surface live progress. The default
    /// implementation is non-streaming (emits no deltas) so mock transports need not implement it.
    async fn step_streaming(
        &self,
        msgs: &[Msg],
        tools: &[ToolSpec],
        _on_delta: &mut dyn FnMut(Delta),
    ) -> Result<Turn, String> {
        self.step(msgs, tools).await
    }
}

/// One recorded tool call + its result (the "lab notebook" of an investigation).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentStepRecord {
    pub tool: String,
    pub arguments: Value,
    pub result: String,
}

/// The outcome of an agent run.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRun {
    pub answer: String,
    pub steps: Vec<AgentStepRecord>,
    pub rounds: usize,
}

/// Drive the model→tools→model loop until a final answer or `max_rounds` is reached.
pub async fn run_agent<M: AgentStep, T: ToolBox + ?Sized>(
    model: &M,
    tools: &T,
    system: &str,
    user: &str,
    max_rounds: usize,
) -> Result<AgentRun, String> {
    let mut msgs = vec![Msg::System(system.to_string()), Msg::User(user.to_string())];
    run_agent_resumable(model, tools, &mut msgs, max_rounds).await
}

/// Drive the loop over an **existing transcript**, appending the assistant/tool turns
/// (and finally the assistant's answer) to `msgs` in place. This is the multi-turn
/// substrate: the caller seeds `msgs` with the persisted history plus the new user
/// message, runs a turn, and persists the mutated `msgs` so the next turn resumes
/// with full context. `run_agent` is the single-shot special case.
pub async fn run_agent_resumable<M: AgentStep, T: ToolBox + ?Sized>(
    model: &M,
    tools: &T,
    msgs: &mut Vec<Msg>,
    max_rounds: usize,
) -> Result<AgentRun, String> {
    run_agent_resumable_cancellable(model, tools, msgs, max_rounds, None).await
}

/// As [`run_agent_resumable`], but a shared `cancel` flag lets the caller ask the agent to
/// **wrap up early**: when it is set, the loop stops issuing tool calls, instructs the model
/// to report its findings so far (with tools withheld so it must answer in prose), and
/// returns that. Used by the cockpit's "wrap it up" control for long investigations.
pub async fn run_agent_resumable_cancellable<M: AgentStep, T: ToolBox + ?Sized>(
    model: &M,
    tools: &T,
    msgs: &mut Vec<Msg>,
    max_rounds: usize,
    cancel: Option<&std::sync::atomic::AtomicBool>,
) -> Result<AgentRun, String> {
    let mut sink = |_ev: AgentEvent| {};
    run_agent_streaming(model, tools, msgs, max_rounds, cancel, &mut sink).await
}

/// As [`run_agent_resumable_cancellable`], but emits [`AgentEvent`]s through `sink` as the run
/// progresses — token fragments of the droid's thinking and answer, plus tool-call boundaries —
/// so the cockpit can render the investigation live instead of waiting for the whole turn. This
/// is the canonical loop; the non-streaming entry points delegate here with a no-op sink.
pub async fn run_agent_streaming<M: AgentStep, T: ToolBox + ?Sized>(
    model: &M,
    tools: &T,
    msgs: &mut Vec<Msg>,
    max_rounds: usize,
    cancel: Option<&std::sync::atomic::AtomicBool>,
    sink: &mut dyn FnMut(AgentEvent),
) -> Result<AgentRun, String> {
    use std::sync::atomic::Ordering;
    let specs = tools.specs();
    let mut steps: Vec<AgentStepRecord> = Vec::new();

    for round in 1..=max_rounds {
        // Operator asked to wrap up: stop investigating and force a prose summary now.
        if cancel.is_some_and(|c| c.load(Ordering::Relaxed)) {
            return wrap_up(model, msgs, steps, round, sink).await;
        }
        sink(AgentEvent::Round(round));
        // Surface the exact payload this round sends to the model, so the debug view can show
        // everything it receives (system prompt + tool specs + full transcript), not just the
        // operator's latest message.
        if let Some(body) = model.debug_body(msgs, &specs) {
            sink(AgentEvent::Request { round, body });
        }
        // Capture this round's chain-of-thought so a tool-calling turn can persist it (the
        // model often does its real thinking in the round where it decides to call a tool;
        // without this it would vanish from the saved transcript, leaving the droid bubble
        // with no "Thinking" disclosure after the live stream ends).
        let mut round_reasoning = String::new();
        let turn = {
            // Forward token fragments live; scoped so `sink` is free again after the call.
            let mut on_delta = |d: Delta| match d {
                Delta::Reasoning(t) => {
                    round_reasoning.push_str(&t);
                    sink(AgentEvent::Reasoning(t));
                }
                Delta::Answer(t) => sink(AgentEvent::Answer(t)),
            };
            model.step_streaming(msgs, &specs, &mut on_delta).await?
        };
        match turn {
            Turn::Final(answer) => {
                // Record the answer in the transcript so a resumed conversation
                // remembers what the droid concluded last turn.
                msgs.push(Msg::Assistant {
                    text: Some(answer.clone()),
                    tool_calls: Vec::new(),
                });
                return Ok(AgentRun {
                    answer,
                    steps,
                    rounds: round,
                });
            }
            Turn::ToolCalls(calls) if calls.is_empty() => {
                // Defensive: a tool-call turn with nothing to call — treat as done.
                return Ok(AgentRun {
                    answer: String::new(),
                    steps,
                    rounds: round,
                });
            }
            Turn::ToolCalls(calls) => {
                // Persist this round's thinking (as a `<think>` block) on the tool-calling
                // turn so it survives into the rendered transcript even though the visible
                // answer comes from a later round.
                let text = {
                    let r = round_reasoning.trim();
                    (!r.is_empty()).then(|| format!("<think>{r}</think>"))
                };
                msgs.push(Msg::Assistant {
                    text,
                    tool_calls: calls.clone(),
                });
                for call in calls {
                    sink(AgentEvent::ToolCall {
                        tool: call.name.clone(),
                        arguments: call.arguments.clone(),
                    });
                    let result = tools
                        .call(&call.name, &call.arguments)
                        .unwrap_or_else(|e| json!({ "error": e }).to_string());
                    sink(AgentEvent::ToolResult {
                        tool: call.name.clone(),
                        result: result.clone(),
                    });
                    steps.push(AgentStepRecord {
                        tool: call.name.clone(),
                        arguments: call.arguments.clone(),
                        result: result.clone(),
                    });
                    msgs.push(Msg::Tool {
                        call_id: call.id,
                        content: result,
                    });
                }
            }
        }
    }
    // Budget exhausted: rather than erroring, ask the model to summarise what it has so the
    // operator still gets findings from a long run.
    wrap_up(model, msgs, steps, max_rounds, sink).await
}

/// Force a final prose answer from the model with no tools available, recording it in the
/// transcript. Used both when the operator wraps up early and when the round budget is hit.
async fn wrap_up<M: AgentStep>(
    model: &M,
    msgs: &mut Vec<Msg>,
    steps: Vec<AgentStepRecord>,
    round: usize,
    sink: &mut dyn FnMut(AgentEvent),
) -> Result<AgentRun, String> {
    msgs.push(Msg::System(
        "Stop investigating now and report your findings so far, based only on what you \
         have already gathered. Do not call any more tools — answer in clear prose, stating \
         what you found, your confidence, and what you'd recommend or check next."
            .to_string(),
    ));
    let answer = {
        let mut on_delta = |d: Delta| match d {
            Delta::Reasoning(t) => sink(AgentEvent::Reasoning(t)),
            Delta::Answer(t) => sink(AgentEvent::Answer(t)),
        };
        match model.step_streaming(msgs, &[], &mut on_delta).await? {
            Turn::Final(a) => a,
            Turn::ToolCalls(_) => String::new(),
        }
    };
    msgs.push(Msg::Assistant {
        text: Some(answer.clone()),
        tool_calls: Vec::new(),
    });
    Ok(AgentRun {
        answer,
        steps,
        rounds: round,
    })
}

// ─── OpenAI chat-completions tool-calling transport ──────────────────────────

/// An [`AgentStep`] backed by an OpenAI-compatible `chat/completions` endpoint.
pub struct OpenAiAgent {
    pub cfg: LlmConfig,
}

impl OpenAiAgent {
    /// Build the `chat/completions` request body shared by streaming and non-streaming calls.
    fn request_body(&self, msgs: &[Msg], tools: &[ToolSpec], stream: bool) -> Value {
        let tool_defs: Vec<Value> = tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    }
                })
            })
            .collect();
        json!({
            "model": self.cfg.model,
            "temperature": self.cfg.temperature,
            "max_tokens": self.cfg.max_tokens,
            "tools": tool_defs,
            "stream": stream,
            "messages": msgs.iter().map(openai_message).collect::<Vec<_>>(),
        })
    }

    fn endpoint(&self) -> String {
        format!("{}/chat/completions", self.cfg.base_url.trim_end_matches('/'))
    }
}

/// Accumulates one streamed tool call whose `arguments` (and sometimes name) arrive in fragments.
#[derive(Default)]
struct ToolCallAccum {
    id: String,
    name: String,
    args: String,
}

impl AgentStep for OpenAiAgent {
    fn debug_body(&self, msgs: &[Msg], tools: &[ToolSpec]) -> Option<Value> {
        Some(self.request_body(msgs, tools, true))
    }

    async fn step(&self, msgs: &[Msg], tools: &[ToolSpec]) -> Result<Turn, String> {
        if !self.cfg.is_ready() {
            return Err("no LLM model configured (set PROCESSOS_LLM_MODEL)".into());
        }
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| format!("http client: {e}"))?;
        let url = self.endpoint();
        let body = self.request_body(msgs, tools, false);

        let mut req = client.post(&url).json(&body);
        if let Some(key) = &self.cfg.api_key {
            req = req.bearer_auth(key);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("LLM request to {url} failed: {e}"))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| format!("reading LLM response: {e}"))?;
        if !status.is_success() {
            return Err(format!("LLM returned {status}: {}", truncate(&text, 500)));
        }
        let v: Value =
            serde_json::from_str(&text).map_err(|e| format!("LLM response not JSON: {e}"))?;
        parse_openai_turn(&v["choices"][0]["message"])
    }

    async fn step_streaming(
        &self,
        msgs: &[Msg],
        tools: &[ToolSpec],
        on_delta: &mut dyn FnMut(Delta),
    ) -> Result<Turn, String> {
        use futures_util::StreamExt;
        if !self.cfg.is_ready() {
            return Err("no LLM model configured (set PROCESSOS_LLM_MODEL)".into());
        }
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| format!("http client: {e}"))?;
        let url = self.endpoint();
        let body = self.request_body(msgs, tools, true);

        let mut req = client.post(&url).json(&body);
        if let Some(key) = &self.cfg.api_key {
            req = req.bearer_auth(key);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("LLM request to {url} failed: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("LLM returned {status}: {}", truncate(&text, 500)));
        }

        let mut content = String::new();
        let mut reasoning = String::new();
        let mut tool_accum: Vec<ToolCallAccum> = Vec::new();
        let mut buf = String::new();
        let mut stream = resp.bytes_stream();
        'outer: while let Some(chunk) = stream.next().await {
            let bytes = chunk.map_err(|e| format!("LLM stream error: {e}"))?;
            buf.push_str(&String::from_utf8_lossy(&bytes));
            // Process complete SSE lines; keep the trailing partial line in `buf`.
            while let Some(pos) = buf.find('\n') {
                let line = buf[..pos].trim().to_string();
                buf.drain(..=pos);
                let data = match line.strip_prefix("data:") {
                    Some(d) => d.trim(),
                    None => continue, // comments / blank lines
                };
                if data == "[DONE]" {
                    break 'outer;
                }
                let v: Value = match serde_json::from_str(data) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let delta = &v["choices"][0]["delta"];
                if let Some(r) = delta["reasoning_content"].as_str() {
                    if !r.is_empty() {
                        reasoning.push_str(r);
                        on_delta(Delta::Reasoning(r.to_string()));
                    }
                }
                if let Some(c) = delta["content"].as_str() {
                    if !c.is_empty() {
                        content.push_str(c);
                        on_delta(Delta::Answer(c.to_string()));
                    }
                }
                if let Some(tcs) = delta["tool_calls"].as_array() {
                    for tc in tcs {
                        let idx = tc["index"].as_u64().unwrap_or(0) as usize;
                        while tool_accum.len() <= idx {
                            tool_accum.push(ToolCallAccum::default());
                        }
                        let acc = &mut tool_accum[idx];
                        if let Some(id) = tc["id"].as_str() {
                            if !id.is_empty() {
                                acc.id = id.to_string();
                            }
                        }
                        if let Some(n) = tc["function"]["name"].as_str() {
                            if !n.is_empty() {
                                acc.name.push_str(n);
                            }
                        }
                        if let Some(a) = tc["function"]["arguments"].as_str() {
                            acc.args.push_str(a);
                        }
                    }
                }
            }
        }

        if !tool_accum.is_empty() {
            let calls = tool_accum
                .into_iter()
                .enumerate()
                .map(|(i, a)| ToolCall {
                    id: if a.id.is_empty() {
                        format!("call_{i}")
                    } else {
                        a.id
                    },
                    name: a.name,
                    arguments: serde_json::from_str(&a.args).unwrap_or(json!({})),
                })
                .collect();
            return Ok(Turn::ToolCalls(calls));
        }
        // Fold any separate reasoning stream back into a `<think>` block so the persisted
        // transcript (and non-streaming render) keeps the droid's chain-of-thought.
        let reasoning = reasoning.trim();
        let final_text = if reasoning.is_empty() {
            content
        } else {
            format!("<think>{reasoning}</think>\n{content}")
        };
        Ok(Turn::Final(final_text))
    }
}

fn openai_message(m: &Msg) -> Value {
    match m {
        Msg::System(s) => json!({ "role": "system", "content": s }),
        Msg::User(s) => json!({ "role": "user", "content": s }),
        Msg::Assistant { text, tool_calls } => {
            let calls: Vec<Value> = tool_calls
                .iter()
                .map(|c| {
                    json!({
                        "id": c.id,
                        "type": "function",
                        "function": {
                            "name": c.name,
                            "arguments": c.arguments.to_string(),
                        }
                    })
                })
                .collect();
            json!({
                "role": "assistant",
                "content": text.clone().unwrap_or_default(),
                "tool_calls": calls,
            })
        }
        Msg::Tool { call_id, content } => json!({
            "role": "tool",
            "tool_call_id": call_id,
            "content": content,
        }),
    }
}

fn parse_openai_turn(message: &Value) -> Result<Turn, String> {
    let calls = message.get("tool_calls").and_then(|c| c.as_array());
    if let Some(calls) = calls {
        if !calls.is_empty() {
            let mut out = Vec::with_capacity(calls.len());
            for (i, c) in calls.iter().enumerate() {
                let id = c["id"]
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("call_{i}"));
                let name = c["function"]["name"]
                    .as_str()
                    .ok_or("tool call missing function.name")?
                    .to_string();
                let raw = c["function"]["arguments"].as_str().unwrap_or("{}");
                let arguments: Value = serde_json::from_str(raw).unwrap_or(json!({}));
                out.push(ToolCall {
                    id,
                    name,
                    arguments,
                });
            }
            return Ok(Turn::ToolCalls(out));
        }
    }
    let content = message["content"].as_str().unwrap_or_default().to_string();
    // Some backends (e.g. llama.cpp serving Gemma/Qwen) return the model's chain-of-thought in a
    // separate `reasoning_content` field rather than inline `<think>` tags. Fold it back in as a
    // `<think>` block so the cockpit can show it as a collapsed "Thinking" disclosure.
    let reasoning = message
        .get("reasoning_content")
        .and_then(|r| r.as_str())
        .unwrap_or_default()
        .trim();
    let final_text = if reasoning.is_empty() {
        content
    } else {
        format!("<think>{reasoning}</think>\n{content}")
    };
    Ok(Turn::Final(final_text))
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}…", &s[..n])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// A deterministic mock model: replays a scripted list of turns, one per step.
    struct ScriptedModel {
        turns: Vec<Vec<ToolCall>>, // empty vec => final answer
        idx: Cell<usize>,
        final_answer: String,
    }

    impl AgentStep for ScriptedModel {
        async fn step(&self, _msgs: &[Msg], _tools: &[ToolSpec]) -> Result<Turn, String> {
            let i = self.idx.get();
            self.idx.set(i + 1);
            match self.turns.get(i) {
                Some(calls) if !calls.is_empty() => Ok(Turn::ToolCalls(calls.clone())),
                _ => Ok(Turn::Final(self.final_answer.clone())),
            }
        }
    }

    struct EchoTools;
    impl ToolBox for EchoTools {
        fn specs(&self) -> Vec<ToolSpec> {
            vec![ToolSpec {
                name: "echo".into(),
                description: "echo the input".into(),
                parameters: json!({"type":"object","properties":{"x":{"type":"string"}}}),
            }]
        }
        fn call(&self, name: &str, args: &Value) -> Result<String, String> {
            if name != "echo" {
                return Err(format!("unknown tool {name}"));
            }
            Ok(format!("echoed:{}", args["x"].as_str().unwrap_or("")))
        }
    }

    #[tokio::test]
    async fn loop_dispatches_tools_then_returns_final_answer() {
        let model = ScriptedModel {
            turns: vec![vec![ToolCall {
                id: "c1".into(),
                name: "echo".into(),
                arguments: json!({"x": "hi"}),
            }]],
            idx: Cell::new(0),
            final_answer: "done".into(),
        };
        let run = run_agent(&model, &EchoTools, "sys", "go", 5).await.unwrap();
        assert_eq!(run.answer, "done");
        assert_eq!(run.steps.len(), 1);
        assert_eq!(run.steps[0].tool, "echo");
        assert_eq!(run.steps[0].result, "echoed:hi");
        assert_eq!(run.rounds, 2);
    }

    #[tokio::test]
    async fn budget_exhaustion_wraps_up_instead_of_erroring() {
        // A model that keeps asking for tools hits the round budget; rather than erroring,
        // the loop withholds tools and forces a prose wrap-up so the operator still gets
        // findings. The scripted model returns its final answer once tools are withheld.
        let model = ScriptedModel {
            turns: vec![
                vec![ToolCall { id: "c".into(), name: "echo".into(), arguments: json!({"x":"a"}) }],
                vec![ToolCall { id: "c".into(), name: "echo".into(), arguments: json!({"x":"b"}) }],
            ],
            idx: Cell::new(0),
            final_answer: "wrapped up".into(),
        };
        let run = run_agent(&model, &EchoTools, "sys", "go", 2).await.unwrap();
        assert_eq!(run.answer, "wrapped up");
        assert_eq!(run.rounds, 2);
        // Both tool calls were executed before the budget hit.
        assert_eq!(run.steps.len(), 2);
    }

    #[tokio::test]
    async fn cancel_flag_forces_an_early_wrap_up() {
        use std::sync::atomic::{AtomicBool, Ordering};
        // The cancel flag is already set before the loop starts, so round 1 short-circuits
        // straight to the wrap-up (no tools), returning the model's prose answer.
        let model = ScriptedModel {
            turns: vec![],
            idx: Cell::new(0),
            final_answer: "early findings".into(),
        };
        let cancel = AtomicBool::new(true);
        let mut msgs = vec![Msg::System("sys".into()), Msg::User("go".into())];
        let run = run_agent_resumable_cancellable(&model, &EchoTools, &mut msgs, 5, Some(&cancel))
            .await
            .unwrap();
        assert_eq!(run.answer, "early findings");
        // No tools were dispatched — the very first scripted tool turn was never reached.
        assert!(run.steps.is_empty());
        // The wrap-up was triggered on round 1.
        assert_eq!(run.rounds, 1);
        // Flag untouched by the loop.
        assert!(cancel.load(Ordering::Relaxed));
    }

    #[test]
    fn parses_openai_tool_call_message() {
        let msg = json!({
            "content": null,
            "tool_calls": [{
                "id": "abc",
                "type": "function",
                "function": { "name": "query_traces", "arguments": "{\"sql\":\"SELECT 1\"}" }
            }]
        });
        match parse_openai_turn(&msg).unwrap() {
            Turn::ToolCalls(calls) => {
                assert_eq!(calls[0].name, "query_traces");
                assert_eq!(calls[0].arguments["sql"], "SELECT 1");
            }
            Turn::Final(_) => panic!("expected tool calls"),
        }
    }

    #[test]
    fn parses_openai_final_message() {
        let msg = json!({ "content": "the answer" });
        match parse_openai_turn(&msg).unwrap() {
            Turn::Final(s) => assert_eq!(s, "the answer"),
            Turn::ToolCalls(_) => panic!("expected final"),
        }
    }

    #[test]
    fn folds_reasoning_content_into_a_think_block() {
        let msg = json!({ "content": "the answer", "reasoning_content": "  step one\nstep two  " });
        match parse_openai_turn(&msg).unwrap() {
            Turn::Final(s) => assert_eq!(s, "<think>step one\nstep two</think>\nthe answer"),
            Turn::ToolCalls(_) => panic!("expected final"),
        }
    }
}
