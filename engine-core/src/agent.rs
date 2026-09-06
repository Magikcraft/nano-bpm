//! Engine-native AgentInstance model (Camunda 8.10 parity, Stage 3).
//!
//! The engine is the system-of-record for AgentInstance state. This module
//! holds the *shape* of that state: the `zeebe:agentDefinition` marker
//! ([`AgentType`]) on [`crate::model::ElementKind::ServiceTask`], the definition
//! ([`AgentDefinition`]) and optional limits ([`AgentInstanceLimits`]) supplied
//! by the worker at registration, and the runtime
//! [`AgentInstance`] record — a first-class object keyed by its own dedicated
//! `agent_instance_key`, linked to the activating `element_instance_key`, and
//! driven through the [`AgentInstanceStatus`] state machine.
//!
//! LLM calls / prompt assembly / tool dispatch are deliberately **out of
//! scope**: those live in the worker layer. Here we only model the state the
//! engine owns. Every agent marker retains the normal job-worker lifecycle;
//! CREATE/UPDATE/COMPLETE explicitly manage the persisted agent state.

use crate::model::ElementId;
use crate::state::Key;

/// The `zeebe:agentDefinition agentType` marker value (Camunda stable/8.10).
///
/// Placement rules (from Camunda `AgentDefinitionValidator`): `aiAgentTask` is
/// only valid on a `bpmn:serviceTask`; `aiAgentSubProcess` is only valid on a
/// `bpmn:adHocSubProcess`. `external` is an externally-managed agent and is not
/// placement-constrained. The parser enforces the placement rules at
/// deploy/parse time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AgentType {
    /// `aiAgentTask` — an AI agent backed by a single `bpmn:serviceTask`.
    AiAgentTask,
    /// `aiAgentSubProcess` — an AI agent backed by a `bpmn:adHocSubProcess`.
    AiAgentSubProcess,
    /// `external` — an externally-managed, **job-backed** agent (Camunda parity
    /// #1099). It activates as a normal service-task job and its
    /// [`AgentInstance`] is minted lazily by the worker (a lease-gated
    /// `CreateAgentInstance`), not auto-minted at activation.
    External,
}

impl AgentType {
    /// Parse the raw `agentType` attribute value; `None` for an unknown value.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "aiAgentTask" => Some(AgentType::AiAgentTask),
            "aiAgentSubProcess" => Some(AgentType::AiAgentSubProcess),
            "external" => Some(AgentType::External),
            _ => None,
        }
    }

    /// The canonical `agentType` attribute value.
    pub fn as_str(self) -> &'static str {
        match self {
            AgentType::AiAgentTask => "aiAgentTask",
            AgentType::AiAgentSubProcess => "aiAgentSubProcess",
            AgentType::External => "external",
        }
    }
}

/// The static definition of an agent, set once at creation
/// (`definition{model,provider,systemPrompt}`). Fields are optional because the
/// concrete values are typically supplied by the worker/config at CREATE time.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AgentDefinition {
    /// The LLM model identifier (for example, `gpt-4o`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub model: Option<String>,
    /// The LLM provider (for example, `openai` or `anthropic`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub provider: Option<String>,
    /// The system prompt configured for this agent.
    #[cfg_attr(feature = "serde", serde(default))]
    pub system_prompt: Option<String>,
}

impl AgentDefinition {
    /// A cheap O(n) estimate of this definition's heap payload in bytes,
    /// dominated by the (potentially large) system prompt. Used by
    /// [`crate::command::Command::approx_bytes`] so the Raft propose batcher can
    /// bound coalesced agent-instance command entries by bytes.
    pub fn approx_bytes(&self) -> u64 {
        opt_str_bytes(&self.model)
            + opt_str_bytes(&self.provider)
            + opt_str_bytes(&self.system_prompt)
    }
}

/// The heap payload of an optional string field, in bytes (`0` when absent).
fn opt_str_bytes(s: &Option<String>) -> u64 {
    s.as_ref().map_or(0, |v| v.len() as u64)
}

/// A limit value meaning "no limit is configured".
pub const AGENT_LIMIT_UNLIMITED: i64 = -1;

/// The configured limits for an agent instance, set once at creation. `-1`
/// means unlimited (the default for an omitted limit).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AgentInstanceLimits {
    /// Maximum total tokens allowed; `-1` = unlimited.
    pub max_tokens: i64,
    /// Maximum LLM calls allowed; `-1` = unlimited.
    pub max_model_calls: i64,
    /// Maximum tool calls allowed; `-1` = unlimited.
    pub max_tool_calls: i64,
}

impl Default for AgentInstanceLimits {
    fn default() -> Self {
        AgentInstanceLimits {
            max_tokens: AGENT_LIMIT_UNLIMITED,
            max_model_calls: AGENT_LIMIT_UNLIMITED,
            max_tool_calls: AGENT_LIMIT_UNLIMITED,
        }
    }
}

/// The kind of limit a batch would breach — the single source of truth for
/// which counter a configured (`!= -1`) limit governs. Returned by
/// [`AgentInstanceLimits::first_breach`] so the processor can report a precise
/// rejection without re-deriving the mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentLimitKind {
    /// Total tokens (`input_tokens + output_tokens`) exceeded `max_tokens`.
    Tokens,
    /// `model_calls` exceeded `max_model_calls`.
    ModelCalls,
    /// `tool_calls` exceeded `max_tool_calls`.
    ToolCalls,
}

impl AgentLimitKind {
    /// The canonical label used in rejection messages.
    pub fn as_str(self) -> &'static str {
        match self {
            AgentLimitKind::Tokens => "maxTokens",
            AgentLimitKind::ModelCalls => "maxModelCalls",
            AgentLimitKind::ToolCalls => "maxToolCalls",
        }
    }
}

impl AgentInstanceLimits {
    /// The first limit that `metrics` breaches, or `None` when every configured
    /// limit still has headroom. A limit of `-1` ([`AGENT_LIMIT_UNLIMITED`]) is
    /// unbounded and never breached. `max_tokens` governs the combined
    /// `input_tokens + output_tokens` total; `max_model_calls` / `max_tool_calls`
    /// govern their same-named counters. This is the sole place the limit ->
    /// counter mapping lives, so the CREATE/UPDATE processors enforce limits by
    /// calling it rather than duplicating the comparison.
    pub fn first_breach(&self, metrics: &AgentInstanceMetrics) -> Option<AgentLimitKind> {
        let total_tokens = metrics.input_tokens.saturating_add(metrics.output_tokens);
        if self.max_tokens != AGENT_LIMIT_UNLIMITED && total_tokens > self.max_tokens {
            return Some(AgentLimitKind::Tokens);
        }
        if self.max_model_calls != AGENT_LIMIT_UNLIMITED
            && metrics.model_calls > self.max_model_calls
        {
            return Some(AgentLimitKind::ModelCalls);
        }
        if self.max_tool_calls != AGENT_LIMIT_UNLIMITED && metrics.tool_calls > self.max_tool_calls
        {
            return Some(AgentLimitKind::ToolCalls);
        }
        None
    }
}

/// Metric increments applied to an agent instance's aggregate counters on
/// UPDATE (Camunda 8.10 `AgentInstanceMetricsDelta`). Each field is a
/// non-negative delta folded into the running totals; omitted fields default to
/// `0` (no change). Mirrors the spec `AgentInstanceMetricsDelta` request shape.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AgentInstanceMetricsDelta {
    #[cfg_attr(feature = "serde", serde(default))]
    pub input_tokens: i64,
    #[cfg_attr(feature = "serde", serde(default))]
    pub output_tokens: i64,
    #[cfg_attr(feature = "serde", serde(default))]
    pub reasoning_token_count: i64,
    #[cfg_attr(feature = "serde", serde(default))]
    pub cache_creation_token_count: i64,
    #[cfg_attr(feature = "serde", serde(default))]
    pub cache_read_token_count: i64,
    #[cfg_attr(feature = "serde", serde(default))]
    pub model_calls: i64,
    #[cfg_attr(feature = "serde", serde(default))]
    pub tool_calls: i64,
}

/// Aggregated metrics for an agent instance across all loop iterations. All
/// counters start at zero and only grow (UPDATE processors — a later slice —
/// advance them).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AgentInstanceMetrics {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_token_count: i64,
    pub cache_creation_token_count: i64,
    pub cache_read_token_count: i64,
    pub model_calls: i64,
    pub tool_calls: i64,
}

impl AgentInstanceMetrics {
    /// This counter set with `delta` folded in (each field summed). Counters
    /// only ever grow, so this is a saturating add and each delta field is
    /// **clamped to `>= 0`** before accumulation: a negative delta would
    /// otherwise decrease a counter (even below zero), breaking the
    /// "counters only ever grow" invariant and letting an UPDATE bypass
    /// [`AgentInstanceLimits::first_breach`] enforcement. Used by the UPDATE
    /// processor to compute the post-batch totals it both limit-checks and
    /// stores, keeping the accumulation in one place.
    pub fn with_delta(&self, delta: &AgentInstanceMetricsDelta) -> AgentInstanceMetrics {
        AgentInstanceMetrics {
            input_tokens: self.input_tokens.saturating_add(delta.input_tokens.max(0)),
            output_tokens: self
                .output_tokens
                .saturating_add(delta.output_tokens.max(0)),
            reasoning_token_count: self
                .reasoning_token_count
                .saturating_add(delta.reasoning_token_count.max(0)),
            cache_creation_token_count: self
                .cache_creation_token_count
                .saturating_add(delta.cache_creation_token_count.max(0)),
            cache_read_token_count: self
                .cache_read_token_count
                .saturating_add(delta.cache_read_token_count.max(0)),
            model_calls: self.model_calls.saturating_add(delta.model_calls.max(0)),
            tool_calls: self.tool_calls.saturating_add(delta.tool_calls.max(0)),
        }
    }
}

/// A tool available to the agent (`tools[]{name,description,elementId}`).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AgentTool {
    /// The tool name as visible to the LLM.
    pub name: String,
    /// A human-readable description of the tool.
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,
    /// The BPMN element id of the tool element within the ad-hoc sub-process.
    #[cfg_attr(feature = "serde", serde(default))]
    pub element_id: Option<ElementId>,
}

impl AgentTool {
    /// A cheap O(n) estimate of this tool's heap payload in bytes, used by
    /// [`crate::command::Command::approx_bytes`] to meter agent-instance command
    /// sizes for the Raft propose batcher.
    pub fn approx_bytes(&self) -> u64 {
        self.name.len() as u64 + opt_str_bytes(&self.description) + opt_str_bytes(&self.element_id)
    }
}

/// The AgentInstance status state machine (`AgentInstanceStatus`).
///
/// `INITIALIZING,TOOL_DISCOVERY,THINKING,TOOL_CALLING,IDLE` are the *active*
/// states; `COMPLETED` is terminal and reachable only through the COMPLETE
/// processor (a later slice). A freshly-created instance is `INITIALIZING`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AgentInstanceStatus {
    Initializing,
    ToolDiscovery,
    Thinking,
    ToolCalling,
    Idle,
    Completed,
}

impl AgentInstanceStatus {
    /// The canonical wire/label value (matches the 8.10 `AgentInstanceStatusEnum`).
    pub fn as_str(self) -> &'static str {
        match self {
            AgentInstanceStatus::Initializing => "INITIALIZING",
            AgentInstanceStatus::ToolDiscovery => "TOOL_DISCOVERY",
            AgentInstanceStatus::Thinking => "THINKING",
            AgentInstanceStatus::ToolCalling => "TOOL_CALLING",
            AgentInstanceStatus::Idle => "IDLE",
            AgentInstanceStatus::Completed => "COMPLETED",
        }
    }

    /// Whether this is an *active* (non-terminal) status. `COMPLETED` is the
    /// only terminal status and is reachable only via the COMPLETE processor.
    pub fn is_active(self) -> bool {
        !matches!(self, AgentInstanceStatus::Completed)
    }
}

/// The lifecycle intents of an AgentInstance record (Camunda
/// `AgentInstanceIntent`). The full set is defined now so downstream slices
/// (the CREATE/UPDATE/COMPLETE processors) reuse it; this slice only needs
/// `CREATED`/status to be representable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AgentInstanceIntent {
    /// Command intent: create an agent instance.
    Create,
    /// Event intent: an agent instance was created (status `INITIALIZING`).
    Created,
    /// Command intent: update an agent instance (append turns, advance status).
    Update,
    /// Event intent: an agent instance was updated.
    Updated,
    /// Command intent: complete an agent instance.
    Complete,
    /// Event intent: an agent instance was completed (status `COMPLETED`).
    Completed,
    /// Event intent: an agent instance was migrated to a new process definition.
    Migrated,
}

impl AgentInstanceIntent {
    /// The canonical intent label.
    pub fn as_str(self) -> &'static str {
        match self {
            AgentInstanceIntent::Create => "CREATE",
            AgentInstanceIntent::Created => "CREATED",
            AgentInstanceIntent::Update => "UPDATE",
            AgentInstanceIntent::Updated => "UPDATED",
            AgentInstanceIntent::Complete => "COMPLETE",
            AgentInstanceIntent::Completed => "COMPLETED",
            AgentInstanceIntent::Migrated => "MIGRATED",
        }
    }
}

/// A first-class, engine-owned AgentInstance runtime object (the AgentInstance
/// record value). Keyed by its own dedicated `agent_instance_key` and linked to
/// the activating `element_instance_key`. Stored per process instance in
/// [`crate::state::ProcessInstance::agent_instances`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AgentInstance {
    /// The dedicated key identifying this agent instance.
    pub agent_instance_key: Key,
    /// The deployed agent-definition key, if assigned (`0` = unset).
    #[cfg_attr(feature = "serde", serde(default))]
    pub agent_definition_key: Key,
    /// The element instance that owns this agent instance (the activating
    /// service task / ad-hoc sub-process element instance).
    pub element_instance_key: Key,
    /// Every element instance associated with this agent instance (Camunda
    /// tracks a set; the owning `element_instance_key` is always the first).
    #[cfg_attr(feature = "serde", serde(default))]
    pub element_instance_keys: Vec<Key>,
    /// The BPMN element id of the agent task / ad-hoc sub-process.
    pub element_id: ElementId,
    /// The owning process instance key.
    pub process_instance_key: Key,
    /// The root (top-level ancestor) process instance key.
    pub root_process_instance_key: Key,
    /// The BPMN process id of the owning process definition.
    pub bpmn_process_id: String,
    /// The process definition key the owning instance runs on.
    pub process_definition_key: Key,
    /// The process definition version.
    #[cfg_attr(feature = "serde", serde(default))]
    pub process_definition_version: i32,
    /// The process definition version tag, if any.
    #[cfg_attr(feature = "serde", serde(default))]
    pub process_definition_version_tag: Option<String>,
    /// The tenant id.
    pub tenant_id: String,
    /// The agent type marker this instance was created from.
    pub agent_type: AgentType,
    /// The current status (state machine position).
    pub status: AgentInstanceStatus,
    /// The static agent definition (model/provider/systemPrompt).
    #[cfg_attr(feature = "serde", serde(default))]
    pub definition: AgentDefinition,
    /// The configured limits (`-1` = unlimited).
    #[cfg_attr(feature = "serde", serde(default))]
    pub limits: AgentInstanceLimits,
    /// Aggregated metrics across all loop iterations.
    #[cfg_attr(feature = "serde", serde(default))]
    pub metrics: AgentInstanceMetrics,
    /// The tools available to the agent.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tools: Vec<AgentTool>,
    /// The key of the agent job driving this instance, if any (`0` = none).
    #[cfg_attr(feature = "serde", serde(default))]
    pub job_key: Key,
    /// The agent job's lease deadline, if leased (`0` = none).
    #[cfg_attr(feature = "serde", serde(default))]
    pub job_lease: u64,
    /// The instant this agent instance was created (ms since Unix epoch).
    #[cfg_attr(feature = "serde", serde(default))]
    pub created_at: u64,
    /// The instant this agent instance was last updated (ms since Unix epoch).
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_updated_at: u64,
    /// The instant this agent instance completed, if completed (`0` = not yet).
    #[cfg_attr(feature = "serde", serde(default))]
    pub completed_at: u64,
}

// ---------------------------------------------------------------------------
// AgentHistory turn log (Camunda 8.10 parity, Stage 3 / slice S2)
// ---------------------------------------------------------------------------
//
// The AgentHistory turn log is an **append-only** record of the turns exchanged
// with an agent, keyed by `agent_instance_key` — one [`AgentHistoryRecord`] per
// turn. Records are stamped with a monotonic `agent_history_key` and ordered
// deterministically by `(loop_iteration, produced_at)` (with the mint order as
// the final tiebreak). A freshly-appended turn is [`AgentHistoryCommitStatus::Pending`];
// the COMMIT / DISCARD transitions move it to `Committed` / `Discarded`. Once a
// turn leaves `Pending` it is immutable — the log only ever grows and a turn's
// content never changes. The CREATE/UPDATE *processors* that drive these
// transitions are a later slice (S3); this module + the engine behavior it
// backs provide the record shape and the append/commit/discard mechanics.

/// The author role of an AgentHistory turn (`role`, Camunda stable/8.10).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AgentHistoryRole {
    /// A user / caller turn.
    #[default]
    User,
    /// An assistant (LLM) turn.
    Assistant,
    /// A tool-result turn.
    ToolResult,
    /// A configuration turn (system prompt / limits seed).
    Configuration,
}

impl AgentHistoryRole {
    /// The canonical wire/label value.
    pub fn as_str(self) -> &'static str {
        match self {
            AgentHistoryRole::User => "USER",
            AgentHistoryRole::Assistant => "ASSISTANT",
            AgentHistoryRole::ToolResult => "TOOL_RESULT",
            AgentHistoryRole::Configuration => "CONFIGURATION",
        }
    }
}

/// The type of a single [`AgentHistoryContent`] block (`contentType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AgentHistoryContentType {
    /// Plain text content (`text`).
    Text,
    /// A structured object content (`object`).
    Object,
    /// A document reference content (`documentReference`).
    Document,
}

impl AgentHistoryContentType {
    /// The canonical wire/label value.
    pub fn as_str(self) -> &'static str {
        match self {
            AgentHistoryContentType::Text => "TEXT",
            AgentHistoryContentType::Object => "OBJECT",
            AgentHistoryContentType::Document => "DOCUMENT",
        }
    }
}

/// A single content block of an AgentHistory turn
/// (`content[]{contentType,text,documentReference,object}`).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AgentHistoryContent {
    /// Which of the payload fields carries this block's content.
    pub content_type: AgentHistoryContentType,
    /// Plain-text payload (for `Text`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub text: Option<String>,
    /// An opaque document reference (for `Document`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub document_reference: Option<String>,
    /// A structured object payload (for `Object`); an opaque JSON string so the
    /// engine core stays free of a `serde_json::Value` dependency.
    #[cfg_attr(feature = "serde", serde(default))]
    pub object: Option<String>,
}

impl AgentHistoryContent {
    /// A cheap O(n) estimate of this content block's heap payload in bytes.
    pub fn approx_bytes(&self) -> u64 {
        opt_str_bytes(&self.text)
            + opt_str_bytes(&self.document_reference)
            + opt_str_bytes(&self.object)
    }
}

/// A tool call recorded on an AgentHistory turn
/// (`toolCalls[]{toolCallId,toolName,elementId,arguments}`).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AgentHistoryToolCall {
    /// The provider-assigned tool-call id.
    pub tool_call_id: String,
    /// The name of the tool being called.
    pub tool_name: String,
    /// The BPMN element id of the tool, if resolved.
    #[cfg_attr(feature = "serde", serde(default))]
    pub element_id: Option<ElementId>,
    /// The tool arguments, as an opaque JSON string.
    #[cfg_attr(feature = "serde", serde(default))]
    pub arguments: Option<String>,
}

impl AgentHistoryToolCall {
    /// A cheap O(n) estimate of this tool call's heap payload in bytes,
    /// dominated by the (potentially large) opaque `arguments` JSON.
    pub fn approx_bytes(&self) -> u64 {
        self.tool_call_id.len() as u64
            + self.tool_name.len() as u64
            + opt_str_bytes(&self.element_id)
            + opt_str_bytes(&self.arguments)
    }
}

/// Per-turn LLM metrics (`metrics{...}`) recorded on an AgentHistory turn.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AgentHistoryMetrics {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_token_count: i64,
    pub cache_creation_token_count: i64,
    pub cache_read_token_count: i64,
    pub duration_ms: i64,
}

/// The derived commit status of an AgentHistory turn (`commitStatus`).
///
/// A freshly-appended turn is [`Pending`](Self::Pending). COMMIT moves it to
/// [`Committed`](Self::Committed); DISCARD moves it to
/// [`Discarded`](Self::Discarded). Search defaults to `Committed`;
/// `Pending`/`Discarded` are debugging views (the *filtering* is a later slice,
/// but the lifecycle state lives here).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AgentHistoryCommitStatus {
    /// The turn has been committed and is durable.
    Committed,
    /// The turn is in-flight (freshly appended, not yet committed).
    Pending,
    /// The turn was discarded (rejected before commit).
    Discarded,
}

impl AgentHistoryCommitStatus {
    /// The canonical wire/label value.
    pub fn as_str(self) -> &'static str {
        match self {
            AgentHistoryCommitStatus::Committed => "COMMITTED",
            AgentHistoryCommitStatus::Pending => "PENDING",
            AgentHistoryCommitStatus::Discarded => "DISCARDED",
        }
    }
}

/// The lifecycle intents of an AgentHistory record (Camunda `AgentHistoryIntent`).
///
/// The full set is defined now so downstream slices reuse it. `Create`/`Created`
/// append a turn (PENDING); `Commit`/`Committed` and `Discard`/`Discarded` drive
/// the [`AgentHistoryCommitStatus`] lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AgentHistoryIntent {
    /// Command intent: append (create) a history turn.
    Create,
    /// Event intent: a history turn was created (PENDING).
    Created,
    /// Command intent: commit the pending history turns.
    Commit,
    /// Event intent: history turns were committed (PENDING -> COMMITTED).
    Committed,
    /// Command intent: discard the pending history turns.
    Discard,
    /// Event intent: history turns were discarded (PENDING -> DISCARDED).
    Discarded,
}

impl AgentHistoryIntent {
    /// The canonical intent label.
    pub fn as_str(self) -> &'static str {
        match self {
            AgentHistoryIntent::Create => "CREATE",
            AgentHistoryIntent::Created => "CREATED",
            AgentHistoryIntent::Commit => "COMMIT",
            AgentHistoryIntent::Committed => "COMMITTED",
            AgentHistoryIntent::Discard => "DISCARD",
            AgentHistoryIntent::Discarded => "DISCARDED",
        }
    }
}

/// The turn-level payload of one AgentHistory turn, as supplied to the
/// append behavior in a batch. It carries everything that is *specific to the
/// turn*; the instance-derived context (`agent_instance_key`,
/// `element_instance_key`, `process_instance_key`, `root_process_instance_key`,
/// `bpmn_process_id`, `process_definition_key`, `tenant_id`) and the minted
/// `agent_history_key` are filled in by the engine when the turn is materialised
/// into an [`AgentHistoryRecord`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AgentHistoryTurn {
    /// The agent loop iteration this turn belongs to (primary ordering key).
    pub loop_iteration: i32,
    /// The instant the turn was produced (ms since Unix epoch; secondary
    /// ordering key).
    pub produced_at: u64,
    /// The author role of the turn.
    pub role: AgentHistoryRole,
    /// The content blocks of the turn.
    #[cfg_attr(feature = "serde", serde(default))]
    pub content: Vec<AgentHistoryContent>,
    /// The system prompt in effect for the turn, if any.
    #[cfg_attr(feature = "serde", serde(default))]
    pub system_prompt: Option<String>,
    /// The tool calls issued on the turn.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tool_calls: Vec<AgentHistoryToolCall>,
    /// Per-turn LLM metrics.
    #[cfg_attr(feature = "serde", serde(default))]
    pub metrics: AgentHistoryMetrics,
    /// A stable, worker-supplied identity for the turn (`historyItemId`), used to
    /// detect duplicates across retries.
    #[cfg_attr(feature = "serde", serde(default))]
    pub history_item_id: Option<String>,
    /// The tools available when the turn was produced.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tools: Vec<AgentTool>,
    /// The model that produced the turn, if applicable.
    #[cfg_attr(feature = "serde", serde(default))]
    pub model: Option<String>,
    /// The provider that produced the turn, if applicable.
    #[cfg_attr(feature = "serde", serde(default))]
    pub provider: Option<String>,
    /// The limits in effect for the turn, if carried.
    #[cfg_attr(feature = "serde", serde(default))]
    pub limits: Option<AgentInstanceLimits>,
    /// Whether the turn is a detected duplicate of an already-recorded one.
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_duplicate: bool,
    /// The agent job key that produced this turn, if any (`0` = none).
    #[cfg_attr(feature = "serde", serde(default))]
    pub job_key: Key,
    /// The agent job's lease deadline for this turn, if any (`0` = none).
    #[cfg_attr(feature = "serde", serde(default))]
    pub job_lease: u64,
}

impl AgentHistoryTurn {
    /// A cheap O(n) estimate of this turn's heap payload in bytes, summing its
    /// content blocks, tool calls, available tools, and string fields. Used by
    /// [`crate::command::Command::approx_bytes`] so the Raft propose batcher can
    /// bound coalesced agent-instance command entries by bytes rather than count
    /// (large histories otherwise underestimate to zero and can form oversized
    /// log entries that fail to replicate within the AppendEntries timeout).
    pub fn approx_bytes(&self) -> u64 {
        let content: u64 = self
            .content
            .iter()
            .map(AgentHistoryContent::approx_bytes)
            .sum();
        let tool_calls: u64 = self
            .tool_calls
            .iter()
            .map(AgentHistoryToolCall::approx_bytes)
            .sum();
        let tools: u64 = self.tools.iter().map(AgentTool::approx_bytes).sum();
        content
            + tool_calls
            + tools
            + opt_str_bytes(&self.system_prompt)
            + opt_str_bytes(&self.history_item_id)
            + opt_str_bytes(&self.model)
            + opt_str_bytes(&self.provider)
    }
}
/// A single materialised turn in the AgentHistory turn log (Camunda
/// stable/8.10). One is produced per [`AgentHistoryTurn`] appended.
///
/// Records are held append-only in
/// [`crate::state::ProcessInstance::agent_history`], keyed by
/// `agent_instance_key` and ordered by `(loop_iteration, produced_at,
/// agent_history_key)`. The `commit_status` is the only field that ever changes
/// after materialisation, and only Pending -> Committed / Pending -> Discarded.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AgentHistoryRecord {
    /// The dedicated, monotonic key identifying this history turn.
    pub agent_history_key: Key,
    /// The agent instance this turn belongs to.
    pub agent_instance_key: Key,
    /// The owning element instance (the agent task / ad-hoc sub-process).
    pub element_instance_key: Key,
    /// The owning process instance key.
    pub process_instance_key: Key,
    /// The root (top-level ancestor) process instance key.
    pub root_process_instance_key: Key,
    /// The BPMN process id of the owning process definition.
    pub bpmn_process_id: String,
    /// The process definition key the owning instance runs on.
    pub process_definition_key: Key,
    /// The tenant id.
    pub tenant_id: String,
    /// The agent job key that produced this turn, if any (`0` = none).
    #[cfg_attr(feature = "serde", serde(default))]
    pub job_key: Key,
    /// The agent job's lease deadline for this turn, if any (`0` = none).
    #[cfg_attr(feature = "serde", serde(default))]
    pub job_lease: u64,
    /// The agent loop iteration this turn belongs to (primary ordering key).
    pub loop_iteration: i32,
    /// The author role of the turn.
    pub role: AgentHistoryRole,
    /// The instant the turn was produced (ms since Unix epoch; secondary
    /// ordering key).
    pub produced_at: u64,
    /// The content blocks of the turn.
    #[cfg_attr(feature = "serde", serde(default))]
    pub content: Vec<AgentHistoryContent>,
    /// The system prompt in effect for the turn, if any.
    #[cfg_attr(feature = "serde", serde(default))]
    pub system_prompt: Option<String>,
    /// The tool calls issued on the turn.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tool_calls: Vec<AgentHistoryToolCall>,
    /// Per-turn LLM metrics.
    #[cfg_attr(feature = "serde", serde(default))]
    pub metrics: AgentHistoryMetrics,
    /// A stable, worker-supplied identity for the turn (`historyItemId`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub history_item_id: Option<String>,
    /// The tools available when the turn was produced.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tools: Vec<AgentTool>,
    /// The model that produced the turn, if applicable.
    #[cfg_attr(feature = "serde", serde(default))]
    pub model: Option<String>,
    /// The provider that produced the turn, if applicable.
    #[cfg_attr(feature = "serde", serde(default))]
    pub provider: Option<String>,
    /// The limits in effect for the turn, if carried.
    #[cfg_attr(feature = "serde", serde(default))]
    pub limits: Option<AgentInstanceLimits>,
    /// Whether the turn is a detected duplicate of an already-recorded one.
    #[cfg_attr(feature = "serde", serde(default))]
    pub is_duplicate: bool,
    /// The derived commit status (PENDING when freshly appended).
    pub commit_status: AgentHistoryCommitStatus,
}

impl AgentHistoryRecord {
    /// The deterministic ordering key: `(loop_iteration, produced_at,
    /// agent_history_key)`. The `agent_history_key` breaks ties so records with
    /// an identical `(loop_iteration, produced_at)` keep a stable, mint-ordered
    /// position.
    pub fn order_key(&self) -> (i32, u64, Key) {
        (
            self.loop_iteration,
            self.produced_at,
            self.agent_history_key,
        )
    }
}
