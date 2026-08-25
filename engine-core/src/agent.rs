//! Engine-native AgentInstance model (Camunda 8.10 parity, Stage 3).
//!
//! The engine is the system-of-record for AgentInstance state. This module
//! holds the *shape* of that state: the `zeebe:agentDefinition` marker
//! ([`AgentType`]), the static per-element agent definition
//! ([`AgentDefinition`]) and its optional limits ([`AgentInstanceLimits`]) that
//! ride on [`crate::model::ElementKind::AgentTask`], and the runtime
//! [`AgentInstance`] record — a first-class object keyed by its own dedicated
//! `agent_instance_key`, linked to the activating `element_instance_key`, and
//! driven through the [`AgentInstanceStatus`] state machine.
//!
//! LLM calls / prompt assembly / tool dispatch are deliberately **out of
//! scope**: those live in the worker layer. Here we only model the state the
//! engine owns. The lifecycle *processors* (CREATE/UPDATE/COMPLETE) are a later
//! slice; this slice makes a `CREATED` AgentInstance (status `INITIALIZING`)
//! representable and defines the full intent enum downstream slices reuse.

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
    /// `external` — an externally-managed agent.
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
/// concrete values are typically supplied by the worker/config at CREATE time;
/// an empty string is normalised to `None`.
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
