//! **Semantic annotation schema (v0)** — the interface between the LLM that
//! labels a BPMN model and the constraint solver that lays it out.
//!
//! The schema is deliberately narrow: we've picked the three or four claims
//! about a model that carry the most layout signal, expressed them declaratively,
//! and left everything else out. As the layout engine learns which annotations
//! actually change diagrams for the better, this schema will grow — but each
//! addition should be judged by whether a hand-authored fixture and its LLM
//! counterpart both move the layout in the same intended direction.
//!
//! # JSON shape
//! ```json
//! {
//!   "flows": [
//!     { "id": "happy",  "kind": "primary",     "nodes": ["Start_1", "Task_A", "End_1"] },
//!     { "id": "err",    "kind": "exception",   "nodes": ["Task_A", "Handle_Error"] },
//!     { "id": "esc",    "kind": "escalation",  "nodes": ["Task_B", "Notify_Manager"] }
//!   ],
//!   "clusters":  [{ "id": "validation", "nodes": ["Task_A", "Task_B"], "affinity": 0.8 }],
//!   "roles":     { "Task_Review": "decision" }
//! }
//! ```
//!
//! The full authored form is described in `docs/layout.md` (also the file we
//! feed the LLM as prompt reference).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Top-level semantic annotations for a single BPMN process.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SemanticAnnotations {
    /// Named node-sequences we want visually grouped by *kind* — the primary
    /// flow is a horizontal centerline, exceptions drop below it, escalations
    /// rise above. Order matters within a flow: the sequence hints that the
    /// solver should keep these nodes near their own kind's band even if their
    /// executable predecessors sit on a different band.
    #[serde(default)]
    pub flows: Vec<AnnotatedFlow>,

    /// Nodes that belong together semantically (e.g. a validation cluster).
    /// Members get pulled toward their shared centroid with a spring stiffness
    /// of `affinity` (0..1). MVP: only affects the y-row; x-column stays under
    /// the Sugiyama rank.
    #[serde(default)]
    pub clusters: Vec<Cluster>,

    /// Per-node role hints — reserved for future shape/colour cues. The v0
    /// solver ignores this; only the debug SVG uses it to colour-code shapes.
    #[serde(default)]
    pub roles: BTreeMap<String, Role>,
}

/// A named sequence of node ids the LLM claims belongs to one narrative flow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnnotatedFlow {
    pub id: String,
    pub kind: FlowKind,
    pub nodes: Vec<String>,
}

/// The four narrative kinds v0 knows about. Each maps to a target y-band the
/// solver pulls its member nodes toward.
///
/// Precedence when a node appears in multiple flows: **exception > escalation
/// > compensation > primary**. Rationale: if a node is genuinely part of the
/// happy path but is *also* how you handle a specific error, the diagram is
/// more useful with the node off the centerline (the exception context is the
/// interesting one).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FlowKind {
    /// The happy path — nodes get pulled to the horizontal centerline (row 0).
    Primary,
    /// Error handling — nodes drop below the centerline.
    Exception,
    /// Escalation to a supervisor / out-of-band notification — nodes rise
    /// above the centerline.
    Escalation,
    /// Compensating actions — well below the centerline, in their own band
    /// (undoing side effects reads naturally "underneath" the forward flow).
    Compensation,
}

impl FlowKind {
    /// Target row offset (in Sugiyama grid rows) for this kind. Positive = down
    /// the page (matches BPMN DI's y-axis convention). Sizes are picked to
    /// leave one clear row between bands at the default 110px `ROW` in
    /// [`append_diagram`](crate::bpmn_model).
    pub fn target_row(self) -> f64 {
        match self {
            FlowKind::Primary => 0.0,
            FlowKind::Escalation => -2.0,
            FlowKind::Exception => 2.0,
            FlowKind::Compensation => 3.0,
        }
    }

    /// Higher = wins when a node is claimed by multiple flows. See the type
    /// doc-comment for the rationale.
    pub fn priority(self) -> u8 {
        match self {
            FlowKind::Exception => 4,
            FlowKind::Escalation => 3,
            FlowKind::Compensation => 2,
            FlowKind::Primary => 1,
        }
    }
}

/// A soft group — members get attracted to each other (blended toward the
/// group centroid) after the flow-based bias is applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    pub id: String,
    pub nodes: Vec<String>,
    #[serde(default = "default_affinity")]
    pub affinity: f64,
}

fn default_affinity() -> f64 {
    0.5
}

/// Presentation hint for a node. Reserved for future shape/colour cues; the
/// v0 solver ignores this. Only the debug SVG uses it to colour-code shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Decision,
    Review,
    Notification,
    Compensation,
    External,
}
