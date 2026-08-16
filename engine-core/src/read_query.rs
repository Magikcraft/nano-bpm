//! Read/query surface: the gateway's REST *read* operations, as a single
//! enum.
//!
//! [`Command`](crate::Command) is the engine's **write** surface — every way to
//! drive execution — and it is the lever the wasm parity gate
//! (`engine-wasm/src/surface_parity.rs`) matches on to force a conscious
//! `TestEngine` decision whenever a new capability lands. The **read** surface
//! had no such lever: the gateway answers its REST reads with plain `&self`
//! query methods on the read model, so there was no enum to match on and a newly
//! served read could never break an exhaustiveness check.
//!
//! [`ReadQuery`] closes that gap. It enumerates the read-model-backed REST read
//! operations the gateway serves (the data-plane reads over process-execution
//! state), so the wasm parity gate can `match` over it wildcard-free exactly the
//! way it matches over `Command`. Adding a new served read means adding a variant
//! here, which then breaks the parity `classify` until a human decides whether
//! the in-browser `TestEngine` should surface it too.
//!
//! It lives in `engine-core` — the same pure-Rust crate as `Command`, depended
//! on unconditionally by both the `server` gateway and `engine-wasm` — precisely
//! because the parity gate is type-checked under the feature-off, C-toolchain-free
//! `wasm32` build (`make engine-wasm-check`) that must stay byte-identical to
//! baseline. The concrete `*Row` result types live in the `read-model` crate, but
//! that crate cannot be referenced from the feature-off wasm build (its backends
//! pull a C SQLite). `ReadQuery` carries no read-model types — it is a pure
//! marker enum, one unit variant per REST read operation — so it shares `Command`'s
//! toolchain footprint exactly.
//!
//! Like the classifier it feeds, `ReadQuery` is a compile-time sentinel: it is
//! never constructed on a runtime hot path, so the feature-off engine links
//! nothing new for it.

/// The read-model-backed REST read operations the gateway serves.
///
/// One unit variant per distinct read-model-backed query surface, keyed to its
/// backing `nanobpmn_read_model::ReadStore` query method. A variant is named
/// after a representative OpenAPI `operationId` (equivalently, the `TestEngine`
/// JS method where one exists), but a single variant may back **several** REST
/// operations that share one read path (e.g. `GetResourceByKey` serves
/// `getResource` / `getResourceContent` / `getResourceContentBinary`) — the doc
/// on each variant lists the operations it covers. The
/// variant set is intentionally scoped to the reads a read model can answer (the
/// same surface the in-browser test engine can serve), not the gateway's
/// identity/admin/statistics endpoints.
///
/// This enum is the read counterpart of [`Command`](crate::Command): extend it
/// when the gateway begins serving a new read, and the wasm parity gate will
/// force a conscious `TestEngine` decision for it.
///
/// Deliberately **not** `#[non_exhaustive]`: the parity gate lives in a different
/// crate (`engine-wasm`), and `#[non_exhaustive]` would force it to add a
/// wildcard `_` arm, silently swallowing new variants — the exact drift this gate
/// exists to prevent. A downstream `match` must stay exhaustive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReadQuery {
    /// `getFormByKey` — the deployed form schema for a form key
    /// (`ReadStore::form_by_key`).
    GetFormByKey,
    /// `getResource` / `getResourceContent` / `getResourceContentBinary` — a
    /// deployed generic resource by key (`ReadStore::resource_by_key`).
    GetResourceByKey,
    /// `searchResources` — deployed resource metadata (`ReadStore::resources_meta`).
    SearchResources,

    /// `searchProcessInstances` — process instances (`ReadStore::process_instances`).
    SearchProcessInstances,
    /// `getProcessInstance` — a single process instance by key
    /// (`ReadStore::process_instance`).
    GetProcessInstance,

    /// `searchUserTasks` — user tasks, honouring the `state` filter
    /// (`ReadStore::user_tasks`).
    SearchUserTasks,
    /// `getUserTask` — a single user task by key. Served by a filtered scan of
    /// `ReadStore::user_tasks` (there is no dedicated point-lookup method).
    GetUserTask,

    /// `searchVariables` — process/element variables (`ReadStore::variables`).
    SearchVariables,
    /// `getVariable` — a single variable by key (`ReadStore::variable`).
    GetVariable,

    /// `searchJobs` — jobs (`ReadStore::jobs`).
    SearchJobs,

    /// `searchIncidents` — incidents (`ReadStore::incidents`).
    SearchIncidents,
    /// `getIncident` — a single incident by key (`ReadStore::incident`).
    GetIncident,

    /// `searchElementInstances` — element (flow-node) instances
    /// (`ReadStore::element_instances`).
    SearchElementInstances,
    /// `getElementInstance` — a single element instance by key
    /// (`ReadStore::element_instance`).
    GetElementInstance,

    /// `searchMessageSubscriptions` — open message subscriptions
    /// (`ReadStore::message_subscriptions`).
    SearchMessageSubscriptions,
    /// `searchCorrelatedMessageSubscriptions` — correlated message subscriptions
    /// (`ReadStore::correlated_message_subscriptions`).
    SearchCorrelatedMessageSubscriptions,

    /// `searchProcessDefinitions` — deployed process definitions
    /// (`ReadStore::process_definitions`).
    SearchProcessDefinitions,
    /// `getProcessDefinitionXML` — a process definition's BPMN XML
    /// (`ReadStore::process_definition_xml`).
    GetProcessDefinitionXml,

    /// `searchDecisionInstances` — DMN decision-evaluation instances
    /// (`ReadStore::decision_instances`).
    SearchDecisionInstances,
    /// `getDecisionInstance` — a single decision instance
    /// (`ReadStore::decision_instance`).
    GetDecisionInstance,
    /// `searchDecisionDefinitions` — deployed decision definitions
    /// (`ReadStore::decision_definitions`).
    SearchDecisionDefinitions,
    /// `getDecisionDefinitionXML` — a decision definition's DMN XML
    /// (`ReadStore::decision_definition_xml`).
    GetDecisionDefinitionXml,
    /// `searchDecisionRequirements` — deployed decision requirements graphs
    /// (`ReadStore::decision_requirements`).
    SearchDecisionRequirements,
    /// `getDecisionRequirementsXML` — a decision requirements graph's DMN XML
    /// (`ReadStore::decision_requirements_xml`).
    GetDecisionRequirementsXml,
}
