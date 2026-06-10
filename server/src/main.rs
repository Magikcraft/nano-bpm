//! Stub server for the generated Orchestration Cluster REST layer.
//!
//! This binary wires the generated `nanobpm_gateway_rest` REST layer (models,
//! routes, and per-tag service traits) into a runnable `axum` server. No backend
//! services are connected yet: every operation is implemented as a stub that
//! responds with `501 Not Implemented`.
//!
//! The per-tag trait implementations live in the generated `stub_impls` module
//! (see scripts/gen-stub-server.py). This file owns only the stable pieces: the
//! `ServerImpl` type, authentication/error glue, and the server bootstrap.

mod journal;
mod query;
mod readstore;
mod stub_impls;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::Multipart;
use axum::response::Response;
use nanobpm_gateway_rest::{apis, models, types};
use http::StatusCode;
use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{
    ActivatedJob, Command, Engine, EngineError, Event, IncidentKind, IncidentState, ProcessBuilder,
    ProcessInstanceState, Value,
};

use crate::journal::Journal;
use crate::readstore::ReadStore;

/// Default long-poll window (ms) when a client passes `requestTimeout` 0.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5_000;

/// The single type that implements every generated API trait.
///
/// It owns an embedded [`Engine`] (the `engine-core` crate), wrapped in a
/// durable [`Journal`], behind a read/write lock. The engine is a single writer,
/// so mutating operations take the write lock (serialized), while the read-only
/// `search*`/`get*` projections take the read lock and therefore run
/// concurrently across cores. Most operations are still 501 stubs (see
/// the generated `stub_impls` module); a few — process-instance creation, job
/// activation, and job completion — are wired to the engine via the inherent
/// methods below and routed from the stub generator's override table. Every
/// durable command is appended to the journal so engine state survives a
/// restart.
#[derive(Clone)]
pub struct ServerImpl {
    journal: Arc<RwLock<Journal>>,
    /// The read model. All `search*`/`get*` queries are answered from here
    /// (eventually consistent), never from hot engine state.
    store: Arc<ReadStore>,
    /// Notified whenever new jobs may have become activatable, so long-polling
    /// `activateJobs` requests can wake immediately instead of waiting out their
    /// full timeout.
    jobs_available: Arc<tokio::sync::Notify>,
}

impl ServerImpl {
    /// Builds a server over `journal` and its read `store`, seeding the demo
    /// process only when the journal is fresh (an existing log already carries
    /// its deployment, and re-deploying would mint a spurious second version on
    /// every restart). The `journal`'s exporter must already be wired so the
    /// seed deployment is projected into the store.
    pub fn new(mut journal: Journal, store: Arc<ReadStore>) -> Self {
        if journal.is_fresh() {
            // Pre-deploy a demo process so `createProcessInstance` (by id "demo")
            // has something to start. A real build would deploy from BPMN XML.
            let demo = ProcessBuilder::new("demo")
                .start_event("start")
                .service_task("work", "demo-work")
                .end_event("end")
                .connect("start", "work")
                .connect("work", "end")
                .build()
                .expect("valid demo process");
            // Seed durability is non-critical: a fresh journal re-seeds on every
            // start, so we don't await the commit here.
            let _ = journal
                .apply_command(Command::DeployProcess(demo))
                .expect("deploy demo process");
        }
        Self {
            journal: Arc::new(RwLock::new(journal)),
            store,
            jobs_available: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl Default for ServerImpl {
    fn default() -> Self {
        let store = Arc::new(ReadStore::open(None).expect("open in-memory read store"));
        build_server(Journal::in_memory(), store)
    }
}

/// Wires the read-model exporter onto `journal`, builds the [`ServerImpl`] (which
/// seeds the demo process when the journal is fresh — that deployment is then
/// forwarded to the exporter), and spawns the background exporter thread. The
/// exporter must be set before `ServerImpl::new` so the seed deployment is
/// projected; the thread is spawned after so it can hold the server's journal
/// handle for hot-state eviction.
fn build_server(mut journal: Journal, store: Arc<ReadStore>) -> ServerImpl {
    let (tx, rx) = mpsc::channel::<Vec<Event>>();
    journal.set_exporter(tx);
    let server = ServerImpl::new(journal, store.clone());
    spawn_exporter(rx, store, server.journal.clone());
    server
}

/// Spawns the read-model exporter thread. It drains the channel (batching every
/// queued command's events), projects the batch into the read store, then evicts
/// any now-completed instances from hot engine state under the journal write
/// lock. The thread exits when the channel closes (all `ServerImpl` clones and
/// the journal are dropped).
fn spawn_exporter(
    rx: mpsc::Receiver<Vec<Event>>,
    store: Arc<ReadStore>,
    journal: Arc<RwLock<Journal>>,
) {
    std::thread::Builder::new()
        .name("nanobpmn-exporter".into())
        .spawn(move || {
            while let Ok(first) = rx.recv() {
                let mut batch = first;
                while let Ok(next) = rx.try_recv() {
                    batch.extend(next);
                }
                let completed = match store.export(&batch) {
                    Ok(keys) => keys,
                    Err(e) => {
                        tracing::error!("read-model export failed: {e}");
                        continue;
                    }
                };
                if !completed.is_empty() {
                    let mut journal = journal.write().expect("engine lock poisoned");
                    for key in completed {
                        journal.evict_instance(key);
                    }
                    journal.shrink();
                }
            }
        })
        .expect("spawn read-model exporter thread");
}

impl AsRef<ServerImpl> for ServerImpl {
    fn as_ref(&self) -> &ServerImpl {
        self
    }
}

fn problem(title: &str, status: u16, detail: String) -> models::ProblemDetail {
    models::ProblemDetail::new(title.to_string(), status, detail, String::new())
}

/// Engine-backed implementations of selected operations. The stub generator
/// (scripts/gen-stub-server.py) emits trait methods that delegate here.
impl ServerImpl {
    async fn create_process_instance_impl(
        &self,
        body: &models::ProcessInstanceCreationInstruction,
    ) -> Result<apis::process_instance::CreateProcessInstanceResponse, ()> {
        use apis::process_instance::CreateProcessInstanceResponse as Resp;

        // Apply under the engine write lock inside a scope so the (non-Send) lock
        // guard is dropped before we await durability. The block yields the
        // response plus the commit to await (None on the error paths, which wrote
        // nothing).
        let (response, commit) = {
            let mut engine = self.journal.write().expect("engine lock poisoned");

            // The engine starts processes by BPMN process id. A creation-by-key
            // request is resolved to its process id by looking up the deployed
            // definition whose key matches; an unknown key is rejected as invalid
            // input (the create endpoint has no 404 variant).
            let process_id = match body {
                models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(
                    b,
                ) => b.process_definition_id.clone(),
                models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(
                    b,
                ) => {
                    let requested = &b.process_definition_key.0;
                    match engine
                        .state()
                        .processes
                        .values()
                        .find(|d| d.key.to_string() == *requested)
                    {
                        Some(d) => d.definition.id.clone(),
                        None => {
                            return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                                "Process not found",
                                400,
                                format!("No deployed process with key '{requested}'."),
                            )));
                        }
                    }
                }
            };

            match engine
                .apply_command_at(Command::create_instance(process_id.clone()), now_millis())
            {
                Ok((events, commit)) => {
                    let instance_key = events
                        .iter()
                        .find_map(Event::instance_key)
                        .expect("created instance has a key");
                    // Project the real deployed key and version now that the
                    // instance exists, so by-id and by-key requests report the
                    // same definition identity.
                    let (definition_key, version) = engine
                        .state()
                        .processes
                        .get(&process_id)
                        .map(|d| (d.key.to_string(), d.version))
                        .unwrap_or_else(|| (process_id.clone(), 1));
                    let result = models::CreateProcessInstanceResult::new(
                        process_id.clone(),
                        version,
                        "<default>".to_string(),
                        std::collections::HashMap::new(),
                        models::ProcessDefinitionKey(definition_key),
                        models::ProcessInstanceKey(instance_key.to_string()),
                        Vec::new(),
                        nanobpm_gateway_rest::types::Nullable::Null,
                    );
                    (
                        Resp::Status200_TheProcessInstanceWasCreated(result),
                        Some(commit),
                    )
                }
                Err(EngineError::ProcessNotFound { process_id }) => (
                    Resp::Status400_TheProvidedDataIsNotValid(problem(
                        "Process not found",
                        400,
                        format!("No deployed process with id '{process_id}'."),
                    )),
                    None,
                ),
                Err(e) => (
                    Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                        "Internal error",
                        500,
                        e.to_string(),
                    )),
                    None,
                ),
            }
        };

        // Block on durability before acknowledging: a returned 200 means the
        // create is fsynced.
        if let Some(commit) = commit {
            commit.wait().await;
            // Starting an instance parks it on its first service task, so new
            // jobs may now be activatable: wake any long-pollers.
            self.jobs_available.notify_waiters();
        }
        Ok(response)
    }

    async fn complete_job_impl(
        &self,
        path_params: &models::CompleteJobPathParams,
        body: &Option<models::JobCompletionRequest>,
    ) -> Result<apis::job::CompleteJobResponse, ()> {
        use apis::job::CompleteJobResponse as Resp;

        let job_key: u64 = match path_params.job_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheJobWithTheGivenKeyWasNotFound(problem(
                    "Job not found",
                    404,
                    format!("Job key '{}' is not a valid key.", path_params.job_key),
                )));
            }
        };

        // Variables the worker returns are merged into the instance, so they can
        // drive downstream gateway routing.
        let variables = body
            .as_ref()
            .and_then(|b| b.variables.as_ref())
            .and_then(|v| match v {
                types::Nullable::Present(map) => Some(from_object_map(map)),
                types::Nullable::Null => None,
            })
            .unwrap_or_default();

        let result = {
            let mut engine = self.journal.write().expect("engine lock poisoned");
            engine.apply_command_at(Command::complete_job_with(job_key, variables), now_millis())
        };
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                // Completing a job may advance the token onto a following service
                // task, creating a new activatable job: wake any long-pollers.
                self.jobs_available.notify_waiters();
                Ok(Resp::Status204_TheJobWasCompletedSuccessfully)
            }
            Err(EngineError::JobNotFound { job_key }) => {
                Ok(Resp::Status404_TheJobWithTheGivenKeyWasNotFound(problem(
                    "Job not found",
                    404,
                    format!("No job with key {job_key}."),
                )))
            }
            Err(EngineError::JobNotActive { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "Job not active",
                    409,
                    format!("Job {job_key} is not active and cannot be completed."),
                )),
            ),
            Err(EngineError::JobNotActivated { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "Job not activated",
                    409,
                    format!("Job {job_key} has not been activated and cannot be completed."),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn fail_job_impl(
        &self,
        path_params: &models::FailJobPathParams,
        body: &Option<models::JobFailRequest>,
    ) -> Result<apis::job::FailJobResponse, ()> {
        use apis::job::FailJobResponse as Resp;

        let job_key: u64 = match path_params.job_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheJobWithTheGivenJobKeyIsNotFound(problem(
                    "Job not found",
                    404,
                    format!("Job key '{}' is not a valid key.", path_params.job_key),
                )));
            }
        };

        let retries = body.as_ref().and_then(|b| b.retries).unwrap_or(0);
        let error_message = body
            .as_ref()
            .and_then(|b| b.error_message.clone())
            .unwrap_or_default();

        let result = {
            let mut engine = self.journal.write().expect("engine lock poisoned");
            engine.apply_command_at(Command::fail_job(job_key, retries, error_message), now_millis())
        };
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                // Failing with retries left returns the job to the activatable
                // pool, so wake any long-pollers.
                self.jobs_available.notify_waiters();
                Ok(Resp::Status204_TheJobIsFailed)
            }
            Err(EngineError::JobNotFound { job_key }) => {
                Ok(Resp::Status404_TheJobWithTheGivenJobKeyIsNotFound(problem(
                    "Job not found",
                    404,
                    format!("No job with key {job_key}."),
                )))
            }
            Err(EngineError::JobNotActive { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongState(problem(
                    "Job in wrong state",
                    409,
                    format!("Job {job_key} cannot be failed in its current state."),
                )),
            ),
            Err(EngineError::JobNotActivated { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongState(problem(
                    "Job not activated",
                    409,
                    format!("Job {job_key} has not been activated and cannot be failed."),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn throw_job_error_impl(
        &self,
        path_params: &models::ThrowJobErrorPathParams,
        body: &models::JobErrorRequest,
    ) -> Result<apis::job::ThrowJobErrorResponse, ()> {
        use apis::job::ThrowJobErrorResponse as Resp;

        let job_key: u64 = match path_params.job_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(
                    Resp::Status404_TheJobWithTheGivenKeyWasNotFoundOrIsNotActivated(problem(
                        "Job not found",
                        404,
                        format!("Job key '{}' is not a valid key.", path_params.job_key),
                    )),
                );
            }
        };

        let error_message = match body.error_message.as_ref() {
            Some(types::Nullable::Present(msg)) => msg.clone(),
            _ => String::new(),
        };

        let result = {
            let mut engine = self.journal.write().expect("engine lock poisoned");
            engine.apply_command_at(
                Command::throw_job_error(job_key, body.error_code.clone(), error_message),
                now_millis(),
            )
        };
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                // A caught error can route the token onto a following service
                // task, creating a new activatable job: wake any long-pollers.
                self.jobs_available.notify_waiters();
                Ok(Resp::Status204_AnErrorIsThrownForTheJob)
            }
            Err(EngineError::JobNotFound { job_key }) => Ok(
                Resp::Status404_TheJobWithTheGivenKeyWasNotFoundOrIsNotActivated(problem(
                    "Job not found",
                    404,
                    format!("No job with key {job_key}."),
                )),
            ),
            Err(EngineError::JobNotActivated { job_key }) => Ok(
                Resp::Status404_TheJobWithTheGivenKeyWasNotFoundOrIsNotActivated(problem(
                    "Job not activated",
                    404,
                    format!("Job {job_key} has not been activated and cannot throw an error."),
                )),
            ),
            Err(EngineError::JobNotActive { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "Job in wrong state",
                    409,
                    format!("Job {job_key} is not active and cannot throw an error."),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn update_job_impl(
        &self,
        path_params: &models::UpdateJobPathParams,
        body: &models::JobUpdateRequest,
    ) -> Result<apis::job::UpdateJobResponse, ()> {
        use apis::job::UpdateJobResponse as Resp;

        let job_key: u64 = match path_params.job_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheJobWithTheJobKeyIsNotFound(problem(
                    "Job not found",
                    404,
                    format!("Job key '{}' is not a valid key.", path_params.job_key),
                )));
            }
        };

        // The POC only acts on the retries part of the changeset (timeout
        // updates are not modelled). A changeset without retries is a no-op.
        let retries = match body.changeset.retries.as_ref() {
            Some(types::Nullable::Present(r)) => *r,
            _ => {
                return Ok(Resp::Status204_TheJobWasUpdatedSuccessfully);
            }
        };

        let result = {
            let mut engine = self.journal.write().expect("engine lock poisoned");
            engine.apply_command_at(Command::update_job_retries(job_key, retries), now_millis())
        };
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                Ok(Resp::Status204_TheJobWasUpdatedSuccessfully)
            }
            Err(EngineError::JobNotFound { job_key }) => {
                Ok(Resp::Status404_TheJobWithTheJobKeyIsNotFound(problem(
                    "Job not found",
                    404,
                    format!("No job with key {job_key}."),
                )))
            }
            Err(EngineError::JobNotActive { job_key }) => Ok(
                Resp::Status409_TheJobWithTheGivenKeyIsInTheWrongStateCurrently(problem(
                    "Job in wrong state",
                    409,
                    format!("Job {job_key} is terminal and its retries cannot be updated."),
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn resolve_incident_impl(
        &self,
        path_params: &models::ResolveIncidentPathParams,
        body: &Option<models::IncidentResolutionRequest>,
    ) -> Result<apis::incident::ResolveIncidentResponse, ()> {
        use apis::incident::ResolveIncidentResponse as Resp;

        let incident_key: u64 = match path_params.incident_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheIncidentWithTheIncidentKeyIsNotFound(
                    problem(
                        "Incident not found",
                        404,
                        format!(
                            "Incident key '{}' is not a valid key.",
                            path_params.incident_key
                        ),
                    ),
                ));
            }
        };

        let operation_reference = body.as_ref().and_then(|b| b.operation_reference);
        let command = Command::ResolveIncident {
            incident_key,
            operation_reference,
        };

        let result = {
            let mut engine = self.journal.write().expect("engine lock poisoned");
            engine.apply_command_at(command, now_millis())
        };
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                // Resolving a job-incident returns the job to the activatable
                // pool, so wake any long-pollers.
                self.jobs_available.notify_waiters();
                Ok(Resp::Status204_TheIncidentIsMarkedAsResolved)
            }
            Err(EngineError::IncidentNotFound { incident_key }) => Ok(
                Resp::Status404_TheIncidentWithTheIncidentKeyIsNotFound(problem(
                    "Incident not found",
                    404,
                    format!("No incident with key {incident_key}."),
                )),
            ),
            Err(EngineError::IncidentNotResolvable { reason, .. }) => Ok(
                Resp::Status409_TheIncidentCannotBeResolvedDueToAnInvalidState(problem(
                    "Incident not resolvable",
                    409,
                    reason,
                )),
            ),
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    /// `PUT /v2/element-instances/{elementInstanceKey}/variables` — merge
    /// variables into a scope so an operator can correct the data behind an
    /// incident before resolving it. The path key may be the process instance
    /// key or an element instance key; both resolve to nano's single
    /// instance-level scope (so `local` is accepted but has no effect).
    async fn create_element_instance_variables_impl(
        &self,
        path_params: &models::CreateElementInstanceVariablesPathParams,
        body: &models::SetVariableRequest,
    ) -> Result<apis::element_instance::CreateElementInstanceVariablesResponse, ()> {
        use apis::element_instance::CreateElementInstanceVariablesResponse as Resp;

        let scope_key: u64 = match path_params.element_instance_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                    "Invalid key",
                    400,
                    format!(
                        "Element instance key '{}' is not a valid key.",
                        path_params.element_instance_key
                    ),
                )));
            }
        };

        let variables = from_object_map(&body.variables);

        let result = {
            let mut engine = self.journal.write().expect("engine lock poisoned");
            engine.apply_command_at(Command::set_variables(scope_key, variables), now_millis())
        };
        match result {
            Ok((_, commit)) => {
                commit.wait().await;
                Ok(Resp::Status204_TheVariablesWereUpdated)
            }
            Err(EngineError::ScopeNotFound { scope_key }) => {
                Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                    "Scope not found",
                    400,
                    format!("No process or element instance with key {scope_key}."),
                )))
            }
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }

    async fn get_process_instance_impl(
        &self,
        path_params: &models::GetProcessInstancePathParams,
    ) -> Result<apis::process_instance::GetProcessInstanceResponse, ()> {
        use apis::process_instance::GetProcessInstanceResponse as Resp;

        let key: u64 = match path_params.process_instance_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(
                    Resp::Status404_TheProcessInstanceWithTheGivenKeyWasNotFound(problem(
                        "Process instance not found",
                        404,
                        format!(
                            "Process instance key '{}' is not a valid key.",
                            path_params.process_instance_key
                        ),
                    )),
                );
            }
        };

        let result = self.store.process_instance(key);
        match result {
            Some(instance) => Ok(Resp::Status200_TheProcessInstanceIsSuccessfullyReturned(
                process_instance_result(&instance),
            )),
            None => Ok(
                Resp::Status404_TheProcessInstanceWithTheGivenKeyWasNotFound(problem(
                    "Process instance not found",
                    404,
                    format!("No process instance with key {key}."),
                )),
            ),
        }
    }

    /// Reports the cluster topology. nanobpmn is a single-writer, single-partition
    /// embedded engine, so it always advertises a one-broker, one-partition cluster
    /// with this gateway acting as the healthy leader of partition 1. The broker and
    /// gateway versions both report the server crate version.
    async fn get_topology_impl(&self) -> Result<apis::cluster::GetTopologyResponse, ()> {
        use apis::cluster::GetTopologyResponse as Resp;

        let version = env!("CARGO_PKG_VERSION").to_string();
        let port: i32 = std::env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8080);

        let partition = models::Partition {
            partition_id: 1,
            role: "leader".to_string(),
            health: "healthy".to_string(),
        };

        let broker = models::BrokerInfo {
            node_id: 0,
            host: "0.0.0.0".to_string(),
            port,
            partitions: vec![partition],
            version: version.clone(),
        };

        let topology = models::TopologyResponse {
            brokers: vec![broker],
            cluster_id: types::Nullable::Null,
            cluster_size: 1,
            partitions_count: 1,
            replication_factor: 1,
            gateway_version: version,
            last_completed_change_id: String::new(),
        };

        Ok(Resp::Status200_ObtainsTheCurrentTopologyOfTheClusterTheGatewayIsPartOf(topology))
    }

    /// Publishes a message and correlates it to any matching open subscriptions.
    /// nanobpmn does not buffer messages (no TTL/dedup): the message is minted,
    /// correlated to every matching open subscription, then dropped. Always
    /// returns 200 with the minted message key.
    async fn publish_message_impl(
        &self,
        body: &models::MessagePublicationRequest,
    ) -> Result<apis::message::PublishMessageResponse, ()> {
        use apis::message::PublishMessageResponse as Resp;

        let correlation_key = body.correlation_key.clone().unwrap_or_default();
        let variables = body
            .variables
            .as_ref()
            .map(from_object_map)
            .unwrap_or_default();

        let (events, commit) = {
            let mut engine = self.journal.write().expect("engine lock poisoned");
            engine
                .apply_command_at(
                    Command::correlate_message_with(body.name.clone(), correlation_key, variables),
                    now_millis(),
                )
                .expect("CorrelateMessage never fails")
        };
        let message_key = message_key_of(&events);
        commit.wait().await;

        // Correlation may have advanced a token onto a service task, creating a
        // new activatable job: wake any long-pollers.
        self.jobs_available.notify_waiters();

        let result = models::MessagePublicationResult::new(
            "<default>".to_string(),
            models::MessageKey(message_key.to_string()),
        );
        Ok(Resp::Status200_TheMessageWasPublished(result))
    }

    /// Correlates a message to a matching open subscription. Unlike
    /// [`Self::publish_message_impl`], returns 404 when nothing correlates, and
    /// reports the first correlated process instance.
    async fn correlate_message_impl(
        &self,
        body: &models::MessageCorrelationRequest,
    ) -> Result<apis::message::CorrelateMessageResponse, ()> {
        use apis::message::CorrelateMessageResponse as Resp;

        let correlation_key = body.correlation_key.clone().unwrap_or_default();
        let variables = body
            .variables
            .as_ref()
            .map(from_object_map)
            .unwrap_or_default();

        let (events, commit) = {
            let mut engine = self.journal.write().expect("engine lock poisoned");
            engine
                .apply_command_at(
                    Command::correlate_message_with(body.name.clone(), correlation_key, variables),
                    now_millis(),
                )
                .expect("CorrelateMessage never fails")
        };
        let message_key = message_key_of(&events);
        // A message correlates either to an existing instance's open subscription
        // (MessageCorrelated) or, via a message start event, by creating a new
        // instance (ProcessInstanceCreated). Either way it correlated to an
        // instance; report the first matched instance key.
        let correlated_instance = events.iter().find_map(|e| match e {
            Event::MessageCorrelated { instance_key, .. } => Some(*instance_key),
            Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
            _ => None,
        });
        commit.wait().await;

        match correlated_instance {
            Some(instance_key) => {
                // Correlation may have advanced a token onto a service task,
                // creating a new activatable job: wake any long-pollers.
                self.jobs_available.notify_waiters();
                let result = models::MessageCorrelationResult::new(
                    "<default>".to_string(),
                    models::MessageKey(message_key.to_string()),
                    models::ProcessInstanceKey(instance_key.to_string()),
                );
                Ok(Resp::Status200_TheMessageIsCorrelatedToOneOrMoreProcessInstances(result))
            }
            None => Ok(Resp::Status404_NotFound(problem(
                "Message not correlated",
                404,
                format!(
                    "No open subscription matched message '{}' with the given correlation key.",
                    body.name
                ),
            ))),
        }
    }

    async fn get_incident_impl(
        &self,
        path_params: &models::GetIncidentPathParams,
    ) -> Result<apis::incident::GetIncidentResponse, ()> {
        use apis::incident::GetIncidentResponse as Resp;

        let key: u64 = match path_params.incident_key.parse() {
            Ok(k) => k,
            Err(_) => {
                return Ok(Resp::Status404_TheIncidentWithTheGivenKeyWasNotFound(
                    problem(
                        "Incident not found",
                        404,
                        format!(
                            "Incident key '{}' is not a valid key.",
                            path_params.incident_key
                        ),
                    ),
                ));
            }
        };

        let result = self.store.incident(key);
        match result {
            Some(incident) => Ok(Resp::Status200_TheIncidentIsSuccessfullyReturned(
                incident_result(&incident),
            )),
            None => Ok(Resp::Status404_TheIncidentWithTheGivenKeyWasNotFound(
                problem(
                    "Incident not found",
                    404,
                    format!("No incident with key {key}."),
                ),
            )),
        }
    }

    async fn search_incidents_impl(
        &self,
        body: &Option<models::IncidentSearchQuery>,
    ) -> Result<apis::incident::SearchIncidentsResponse, ()> {
        use apis::incident::SearchIncidentsResponse as Resp;

        let filter = body.as_ref().and_then(|q| q.filter.as_ref());
        let incidents = self.store.incidents();

        // Apply the filter algebra over each incident's string projections. The
        // process-definition identity is denormalized onto the row at projection
        // time, so no cross-entity lookup is needed here.
        let mut matched: Vec<&readstore::IncidentRow> = incidents
            .iter()
            .filter(|inc| match filter {
                None => true,
                Some(f) => {
                    query::match_basic_string(
                        &f.incident_key,
                        &inc.key.to_string(),
                    ) && query::match_process_instance_key(
                        &f.process_instance_key,
                        &inc.instance_key.to_string(),
                    ) && query::match_element_instance_key(
                        &f.element_instance_key,
                        &inc.element_instance_key.to_string(),
                    ) && query::match_process_definition_key(
                        &f.process_definition_key,
                        &inc.process_definition_key,
                    ) && match &f.job_key {
                        None => true,
                        some => query::match_job_key(
                            some,
                            &inc.job_key.map(|k| k.to_string()).unwrap_or_default(),
                        ),
                    } && query::match_incident_state(
                        &f.state,
                        &incident_state_enum(inc.state).to_string(),
                    ) && query::match_incident_error_type(
                        &f.error_type,
                        &incident_error_type_enum(inc.kind).to_string(),
                    ) && query::match_string(&f.element_id, &inc.element_id)
                        && query::match_string(&f.error_message, &inc.reason)
                }
            })
            .collect();

        // Sort: known fields, defaulting to incidentKey; entity key tiebreaks.
        let sort = query::sort_keys(
            body.as_ref().and_then(|q| q.sort.as_ref()),
            |r: &models::IncidentSearchQuerySortRequest| (r.field.clone(), r.order),
        );
        query::sort_items(
            &mut matched,
            &sort,
            |inc, field| match field {
                "creationTime" => query::SortVal::Num(inc.created_at_ms as i64),
                "state" => query::SortVal::Str(incident_state_enum(inc.state).to_string()),
                "errorType" => {
                    query::SortVal::Str(incident_error_type_enum(inc.kind).to_string())
                }
                "processInstanceKey" => query::SortVal::Num(inc.instance_key as i64),
                "elementId" => query::SortVal::Str(inc.element_id.clone()),
                _ => query::SortVal::Num(inc.key as i64),
            },
            |inc| inc.key,
        );

        let sorted: Vec<(u64, &readstore::IncidentRow)> =
            matched.into_iter().map(|inc| (inc.key, inc)).collect();
        let page = query::paginate(sorted, body.as_ref().and_then(|q| q.page.as_ref()));
        let items: Vec<models::IncidentResult> =
            page.items.into_iter().map(incident_result).collect();

        Ok(Resp::Status200_TheIncidentSearchResult(
            models::IncidentSearchQueryResult::new(page.response, items),
        ))
    }

    async fn search_process_instances_impl(
        &self,
        body: &Option<models::ProcessInstanceSearchQuery>,
    ) -> Result<apis::process_instance::SearchProcessInstancesResponse, ()> {
        use apis::process_instance::SearchProcessInstancesResponse as Resp;

        let filter = body.as_ref().and_then(|q| q.filter.as_ref());
        let instances = self.store.process_instances();

        let mut matched: Vec<&readstore::ProcessInstanceRow> = instances
            .iter()
            .filter(|inst| match filter {
                None => true,
                Some(f) => {
                    let state_str = process_instance_state_enum(inst.state).to_string();
                    query::match_process_instance_key(
                        &f.process_instance_key,
                        &inst.key.to_string(),
                    ) && query::match_process_definition_key(
                        &f.process_definition_key,
                        &inst.process_definition_key,
                    ) && query::match_string(&f.process_definition_id, &inst.process_definition_id)
                        && query::match_process_instance_state(&f.state, &state_str)
                        && f.has_incident.is_none_or(|want| want == inst.has_incident)
                }
            })
            .collect();

        let sort = query::sort_keys(
            body.as_ref().and_then(|q| q.sort.as_ref()),
            |r: &models::ProcessInstanceSearchQuerySortRequest| (r.field.clone(), r.order),
        );
        query::sort_items(
            &mut matched,
            &sort,
            |inst, field| match field {
                "processDefinitionId" => {
                    query::SortVal::Str(inst.process_definition_id.clone())
                }
                "processDefinitionKey" => {
                    query::SortVal::Num(inst.process_definition_key.parse().unwrap_or(0))
                }
                "state" => {
                    query::SortVal::Str(process_instance_state_enum(inst.state).to_string())
                }
                _ => query::SortVal::Num(inst.key as i64),
            },
            |inst| inst.key,
        );

        let sorted: Vec<(u64, &readstore::ProcessInstanceRow)> =
            matched.into_iter().map(|inst| (inst.key, inst)).collect();
        let page = query::paginate(sorted, body.as_ref().and_then(|q| q.page.as_ref()));
        // Build result models only for the returned page, never the whole
        // (potentially very large) matched set.
        let items: Vec<models::ProcessInstanceResult> =
            page.items.into_iter().map(process_instance_result).collect();

        Ok(Resp::Status200_TheProcessInstanceSearchResult(
            models::ProcessInstanceSearchQueryResult::new(page.response, items),
        ))
    }

    async fn search_jobs_impl(
        &self,
        body: &Option<models::JobSearchQuery>,
    ) -> Result<apis::job::SearchJobsResponse, ()> {
        use apis::job::SearchJobsResponse as Resp;

        let filter = body.as_ref().and_then(|q| q.filter.as_ref());
        let jobs = self.store.jobs();

        let mut matched: Vec<&readstore::JobRow> = jobs
            .iter()
            .filter(|job| match filter {
                None => true,
                Some(f) => {
                    query::match_job_key(&f.job_key, &job.key.to_string())
                        && query::match_process_instance_key(
                            &f.process_instance_key,
                            &job.instance_key.to_string(),
                        )
                        && query::match_process_definition_key(
                            &f.process_definition_key,
                            &job.process_definition_key,
                        )
                        && query::match_element_instance_key(
                            &f.element_instance_key,
                            &job.element_instance_key.to_string(),
                        )
                        && query::match_string(&f.r_type, &job.job_type)
                        && query::match_string(&f.element_id, &job.element_id)
                        && query::match_job_state(
                            &f.state,
                            &job_state_enum(job.state).to_string(),
                        )
                }
            })
            .collect();

        let sort = query::sort_keys(
            body.as_ref().and_then(|q| q.sort.as_ref()),
            |r: &models::JobSearchQuerySortRequest| (r.field.clone(), r.order),
        );
        query::sort_items(
            &mut matched,
            &sort,
            |job, field| match field {
                "processInstanceKey" => query::SortVal::Num(job.instance_key as i64),
                "elementId" => query::SortVal::Str(job.element_id.clone()),
                "type" => query::SortVal::Str(job.job_type.clone()),
                "state" => query::SortVal::Str(job_state_enum(job.state).to_string()),
                "retries" => query::SortVal::Num(job.retries as i64),
                _ => query::SortVal::Num(job.key as i64),
            },
            |job| job.key,
        );

        let sorted: Vec<(u64, &readstore::JobRow)> =
            matched.into_iter().map(|job| (job.key, job)).collect();
        let page = query::paginate(sorted, body.as_ref().and_then(|q| q.page.as_ref()));
        let items: Vec<models::JobSearchResult> =
            page.items.into_iter().map(job_search_result).collect();

        Ok(Resp::Status200_TheJobSearchResult(
            models::JobSearchQueryResult::new(page.response, items),
        ))
    }

    /// Searches deployed process definitions. The engine keeps only the latest
    /// version of each process id (in `state.processes`), so every entry is the
    /// latest version; older versions are not retained and therefore not
    /// searchable.
    async fn search_process_definitions_impl(
        &self,
        body: &Option<models::ProcessDefinitionSearchQuery>,
    ) -> Result<apis::process_definition::SearchProcessDefinitionsResponse, ()> {
        use apis::process_definition::SearchProcessDefinitionsResponse as Resp;

        let filter = body.as_ref().and_then(|q| q.filter.as_ref());
        let definitions = self.store.process_definitions();

        let mut matched: Vec<&readstore::ProcessDefinitionRow> = definitions
            .iter()
            .filter(|d| match filter {
                None => true,
                Some(f) => {
                    let id = &d.process_id;
                    // The engine stores no display name, resource name, or
                    // version tag, so those filters match against the best
                    // available proxy (the id) or exclude when we hold no value.
                    query::match_string(&f.name, id)
                        && query::match_string(&f.process_definition_id, id)
                        && f.process_definition_key
                            .as_ref()
                            .is_none_or(|k| k.0 == d.key.to_string())
                        && f.version.is_none_or(|v| v == d.version)
                        && f.resource_name
                            .as_ref()
                            .is_none_or(|r| *r == resource_name(id))
                        && f.version_tag.is_none()
                        && f.has_start_form.is_none_or(|want| !want)
                        // Only latest versions are retained, so they are all
                        // "latest"; an explicit `false` therefore matches none.
                        && f.is_latest_version.is_none_or(|want| want)
                }
            })
            .collect();

        let sort = query::sort_keys(
            body.as_ref().and_then(|q| q.sort.as_ref()),
            |r: &models::ProcessDefinitionSearchQuerySortRequest| (r.field.clone(), r.order),
        );
        query::sort_items(
            &mut matched,
            &sort,
            |d, field| match field {
                "processDefinitionId" | "name" => {
                    query::SortVal::Str(d.process_id.clone())
                }
                "version" => query::SortVal::Num(d.version as i64),
                _ => query::SortVal::Num(d.key as i64),
            },
            |d| d.key,
        );

        let sorted: Vec<(u64, &readstore::ProcessDefinitionRow)> =
            matched.into_iter().map(|d| (d.key, d)).collect();
        let page = query::paginate(sorted, body.as_ref().and_then(|q| q.page.as_ref()));
        let items: Vec<models::ProcessDefinitionResult> = page
            .items
            .into_iter()
            .map(process_definition_result)
            .collect();

        Ok(Resp::Status200_TheProcessDefinitionSearchResult(
            models::ProcessDefinitionSearchQueryResult::new(page.response, items),
        ))
    }

    async fn create_deployment_impl(
        &self,
        mut body: Multipart,
    ) -> Result<apis::resource::CreateDeploymentResponse, ()> {
        use apis::resource::CreateDeploymentResponse as Resp;

        // Drain the multipart body first: reading fields is async and we must not
        // hold the engine lock across an `.await`.
        let mut resources: Vec<(String, String)> = Vec::new();
        let mut tenant_id = "<default>".to_string();
        loop {
            match body.next_field().await {
                Ok(Some(field)) => {
                    let name = field.name().unwrap_or_default().to_string();
                    let file_name = field.file_name().map(str::to_string);
                    match field.bytes().await {
                        Ok(bytes) => {
                            if name == "tenantId" {
                                if let Ok(text) = std::str::from_utf8(&bytes) {
                                    let text = text.trim();
                                    if !text.is_empty() {
                                        tenant_id = text.to_string();
                                    }
                                }
                            } else {
                                match String::from_utf8(bytes.to_vec()) {
                                    Ok(xml) => {
                                        let resource_name = file_name.unwrap_or_else(|| {
                                            format!("resource-{}.bpmn", resources.len())
                                        });
                                        resources.push((resource_name, xml));
                                    }
                                    Err(_) => {
                                        return Ok(Resp::Status400_TheProvidedDataIsNotValid(
                                            problem(
                                                "Invalid resource",
                                                400,
                                                "A deployment resource was not valid UTF-8 BPMN XML.".to_string(),
                                            ),
                                        ));
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                                "Invalid request",
                                400,
                                format!("Could not read a deployment resource: {e}."),
                            )));
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                        "Invalid request",
                        400,
                        format!("Malformed multipart request: {e}."),
                    )));
                }
            }
        }

        if resources.is_empty() {
            return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                "No resources",
                400,
                "At least one deployment resource is required.".to_string(),
            )));
        }

        // Parse every resource up front so the deployment is all-or-nothing.
        let mut processes = Vec::new();
        let mut resource_names: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for (resource_name, xml) in &resources {
            match parse_bpmn(xml) {
                Ok(defs) => {
                    for def in defs {
                        resource_names.insert(def.id.clone(), resource_name.clone());
                        processes.push(def);
                    }
                }
                Err(e) => {
                    return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                        "Invalid BPMN",
                        400,
                        format!("Failed to parse '{resource_name}': {e}."),
                    )));
                }
            }
        }

        let (events, commit) = {
            let mut engine = self.journal.write().expect("engine lock poisoned");
            match engine.apply_command(Command::DeployResources(processes)) {
                Ok(pair) => pair,
                Err(e) => {
                    return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                        "Invalid deployment",
                        400,
                        e.to_string(),
                    )));
                }
            }
        };

        let mut deployment_key = String::new();
        let mut deployments = Vec::new();
        for event in &events {
            if let Event::ProcessDeployed {
                deployment_key: dk,
                process_definition_key,
                version,
                process,
            } = event
            {
                deployment_key = dk.to_string();
                let resource_name = resource_names.get(&process.id).cloned().unwrap_or_default();
                let process_result = models::DeploymentProcessResult::new(
                    process.id.clone(),
                    *version,
                    resource_name,
                    tenant_id.clone(),
                    models::ProcessDefinitionKey(process_definition_key.to_string()),
                );
                deployments.push(models::DeploymentMetadataResult::new(
                    nanobpm_gateway_rest::types::Nullable::Present(process_result),
                    nanobpm_gateway_rest::types::Nullable::Null,
                    nanobpm_gateway_rest::types::Nullable::Null,
                    nanobpm_gateway_rest::types::Nullable::Null,
                    nanobpm_gateway_rest::types::Nullable::Null,
                ));
            }
        }

        let result = models::DeploymentResult::new(
            models::DeploymentKey(deployment_key),
            tenant_id,
            deployments,
        );
        // Deployments don't create jobs, but a freshly available process means a
        // later createProcessInstance can; nothing to notify here.
        commit.wait().await;
        Ok(Resp::Status200_TheResourcesAreDeployed(result))
    }

    async fn activate_jobs_impl(
        &self,
        body: &models::JobActivationRequest,
    ) -> Result<apis::job::ActivateJobsResponse, ()> {
        use apis::job::ActivateJobsResponse as Resp;

        let job_type = body.r_type.clone();
        let worker = body.worker.clone().unwrap_or_else(|| "default".to_string());
        let max_jobs = body.max_jobs_to_activate.max(0) as usize;
        let timeout = body.timeout.max(0) as u64;

        // Long-poll window: None/0 -> default; >0 -> that window; <0 -> no waiting.
        let request_timeout = body.request_timeout.unwrap_or(0);
        let long_poll_until = if request_timeout < 0 {
            None
        } else if request_timeout == 0 {
            Some(Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS))
        } else {
            Some(Duration::from_millis(request_timeout as u64))
        };
        let deadline = long_poll_until.map(|d| tokio::time::Instant::now() + d);

        loop {
            let jobs = self.try_activate(&job_type, &worker, max_jobs, timeout);
            if !jobs.is_empty() {
                return Ok(Resp::Status200_TheListOfActivatedJobs(
                    models::JobActivationResult::new(jobs),
                ));
            }

            // No jobs right now. Either return immediately (long polling off) or
            // wait until either new jobs are signalled or the window elapses.
            match deadline {
                None => {
                    return Ok(Resp::Status200_TheListOfActivatedJobs(
                        models::JobActivationResult::new(Vec::new()),
                    ));
                }
                Some(deadline) => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Ok(Resp::Status200_TheListOfActivatedJobs(
                            models::JobActivationResult::new(Vec::new()),
                        ));
                    }
                    // Wait for a wake-up or the remaining window, then retry.
                    let notified = self.jobs_available.notified();
                    let _ = tokio::time::timeout(deadline - now, notified).await;
                }
            }
        }
    }

    /// Takes the engine write lock, activates up to `max_jobs` jobs of `job_type`,
    /// and maps them into the generated REST result type. Synchronous: never
    /// `.await`s while holding the engine lock. Activation mutates volatile lease
    /// state, so it takes the write lock even though nothing is journaled.
    fn try_activate(
        &self,
        job_type: &str,
        worker: &str,
        max_jobs: usize,
        timeout: u64,
    ) -> Vec<models::ActivatedJobResult> {
        let mut engine = self.journal.write().expect("engine lock poisoned");
        let now = now_millis();
        let activated = engine.activate_jobs(job_type, worker, max_jobs, timeout, now);
        activated
            .into_iter()
            .map(|job| activated_job_result(engine.engine(), job))
            .collect()
    }
}

/// Current wall-clock time in milliseconds since the Unix epoch. The engine is
/// clock-free; the server owns the real clock and feeds it logical instants.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A placeholder timestamp. The clock-free engine does not record wall-clock
/// times for process instances, so their read projections report the Unix
/// epoch. (Incidents do carry a real `created_at` fed in at command time.)
fn epoch() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).expect("epoch is valid")
}

/// Maps an engine [`IncidentKind`] to the REST `errorType` taxonomy.
fn incident_error_type_enum(kind: IncidentKind) -> models::IncidentErrorTypeEnum {
    match kind {
        IncidentKind::JobNoRetries => models::IncidentErrorTypeEnum::JobNoRetries,
        IncidentKind::NoMatchingSequenceFlow => models::IncidentErrorTypeEnum::ConditionError,
        IncidentKind::UnhandledError => models::IncidentErrorTypeEnum::UnhandledErrorEvent,
    }
}

/// Maps an engine [`IncidentState`] to the REST incident `state` enum.
fn incident_state_enum(state: IncidentState) -> models::IncidentStateEnum {
    match state {
        IncidentState::Active => models::IncidentStateEnum::Active,
        IncidentState::Resolved => models::IncidentStateEnum::Resolved,
    }
}

/// Projects an [`IncidentRow`] into the generated `IncidentResult`. The
/// process-definition identity is denormalized onto the row at projection time.
fn incident_result(incident: &readstore::IncidentRow) -> models::IncidentResult {
    let process_definition_id = incident.process_definition_id.clone();
    let process_definition_key = incident.process_definition_key.clone();

    let error_type = incident_error_type_enum(incident.kind);
    let job_key = match incident.job_key {
        Some(k) => types::Nullable::Present(models::JobKey(k.to_string())),
        None => types::Nullable::Null,
    };
    let creation_time = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
        incident.created_at_ms as i64,
    )
    .unwrap_or_else(epoch);

    let incident_state = incident_state_enum(incident.state);

    models::IncidentResult::new(
        process_definition_id,
        error_type,
        incident.reason.clone(),
        incident.element_id.clone(),
        creation_time,
        incident_state,
        "<default>".to_string(),
        models::IncidentKey(incident.key.to_string()),
        models::ProcessDefinitionKey(process_definition_key),
        models::ProcessInstanceKey(incident.instance_key.to_string()),
        types::Nullable::Null,
        models::ElementInstanceKey(incident.element_instance_key.to_string()),
        job_key,
    )
}

/// Projects a [`ProcessInstanceRow`] into the generated `ProcessInstanceResult`.
fn process_instance_result(
    instance: &readstore::ProcessInstanceRow,
) -> models::ProcessInstanceResult {
    let process_definition_id = instance.process_definition_id.clone();
    let version = instance.version;
    let process_definition_key = instance.process_definition_key.clone();

    let state_enum = process_instance_state_enum(instance.state);

    let start_date = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
        instance.start_date_ms as i64,
    )
    .unwrap_or_else(epoch);

    models::ProcessInstanceResult::new(
        process_definition_id,
        types::Nullable::Null,
        version,
        types::Nullable::Null,
        start_date,
        types::Nullable::Null,
        state_enum,
        instance.has_incident,
        "<default>".to_string(),
        models::ProcessInstanceKey(instance.key.to_string()),
        models::ProcessDefinitionKey(process_definition_key),
        types::Nullable::Null,
        types::Nullable::Null,
        types::Nullable::Null,
        Vec::new(),
        types::Nullable::Null,
    )
}

/// A synthesized resource (file) name for a process id. The engine does not
/// retain the original deployment resource name, so derive a stable `.bpmn`
/// name from the id.
fn resource_name(process_id: &str) -> String {
    format!("{process_id}.bpmn")
}

/// Projects a [`ProcessDefinitionRow`] into the generated
/// `ProcessDefinitionResult`. The engine stores no display name or version tag,
/// so `name` mirrors the id and `versionTag` is null.
fn process_definition_result(
    deployed: &readstore::ProcessDefinitionRow,
) -> models::ProcessDefinitionResult {
    let id = deployed.process_id.clone();
    models::ProcessDefinitionResult::new(
        types::Nullable::Present(id.clone()),
        resource_name(&id),
        deployed.version,
        types::Nullable::Null,
        id,
        "<default>".to_string(),
        models::ProcessDefinitionKey(deployed.key.to_string()),
        false,
    )
}

/// Maps an engine [`ProcessInstanceState`] to the REST state enum.
fn process_instance_state_enum(
    state: ProcessInstanceState,
) -> models::ProcessInstanceStateEnum {
    match state {
        ProcessInstanceState::Active => models::ProcessInstanceStateEnum::Active,
        ProcessInstanceState::Completed => models::ProcessInstanceStateEnum::Completed,
    }
}

/// Maps an engine [`nanobpmn_engine_core::JobState`] to the REST job state enum.
/// The engine's transient `Activated` (locked to a worker) has no distinct wire
/// state, so it projects to `CREATED` like any other activatable job.
fn job_state_enum(state: nanobpmn_engine_core::JobState) -> models::JobStateEnum {
    use nanobpmn_engine_core::JobState;
    match state {
        JobState::Created | JobState::Activated => models::JobStateEnum::Created,
        JobState::Failed => models::JobStateEnum::Failed,
        JobState::Errored => models::JobStateEnum::ErrorThrown,
        JobState::Completed => models::JobStateEnum::Completed,
        JobState::Canceled => models::JobStateEnum::Canceled,
    }
}

/// Projects a [`JobRow`] into the generated `JobSearchResult`. The
/// process-definition identity is denormalized onto the row at projection time.
fn job_search_result(job: &readstore::JobRow) -> models::JobSearchResult {
    let process_definition_id = job.process_definition_id.clone();
    let process_definition_key = job.process_definition_key.clone();

    let deadline = match job.deadline_ms {
        Some(ms) => chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms as i64)
            .map(types::Nullable::Present)
            .unwrap_or(types::Nullable::Null),
        None => types::Nullable::Null,
    };

    models::JobSearchResult::new(
        std::collections::HashMap::new(),
        deadline,
        types::Nullable::Null,
        types::Nullable::Present(job.element_id.clone()),
        models::ElementInstanceKey(job.element_instance_key.to_string()),
        types::Nullable::Null,
        types::Nullable::Null,
        types::Nullable::Null,
        false,
        types::Nullable::Null,
        models::JobKey(job.key.to_string()),
        models::JobKindEnum::BpmnElement,
        models::JobListenerEventTypeEnum::Unspecified,
        process_definition_id,
        models::ProcessDefinitionKey(process_definition_key),
        models::ProcessInstanceKey(job.instance_key.to_string()),
        types::Nullable::Null,
        job.retries,
        job_state_enum(job.state),
        "<default>".to_string(),
        job.job_type.clone(),
        job.worker.clone().unwrap_or_default(),
        types::Nullable::Null,
        types::Nullable::Null,
        0,
    )
}

/// Maps an engine [`ActivatedJob`] into the generated `ActivatedJobResult`,
/// resolving process-definition identity from engine state.
fn activated_job_result(engine: &Engine, job: ActivatedJob) -> models::ActivatedJobResult {
    let (process_id, version, process_definition_key) = engine
        .instance(job.instance_key)
        .and_then(|instance| engine.state().processes.get(&instance.process_id))
        .map(|deployed| {
            (
                deployed.definition.id.clone(),
                deployed.version,
                deployed.key.to_string(),
            )
        })
        .unwrap_or_else(|| (String::new(), 1, String::new()));

    models::ActivatedJobResult::new(
        job.job_type,
        process_id,
        version,
        job.element_id,
        std::collections::HashMap::new(),
        job.worker,
        job.retries,
        job.deadline as i64,
        to_object_map(job.variables),
        "<default>".to_string(),
        models::JobKey(job.key.to_string()),
        models::ProcessInstanceKey(job.instance_key.to_string()),
        models::ProcessDefinitionKey(process_definition_key),
        models::ElementInstanceKey(job.element_instance_key.to_string()),
        models::JobKindEnum::BpmnElement,
        models::JobListenerEventTypeEnum::Unspecified,
        nanobpm_gateway_rest::types::Nullable::Null,
        Vec::new(),
        nanobpm_gateway_rest::types::Nullable::Null,
        0,
    )
}

/// Converts engine variables into the generated `Object` (JSON) map used by the
/// REST models.
fn to_object_map(
    variables: std::collections::HashMap<String, Value>,
) -> std::collections::HashMap<String, types::Object> {
    variables
        .into_iter()
        .map(|(name, value)| {
            let json = match value {
                Value::Bool(b) => serde_json::Value::Bool(b),
                Value::Int(i) => serde_json::Value::Number(i.into()),
                Value::Str(s) => serde_json::Value::String(s),
            };
            (name, types::Object(json))
        })
        .collect()
}

/// Converts a REST `Object` (JSON) variable map into engine variables. The POC
/// engine only models `Bool`/`Int`/`Str`; JSON booleans, integers, and strings
/// map directly, and any richer JSON value is kept as its compact string form so
/// routing on it stays deterministic.
fn from_object_map(
    variables: &std::collections::HashMap<String, types::Object>,
) -> std::collections::HashMap<String, Value> {
    variables
        .iter()
        .map(|(name, object)| {
            let value = match &object.0 {
                serde_json::Value::Bool(b) => Value::Bool(*b),
                serde_json::Value::Number(n) if n.is_i64() => Value::Int(n.as_i64().unwrap()),
                serde_json::Value::String(s) => Value::Str(s.clone()),
                other => Value::Str(other.to_string()),
            };
            (name.clone(), value)
        })
        .collect()
}

/// The minted message key from a `CorrelateMessage`'s events: the heading
/// [`Event::MessagePublished`] always carries it.
fn message_key_of(events: &[Event]) -> u64 {
    events
        .iter()
        .find_map(|e| match e {
            Event::MessagePublished { message_key, .. } => Some(*message_key),
            _ => None,
        })
        .expect("CorrelateMessage always emits MessagePublished")
}

/// Maps the stub `Err(())` returned by every operation to `501 Not Implemented`.
#[async_trait::async_trait]
impl apis::ErrorHandler<()> for ServerImpl {
    async fn handle_error(
        &self,
        _method: &http::Method,
        _host: &headers::Host,
        _cookies: &axum_extra::extract::CookieJar,
        _error: (),
    ) -> Result<Response, StatusCode> {
        Response::builder()
            .status(StatusCode::NOT_IMPLEMENTED)
            .header(http::header::CONTENT_TYPE, "text/plain")
            .body(Body::from(
                "Not implemented: backend services are not wired yet.\n",
            ))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}

/// Accepts any credentials. Authentication is not enforced in the stub server;
/// real claim extraction will be added when authentication is wired.
#[async_trait::async_trait]
impl apis::ApiAuthBasic for ServerImpl {
    type Claims = ();

    async fn extract_claims_from_auth_header(
        &self,
        _kind: apis::BasicAuthKind,
        _headers: &http::header::HeaderMap,
        _key: &str,
    ) -> Option<Self::Claims> {
        Some(())
    }
}

/// Maximum number of body bytes rendered in a `DEBUG_REST` log line. Larger
/// bodies are truncated in the log; the full body still reaches the handler.
const REST_LOG_BODY_PREVIEW: usize = 4096;

/// Whether `DEBUG_REST` requests verbose REST request/response logging. Accepts
/// the usual truthy spellings (`1`, `true`, `yes`, `on`); unset or anything
/// else leaves it off.
fn debug_rest_enabled() -> bool {
    std::env::var("DEBUG_REST")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Renders a body as a single-line, length-prefixed preview for logging,
/// truncating long payloads and collapsing newlines so each request stays on
/// one log line. An empty body renders as the empty string.
fn body_preview(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    let shown = bytes.len().min(REST_LOG_BODY_PREVIEW);
    let text = String::from_utf8_lossy(&bytes[..shown])
        .replace('\n', " ")
        .replace('\r', "");
    let ellipsis = if bytes.len() > shown { "…" } else { "" };
    format!(" [{} bytes] {text}{ellipsis}", bytes.len())
}

/// axum middleware, mounted only when `DEBUG_REST` is enabled, that logs each
/// REST request and its response (method, URI, status, latency, and a preview
/// of both bodies). Bodies are buffered so they can be logged and then handed
/// on unchanged — intentionally opt-in, since buffering defeats streaming.
async fn log_rest(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();

    let (parts, body) = req.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_default();
    tracing::info!(target: "rest", "--> {method} {uri}{}", body_preview(&bytes));
    let req = axum::extract::Request::from_parts(parts, Body::from(bytes));

    let started = std::time::Instant::now();
    let resp = next.run(req).await;
    let elapsed = started.elapsed();

    let status = resp.status();
    let (parts, body) = resp.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .unwrap_or_default();
    tracing::info!(
        target: "rest",
        "<-- {method} {uri} {status} ({elapsed:.1?}){}",
        body_preview(&bytes)
    );
    Response::from_parts(parts, Body::from(bytes))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    // Resolve where the journal (durable event log) and read-model database live.
    let (journal_path, db_path) = resolve_data_paths();

    let server = match journal_path {
        Some(journal_path) => {
            // Persistent run: the read store is a derived projection of the
            // journal, so reconcile it against the log before serving. Open the
            // store, replay any journal events the store has not yet projected
            // (a full rebuild on a fresh/reset store), then open the journal
            // (which replays into the engine) and wire the live exporter.
            let store = Arc::new(
                ReadStore::open(db_path.as_deref())
                    .unwrap_or_else(|e| panic!("failed to open read store: {e}")),
            );
            let events = Journal::read_events(&journal_path).unwrap_or_else(|e| {
                panic!("failed to read journal {}: {e}", journal_path.display())
            });
            let mut pos = store.exported_position();
            if pos > events.len() {
                // The store is ahead of the log (truncated/corrupt journal):
                // rebuild from scratch.
                store.reset().expect("reset read store");
                pos = 0;
            }
            if pos < events.len() {
                store
                    .export(&events[pos..])
                    .expect("catch up read model from journal");
            }

            let journal = Journal::open(&journal_path).unwrap_or_else(|e| {
                panic!("failed to open journal {}: {e}", journal_path.display())
            });
            let recovered = !journal.is_fresh();
            let server = build_server(journal, store);

            // The read model now has every completed instance, so shed them from
            // hot engine state to bound memory.
            {
                let mut journal = server.journal.write().expect("engine lock poisoned");
                let evicted = journal.evict_completed();
                if evicted > 0 {
                    tracing::info!("evicted {evicted} completed instance(s) from hot state");
                }
            }

            if recovered {
                tracing::info!(
                    "recovered engine state by replaying journal at {}",
                    journal_path.display()
                );
            } else {
                tracing::info!("started a fresh journal at {}", journal_path.display());
            }
            server
        }
        None => {
            tracing::info!(
                "no journal configured; running in-memory (state is not persisted)"
            );
            let store = Arc::new(
                ReadStore::open(db_path.as_deref()).expect("open in-memory read store"),
            );
            build_server(Journal::in_memory(), store)
        }
    };

    // Capture handles for the background tick before `server` is moved into the
    // router.
    let tick_journal = server.journal.clone();
    let tick_jobs_available = server.jobs_available.clone();

    let mut app = nanobpm_gateway_rest::server::new::<ServerImpl, ServerImpl, (), ()>(server);

    if debug_rest_enabled() {
        app = app.layer(axum::middleware::from_fn(log_rest));
        tracing::info!("DEBUG_REST enabled: logging every REST request and response");
    }

    // Background "tick": drives the host clock into the engine so timers fire and
    // activation locks expire without an inbound request. Timer firing is durable
    // (journaled); lock expiry is volatile (not journaled). Wakes any long-polling
    // activateJobs when a tick produced events (a fired timer may create jobs).
    {
        let journal = tick_journal;
        let jobs_available = tick_jobs_available;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;
                let now = now_millis();
                let produced = {
                    let mut journal = journal.write().expect("engine lock poisoned");
                    let (fired, _commit) = journal.trigger_timers(now);
                    journal.expire_jobs(now);
                    !fired.is_empty()
                };
                if produced {
                    jobs_available.notify_waiters();
                }
            }
        });
    }

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    // The actual bound port (may differ from `port` when `PORT=0`, i.e. the OS
    // assigns a free one). Print it on stdout so a supervising process (the e2e
    // harness) can learn it without racing on a pre-reserved port.
    let local_port = listener
        .local_addr()
        .map(|a| a.port())
        .unwrap_or(port);
    println!("LISTENING_PORT={local_port}");
    let _ = std::io::Write::flush(&mut std::io::stdout());

    tracing::info!(
        "NanoBPM gateway REST stub server listening on http://{addr}{}",
        nanobpm_gateway_rest::BASE_PATH
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

/// Resolves the journal and read-model database paths from the environment.
///
/// - `NANOBPMN_DATA_DIR=<dir>` co-locates both under one directory:
///   `<dir>/journal.jsonl` and `<dir>/read-model.sqlite` (the directory is
///   created if absent).
/// - Otherwise `NANOBPMN_JOURNAL=<file>` (back-compat) selects the journal; the
///   database is `NANOBPMN_READ_DB` if set, else a sibling `read-model.sqlite`
///   next to the journal.
/// - With neither set, both are `None`: an in-memory journal and an in-memory
///   (`:memory:`) read store (nothing is persisted).
fn resolve_data_paths() -> (Option<PathBuf>, Option<PathBuf>) {
    if let Ok(dir) = std::env::var("NANOBPMN_DATA_DIR")
        && !dir.is_empty()
    {
        let dir = PathBuf::from(dir);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            panic!("failed to create data dir {}: {e}", dir.display());
        }
        return (
            Some(dir.join("journal.jsonl")),
            Some(dir.join("read-model.sqlite")),
        );
    }

    match std::env::var("NANOBPMN_JOURNAL") {
        Ok(path) if !path.is_empty() => {
            let journal = PathBuf::from(path);
            let db = match std::env::var("NANOBPMN_READ_DB") {
                Ok(db) if !db.is_empty() => PathBuf::from(db),
                _ => journal.with_file_name("read-model.sqlite"),
            };
            (Some(journal), Some(db))
        }
        _ => (None, None),
    }
}
