//! Stub server for the generated Orchestration Cluster REST layer.
//!
//! This binary wires the generated `camunda_gateway_rest` REST layer (models,
//! routes, and per-tag service traits) into a runnable `axum` server. No backend
//! services are connected yet: every operation is implemented as a stub that
//! responds with `501 Not Implemented`.
//!
//! The per-tag trait implementations live in the generated `stub_impls` module
//! (see scripts/gen-stub-server.py). This file owns only the stable pieces: the
//! `ServerImpl` type, authentication/error glue, and the server bootstrap.

mod stub_impls;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::Multipart;
use axum::response::Response;
use camunda_gateway_rest::{apis, models, types};
use http::StatusCode;
use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{
    ActivatedJob, Command, Engine, EngineError, Event, ProcessBuilder, Value,
};

/// Default long-poll window (ms) when a client passes `requestTimeout` 0.
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 5_000;

/// The single type that implements every generated API trait.
///
/// It owns an embedded [`Engine`] (the `engine-core` crate) behind a mutex.
/// Most operations are still 501 stubs (see the generated `stub_impls` module);
/// a few — process-instance creation, job activation, and job completion — are
/// wired to the engine via the inherent methods below and routed from the stub
/// generator's override table.
#[derive(Clone)]
pub struct ServerImpl {
    engine: Arc<Mutex<Engine>>,
    /// Notified whenever new jobs may have become activatable, so long-polling
    /// `activateJobs` requests can wake immediately instead of waiting out their
    /// full timeout.
    jobs_available: Arc<tokio::sync::Notify>,
}

impl Default for ServerImpl {
    fn default() -> Self {
        let mut engine = Engine::new();
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
        engine
            .apply_command(Command::DeployProcess(demo))
            .expect("deploy demo process");
        Self {
            engine: Arc::new(Mutex::new(engine)),
            jobs_available: Arc::new(tokio::sync::Notify::new()),
        }
    }
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

        // The POC engine starts processes by BPMN process id only.
        let process_id = match body {
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionById(
                b,
            ) => b.process_definition_id.clone(),
            models::ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(
                _,
            ) => {
                return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                    "Unsupported",
                    400,
                    "Starting by processDefinitionKey is not supported by the nanobpmn engine; use processDefinitionId.".to_string(),
                )));
            }
        };

        let mut engine = self.engine.lock().expect("engine mutex poisoned");
        match engine.apply_command(Command::create_instance(process_id.clone())) {
            Ok(events) => {
                let instance_key = events
                    .iter()
                    .find_map(Event::instance_key)
                    .expect("created instance has a key");
                let result = models::CreateProcessInstanceResult::new(
                    process_id.clone(),
                    1,
                    "<default>".to_string(),
                    std::collections::HashMap::new(),
                    models::ProcessDefinitionKey(process_id),
                    models::ProcessInstanceKey(instance_key.to_string()),
                    Vec::new(),
                    camunda_gateway_rest::types::Nullable::Null,
                );
                // Starting an instance parks it on its first service task, so new
                // jobs may now be activatable: wake any long-pollers.
                self.jobs_available.notify_waiters();
                Ok(Resp::Status200_TheProcessInstanceWasCreated(result))
            }
            Err(EngineError::ProcessNotFound { process_id }) => {
                Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                    "Process not found",
                    400,
                    format!("No deployed process with id '{process_id}'."),
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

        let mut engine = self.engine.lock().expect("engine mutex poisoned");
        match engine.apply_command(Command::complete_job_with(job_key, variables)) {
            Ok(_) => {
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

        let mut engine = self.engine.lock().expect("engine mutex poisoned");
        match engine.apply_command(Command::fail_job(job_key, retries, error_message)) {
            Ok(_) => {
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

        let mut engine = self.engine.lock().expect("engine mutex poisoned");
        match engine.apply_command(Command::throw_job_error(
            job_key,
            body.error_code.clone(),
            error_message,
        )) {
            Ok(_) => {
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

        let mut engine = self.engine.lock().expect("engine mutex poisoned");
        let events = match engine.apply_command(Command::DeployResources(processes)) {
            Ok(events) => events,
            Err(e) => {
                return Ok(Resp::Status400_TheProvidedDataIsNotValid(problem(
                    "Invalid deployment",
                    400,
                    e.to_string(),
                )));
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
                    camunda_gateway_rest::types::Nullable::Present(process_result),
                    camunda_gateway_rest::types::Nullable::Null,
                    camunda_gateway_rest::types::Nullable::Null,
                    camunda_gateway_rest::types::Nullable::Null,
                    camunda_gateway_rest::types::Nullable::Null,
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

    /// Locks the engine, activates up to `max_jobs` jobs of `job_type`, and maps
    /// them into the generated REST result type. Synchronous: never `.await`s
    /// while holding the engine mutex.
    fn try_activate(
        &self,
        job_type: &str,
        worker: &str,
        max_jobs: usize,
        timeout: u64,
    ) -> Vec<models::ActivatedJobResult> {
        let mut engine = self.engine.lock().expect("engine mutex poisoned");
        let now = now_millis();
        let activated = engine.activate_jobs(job_type, worker, max_jobs, timeout, now);
        activated
            .into_iter()
            .map(|job| activated_job_result(&engine, job))
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
        camunda_gateway_rest::types::Nullable::Null,
        Vec::new(),
        camunda_gateway_rest::types::Nullable::Null,
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let app =
        camunda_gateway_rest::server::new::<ServerImpl, ServerImpl, (), ()>(ServerImpl::default());

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    tracing::info!(
        "Camunda gateway REST stub server listening on http://{addr}{}",
        camunda_gateway_rest::BASE_PATH
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
