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

use axum::body::Body;
use axum::response::Response;
use camunda_gateway_rest::{apis, models};
use http::StatusCode;
use nanobpmn_engine_core::{Command, Engine, EngineError, Event, ProcessBuilder};

/// The single type that implements every generated API trait.
///
/// It owns an embedded [`Engine`] (the `engine-core` crate) behind a mutex.
/// Most operations are still 501 stubs (see the generated `stub_impls` module);
/// a few — process-instance creation and job completion — are wired to the
/// engine via the inherent methods below and routed from the stub generator's
/// override table.
#[derive(Clone)]
pub struct ServerImpl {
    engine: Arc<Mutex<Engine>>,
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
        _body: &Option<models::JobCompletionRequest>,
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

        let mut engine = self.engine.lock().expect("engine mutex poisoned");
        match engine.apply_command(Command::complete_job(job_key)) {
            Ok(_) => Ok(Resp::Status204_TheJobWasCompletedSuccessfully),
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
            Err(e) => Ok(
                Resp::Status500_AnInternalErrorOccurredWhileProcessingTheRequest(problem(
                    "Internal error",
                    500,
                    e.to_string(),
                )),
            ),
        }
    }
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
