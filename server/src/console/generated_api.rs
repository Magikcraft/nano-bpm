//! Implements the generated `nanobpm-console-api` per-tag `Api` traits for
//! [`ServerImpl`] by delegating to the hand-written console handler logic in
//! [`super`]. The generated router (`nanobpm_console_api::server::new`) is
//! mounted alongside the reduced hand-written router in `main.rs`.
//!
//! ## Strategy
//! The OpenAPI spec was authored *from* the hand-written DTOs, so the generated
//! models serialize to identical JSON. Each trait method therefore delegates to
//! a `super::` core function (which returns a DTO or a `serde_json::Value`) and
//! round-trips through `serde_json` into the generated response model.
//!
//! ## Deviations from the hand-written handlers
//! * **RunStatus `crashed` → `error`**: the project `Phase` enum serializes
//!   `crashed`, but the spec's `RunStatus` has no such variant (it uses
//!   `error`). [`fix_run_status`] remaps it on any `RunState`-bearing value
//!   (identified by the presence of a `compiling` key) before conversion.
//! * **Collapsed status codes**: the spec exposes fewer status codes than some
//!   handlers produce. Where a handler's error status has no matching response
//!   variant it is mapped to the nearest available one (e.g. `500`/`400` on
//!   `GetModel` → `404`; list endpoints that only declare `200` fall back to an
//!   empty/default `200` body).

use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;
use nanobpm_console_api::apis;
use nanobpm_console_api::models;
use nanobpm_console_api::types::Nullable;

use crate::ServerImpl;

// --- conversion helpers ---------------------------------------------------

/// Serialize a hand-written DTO and deserialize it into the generated model.
/// Safe because the spec's JSON property names/shapes match the DTOs.
fn from_dto<T, D>(dto: D) -> T
where
    D: serde::Serialize,
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(serde_json::to_value(dto).expect("dto serializes"))
        .expect("generated model matches DTO shape")
}

/// Deserialize a `serde_json::Value` (already the response body shape) into the
/// generated model.
fn from_val<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("generated model matches value shape")
}

/// Remap the project run `status` `crashed` → `error` on every `RunState`
/// object (identified by the `compiling` discriminator key) nested in `v`.
fn fix_run_status(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            if map.contains_key("compiling")
                && let Some(serde_json::Value::String(s)) = map.get_mut("status")
                && s == "crashed"
            {
                *s = "error".to_string();
            }
            for child in map.values_mut() {
                fix_run_status(child);
            }
        }
        serde_json::Value::Array(arr) => {
            for child in arr.iter_mut() {
                fix_run_status(child);
            }
        }
        _ => {}
    }
}

/// Flatten an optional nullable request field to a plain `Option`.
fn flatten_nullable<T: Clone>(v: &Option<Nullable<T>>) -> Option<T> {
    match v {
        Some(Nullable::Present(x)) => Some(x.clone()),
        _ => None,
    }
}

// --- error handler --------------------------------------------------------

#[async_trait]
impl apis::ErrorHandler<()> for ServerImpl {}

// --- cluster --------------------------------------------------------------

#[async_trait]
impl apis::cluster::Cluster for ServerImpl {
    async fn get_cluster_health(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::cluster::GetClusterHealthResponse, ()> {
        let dto = super::cluster_health(self).await;
        Ok(apis::cluster::GetClusterHealthResponse::Status200_ClusterHealth(from_dto(dto)))
    }

    async fn get_cluster_metrics(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::cluster::GetClusterMetricsResponse, ()> {
        let dto = super::cluster_metrics(self).await;
        Ok(apis::cluster::GetClusterMetricsResponse::Status200_ClusterMetrics(from_dto(dto)))
    }

    async fn get_metrics(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::cluster::GetMetricsResponse, ()> {
        let dto = super::build_local_metrics(self);
        Ok(apis::cluster::GetMetricsResponse::Status200_MetricsSnapshot(from_dto(dto)))
    }

    async fn get_topology(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::cluster::GetTopologyResponse, ()> {
        let dto = super::topology(self);
        Ok(apis::cluster::GetTopologyResponse::Status200_ClusterTopology(from_dto(dto)))
    }
}

// --- config ---------------------------------------------------------------

#[async_trait]
impl apis::config::Config for ServerImpl {
    async fn get_ide_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::config::GetIdeConfigResponse, ()> {
        // Only 200 is declared; the sole error path is a task-join failure, so
        // fall back to computing the config on the current thread.
        let v = super::config_ide()
            .await
            .unwrap_or_else(|_| super::config::ide_config_json());
        Ok(apis::config::GetIdeConfigResponse::Status200_IDEConfig(
            from_val(v),
        ))
    }

    async fn get_server_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::config::GetServerConfigResponse, ()> {
        let v = super::config_server(self);
        Ok(apis::config::GetServerConfigResponse::Status200_ServerConfig(from_val(v)))
    }

    async fn set_sla_mode(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::SetSlaRequest,
    ) -> Result<apis::config::SetSlaModeResponse, ()> {
        match super::config_server_sla(self, &body.mode).await {
            Ok(v) => {
                Ok(apis::config::SetSlaModeResponse::Status200_UpdatedServerConfig(from_val(v)))
            }
            Err((_, msg)) => Ok(apis::config::SetSlaModeResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }
}

// --- extensions -----------------------------------------------------------

#[async_trait]
impl apis::server::Server for ServerImpl {
    async fn get_server_update(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::server::GetServerUpdateResponse, ()> {
        // Only 200 is declared; the handler is offline-soft and never errors.
        let v = super::server_update()
            .await
            .unwrap_or_else(|_| serde_json::json!({}));
        Ok(apis::server::GetServerUpdateResponse::Status200_ServerUpdateStatus(from_val(v)))
    }
}

#[async_trait]
impl apis::extensions::Extensions for ServerImpl {
    async fn get_extensions(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::extensions::GetExtensionsResponse, ()> {
        let v = super::extensions_list();
        Ok(apis::extensions::GetExtensionsResponse::Status200_ExtensionsOverview(from_val(v)))
    }

    async fn get_marketplace(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::extensions::GetMarketplaceResponse, ()> {
        // Only 200 is declared; on a registry error fall back to empty entries.
        let v = super::extensions_marketplace()
            .await
            .unwrap_or_else(|_| serde_json::json!({ "entries": [] }));
        Ok(apis::extensions::GetMarketplaceResponse::Status200_MarketplaceListing(from_val(v)))
    }

    async fn get_extension_readme(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::GetExtensionReadmeQueryParams,
    ) -> Result<apis::extensions::GetExtensionReadmeResponse, ()> {
        match super::extensions_readme(query_params.pkg.clone()).await {
            Ok(v) => {
                Ok(apis::extensions::GetExtensionReadmeResponse::Status200_PackREADME(from_val(v)))
            }
            Err((_, msg)) => {
                Ok(apis::extensions::GetExtensionReadmeResponse::Status404_NotFound(msg))
            }
        }
    }

    async fn install_extension(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::ExtPkgRequest,
    ) -> Result<apis::extensions::InstallExtensionResponse, ()> {
        match super::extensions_install(body.pkg.clone()).await {
            Ok(v) => Ok(
                apis::extensions::InstallExtensionResponse::Status201_InstalledExtension(from_val(
                    v,
                )),
            ),
            Err((_, msg)) => {
                Ok(apis::extensions::InstallExtensionResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn remove_extension(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::ExtPkgRequest,
    ) -> Result<apis::extensions::RemoveExtensionResponse, ()> {
        match super::extensions_remove(&body.pkg) {
            Ok(_) => Ok(apis::extensions::RemoveExtensionResponse::Status204_Removed),
            Err((_, msg)) => {
                Ok(apis::extensions::RemoveExtensionResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn trust_extension(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::TrustRequest,
    ) -> Result<apis::extensions::TrustExtensionResponse, ()> {
        let yolo = flatten_nullable(&body.yolo);
        let approve = flatten_nullable(&body.approve);
        let revoke = flatten_nullable(&body.revoke);
        // Only 200 is declared; on a save error fall back to the current view.
        let v = super::extensions_trust(yolo, approve, revoke)
            .unwrap_or_else(|_| super::extensions_list());
        Ok(
            apis::extensions::TrustExtensionResponse::Status200_UpdatedExtensionsOverview(
                from_val(v),
            ),
        )
    }
}

// --- instances ------------------------------------------------------------

#[async_trait]
impl apis::instances::Instances for ServerImpl {
    async fn get_instance(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetInstancePathParams,
    ) -> Result<apis::instances::GetInstanceResponse, ()> {
        match super::instance_detail(self, &path_params.key) {
            Some(dto) => {
                Ok(apis::instances::GetInstanceResponse::Status200_InstanceDetail(from_dto(dto)))
            }
            None => Ok(apis::instances::GetInstanceResponse::Status404_NotFound(
                "no such instance".to_string(),
            )),
        }
    }

    async fn list_instances(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::ListInstancesQueryParams,
    ) -> Result<apis::instances::ListInstancesResponse, ()> {
        let page = query_params.page.map(|p| p as i64).unwrap_or(0);
        let page_size = query_params.page_size.map(|p| p as i64).unwrap_or(50);
        let dto = super::instances(self, page, page_size);
        Ok(
            apis::instances::ListInstancesResponse::Status200_OnePageOfProcessInstances(from_dto(
                dto,
            )),
        )
    }
}

// --- traces ---------------------------------------------------------------

#[async_trait]
impl apis::traces::Traces for ServerImpl {
    async fn get_trace(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetTracePathParams,
    ) -> Result<apis::traces::GetTraceResponse, ()> {
        match super::trace_detail(self, &path_params.key) {
            Some(dto) => Ok(apis::traces::GetTraceResponse::Status200_InstanceTrace(
                from_dto(dto),
            )),
            None => Ok(apis::traces::GetTraceResponse::Status404_NotFound(
                "no such trace".to_string(),
            )),
        }
    }

    async fn get_trace_otel(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetTraceOtelPathParams,
    ) -> Result<apis::traces::GetTraceOtelResponse, ()> {
        match super::trace_otel(self, &path_params.key) {
            Some(v) => Ok(apis::traces::GetTraceOtelResponse::Status200_OpaqueOTLP(
                from_val(v),
            )),
            None => Ok(apis::traces::GetTraceOtelResponse::Status404_NotFound(
                "no such trace".to_string(),
            )),
        }
    }

    async fn list_traces(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::ListTracesQueryParams,
    ) -> Result<apis::traces::ListTracesResponse, ()> {
        let limit = query_params.limit.map(|l| l as usize).unwrap_or(100);
        let dtos = super::traces(self, limit);
        Ok(apis::traces::ListTracesResponse::Status200_TraceSummaries(
            from_dto(dtos),
        ))
    }
}

// --- models ---------------------------------------------------------------

#[async_trait]
impl apis::models::Models for ServerImpl {
    async fn create_model(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::CreateModelRequest,
    ) -> Result<apis::models::CreateModelResponse, ()> {
        match super::model_create(self, body.name.clone(), body.xml.clone()) {
            Ok(v) => Ok(apis::models::CreateModelResponse::Status201_ModelCreated(
                from_val(v),
            )),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => Ok(
                apis::models::CreateModelResponse::Status409_AlreadyExists(msg),
            ),
            Err((_, msg)) => Ok(apis::models::CreateModelResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }

    async fn delete_model(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteModelPathParams,
    ) -> Result<apis::models::DeleteModelResponse, ()> {
        match super::model_delete(&path_params.name) {
            Ok(_) => Ok(apis::models::DeleteModelResponse::Status204_Deleted),
            Err((_, msg)) => Ok(apis::models::DeleteModelResponse::Status404_NotFound(msg)),
        }
    }

    async fn get_model(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetModelPathParams,
    ) -> Result<apis::models::GetModelResponse, ()> {
        match super::model_get(self, &path_params.name) {
            Ok(v) => Ok(apis::models::GetModelResponse::Status200_Model(from_val(v))),
            Err((_, msg)) => Ok(apis::models::GetModelResponse::Status404_NotFound(msg)),
        }
    }

    async fn list_models(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::models::ListModelsResponse, ()> {
        // Only 200 is declared; on a workspace error fall back to empty.
        let v = super::models(self).unwrap_or_else(|_| serde_json::json!([]));
        Ok(apis::models::ListModelsResponse::Status200_ModelSummaries(
            from_val(v),
        ))
    }

    async fn save_model(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::SaveModelPathParams,
        body: &String,
    ) -> Result<apis::models::SaveModelResponse, ()> {
        match super::model_save(self, &path_params.name, body.clone()) {
            Ok(v) => Ok(apis::models::SaveModelResponse::Status200_SavedModel(
                from_val(v),
            )),
            Err((_, msg)) => Ok(apis::models::SaveModelResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }
}

// --- lib ------------------------------------------------------------------

#[async_trait]
impl apis::lib::Lib for ServerImpl {
    async fn create_lib_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::CreateFileRequest,
    ) -> Result<apis::lib::CreateLibFileResponse, ()> {
        match super::lib_file_create(&body.path) {
            Ok(_) => Ok(apis::lib::CreateLibFileResponse::Status201_Created),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => Ok(
                apis::lib::CreateLibFileResponse::Status409_AlreadyExists(msg),
            ),
            Err((_, msg)) => Ok(apis::lib::CreateLibFileResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }

    async fn delete_lib_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::DeleteLibFileQueryParams,
    ) -> Result<apis::lib::DeleteLibFileResponse, ()> {
        match super::lib_file_delete(&query_params.path) {
            Ok(_) => Ok(apis::lib::DeleteLibFileResponse::Status204_Deleted),
            Err((_, msg)) => Ok(apis::lib::DeleteLibFileResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }

    async fn get_lib_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::GetLibFileQueryParams,
    ) -> Result<apis::lib::GetLibFileResponse, ()> {
        match super::lib_file_get(&query_params.path) {
            Ok(contents) => Ok(apis::lib::GetLibFileResponse::Status200_FileContents(
                contents,
            )),
            Err((code, msg)) if code == http::StatusCode::NOT_FOUND => {
                Ok(apis::lib::GetLibFileResponse::Status404_NotFound(msg))
            }
            Err((_, msg)) => Ok(apis::lib::GetLibFileResponse::Status400_InvalidRequest(msg)),
        }
    }

    async fn list_lib_files(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::lib::ListLibFilesResponse, ()> {
        // Only 200 is declared; on error fall back to empty.
        let v = super::lib_list().unwrap_or_else(|_| serde_json::json!({ "files": [] }));
        Ok(apis::lib::ListLibFilesResponse::Status200_LibraryFilePaths(
            from_val(v),
        ))
    }

    async fn save_lib_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        query_params: &models::SaveLibFileQueryParams,
        body: &String,
    ) -> Result<apis::lib::SaveLibFileResponse, ()> {
        match super::lib_file_save(&query_params.path, body) {
            Ok(_) => Ok(apis::lib::SaveLibFileResponse::Status204_Saved),
            Err((_, msg)) => Ok(apis::lib::SaveLibFileResponse::Status400_InvalidRequest(
                msg,
            )),
        }
    }
}

// --- workers --------------------------------------------------------------

#[async_trait]
impl apis::workers::Workers for ServerImpl {
    async fn create_worker(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::CreateWorkerRequest,
    ) -> Result<apis::workers::CreateWorkerResponse, ()> {
        let job_type = flatten_nullable(&body.job_type);
        match super::worker_create(body.name.clone(), job_type).await {
            Ok(v) => Ok(apis::workers::CreateWorkerResponse::Status201_WorkerCreated(from_val(v))),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => {
                Ok(apis::workers::CreateWorkerResponse::Status409_AlreadyExists(msg))
            }
            Err((_, msg)) => Ok(apis::workers::CreateWorkerResponse::Status400_InvalidRequest(msg)),
        }
    }

    async fn create_worker_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::CreateWorkerFilePathParams,
        body: &models::CreateFileRequest,
    ) -> Result<apis::workers::CreateWorkerFileResponse, ()> {
        match super::worker_file_create(&path_params.name, &body.path) {
            Ok(_) => Ok(apis::workers::CreateWorkerFileResponse::Status201_Created),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => {
                Ok(apis::workers::CreateWorkerFileResponse::Status409_AlreadyExists(msg))
            }
            Err((code, msg)) if code == http::StatusCode::NOT_FOUND => Ok(
                apis::workers::CreateWorkerFileResponse::Status404_NotFound(msg),
            ),
            Err((_, msg)) => {
                Ok(apis::workers::CreateWorkerFileResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn delete_worker(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteWorkerPathParams,
    ) -> Result<apis::workers::DeleteWorkerResponse, ()> {
        match super::worker_delete(&path_params.name).await {
            Ok(_) => Ok(apis::workers::DeleteWorkerResponse::Status204_Deleted),
            Err((_, msg)) => Ok(apis::workers::DeleteWorkerResponse::Status404_NotFound(msg)),
        }
    }

    async fn delete_worker_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteWorkerFilePathParams,
        query_params: &models::DeleteWorkerFileQueryParams,
    ) -> Result<apis::workers::DeleteWorkerFileResponse, ()> {
        match super::worker_file_delete(&path_params.name, &query_params.path) {
            Ok(_) => Ok(apis::workers::DeleteWorkerFileResponse::Status204_Deleted),
            Err((_, msg)) => {
                Ok(apis::workers::DeleteWorkerFileResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn get_deno_types(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::workers::GetDenoTypesResponse, ()> {
        Ok(
            apis::workers::GetDenoTypesResponse::Status200_DenoTypeDeclarations(
                super::deno_types_source(),
            ),
        )
    }

    async fn get_worker(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetWorkerPathParams,
    ) -> Result<apis::workers::GetWorkerResponse, ()> {
        match super::worker_get(&path_params.name).await {
            Ok(v) => Ok(apis::workers::GetWorkerResponse::Status200_WorkerSummary(
                from_val(v),
            )),
            Err((_, msg)) => Ok(apis::workers::GetWorkerResponse::Status404_NotFound(msg)),
        }
    }

    async fn get_worker_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetWorkerFilePathParams,
        query_params: &models::GetWorkerFileQueryParams,
    ) -> Result<apis::workers::GetWorkerFileResponse, ()> {
        match super::worker_file_get(&path_params.name, &query_params.path) {
            Ok(contents) => {
                Ok(apis::workers::GetWorkerFileResponse::Status200_FileContents(contents))
            }
            Err((code, msg)) if code == http::StatusCode::NOT_FOUND => Ok(
                apis::workers::GetWorkerFileResponse::Status404_NotFound(msg),
            ),
            Err((_, msg)) => {
                Ok(apis::workers::GetWorkerFileResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn get_worker_sdk(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::workers::GetWorkerSdkResponse, ()> {
        Ok(
            apis::workers::GetWorkerSdkResponse::Status200_WorkerSDKSource(
                super::worker_sdk_source(),
            ),
        )
    }

    async fn list_workers(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::workers::ListWorkersResponse, ()> {
        // Only 200 is declared; on a workspace error fall back to empty.
        let v = super::workers_list()
            .await
            .unwrap_or_else(|_| serde_json::json!({ "workers": [], "denoAvailable": false }));
        Ok(
            apis::workers::ListWorkersResponse::Status200_WorkersPlusRuntimeAvailability(from_val(
                v,
            )),
        )
    }

    async fn save_worker_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::SaveWorkerFilePathParams,
        query_params: &models::SaveWorkerFileQueryParams,
        body: &String,
    ) -> Result<apis::workers::SaveWorkerFileResponse, ()> {
        match super::worker_file_save(&path_params.name, &query_params.path, body) {
            Ok(_) => Ok(apis::workers::SaveWorkerFileResponse::Status204_Saved),
            Err((_, msg)) => {
                Ok(apis::workers::SaveWorkerFileResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn start_worker(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::StartWorkerPathParams,
    ) -> Result<apis::workers::StartWorkerResponse, ()> {
        match super::worker_start(&path_params.name).await {
            Ok(v) => {
                Ok(apis::workers::StartWorkerResponse::Status200_WorkerRuntimeState(from_val(v)))
            }
            Err((_, msg)) => Ok(apis::workers::StartWorkerResponse::Status404_NotFound(msg)),
        }
    }

    async fn stop_worker(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::StopWorkerPathParams,
    ) -> Result<apis::workers::StopWorkerResponse, ()> {
        match super::worker_stop(&path_params.name).await {
            Ok(v) => {
                Ok(apis::workers::StopWorkerResponse::Status200_WorkerRuntimeState(from_val(v)))
            }
            Err((_, msg)) => Ok(apis::workers::StopWorkerResponse::Status404_NotFound(msg)),
        }
    }
}

// --- projects -------------------------------------------------------------

#[async_trait]
impl apis::projects::Projects for ServerImpl {
    async fn compile_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::CompileProjectPathParams,
        body: &models::CompileRequest,
    ) -> Result<apis::projects::CompileProjectResponse, ()> {
        let targets = body.targets.clone().unwrap_or_default();
        match super::project_compile(&path_params.name, targets) {
            Ok(v) => Ok(
                apis::projects::CompileProjectResponse::Status200_WhetherACompileWasStarted(
                    from_val(v),
                ),
            ),
            Err((_, msg)) => Ok(apis::projects::CompileProjectResponse::Status404_NotFound(
                msg,
            )),
        }
    }

    async fn create_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::CreateProjectRequest,
    ) -> Result<apis::projects::CreateProjectResponse, ()> {
        let description = body.description.clone().unwrap_or_default();
        let template = flatten_nullable(&body.template).unwrap_or_else(|| "starter".to_string());
        match super::project_create(&body.name, &description, &template) {
            Ok(v) => {
                Ok(apis::projects::CreateProjectResponse::Status201_ProjectCreated(from_val(v)))
            }
            Err((code, msg)) if code == http::StatusCode::CONFLICT => {
                Ok(apis::projects::CreateProjectResponse::Status409_AlreadyExists(msg))
            }
            Err((_, msg)) => {
                Ok(apis::projects::CreateProjectResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn create_project_path(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::CreateProjectPathPathParams,
        body: &models::CreateProjectPathRequest,
    ) -> Result<apis::projects::CreateProjectPathResponse, ()> {
        let dir = body.dir.unwrap_or(false);
        match super::project_path_create(&path_params.name, &body.path, dir) {
            Ok(_) => Ok(apis::projects::CreateProjectPathResponse::Status201_Created),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => {
                Ok(apis::projects::CreateProjectPathResponse::Status409_AlreadyExists(msg))
            }
            Err((_, msg)) => {
                Ok(apis::projects::CreateProjectPathResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn delete_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteProjectPathParams,
    ) -> Result<apis::projects::DeleteProjectResponse, ()> {
        match super::project_delete(&path_params.name).await {
            Ok(_) => Ok(apis::projects::DeleteProjectResponse::Status204_Deleted),
            Err((_, msg)) => Ok(apis::projects::DeleteProjectResponse::Status404_NotFound(
                msg,
            )),
        }
    }

    async fn delete_project_path(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteProjectPathPathParams,
        query_params: &models::DeleteProjectPathQueryParams,
    ) -> Result<apis::projects::DeleteProjectPathResponse, ()> {
        match super::project_path_delete(&path_params.name, &query_params.path) {
            Ok(_) => Ok(apis::projects::DeleteProjectPathResponse::Status204_Deleted),
            Err((_, msg)) => {
                Ok(apis::projects::DeleteProjectPathResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn get_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetProjectPathParams,
    ) -> Result<apis::projects::GetProjectResponse, ()> {
        match super::project_detail(&path_params.name).await {
            Ok(mut v) => {
                fix_run_status(&mut v);
                Ok(apis::projects::GetProjectResponse::Status200_ProjectDetail(
                    from_val(v),
                ))
            }
            Err((_, msg)) => Ok(apis::projects::GetProjectResponse::Status404_NotFound(msg)),
        }
    }

    async fn get_project_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetProjectConfigPathParams,
    ) -> Result<apis::projects::GetProjectConfigResponse, ()> {
        match super::project_config_get(&path_params.name) {
            Ok(v) => {
                Ok(apis::projects::GetProjectConfigResponse::Status200_ProjectConfig(from_val(v)))
            }
            Err((_, msg)) => Ok(apis::projects::GetProjectConfigResponse::Status404_NotFound(msg)),
        }
    }

    async fn get_run_configs(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetRunConfigsPathParams,
    ) -> Result<apis::projects::GetRunConfigsResponse, ()> {
        match super::project_run_configs_list(&path_params.name) {
            Ok(v) => Ok(
                apis::projects::GetRunConfigsResponse::Status200_RunConfigurationsPlusTheActiveId(
                    from_val(v),
                ),
            ),
            Err((_, msg)) => Ok(apis::projects::GetRunConfigsResponse::Status404_NotFound(
                msg,
            )),
        }
    }

    async fn list_project_files(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::ListProjectFilesPathParams,
    ) -> Result<apis::projects::ListProjectFilesResponse, ()> {
        match super::project_files(&path_params.name) {
            Ok(v) => Ok(apis::projects::ListProjectFilesResponse::Status200_FileTree(from_val(v))),
            Err((_, msg)) => Ok(apis::projects::ListProjectFilesResponse::Status404_NotFound(msg)),
        }
    }

    async fn list_projects(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<apis::projects::ListProjectsResponse, ()> {
        // Only 200 is declared; on error fall back to empty.
        let mut v = super::projects_list().await.unwrap_or_else(|_| {
            serde_json::json!({
                "projects": [],
                "denoAvailable": false,
                "platforms": super::projects::PLATFORMS,
            })
        });
        fix_run_status(&mut v);
        Ok(
            apis::projects::ListProjectsResponse::Status200_ProjectsPlusRuntimeAvailability(
                from_val(v),
            ),
        )
    }

    async fn rename_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::RenameProjectPathParams,
        body: &models::RenameProjectRequest,
    ) -> Result<apis::projects::RenameProjectResponse, ()> {
        match super::project_rename(&path_params.name, &body.new_name).await {
            Ok(v) => Ok(
                apis::projects::RenameProjectResponse::Status200_UpdatedProjectConfig(from_val(v)),
            ),
            Err((code, msg)) if code == http::StatusCode::CONFLICT => {
                Ok(apis::projects::RenameProjectResponse::Status409_AlreadyExists(msg))
            }
            Err((_, msg)) => {
                Ok(apis::projects::RenameProjectResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn run_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::RunProjectPathParams,
    ) -> Result<apis::projects::RunProjectResponse, ()> {
        match super::project_run(&path_params.name).await {
            Ok(mut v) => {
                fix_run_status(&mut v);
                Ok(apis::projects::RunProjectResponse::Status200_RunState(
                    from_val(v),
                ))
            }
            Err((_, msg)) => Ok(apis::projects::RunProjectResponse::Status404_NotFound(msg)),
        }
    }

    async fn save_project_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::SaveProjectConfigPathParams,
        body: &models::ProjectConfig,
    ) -> Result<apis::projects::SaveProjectConfigResponse, ()> {
        let cfg: super::projects::ProjectConfig =
            from_val(serde_json::to_value(body).expect("config serializes"));
        match super::project_config_put(&path_params.name, cfg) {
            Ok(v) => Ok(
                apis::projects::SaveProjectConfigResponse::Status200_SavedProjectConfig(from_val(
                    v,
                )),
            ),
            Err((_, msg)) => {
                Ok(apis::projects::SaveProjectConfigResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn save_project_file(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::SaveProjectFilePathParams,
        query_params: &models::SaveProjectFileQueryParams,
        body: &String,
    ) -> Result<apis::projects::SaveProjectFileResponse, ()> {
        match super::project_file_save(&path_params.name, &query_params.path, body) {
            Ok(_) => Ok(apis::projects::SaveProjectFileResponse::Status204_Saved),
            Err((_, msg)) => {
                Ok(apis::projects::SaveProjectFileResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn set_active_run_config(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::SetActiveRunConfigPathParams,
        body: &models::ActiveRunConfigRequest,
    ) -> Result<apis::projects::SetActiveRunConfigResponse, ()> {
        let id = flatten_nullable(&body.id);
        match super::project_active_run_config_put(&path_params.name, id) {
            Ok(v) => {
                Ok(apis::projects::SetActiveRunConfigResponse::Status200_TheActiveRun(from_val(v)))
            }
            Err((code, msg)) if code == http::StatusCode::NOT_FOUND => {
                Ok(apis::projects::SetActiveRunConfigResponse::Status404_NotFound(msg))
            }
            Err((_, msg)) => {
                Ok(apis::projects::SetActiveRunConfigResponse::Status400_InvalidRequest(msg))
            }
        }
    }

    async fn stop_project(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::StopProjectPathParams,
    ) -> Result<apis::projects::StopProjectResponse, ()> {
        match super::project_stop(&path_params.name).await {
            Ok(mut v) => {
                fix_run_status(&mut v);
                Ok(apis::projects::StopProjectResponse::Status200_RunState(
                    from_val(v),
                ))
            }
            Err((_, msg)) => Ok(apis::projects::StopProjectResponse::Status404_NotFound(msg)),
        }
    }
}

// --- data (datasources / DB Manager, ADR 0024) ----------------------------

/// Map a datasource `ApiResult` error to a data response's collapsed status set
/// (only 400/404 exist on these operations): `NOT_FOUND` → 404, everything else
/// (bad SQL/unknown source → 400; no-Deno → 503; gateway → 500) → 400.
macro_rules! data_ok_or {
    ($res:expr, $ok:path, $bad:path, $nf:path) => {
        match $res {
            Ok(v) => Ok($ok(from_val(v))),
            Err((code, msg)) if code == http::StatusCode::NOT_FOUND => Ok($nf(msg)),
            Err((_, msg)) => Ok($bad(msg)),
        }
    };
}

#[async_trait]
impl apis::data::Data for ServerImpl {
    async fn get_data_sources(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetDataSourcesPathParams,
    ) -> Result<apis::data::GetDataSourcesResponse, ()> {
        use apis::data::GetDataSourcesResponse as R;
        data_ok_or!(
            super::project_data_sources(&path_params.name).await,
            R::Status200_DatasourcesPlusTheDefaultSourceName,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn get_data_schema(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetDataSchemaPathParams,
    ) -> Result<apis::data::GetDataSchemaResponse, ()> {
        use apis::data::GetDataSchemaResponse as R;
        data_ok_or!(
            super::project_data_schema(&path_params.name, &path_params.source).await,
            R::Status200_DatasourceSchema,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn query_data(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::QueryDataPathParams,
        body: &models::DataQueryRequest,
    ) -> Result<apis::data::QueryDataResponse, ()> {
        use apis::data::QueryDataResponse as R;
        let params = body
            .params
            .as_ref()
            .map(|ps| ps.iter().map(|o| o.0.clone()).collect())
            .unwrap_or_default();
        data_ok_or!(
            super::project_data_query(&path_params.name, &path_params.source, &body.sql, params)
                .await,
            R::Status200_QueryResult,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn exec_data(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::ExecDataPathParams,
        body: &models::DataQueryRequest,
    ) -> Result<apis::data::ExecDataResponse, ()> {
        use apis::data::ExecDataResponse as R;
        let params = body
            .params
            .as_ref()
            .map(|ps| ps.iter().map(|o| o.0.clone()).collect())
            .unwrap_or_default();
        data_ok_or!(
            super::project_data_exec(&path_params.name, &path_params.source, &body.sql, params)
                .await,
            R::Status200_ExecResult,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn get_data_migrations(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetDataMigrationsPathParams,
    ) -> Result<apis::data::GetDataMigrationsResponse, ()> {
        use apis::data::GetDataMigrationsResponse as R;
        data_ok_or!(
            super::project_data_migrations(&path_params.name, &path_params.source).await,
            R::Status200_MigrationStatus,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn migrate_data(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::MigrateDataPathParams,
    ) -> Result<apis::data::MigrateDataResponse, ()> {
        use apis::data::MigrateDataResponse as R;
        data_ok_or!(
            super::project_data_migrate(&path_params.name, &path_params.source).await,
            R::Status200_MigrationsApplied,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }
}

#[async_trait]
impl apis::triggers::Triggers for ServerImpl {
    async fn enqueue_trigger_event(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::EnqueueTriggerEventPathParams,
        body: &models::TriggerEnqueueRequest,
    ) -> Result<apis::triggers::EnqueueTriggerEventResponse, ()> {
        use apis::triggers::EnqueueTriggerEventResponse as R;
        let event_body = serde_json::Value::Object(
            body.body
                .iter()
                .map(|(k, v)| (k.clone(), v.0.clone()))
                .collect(),
        );
        let idem = flatten_nullable(&body.idempotency_key);
        data_ok_or!(
            super::project_trigger_enqueue(&path_params.name, &body.trigger_id, idem, event_body)
                .await,
            R::Status200_EnqueueOutcome,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn get_trigger_inbox(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetTriggerInboxPathParams,
    ) -> Result<apis::triggers::GetTriggerInboxResponse, ()> {
        use apis::triggers::GetTriggerInboxResponse as R;
        data_ok_or!(
            super::project_trigger_inbox(&path_params.name).await,
            R::Status200_InboxStatus,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }

    async fn get_triggers(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetTriggersPathParams,
    ) -> Result<apis::triggers::GetTriggersResponse, ()> {
        use apis::triggers::GetTriggersResponse as R;
        data_ok_or!(
            super::project_triggers(&path_params.name).await,
            R::Status200_Triggers,
            R::Status400_InvalidRequest,
            R::Status404_NotFound
        )
    }
}
