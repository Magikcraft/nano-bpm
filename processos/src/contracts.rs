//! The **read contract** with Nano: typed mirrors of the gateway's public
//! `/console/api` trace + metrics DTOs, and a thin HTTP client that fetches them.
//!
//! These structs are deliberately a *copy* of the gateway's wire shapes (camelCase
//! JSON), not a shared crate import — that is what keeps the dependency one-way.
//! Nano owns the producer side; ProcessOS owns this consumer mirror and versions it
//! independently. If a field Nano emits is missing here, `serde` ignores it; if a
//! field we read is absent, it deserialises to `None`/default.
//!
//! Some mirrored fields aren't consumed by the Stage-T1 Insights fold yet (e.g.
//! `version`, `business_id`, `job_type`); they are kept so the contract type is
//! faithful and later stages can read them without re-deriving the shape.
#![allow(dead_code)]

use serde::Deserialize;

/// One row of `GET /console/api/traces` (a `trace::TraceSummaryDto`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceSummary {
    pub instance_key: String,
    pub process_id: String,
    pub version: Option<i32>,
    #[serde(default)]
    pub business_id: Option<String>,
    pub outcome: String,
    pub started_at: u64,
    #[serde(default)]
    pub ended_at: Option<u64>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    pub element_count: usize,
    pub incident_count: usize,
}

/// `GET /console/api/traces/{key}` (a `trace::InstanceTraceDto`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceTrace {
    pub instance_key: String,
    pub process_id: String,
    pub version: Option<i32>,
    pub outcome: String,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub elements: Vec<Element>,
    #[serde(default)]
    pub incidents: Vec<Incident>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Element {
    pub element_id: String,
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub incidents: u32,
    #[serde(default)]
    pub job: Option<Job>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    #[serde(rename = "type")]
    pub job_type: String,
    #[serde(default)]
    pub queue_ms: Option<u64>,
    #[serde(default)]
    pub service_ms: Option<u64>,
    #[serde(default)]
    pub failures: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Incident {
    pub element_id: String,
    pub kind: String,
    pub reason: String,
}

/// The subset of `GET /console/api/metrics` (a `MetricsDto`) ProcessOS surfaces as
/// live gauges. Unused fields are ignored by `serde`. It is re-emitted verbatim in
/// the Insights document, so it is `Serialize` as well as `Deserialize`.
#[derive(Debug, Clone, Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    pub timestamp_ms: u64,
    pub active_instances: i64,
    pub creates_total: u64,
    pub completions_total: u64,
    #[serde(default)]
    pub connections_active: i64,
    #[serde(default)]
    pub resident_bytes: Option<u64>,
}

/// HTTP client for Nano's public read contract. One-way: it only ever issues GETs
/// against the gateway's `/console/api` surface — never the journal, read DB, or
/// engine internals.
#[derive(Clone)]
pub struct NanoClient {
    base_url: String,
    http: reqwest::Client,
}

impl NanoClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// `GET /console/api/traces?limit=` — recent instance trace summaries.
    pub async fn list_traces(&self, limit: usize) -> Result<Vec<TraceSummary>, String> {
        let url = format!("{}/console/api/traces?limit={}", self.base_url, limit);
        self.get_json(&url).await
    }

    /// `GET /console/api/traces/{key}` — one instance's full canonical trace.
    pub async fn trace(&self, instance_key: &str) -> Result<InstanceTrace, String> {
        let url = format!("{}/console/api/traces/{}", self.base_url, instance_key);
        self.get_json(&url).await
    }

    /// `GET /console/api/metrics` — live node gauges.
    pub async fn metrics(&self) -> Result<Metrics, String> {
        let url = format!("{}/console/api/metrics", self.base_url);
        self.get_json(&url).await
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T, String> {
        let res = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| format!("GET {url}: {e}"))?;
        if !res.status().is_success() {
            return Err(format!("GET {url}: HTTP {}", res.status()));
        }
        res.json::<T>()
            .await
            .map_err(|e| format!("decode {url}: {e}"))
    }
}
