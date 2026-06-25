//! Pluggable LLM client for the hypothesis stage (M2).
//!
//! Two providers cover the field:
//!
//! * **`openai`** — the OpenAI **chat-completions** wire shape, which is what a
//!   local `llama.cpp` server (`--api`), vLLM, Ollama (`/v1`), LM Studio, and
//!   OpenAI itself all speak. This is the default; point `baseUrl` at your local
//!   model on the network.
//! * **`anthropic`** — the Anthropic **messages** API (`api.anthropic.com`).
//!
//! Configuration comes from the environment, and any field may be overridden
//! per request so you can A/B a local model against a hosted one without a
//! restart. The client is only ever invoked on the explicit `/api/harness/hypothesize`
//! path — the deterministic baked harness never touches the network.

use serde::{Deserialize, Serialize};
use serde_json::json;

/// Which wire protocol to speak.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    /// OpenAI-compatible `chat/completions` (llama.cpp, vLLM, Ollama, LM Studio, OpenAI).
    Openai,
    /// Anthropic `messages` API.
    Anthropic,
}

impl Provider {
    fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "openai" | "openai-compatible" | "llamacpp" | "llama.cpp" | "ollama" | "vllm"
            | "local" => Some(Provider::Openai),
            "anthropic" | "claude" => Some(Provider::Anthropic),
            _ => None,
        }
    }

    /// Stable lowercase label for reports.
    pub fn as_str(&self) -> &'static str {
        match self {
            Provider::Openai => "openai",
            Provider::Anthropic => "anthropic",
        }
    }
}

/// Resolved LLM configuration. `model` is required to make any call.
#[derive(Clone, Debug)]
pub struct LlmConfig {
    pub provider: Provider,
    pub base_url: String,
    pub model: String,
    pub api_key: Option<String>,
    pub max_tokens: u32,
    pub temperature: f32,
    /// OpenAI-style frequency penalty, forwarded to the model. Small quantised local
    /// models readily fall into a repetition attractor (emitting the same line forever
    /// until they hit the token budget); a modest penalty (>0) discourages that at the
    /// sampler. Default 0.3; set `PROCESSOS_LLM_FREQUENCY_PENALTY` to tune (0 disables).
    pub frequency_penalty: f32,
}

/// Per-request overrides (any subset) accepted on the hypothesize endpoint.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LlmOverride {
    pub provider: Option<String>,
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
}

impl LlmConfig {
    /// Read configuration from the environment.
    ///
    /// | Env | Default |
    /// |-----|---------|
    /// | `PROCESSOS_LLM_PROVIDER` | `openai` |
    /// | `PROCESSOS_LLM_BASE_URL` | provider default (`http://127.0.0.1:8080/v1` / `https://api.anthropic.com`) |
    /// | `PROCESSOS_LLM_MODEL` | _(none — required to call)_ |
    /// | `PROCESSOS_LLM_API_KEY` | _(none; local models usually need none)_ |
    /// | `PROCESSOS_LLM_MAX_TOKENS` | `2048` |
    /// | `PROCESSOS_LLM_TEMPERATURE` | `0.2` |
    pub fn from_env() -> Self {
        let provider = std::env::var("PROCESSOS_LLM_PROVIDER")
            .ok()
            .and_then(|s| Provider::parse(&s))
            .unwrap_or(Provider::Openai);
        let base_url = std::env::var("PROCESSOS_LLM_BASE_URL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| default_base_url(provider));
        let model = std::env::var("PROCESSOS_LLM_MODEL").unwrap_or_default();
        let api_key = std::env::var("PROCESSOS_LLM_API_KEY")
            .ok()
            .filter(|s| !s.is_empty());
        let max_tokens = std::env::var("PROCESSOS_LLM_MAX_TOKENS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2048);
        let temperature = std::env::var("PROCESSOS_LLM_TEMPERATURE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.2);
        let frequency_penalty = std::env::var("PROCESSOS_LLM_FREQUENCY_PENALTY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.3);
        Self {
            provider,
            base_url,
            model,
            api_key,
            max_tokens,
            temperature,
            frequency_penalty,
        }
    }

    /// Apply per-request overrides onto the env-resolved base.
    pub fn with_override(mut self, o: &LlmOverride) -> Self {
        if let Some(p) = o.provider.as_deref().and_then(Provider::parse) {
            // If the provider changes and no base_url override is given, move to
            // that provider's default base so we don't POST Anthropic JSON at a
            // local OpenAI endpoint (or vice versa).
            if o.base_url.is_none() && self.provider != p {
                self.base_url = default_base_url(p);
            }
            self.provider = p;
        }
        if let Some(b) = o.base_url.clone().filter(|s| !s.is_empty()) {
            self.base_url = b;
        }
        if let Some(m) = o.model.clone().filter(|s| !s.is_empty()) {
            self.model = m;
        }
        if let Some(k) = o.api_key.clone().filter(|s| !s.is_empty()) {
            self.api_key = Some(k);
        }
        if let Some(t) = o.max_tokens {
            self.max_tokens = t;
        }
        if let Some(t) = o.temperature {
            self.temperature = t;
        }
        self
    }

    /// Whether the config is usable (a model name is set).
    pub fn is_ready(&self) -> bool {
        !self.model.is_empty()
    }
}

fn default_base_url(provider: Provider) -> String {
    match provider {
        Provider::Openai => "http://127.0.0.1:8080/v1".to_string(),
        Provider::Anthropic => "https://api.anthropic.com".to_string(),
    }
}

/// Send a system+user prompt and return the model's text completion.
pub async fn complete(cfg: &LlmConfig, system: &str, user: &str) -> Result<String, String> {
    if !cfg.is_ready() {
        return Err(
            "no LLM model configured (set PROCESSOS_LLM_MODEL or pass llm.model in the request)"
                .to_string(),
        );
    }
    let client = reqwest::Client::builder()
        .build()
        .map_err(|e| format!("http client: {e}"))?;
    let base = cfg.base_url.trim_end_matches('/');
    match cfg.provider {
        Provider::Openai => complete_openai(&client, cfg, base, system, user).await,
        Provider::Anthropic => complete_anthropic(&client, cfg, base, system, user).await,
    }
}

async fn complete_openai(
    client: &reqwest::Client,
    cfg: &LlmConfig,
    base: &str,
    system: &str,
    user: &str,
) -> Result<String, String> {
    let url = format!("{base}/chat/completions");
    let body = json!({
        "model": cfg.model,
        "temperature": cfg.temperature,
        "max_tokens": cfg.max_tokens,
        "frequency_penalty": cfg.frequency_penalty,
        "messages": [
            { "role": "system", "content": system },
            { "role": "user", "content": user },
        ],
    });
    let mut req = client.post(&url).json(&body);
    if let Some(key) = &cfg.api_key {
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
    let v: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("LLM response not JSON: {e}"))?;
    v["choices"][0]["message"]["content"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| {
            format!(
                "LLM response missing choices[0].message.content: {}",
                truncate(&text, 300)
            )
        })
}

async fn complete_anthropic(
    client: &reqwest::Client,
    cfg: &LlmConfig,
    base: &str,
    system: &str,
    user: &str,
) -> Result<String, String> {
    let url = format!("{base}/v1/messages");
    let body = json!({
        "model": cfg.model,
        "max_tokens": cfg.max_tokens,
        "temperature": cfg.temperature,
        "system": system,
        "messages": [ { "role": "user", "content": user } ],
    });
    let key = cfg
        .api_key
        .as_deref()
        .ok_or("Anthropic requires an API key (set PROCESSOS_LLM_API_KEY or llm.apiKey)")?;
    let resp = client
        .post(&url)
        .header("x-api-key", key)
        .header("anthropic-version", "2023-06-01")
        .json(&body)
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
    let v: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("LLM response not JSON: {e}"))?;
    v["content"][0]["text"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| {
            format!(
                "LLM response missing content[0].text: {}",
                truncate(&text, 300)
            )
        })
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}…", &s[..n])
    }
}

/// A model advertised by an endpoint, with its context window when the endpoint reports
/// one (llama.cpp exposes `meta.n_ctx` / `n_ctx_train`; some servers use `context_length`).
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelInfo {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u64>,
}

/// Pull a context-window size out of a model entry, tolerating the several shapes endpoints
/// use: llama.cpp nests it under `meta` (`n_ctx`, the server's configured window, else
/// `n_ctx_train`); others expose a top-level `context_length` / `max_context_length` /
/// `max_model_len` / `context_window`.
fn extract_context_window(item: &serde_json::Value) -> Option<u64> {
    let as_u64 = |v: &serde_json::Value| v.as_u64().filter(|n| *n > 0);
    if let Some(meta) = item.get("meta") {
        if let Some(n) = meta.get("n_ctx").and_then(as_u64) {
            return Some(n);
        }
        if let Some(n) = meta.get("n_ctx_train").and_then(as_u64) {
            return Some(n);
        }
    }
    for key in [
        "context_length",
        "max_context_length",
        "max_model_len",
        "context_window",
        "n_ctx",
    ] {
        if let Some(n) = item.get(key).and_then(as_u64) {
            return Some(n);
        }
    }
    None
}

/// List the models the configured endpoint advertises (the OpenAI-compatible `/models`
/// route, or Anthropic's `/v1/models`), each with its context window when reported. Used
/// by the console to populate the model picker and the max-tokens field by querying the
/// live endpoint. A `model` need not be set on `cfg`.
pub async fn list_models(cfg: &LlmConfig) -> Result<Vec<ModelInfo>, String> {
    let client = reqwest::Client::builder()
        .build()
        .map_err(|e| format!("http client: {e}"))?;
    let base = cfg.base_url.trim_end_matches('/');
    let (url, req) = match cfg.provider {
        Provider::Openai => {
            let url = format!("{base}/models");
            let mut r = client.get(&url);
            if let Some(key) = &cfg.api_key {
                r = r.bearer_auth(key);
            }
            (url, r)
        }
        Provider::Anthropic => {
            let url = format!("{base}/v1/models");
            let key = cfg
                .api_key
                .as_deref()
                .ok_or("Anthropic requires an API key to list models")?;
            let r = client
                .get(&url)
                .header("x-api-key", key)
                .header("anthropic-version", "2023-06-01");
            (url, r)
        }
    };
    let resp = req
        .send()
        .await
        .map_err(|e| format!("model list request to {url} failed: {e}"))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("reading model list: {e}"))?;
    if !status.is_success() {
        return Err(format!(
            "endpoint returned {status}: {}",
            truncate(&text, 300)
        ));
    }
    let v: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("model list not JSON: {e}"))?;
    // OpenAI / Anthropic / llama.cpp all expose `data: [{ id }]`; fall back to a bare
    // top-level `models: [{ id | name }]` (some llama.cpp builds). Capture a context
    // window per entry when the endpoint reports one.
    let mut models: Vec<ModelInfo> = Vec::new();
    let arrays = [v.get("data"), v.get("models")];
    for arr in arrays.into_iter().flatten() {
        if let Some(items) = arr.as_array() {
            for it in items {
                let id = it
                    .get("id")
                    .and_then(|x| x.as_str())
                    .or_else(|| it.get("name").and_then(|x| x.as_str()))
                    .or_else(|| it.as_str());
                if let Some(id) = id {
                    if !id.is_empty() && !models.iter().any(|m| m.id == id) {
                        models.push(ModelInfo {
                            id: id.to_string(),
                            context_window: extract_context_window(it),
                        });
                    }
                }
            }
        }
        if !models.is_empty() {
            break;
        }
    }
    if models.is_empty() {
        return Err(format!(
            "no models found in response: {}",
            truncate(&text, 300)
        ));
    }
    models.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(models)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_provider_aliases() {
        assert_eq!(Provider::parse("openai"), Some(Provider::Openai));
        assert_eq!(Provider::parse("llama.cpp"), Some(Provider::Openai));
        assert_eq!(Provider::parse("Local"), Some(Provider::Openai));
        assert_eq!(Provider::parse("anthropic"), Some(Provider::Anthropic));
        assert_eq!(Provider::parse("claude"), Some(Provider::Anthropic));
        assert_eq!(Provider::parse("nope"), None);
    }

    #[test]
    fn override_switches_default_base_url_with_provider() {
        let cfg = LlmConfig {
            provider: Provider::Openai,
            base_url: default_base_url(Provider::Openai),
            model: "local-model".into(),
            api_key: None,
            max_tokens: 1024,
            temperature: 0.2,
            frequency_penalty: 0.0,
        };
        let o = LlmOverride {
            provider: Some("anthropic".into()),
            model: Some("claude-3-5-sonnet-latest".into()),
            ..Default::default()
        };
        let merged = cfg.with_override(&o);
        assert_eq!(merged.provider, Provider::Anthropic);
        assert_eq!(merged.base_url, "https://api.anthropic.com");
        assert_eq!(merged.model, "claude-3-5-sonnet-latest");
    }

    #[test]
    fn explicit_base_url_override_is_kept() {
        let cfg = LlmConfig {
            provider: Provider::Openai,
            base_url: "http://127.0.0.1:8080/v1".into(),
            model: "m".into(),
            api_key: None,
            max_tokens: 1024,
            temperature: 0.2,
            frequency_penalty: 0.0,
        };
        let o = LlmOverride {
            base_url: Some("http://gpu-box.lan:8000/v1".into()),
            ..Default::default()
        };
        let merged = cfg.with_override(&o);
        assert_eq!(merged.base_url, "http://gpu-box.lan:8000/v1");
    }
}
