//! Optional **reasoning-control surface** probe + best-effort interrupt.
//!
//! Newer llama.cpp builds may expose a `POST /v1/chat/completions/control` endpoint that, with
//! `{"reasoning_control": true, ...}`, can halt or steer an in-flight generation **mid-thinking**
//! (server-side) rather than only at our agent loop's round boundary. When that surface exists we
//! prefer it for the loop monitor's wrap-up and the operator's "wrap it up" command; when it does
//! not (the currently-installed build does not), callers gracefully fall back to the existing
//! cancel/steer mechanism, which only takes effect at a round boundary.
//!
//! This module never *replaces* the runtime cancel/steer path — it layers on top of it as a
//! best-effort enhancement, so behaviour is unchanged on builds without the control endpoint.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// Map an HTTP status from a control-surface probe to "is this surface supported?".
///
/// A 2xx means the endpoint exists and accepted the control request. Anything else — most
/// commonly `404 Not Found` / `405 Method Not Allowed` / `501 Not Implemented` on builds without
/// the feature, but also any other 4xx/5xx — is treated as unsupported so callers fall back.
pub fn classify_probe(status: u16) -> bool {
    (200..300).contains(&status)
}

/// Build the control endpoint URL from an OpenAI-style base URL (e.g.
/// `http://127.0.0.1:8080/v1` → `http://127.0.0.1:8080/v1/chat/completions/control`).
fn control_url(base_url: &str) -> String {
    format!("{}/chat/completions/control", base_url.trim_end_matches('/'))
}

/// Per-`base_url` cache of the probe result, so we pay the round-trip at most once per endpoint.
/// Only **definitive HTTP responses** are cached; transport errors (server not up yet) are not,
/// so a sidecar that starts later can still be detected on a subsequent call.
fn cache() -> &'static Mutex<HashMap<String, bool>> {
    static CACHE: OnceLock<Mutex<HashMap<String, bool>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cached(base_url: &str) -> Option<bool> {
    cache().lock().ok().and_then(|m| m.get(base_url).copied())
}

fn remember(base_url: &str, supported: bool) {
    if let Ok(mut m) = cache().lock() {
        m.insert(base_url.to_string(), supported);
    }
}

fn client() -> Option<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .ok()
}

/// Probe whether `base_url` exposes the reasoning-control surface. Cached per endpoint. Returns
/// `false` on any non-2xx response or transport error (the safe default — callers fall back).
pub async fn supports_control(base_url: &str) -> bool {
    if base_url.trim().is_empty() {
        return false;
    }
    if let Some(hit) = cached(base_url) {
        return hit;
    }
    let Some(client) = client() else {
        return false;
    };
    // A minimal, side-effect-free probe: ask the surface to acknowledge reasoning control.
    let body = serde_json::json!({ "reasoning_control": true });
    match client.post(control_url(base_url)).json(&body).send().await {
        Ok(resp) => {
            let supported = classify_probe(resp.status().as_u16());
            remember(base_url, supported);
            supported
        }
        // Transport error (e.g. server not started) — not cached, treated as unsupported for now.
        Err(_) => false,
    }
}

/// Best-effort mid-generation interrupt via the control surface. `action` is a short verb the
/// surface understands (e.g. `"wrapup"` to stop thinking and conclude, `"steer"` to redirect).
/// Returns `true` only if the surface exists *and* accepted the request; on `false` the caller
/// must fall back to the runtime cancel/steer path.
pub async fn interrupt(base_url: &str, model: &str, action: &str, note: Option<&str>) -> bool {
    if !supports_control(base_url).await {
        return false;
    }
    let Some(client) = client() else {
        return false;
    };
    let mut body = serde_json::json!({
        "reasoning_control": true,
        "model": model,
        "action": action,
    });
    if let (Some(obj), Some(note)) = (body.as_object_mut(), note) {
        obj.insert("note".into(), serde_json::json!(note));
    }
    match client.post(control_url(base_url)).json(&body).send().await {
        Ok(resp) => classify_probe(resp.status().as_u16()),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_probe_accepts_only_2xx() {
        assert!(classify_probe(200));
        assert!(classify_probe(202));
        assert!(classify_probe(299));
        for s in [301, 400, 404, 405, 500, 501, 503] {
            assert!(!classify_probe(s), "status {s} should be unsupported");
        }
    }

    #[test]
    fn control_url_is_built_under_the_base() {
        assert_eq!(
            control_url("http://127.0.0.1:8080/v1"),
            "http://127.0.0.1:8080/v1/chat/completions/control"
        );
        // Trailing slash is tolerated (no double slash).
        assert_eq!(
            control_url("http://127.0.0.1:8080/v1/"),
            "http://127.0.0.1:8080/v1/chat/completions/control"
        );
    }

    #[tokio::test]
    async fn empty_base_url_is_never_supported() {
        assert!(!supports_control("   ").await);
    }
}
