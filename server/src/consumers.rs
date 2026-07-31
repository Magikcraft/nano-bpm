//! "Who is polling what" consumer registry — the engine-side truth behind the
//! console's live consumers panel (issue #404).
//!
//! The console's Workers view lists console-*authored* worker directories
//! (`createWorker` + Monaco). It cannot see a **hired agent** that connects over
//! the SDK and polls a job type, because that agent never touches the console —
//! it talks to the engine. This module records those live consumers so the
//! console can show, e.g., "your agent is polling `convergence-loop:review-round`".
//!
//! Consumers arrive over two transports with fundamentally different liveness
//! models, so the panel is split by transport:
//!
//! * **Falcon** (the `/falcon` command-stream): a persistent WebSocket. The
//!   [`crate::falcon::Registry`] already tracks each connection's `last_seen_ms`
//!   (updated on every frame, including heartbeats) and a reaper evicts a
//!   connection silent past [`crate::falcon::falcon_liveness_timeout_ms`]. We
//!   reuse that same deadline so the panel's notion of "stale" matches the
//!   engine's own — no new timers.
//! * **REST** (`activateJobs` long-poll): stateless — there is no connection to
//!   observe. A healthy worker simply re-issues `activateJobs` in a loop, so we
//!   record the wall-clock of each poll per `(jobType, worker)` and infer
//!   liveness from its age against the windows below.
//!
//! This is best-effort *observability only*. Recording a poll is a single map
//! insert off the activation result path; it never affects job-activation
//! semantics.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::falcon::{self, Registry};

/// A REST consumer counts as **live** while a poll was seen within this window.
/// A healthy worker re-issues `activateJobs` at least once per long-poll window
/// ([`crate::DEFAULT_REQUEST_TIMEOUT_MS`], 5s), so 2× that (10s) tolerates one
/// fully missed cycle before the row greys to "idle". Overridable via
/// `NANOBPMN_CONSUMER_REST_STALE_MS`.
const REST_STALE_MS: u64 = 10_000;

/// A REST consumer with no poll for this long is dropped from the panel entirely
/// (rather than merely greyed at [`REST_STALE_MS`]) — the worker is gone, not
/// idle. Chosen well above the stale window so a briefly-paused worker flickers
/// to "idle" but does not vanish and re-appear. Overridable via
/// `NANOBPMN_CONSUMER_REST_EVICT_MS`.
const REST_EVICT_MS: u64 = 60_000;

/// Last-poll wall-clock (epoch millis) per REST `(jobType, worker)` consumer.
/// A process-global best-effort map fed by [`record_rest_poll`] on every
/// `activateJobs`; pruned on read in [`snapshot`].
static REST_POLLS: LazyLock<Mutex<HashMap<(String, String), u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Wall-clock millis since the Unix epoch. Liveness only — never journaled.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// REST liveness window (millis), overridable via `NANOBPMN_CONSUMER_REST_STALE_MS`.
fn rest_stale_ms() -> u64 {
    env_u64("NANOBPMN_CONSUMER_REST_STALE_MS").unwrap_or(REST_STALE_MS)
}

/// REST eviction window (millis), overridable via `NANOBPMN_CONSUMER_REST_EVICT_MS`.
fn rest_evict_ms() -> u64 {
    env_u64("NANOBPMN_CONSUMER_REST_EVICT_MS").unwrap_or(REST_EVICT_MS)
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
}

/// Records that `worker` polled `job_type` over REST (an `activateJobs` call).
/// Best-effort: a single map insert, called off the activation-result path.
pub fn record_rest_poll(job_type: &str, worker: &str) {
    let mut polls = REST_POLLS.lock().expect("consumer rest-polls poisoned");
    polls.insert((job_type.to_string(), worker.to_string()), now_ms());
}

/// One live job consumer surfaced to the console panel.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Consumer {
    /// The job type being polled, e.g. `convergence-loop:review-round`.
    pub job_type: String,
    /// The worker / lease-owner name the consumer registered under.
    pub worker: String,
    /// Transport the consumer arrived on: `"rest"` or `"falcon"`.
    pub transport: &'static str,
    /// Wall-clock (epoch millis) of the consumer's most recent activity.
    pub last_seen_ms: u64,
    /// Age of `last_seen_ms` relative to the snapshot's `now_ms`.
    pub age_ms: u64,
    /// `"live"` while within the transport's liveness window, else `"idle"`.
    pub status: &'static str,
}

/// The consumers panel payload: the live consumer rows plus the windows used to
/// compute their status, so the console can label the thresholds it is showing.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConsumersResponse {
    /// Snapshot wall-clock (epoch millis) — the reference for every `age_ms`.
    pub now_ms: u64,
    /// REST liveness window in effect (millis).
    pub rest_stale_ms: u64,
    /// Falcon liveness deadline in effect (millis) — the engine's reaper timeout.
    pub falcon_liveness_ms: u64,
    /// Live consumers, sorted by job type, then worker, then transport.
    pub consumers: Vec<Consumer>,
}

/// Builds the consumers snapshot: pruned REST polls + live Falcon subscriptions,
/// each tagged with a transport-appropriate live/idle status. Prunes evicted
/// REST entries as a side effect (read is the natural sweep point).
pub fn snapshot(registry: &Registry) -> ConsumersResponse {
    let now = now_ms();
    let stale = rest_stale_ms();
    let evict = rest_evict_ms();
    let falcon_liveness = falcon::falcon_liveness_timeout_ms();
    let mut consumers = Vec::new();

    // REST: drop consumers gone past the eviction window, then emit the rest.
    {
        let mut polls = REST_POLLS.lock().expect("consumer rest-polls poisoned");
        polls.retain(|_, &mut last| now.saturating_sub(last) < evict);
        for ((job_type, worker), &last) in polls.iter() {
            let age = now.saturating_sub(last);
            consumers.push(Consumer {
                job_type: job_type.clone(),
                worker: worker.clone(),
                transport: "rest",
                last_seen_ms: last,
                age_ms: age,
                status: if age < stale { "live" } else { "idle" },
            });
        }
    }

    // Falcon: one row per (connection, job type) subscription. The reaper evicts
    // connections past `falcon_liveness`, so these are effectively always live;
    // the status is still computed for the brief window before a reap.
    for c in registry.consumers() {
        let age = now.saturating_sub(c.last_seen_ms);
        consumers.push(Consumer {
            job_type: c.job_type,
            worker: c.worker,
            transport: "falcon",
            last_seen_ms: c.last_seen_ms,
            age_ms: age,
            status: if age < falcon_liveness {
                "live"
            } else {
                "idle"
            },
        });
    }

    consumers.sort_by(|a, b| {
        a.job_type
            .cmp(&b.job_type)
            .then_with(|| a.worker.cmp(&b.worker))
            .then_with(|| a.transport.cmp(b.transport))
    });

    ConsumersResponse {
        now_ms: now,
        rest_stale_ms: stale,
        falcon_liveness_ms: falcon_liveness,
        consumers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each test owns a private key space (unique job type) and clears it after,
    /// so the process-global `REST_POLLS` can't leak between parallel tests.
    fn clear(job_type_prefix: &str) {
        REST_POLLS
            .lock()
            .unwrap()
            .retain(|(jt, _), _| !jt.starts_with(job_type_prefix));
    }

    fn rest_rows(resp: &ConsumersResponse, jt_prefix: &str) -> Vec<Consumer> {
        resp.consumers
            .iter()
            .filter(|c| c.transport == "rest" && c.job_type.starts_with(jt_prefix))
            .cloned()
            .collect()
    }

    #[test]
    fn records_a_rest_poll_as_live() {
        let jt = "t-live:review";
        clear("t-live:");
        record_rest_poll(jt, "agent-a");
        let reg = falcon::Registry::new();
        let resp = snapshot(&reg);
        let rows = rest_rows(&resp, "t-live:");
        assert_eq!(rows.len(), 1, "one REST consumer recorded");
        assert_eq!(rows[0].worker, "agent-a");
        assert_eq!(rows[0].transport, "rest");
        assert_eq!(rows[0].status, "live");
        clear("t-live:");
    }

    #[test]
    fn stale_rest_poll_greys_to_idle_but_is_retained() {
        let jt = "t-stale:review";
        clear("t-stale:");
        let stale = rest_stale_ms();
        // Backdate the poll past the stale window but within the evict window.
        let aged = now_ms() - (stale + 1_000);
        REST_POLLS
            .lock()
            .unwrap()
            .insert((jt.to_string(), "agent-b".to_string()), aged);
        let reg = falcon::Registry::new();
        let resp = snapshot(&reg);
        let rows = rest_rows(&resp, "t-stale:");
        assert_eq!(rows.len(), 1, "stale consumer still listed");
        assert_eq!(rows[0].status, "idle", "past stale window ⇒ idle");
        clear("t-stale:");
    }

    #[test]
    fn evicted_rest_poll_disappears() {
        let jt = "t-evict:review";
        clear("t-evict:");
        let evict = rest_evict_ms();
        let gone = now_ms() - (evict + 1_000);
        REST_POLLS
            .lock()
            .unwrap()
            .insert((jt.to_string(), "agent-c".to_string()), gone);
        let reg = falcon::Registry::new();
        let resp = snapshot(&reg);
        let rows = rest_rows(&resp, "t-evict:");
        assert!(rows.is_empty(), "consumer past evict window is dropped");
        // And the prune actually removed it from the backing map.
        assert!(
            !REST_POLLS
                .lock()
                .unwrap()
                .contains_key(&(jt.to_string(), "agent-c".to_string())),
            "evicted key pruned from REST_POLLS"
        );
    }

    #[test]
    fn re_polling_refreshes_last_seen() {
        let jt = "t-refresh:review";
        clear("t-refresh:");
        let aged = now_ms() - 30_000;
        REST_POLLS
            .lock()
            .unwrap()
            .insert((jt.to_string(), "agent-d".to_string()), aged);
        // A fresh poll should move it back to live.
        record_rest_poll(jt, "agent-d");
        let reg = falcon::Registry::new();
        let rows = rest_rows(&snapshot(&reg), "t-refresh:");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "live", "re-poll refreshes to live");
        clear("t-refresh:");
    }

    #[test]
    fn response_reports_the_active_windows() {
        let reg = falcon::Registry::new();
        let resp = snapshot(&reg);
        assert_eq!(resp.rest_stale_ms, rest_stale_ms());
        assert_eq!(
            resp.falcon_liveness_ms,
            falcon::falcon_liveness_timeout_ms()
        );
        assert!(resp.now_ms > 0);
    }
}
