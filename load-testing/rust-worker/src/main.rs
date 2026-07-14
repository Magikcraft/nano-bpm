//! worker-unit — SDK-based job worker for the over-provisioning / node-scaling harness.
//!
//! Rewritten on top of the published `camunda-orchestration-sdk` crate so the worker
//! side exercises the SAME client stack production uses — the SDK's transport
//! selection (Falcon command stream vs REST long-poll), subscription handshake,
//! heartbeat-liveness, and adaptive job activation — instead of a hand-rolled raw
//! WebSocket. Each "client" is one managed [`JobWorker`] with its own subscription,
//! so `CLIENTS` workers reproduce `CLIENTS` independent command-stream connections.
//!
//! Honors the same file-barrier protocol as the TS unit so `scale-nodes.ts` can spawn
//! it interchangeably:
//!   1. open CLIENTS worker subscriptions to `test-job` (each with a MAXPAR window),
//!   2. touch READY_FILE once every worker is actually subscribed — surfaced via the
//!      SDK's `on_ready` callback, so the producer gate still waits on real
//!      subscriptions (dispatch is edge-triggered),
//!   3. record completion count + raw e2e latency for window jobs (vars.w === 1),
//!   4. on STOP_FILE, write {unit,clients,completed,w1Completed,latencies[]} to
//!      STATS_FILE and exit.
//!
//! Transport is auto-selected by the SDK from the gateway's advertised capabilities;
//! `TRANSPORT=stream` (default) prefers the Falcon command stream, `TRANSPORT=rest`
//! forces REST long-poll.
//!
//! Env: CLIENTS, BASE_URL, MAXPAR, READY_FILE, STOP_FILE, STATS_FILE, UNIT_ID, TRANSPORT

use std::fs;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use camunda_orchestration_sdk::{CamundaClient, CamundaOptions, JobAction, JobWorkerConfig};
use serde_json::{json, Value};
use tokio::sync::mpsc;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let clients: usize = env_or("CLIENTS", "8").parse().unwrap_or(8);
    let base_url = env_or("BASE_URL", "http://localhost:8090");
    let maxpar: i32 = env_or("MAXPAR", "4").parse().unwrap_or(4);
    let ready_file = env_or("READY_FILE", "");
    let stop_file = env_or("STOP_FILE", "");
    let stats_file = env_or("STATS_FILE", "");
    let unit_id = env_or("UNIT_ID", "0");

    // Select the SDK transport BEFORE constructing the client: the command-stream
    // toggle is read from the process environment when the client first probes the
    // gateway. `stream` (default) → command stream if advertised; `rest` → force REST.
    let transport = env_or("TRANSPORT", "stream").to_lowercase();
    std::env::set_var("CAMUNDA_FALCON", if transport == "rest" { "0" } else { "1" });

    // Workers each open their own subscription socket regardless of client, so a
    // single shared client is fine for the worker side (mirrors loadgen).
    let client = CamundaClient::new(
        CamundaOptions::new()
            .with("CAMUNDA_REST_ADDRESS", base_url.clone())
            .with("CAMUNDA_AUTH_STRATEGY", "NONE"),
    )
    .expect("build camunda client");

    let completed = Arc::new(AtomicU64::new(0));
    let w1_completed = Arc::new(AtomicU64::new(0));
    let subscribed = Arc::new(AtomicUsize::new(0));

    // Latency samples flow over an mpsc channel rather than a shared Mutex: a blocked
    // std-Mutex waiter on the completion hot path can park a whole tokio worker thread
    // under load. A collector task drains into a Vec, yielded once all senders drop.
    let (lat_tx, mut lat_rx) = mpsc::unbounded_channel::<i64>();
    let lat_collector = tokio::spawn(async move {
        let mut v: Vec<i64> = Vec::new();
        while let Some(x) = lat_rx.recv().await {
            v.push(x);
        }
        v
    });

    // One managed worker per client, each opening its own command-stream subscription
    // (or REST long-poll) with a `maxpar` in-flight window. `on_ready` fires once when
    // the worker's subscription actually lands, so READY reflects real subscriptions.
    let mut handles = Vec::with_capacity(clients);
    for i in 0..clients {
        let subscribed = subscribed.clone();
        let cfg = JobWorkerConfig::new("test-job")
            .max_jobs_to_activate(maxpar)
            .worker_name(format!("u{unit_id}-w{i}"))
            .fetch_variables(["t0", "w"])
            .on_ready(move || {
                subscribed.fetch_add(1, Ordering::Relaxed);
            });
        let worker = client.create_job_worker(cfg);

        let completed = completed.clone();
        let w1_completed = w1_completed.clone();
        let lat_tx = lat_tx.clone();
        let handle = worker.spawn(move |job| {
            let completed = completed.clone();
            let w1_completed = w1_completed.clone();
            let lat_tx = lat_tx.clone();
            async move {
                completed.fetch_add(1, Ordering::Relaxed);
                if job.variables().get("w").and_then(Value::as_i64) == Some(1) {
                    if let Some(t0) = job.variables().get("t0").and_then(Value::as_f64) {
                        w1_completed.fetch_add(1, Ordering::Relaxed);
                        let _ = lat_tx.send((now_ms() as f64 - t0) as i64);
                    }
                }
                JobAction::complete_with(json!({ "done": true }))
            }
        });
        handles.push(handle);
    }
    // Drop our own sender clone so only the handler closures keep the channel open.
    drop(lat_tx);

    // Wait for every subscription to land before signalling READY (dispatch is
    // edge-triggered: workers must be subscribed before the producer creates).
    let ready_deadline = now_ms() + 60_000;
    while subscribed.load(Ordering::Relaxed) < clients && now_ms() < ready_deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if !ready_file.is_empty() {
        let _ = fs::write(&ready_file, "1");
    }
    eprintln!(
        "[unit {unit_id}] READY transport=sdk-{transport} clients={clients} subscribed={}",
        subscribed.load(Ordering::Relaxed)
    );

    // Run until the coordinator drops STOP_FILE.
    loop {
        if !stop_file.is_empty() && std::path::Path::new(&stop_file).exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Graceful shutdown: lets each worker drain in-flight jobs, then drops its handler
    // closure (and lat_tx clone), closing the channel so the collector yields.
    for h in handles {
        let _ = h.shutdown().await;
    }
    let latencies = lat_collector.await.unwrap_or_default();

    let completed = completed.load(Ordering::Relaxed);
    let w1 = w1_completed.load(Ordering::Relaxed);
    if !stats_file.is_empty() {
        let out = json!({
            "unit": unit_id,
            "clients": clients,
            "completed": completed,
            "w1Completed": w1,
            "latencies": latencies,
        });
        let _ = fs::write(&stats_file, out.to_string());
    }
    eprintln!("[unit {unit_id}] done completed={completed} w1={w1}");
}
