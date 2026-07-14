//! loadgen — SDK-based saturation driver for the nanobpmn gateway.
//!
//! Rewritten on top of the published `camunda-orchestration-sdk` crate. Producers
//! call [`CamundaClient::create_process_instance`] and workers use
//! [`CamundaClient::create_job_worker`]; the SDK transparently routes over the
//! Falcon command stream when the gateway advertises it (the `nano` field on
//! `/v2/topology`) or REST long-poll otherwise. The transport is selected here
//! from `TRANSPORT`:
//!   - `stream` (default) → `CAMUNDA_FALCON=1` (command stream if available),
//!   - `rest`             → `CAMUNDA_FALCON=0` (force REST).
//!
//! Runs PRODUCERS and WORKERS in a single process so the whole hot path —
//! instance creation AND job completion — is native Rust. Flow control is the
//! SDK's own: the Falcon producer self-paces on server-granted submission
//! credits, and each managed worker bounds in-flight jobs by `max_jobs_to_activate`.
//!
//! Measurement + env knobs are unchanged from the raw-socket loadgen, and the
//! `RESULT ...` line is byte-compatible, so `soak.sh` drives it identically.
//!
//! Env: BASE_URL, PDK (processDefinitionKey), WORKERS, PROD_CONNS, MAXPAR,
//!      RATE (0 = unbounded/credit-gated), WARMUP_S, DURATION_S, DRAIN_S,
//!      MAX_INFLIGHT (0 = uncapped), VAR_BYTES, VAR_MODE, TRANSPORT, PROGRESS.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use camunda_orchestration_sdk::models::{
    ProcessDefinitionKey, ProcessInstanceCreationInstruction,
    ProcessInstanceCreationInstructionByKey,
};
use camunda_orchestration_sdk::{CamundaClient, CamundaOptions, JobAction, JobWorkerConfig};
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};

struct Shared {
    produced: AtomicU64,
    completed: AtomicU64,
    measuring: AtomicBool,
}

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
    let base_url = env_or("BASE_URL", "http://localhost:8090");
    let pdk = env_or("PDK", "");
    let workers: usize = env_or("WORKERS", "256").parse().unwrap_or(256);
    let prod_conns: usize = env_or("PROD_CONNS", "64").parse().unwrap_or(64);
    let maxpar: i64 = env_or("MAXPAR", "4").parse().unwrap_or(4);
    let rate: f64 = env_or("RATE", "0").parse().unwrap_or(0.0);
    let warmup_s: f64 = env_or("WARMUP_S", "2").parse().unwrap_or(2.0);
    let duration_s: f64 = env_or("DURATION_S", "12").parse().unwrap_or(12.0);
    let drain_s: f64 = env_or("DRAIN_S", "3").parse().unwrap_or(3.0);
    // Full-cycle in-flight cap (produced - completed). The collapse knob: a bounded
    // value lets the system self-regulate to a plateau; a huge value floods creates
    // past the drain rate (latency blow-up). 0 = uncapped.
    let max_inflight: u64 = env_or("MAX_INFLIGHT", "6000").parse().unwrap_or(6000);
    // Optional variable payload: pad each create's variables with a `p` filler
    // string of VAR_BYTES bytes (0 = negligible payload). Built once, shared by ref.
    let var_bytes: usize = env_or("VAR_BYTES", "0").parse().unwrap_or(0);
    let var_mode = env_or("VAR_MODE", "filler");
    let payload: Arc<String> = Arc::new(if var_bytes == 0 {
        String::new()
    } else if var_mode == "json" {
        // Honest, ~4-5x-compressible business-like payload (NOT the "x".repeat
        // mirage): a small token dictionary + incrementing integers padded to
        // VAR_BYTES, representative of real variable JSON on the journal.
        let words = [
            "order", "customer", "invoice", "amount", "status", "region", "product", "quantity",
            "timestamp", "reference", "approved", "pending", "shipped", "north", "south", "east",
            "west", "gold", "silver", "bronze",
        ];
        let mut s = String::with_capacity(var_bytes + 32);
        let mut i = 0usize;
        while s.len() < var_bytes {
            let w = words[i % words.len()];
            let w2 = words[(i / 3 + 5) % words.len()];
            s.push_str(w);
            s.push('-');
            s.push_str(w2);
            s.push(' ');
            s.push_str(&i.to_string());
            s.push(' ');
            i += 1;
        }
        s.truncate(var_bytes);
        s
    } else {
        "x".repeat(var_bytes)
    });

    if pdk.is_empty() {
        eprintln!("PDK (processDefinitionKey) env is required");
        std::process::exit(2);
    }

    let transport = env_or("TRANSPORT", "stream").to_lowercase();
    let rest = transport == "rest";

    // Select the SDK transport BEFORE constructing the client: the command-stream
    // toggle is read from the process environment when the client first probes the
    // gateway. Set once here, before any client / worker is created.
    std::env::set_var("CAMUNDA_FALCON", if rest { "0" } else { "1" });

    // A `CamundaClient` keeps ONE shared falcon create-producer socket (an
    // `OnceCell<Arc<FalconProducer>>`) with a single per-connection submission-credit
    // window. Cloning the client shares that one socket, so funnelling every producer
    // task through a single client head-of-line-blocks all creates behind one credit
    // gate — a hard global stall under load. The raw-socket loadgen this replaces
    // opened PROD_CONNS independent websockets; we reproduce that by giving each
    // producer its OWN client (hence its own falcon socket + credit window).
    let new_client = || {
        CamundaClient::new(
            CamundaOptions::new()
                .with("CAMUNDA_REST_ADDRESS", base_url.clone())
                .with("CAMUNDA_AUTH_STRATEGY", "NONE"),
        )
        .expect("build camunda client")
    };

    // Workers each open their own subscription socket regardless of client, so a
    // single shared client is fine for the worker side.
    let worker_client = new_client();

    let shared = Arc::new(Shared {
        produced: AtomicU64::new(0),
        completed: AtomicU64::new(0),
        measuring: AtomicBool::new(false),
    });
    let (stop_tx, stop_rx) = watch::channel(false);

    // Latency samples flow over an mpsc channel rather than a shared std::sync::Mutex:
    // a blocked std-Mutex waiter on the completion hot path can park a whole tokio
    // worker thread (including the I/O reactor) under CPU pressure. A collector task
    // drains the channel into a Vec and yields it once every sender is dropped.
    let (lat_tx, mut lat_rx) = mpsc::unbounded_channel::<i64>();
    let lat_collector = tokio::spawn(async move {
        let mut v: Vec<i64> = Vec::new();
        while let Some(x) = lat_rx.recv().await {
            v.push(x);
        }
        v
    });

    // ── Workers ──────────────────────────────────────────────
    // One managed worker per `workers`, each opening its own command-stream
    // subscription (or REST long-poll) with a `maxpar` in-flight window.
    let mut worker_handles = Vec::with_capacity(workers);
    for i in 0..workers {
        let cfg = JobWorkerConfig::new("test-job")
            .max_jobs_to_activate(maxpar as i32)
            .worker_name(format!("rw{i}"))
            .fetch_variables(["t0", "w"]);
        let worker = worker_client.create_job_worker(cfg);
        let shared = shared.clone();
        let lat_tx = lat_tx.clone();
        let handle = worker.spawn(move |job| {
            let shared = shared.clone();
            let lat_tx = lat_tx.clone();
            async move {
                shared.completed.fetch_add(1, Ordering::Relaxed);
                if shared.measuring.load(Ordering::Relaxed) {
                    if let Some(t0) = job.variables().get("t0").and_then(Value::as_f64) {
                        let _ = lat_tx.send((now_ms() as f64 - t0) as i64);
                    }
                }
                JobAction::complete_with(json!({ "done": true }))
            }
        });
        worker_handles.push(handle);
    }
    // Drop our own sender clone so only the workers' clones keep the channel open.
    drop(lat_tx);

    // The managed worker has no subscription-ready callback, so allow the
    // subscriptions a moment to land before offering load (scaled by worker count,
    // capped); any residual lag is absorbed by the warmup window below.
    let ready_pause = (workers as f64 / 200.0).clamp(1.0, 5.0);
    tokio::time::sleep(Duration::from_secs_f64(ready_pause)).await;
    eprintln!("[loadgen] {workers} workers spawned (transport={transport})");

    // ── Producers ────────────────────────────────────────────
    // Per-producer rate: split the global target across producers so each paces
    // with its OWN local token bucket (no shared lock on the hot path).
    let per_rate = if rate > 0.0 {
        rate / prod_conns as f64
    } else {
        rate
    };
    let mut prod_handles = Vec::with_capacity(prod_conns);
    for _ in 0..prod_conns {
        // Each producer gets its OWN client → its own falcon socket → its own
        // server-granted submission-credit window, so one metered connection can't
        // head-of-line-block the rest (the raw-socket loadgen's per-connection model).
        let client = new_client();
        let pdk = pdk.clone();
        let shared = shared.clone();
        let stop_rx = stop_rx.clone();
        let payload = payload.clone();
        prod_handles.push(tokio::spawn(async move {
            run_producer(client, pdk, per_rate, max_inflight, shared, stop_rx, payload).await;
        }));
    }

    // ── Optional per-second progress ticker (PROGRESS=1) ─────
    if std::env::var("PROGRESS").ok().as_deref() == Some("1") {
        let shared = shared.clone();
        let mut stop_rx = stop_rx.clone();
        tokio::spawn(async move {
            let t0 = Instant::now();
            let mut last_p = shared.produced.load(Ordering::Relaxed);
            let mut last_c = shared.completed.load(Ordering::Relaxed);
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = stop_rx.changed() => break,
                }
                let p = shared.produced.load(Ordering::Relaxed);
                let c = shared.completed.load(Ordering::Relaxed);
                eprintln!(
                    "PROGRESS t={:.1} produced={} completed={}",
                    t0.elapsed().as_secs_f64(),
                    p.saturating_sub(last_p),
                    c.saturating_sub(last_c),
                );
                last_p = p;
                last_c = c;
            }
        });
    }

    // ── Measurement window ───────────────────────────────────
    tokio::time::sleep(Duration::from_secs_f64(warmup_s)).await;
    let p0 = shared.produced.load(Ordering::Relaxed);
    let c0 = shared.completed.load(Ordering::Relaxed);
    shared.measuring.store(true, Ordering::Relaxed);
    let t_measure = Instant::now();
    tokio::time::sleep(Duration::from_secs_f64(duration_s)).await;
    let elapsed = t_measure.elapsed().as_secs_f64();
    let p1 = shared.produced.load(Ordering::Relaxed);
    let c1 = shared.completed.load(Ordering::Relaxed);
    shared.measuring.store(false, Ordering::Relaxed);

    // ── Stop + drain ─────────────────────────────────────────
    let _ = stop_tx.send(true);
    tokio::time::sleep(Duration::from_secs_f64(drain_s)).await;
    for h in prod_handles {
        let _ = h.await;
    }
    // Shut workers down gracefully: this drops each handler closure (and its
    // lat_tx clone), so the collector's channel closes and it yields the samples.
    for h in worker_handles {
        let _ = h.shutdown().await;
    }
    let mut lat = lat_collector.await.unwrap_or_default();

    // ── Report ───────────────────────────────────────────────
    let produced_rate = (p1.saturating_sub(p0)) as f64 / elapsed;
    let tput = (c1.saturating_sub(c0)) as f64 / elapsed;
    lat.sort_unstable();
    let pct = |q: f64| -> i64 {
        if lat.is_empty() {
            return 0;
        }
        let idx = ((lat.len() as f64 - 1.0) * q).round() as usize;
        lat[idx.min(lat.len() - 1)]
    };
    let mean = if lat.is_empty() {
        0.0
    } else {
        lat.iter().sum::<i64>() as f64 / lat.len() as f64
    };
    let p50 = pct(0.50);
    let p90 = pct(0.90);
    let p99 = pct(0.99);
    let max = lat.last().copied().unwrap_or(0);

    println!(
        "RESULT transport={} workers={workers} prodConns={prod_conns} maxpar={maxpar} rate={} maxInflight={max_inflight} producedRate={:.0} tput={:.0} n={} mean={:.0} p50={p50} p90={p90} p99={p99} max={max}",
        transport,
        if rate > 0.0 { rate as i64 } else { 0 },
        produced_rate,
        tput,
        lat.len(),
        mean,
    );
    eprintln!(
        "[loadgen] transport={} producedRate={:.0}/s tput={:.0}/s p50={p50}ms p90={p90}ms p99={p99}ms max={max}ms (n={})",
        transport,
        produced_rate,
        tput,
        lat.len()
    );
}

/// One producer: loop creating instances (await=false) through the SDK, gated by
/// the same full-cycle in-flight cap and optional token bucket as before. The SDK
/// picks the Falcon command stream (credit-metered, no client-side 503) or REST.
async fn run_producer(
    client: CamundaClient,
    pdk: String,
    rate: f64,
    max_inflight: u64,
    shared: Arc<Shared>,
    mut stop_rx: watch::Receiver<bool>,
    payload: Arc<String>,
) {
    // Per-producer local rate bucket (no shared lock): (tokens, last_refill).
    let mut bucket = (0.0f64, Instant::now());

    loop {
        if *stop_rx.borrow() {
            break;
        }
        // Full-cycle in-flight gate (produced - completed) — the collapse knob.
        if max_inflight > 0 {
            loop {
                if *stop_rx.borrow() {
                    return;
                }
                let outstanding = shared
                    .produced
                    .load(Ordering::Relaxed)
                    .saturating_sub(shared.completed.load(Ordering::Relaxed));
                if outstanding < max_inflight {
                    break;
                }
                tokio::select! {
                    _ = stop_rx.changed() => return,
                    _ = tokio::time::sleep(Duration::from_micros(200)) => {},
                }
            }
        }
        // Optional pacing: per-producer local token bucket.
        if rate > 0.0 {
            let got = {
                let b = &mut bucket;
                let now = Instant::now();
                let dt = now.duration_since(b.1).as_secs_f64();
                b.1 = now;
                b.0 = (b.0 + rate * dt).min(rate); // cap burst at 1s worth
                if b.0 >= 1.0 {
                    b.0 -= 1.0;
                    true
                } else {
                    false
                }
            };
            if !got {
                tokio::select! {
                    _ = stop_rx.changed() => break,
                    _ = tokio::time::sleep(Duration::from_micros(200)) => {},
                }
                continue;
            }
        }

        let mut vars: HashMap<String, Value> = HashMap::with_capacity(3);
        vars.insert("t0".to_string(), json!(now_ms()));
        vars.insert("w".to_string(), json!(1));
        if !payload.is_empty() {
            vars.insert("p".to_string(), Value::String(payload.as_str().to_string()));
        }
        let instruction = ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(
            Box::new(ProcessInstanceCreationInstructionByKey {
                process_definition_key: Box::new(ProcessDefinitionKey::assume_exists(pdk.clone())),
                variables: Some(vars),
                await_completion: Some(false),
                ..Default::default()
            }),
        );

        tokio::select! {
            _ = stop_rx.changed() => break,
            r = client.create_process_instance(instruction) => {
                match r {
                    Ok(_) => {
                        shared.produced.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        // Backpressure / transient error: brief backoff, do not count.
                        tokio::time::sleep(Duration::from_micros(500)).await;
                    }
                }
            }
        }
    }
}
