//! diffgen — differential two-lane driver for the ADR-0020 Tier-2 admission
//! compressor (per-process-definition backlog isolation).
//!
//! It drives TWO process definitions that SHARE a job type but have different
//! service topologies, to prove the Tier-2 compressor throttles only the *sick*
//! definition and leaves its healthy sibling fully admitted:
//!
//!   FAST lane  (process `orders-fast`): start → [common-job] → end.
//!   SLOW lane  (process `orders-slow`): start → [common-job] → [slow-job] → end.
//!
//! Both lanes emit `common-job`, served by a shared, well-provisioned worker
//! pool (COMMON_WORKERS, fast). Only the SLOW lane emits `slow-job`, served by a
//! deliberately under-provisioned / delayed pool (SLOW_WORKERS few, SLOW_DELAY_MS
//! each). SLOW instances therefore pile up in-flight while FAST instances drain
//! promptly, so ONLY `orders-slow`'s per-definition backlog L_P climbs. Tier-2
//! must raise `nanobpm_tier2_pressure{proc=orders-slow}` and shed its creates
//! while `orders-fast` stays at zero pressure and full admission.
//!
//! Each lane runs its OWN producers (each with its own `CamundaClient` → own
//! Falcon socket + credit window, so one lane can't head-of-line-block the other)
//! with an independent RATE / PROD_CONNS / MAX_INFLIGHT. Per lane we report
//! producedRate (attempts), acceptedRate (Ok creates), shedRate (rejected creates
//! — the Tier-2 shed signal), tput (completions) and e2e p50/p99, recorded on
//! each lane's TERMINAL task (common-job for FAST, slow-job for SLOW).
//!
//! Env:
//!   BASE_URL
//!   FAST_PDK, FAST_RATE, FAST_CONNS, FAST_MAX_INFLIGHT
//!   SLOW_PDK, SLOW_RATE, SLOW_CONNS, SLOW_MAX_INFLIGHT
//!   COMMON_WORKERS, COMMON_MAXPAR      (shared healthy pool for `common-job`)
//!   SLOW_WORKERS,  SLOW_MAXPAR, SLOW_DELAY_MS  (starved pool for `slow-job`)
//!   WARMUP_S, DURATION_S, DRAIN_S, TRANSPORT, PROGRESS

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

/// Per-lane counters. `accepted` (create returned Ok) vs `shed` (create rejected
/// — the Tier-2 admission-shed signal) are tracked separately so the soak can
/// assert the compressor sheds the sick lane and not the healthy one.
struct Lane {
    produced: AtomicU64,
    accepted: AtomicU64,
    shed: AtomicU64,
    completed: AtomicU64,
}

impl Lane {
    fn new() -> Self {
        Lane {
            produced: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            shed: AtomicU64::new(0),
            completed: AtomicU64::new(0),
        }
    }
}

struct Shared {
    fast: Lane,
    slow: Lane,
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

    let fast_pdk = env_or("FAST_PDK", "");
    let slow_pdk = env_or("SLOW_PDK", "");
    if fast_pdk.is_empty() || slow_pdk.is_empty() {
        eprintln!("FAST_PDK and SLOW_PDK (processDefinitionKey) envs are required");
        std::process::exit(2);
    }

    let fast_rate: f64 = env_or("FAST_RATE", "2000").parse().unwrap_or(2000.0);
    let slow_rate: f64 = env_or("SLOW_RATE", "2000").parse().unwrap_or(2000.0);
    let fast_conns: usize = env_or("FAST_CONNS", "32").parse().unwrap_or(32);
    let slow_conns: usize = env_or("SLOW_CONNS", "32").parse().unwrap_or(32);
    let fast_max_inflight: u64 = env_or("FAST_MAX_INFLIGHT", "0").parse().unwrap_or(0);
    let slow_max_inflight: u64 = env_or("SLOW_MAX_INFLIGHT", "0").parse().unwrap_or(0);

    // Shared healthy pool for `common-job` (keeps up for both lanes).
    let common_workers: usize = env_or("COMMON_WORKERS", "128").parse().unwrap_or(128);
    let common_maxpar: i64 = env_or("COMMON_MAXPAR", "8").parse().unwrap_or(8);
    // Starved pool for `slow-job` (few workers + per-job delay → SLOW backs up).
    let slow_workers: usize = env_or("SLOW_WORKERS", "4").parse().unwrap_or(4);
    let slow_maxpar: i64 = env_or("SLOW_MAXPAR", "1").parse().unwrap_or(1);
    let slow_delay_ms: u64 = env_or("SLOW_DELAY_MS", "250").parse().unwrap_or(250);

    let warmup_s: f64 = env_or("WARMUP_S", "5").parse().unwrap_or(5.0);
    let duration_s: f64 = env_or("DURATION_S", "120").parse().unwrap_or(120.0);
    let drain_s: f64 = env_or("DRAIN_S", "3").parse().unwrap_or(3.0);

    let transport = env_or("TRANSPORT", "stream").to_lowercase();
    let rest = transport == "rest";
    std::env::set_var("CAMUNDA_FALCON", if rest { "0" } else { "1" });

    let new_client = || {
        CamundaClient::new(
            CamundaOptions::new()
                .with("CAMUNDA_REST_ADDRESS", base_url.clone())
                .with("CAMUNDA_AUTH_STRATEGY", "NONE"),
        )
        .expect("build camunda client")
    };

    let shared = Arc::new(Shared {
        fast: Lane::new(),
        slow: Lane::new(),
        measuring: AtomicBool::new(false),
    });
    let (stop_tx, stop_rx) = watch::channel(false);

    // Per-lane latency channels (e2e ms), collected into Vecs once senders drop.
    let (fast_lat_tx, mut fast_lat_rx) = mpsc::unbounded_channel::<i64>();
    let (slow_lat_tx, mut slow_lat_rx) = mpsc::unbounded_channel::<i64>();
    let fast_collector = tokio::spawn(async move {
        let mut v = Vec::new();
        while let Some(x) = fast_lat_rx.recv().await {
            v.push(x);
        }
        v
    });
    let slow_collector = tokio::spawn(async move {
        let mut v = Vec::new();
        while let Some(x) = slow_lat_rx.recv().await {
            v.push(x);
        }
        v
    });

    // ── Workers ──────────────────────────────────────────────
    // `common-job` is the TERMINAL task of the FAST lane but a MID task of the
    // SLOW lane; record e2e only when lane=="fast". Each worker opens its own
    // subscription socket so one shared client is fine.
    let worker_client = new_client();
    let mut worker_handles = Vec::new();

    for i in 0..common_workers {
        let cfg = JobWorkerConfig::new("common-job")
            .max_jobs_to_activate(common_maxpar as i32)
            .worker_name(format!("common{i}"))
            .fetch_variables(["t0", "lane"]);
        let worker = worker_client.create_job_worker(cfg);
        let shared = shared.clone();
        let fast_lat_tx = fast_lat_tx.clone();
        let handle = worker.spawn(move |job| {
            let shared = shared.clone();
            let fast_lat_tx = fast_lat_tx.clone();
            async move {
                let is_fast = job
                    .variables()
                    .get("lane")
                    .and_then(Value::as_str)
                    .map(|l| l == "fast")
                    .unwrap_or(false);
                if is_fast {
                    // FAST terminal task → this completion finishes a FAST instance.
                    shared.fast.completed.fetch_add(1, Ordering::Relaxed);
                    if shared.measuring.load(Ordering::Relaxed) {
                        if let Some(t0) = job.variables().get("t0").and_then(Value::as_f64) {
                            let _ = fast_lat_tx.send((now_ms() as f64 - t0) as i64);
                        }
                    }
                }
                JobAction::complete_with(json!({ "common": true }))
            }
        });
        worker_handles.push(handle);
    }

    // `slow-job` is the TERMINAL task of the SLOW lane, deliberately starved.
    for i in 0..slow_workers {
        let cfg = JobWorkerConfig::new("slow-job")
            .max_jobs_to_activate(slow_maxpar as i32)
            .worker_name(format!("slow{i}"))
            .fetch_variables(["t0", "lane"]);
        let worker = worker_client.create_job_worker(cfg);
        let shared = shared.clone();
        let slow_lat_tx = slow_lat_tx.clone();
        let handle = worker.spawn(move |job| {
            let shared = shared.clone();
            let slow_lat_tx = slow_lat_tx.clone();
            async move {
                if slow_delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(slow_delay_ms)).await;
                }
                shared.slow.completed.fetch_add(1, Ordering::Relaxed);
                if shared.measuring.load(Ordering::Relaxed) {
                    if let Some(t0) = job.variables().get("t0").and_then(Value::as_f64) {
                        let _ = slow_lat_tx.send((now_ms() as f64 - t0) as i64);
                    }
                }
                JobAction::complete_with(json!({ "slow": true }))
            }
        });
        worker_handles.push(handle);
    }
    drop(fast_lat_tx);
    drop(slow_lat_tx);

    let ready_pause = ((common_workers + slow_workers) as f64 / 200.0).clamp(1.0, 5.0);
    tokio::time::sleep(Duration::from_secs_f64(ready_pause)).await;
    eprintln!(
        "[diffgen] workers spawned: common={common_workers} slow={slow_workers} (transport={transport})"
    );

    // ── Producers (per lane) ─────────────────────────────────
    let mut prod_handles = Vec::new();
    for lane in ["fast", "slow"] {
        let (pdk, rate, conns, max_inflight) = if lane == "fast" {
            (&fast_pdk, fast_rate, fast_conns, fast_max_inflight)
        } else {
            (&slow_pdk, slow_rate, slow_conns, slow_max_inflight)
        };
        let per_rate = if rate > 0.0 {
            rate / conns as f64
        } else {
            rate
        };
        for _ in 0..conns {
            let client = new_client();
            let pdk = pdk.clone();
            let shared = shared.clone();
            let stop_rx = stop_rx.clone();
            let lane = lane.to_string();
            prod_handles.push(tokio::spawn(async move {
                run_producer(client, lane, pdk, per_rate, max_inflight, shared, stop_rx).await;
            }));
        }
    }

    // ── Optional per-second progress ticker ──────────────────
    if std::env::var("PROGRESS").ok().as_deref() == Some("1") {
        let shared = shared.clone();
        let mut stop_rx = stop_rx.clone();
        tokio::spawn(async move {
            let t0 = Instant::now();
            let snap = |l: &Lane| {
                (
                    l.produced.load(Ordering::Relaxed),
                    l.accepted.load(Ordering::Relaxed),
                    l.shed.load(Ordering::Relaxed),
                    l.completed.load(Ordering::Relaxed),
                )
            };
            let mut last_f = snap(&shared.fast);
            let mut last_s = snap(&shared.slow);
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = stop_rx.changed() => break,
                }
                let f = snap(&shared.fast);
                let s = snap(&shared.slow);
                let fin = shared.fast.produced.load(Ordering::Relaxed)
                    - shared.fast.completed.load(Ordering::Relaxed);
                let sin = shared.slow.produced.load(Ordering::Relaxed)
                    - shared.slow.completed.load(Ordering::Relaxed);
                eprintln!(
                    "PROGRESS t={:.1} \
                     fast[acc={} shed={} cmp={} inflight={}] \
                     slow[acc={} shed={} cmp={} inflight={}]",
                    t0.elapsed().as_secs_f64(),
                    f.1.saturating_sub(last_f.1),
                    f.2.saturating_sub(last_f.2),
                    f.3.saturating_sub(last_f.3),
                    fin,
                    s.1.saturating_sub(last_s.1),
                    s.2.saturating_sub(last_s.2),
                    s.3.saturating_sub(last_s.3),
                    sin,
                );
                last_f = f;
                last_s = s;
            }
        });
    }

    // ── Measurement window ───────────────────────────────────
    tokio::time::sleep(Duration::from_secs_f64(warmup_s)).await;
    let base = |l: &Lane| {
        (
            l.produced.load(Ordering::Relaxed),
            l.accepted.load(Ordering::Relaxed),
            l.shed.load(Ordering::Relaxed),
            l.completed.load(Ordering::Relaxed),
        )
    };
    let f0 = base(&shared.fast);
    let s0 = base(&shared.slow);
    shared.measuring.store(true, Ordering::Relaxed);
    let t_measure = Instant::now();
    tokio::time::sleep(Duration::from_secs_f64(duration_s)).await;
    let elapsed = t_measure.elapsed().as_secs_f64();
    let f1 = base(&shared.fast);
    let s1 = base(&shared.slow);
    shared.measuring.store(false, Ordering::Relaxed);

    // ── Stop + drain ─────────────────────────────────────────
    let _ = stop_tx.send(true);
    tokio::time::sleep(Duration::from_secs_f64(drain_s)).await;
    for h in prod_handles {
        let _ = h.await;
    }
    for h in worker_handles {
        let _ = h.shutdown().await;
    }
    let fast_lat = fast_collector.await.unwrap_or_default();
    let slow_lat = slow_collector.await.unwrap_or_default();

    // ── Report ───────────────────────────────────────────────
    report("fast", f0, f1, elapsed, fast_lat);
    report("slow", s0, s1, elapsed, slow_lat);
}

fn report(
    lane: &str,
    a: (u64, u64, u64, u64),
    b: (u64, u64, u64, u64),
    elapsed: f64,
    mut lat: Vec<i64>,
) {
    let produced_rate = b.0.saturating_sub(a.0) as f64 / elapsed;
    let accepted_rate = b.1.saturating_sub(a.1) as f64 / elapsed;
    let shed_rate = b.2.saturating_sub(a.2) as f64 / elapsed;
    let tput = b.3.saturating_sub(a.3) as f64 / elapsed;
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
    let (p50, p90, p99) = (pct(0.50), pct(0.90), pct(0.99));
    let max = lat.last().copied().unwrap_or(0);
    println!(
        "RESULT lane={lane} producedRate={produced_rate:.0} acceptedRate={accepted_rate:.0} shedRate={shed_rate:.0} tput={tput:.0} n={} mean={mean:.0} p50={p50} p90={p90} p99={p99} max={max}",
        lat.len()
    );
    eprintln!(
        "[diffgen] lane={lane} producedRate={produced_rate:.0}/s acceptedRate={accepted_rate:.0}/s shedRate={shed_rate:.0}/s tput={tput:.0}/s e2e p50={p50}ms p99={p99}ms max={max}ms (n={})",
        lat.len()
    );
}

/// One producer for a lane: create instances (await=false) by-key through the
/// SDK, gated by an optional full-cycle in-flight cap and token bucket. A create
/// that returns Ok counts `accepted`; a rejected create counts `shed` (the
/// Tier-2 admission-shed signal for this lane's definition).
#[allow(clippy::too_many_arguments)]
async fn run_producer(
    client: CamundaClient,
    lane: String,
    pdk: String,
    rate: f64,
    max_inflight: u64,
    shared: Arc<Shared>,
    mut stop_rx: watch::Receiver<bool>,
) {
    let is_fast = lane == "fast";
    fn lane_ref(s: &Shared, is_fast: bool) -> &Lane {
        if is_fast {
            &s.fast
        } else {
            &s.slow
        }
    }
    let mut bucket = (0.0f64, Instant::now());

    loop {
        if *stop_rx.borrow() {
            break;
        }
        if max_inflight > 0 {
            loop {
                if *stop_rx.borrow() {
                    return;
                }
                let outstanding = lane_ref(&shared, is_fast)
                    .produced
                    .load(Ordering::Relaxed)
                    .saturating_sub(lane_ref(&shared, is_fast).completed.load(Ordering::Relaxed));
                if outstanding < max_inflight {
                    break;
                }
                tokio::select! {
                    _ = stop_rx.changed() => return,
                    _ = tokio::time::sleep(Duration::from_micros(200)) => {},
                }
            }
        }
        if rate > 0.0 {
            let got = {
                let b = &mut bucket;
                let now = Instant::now();
                let dt = now.duration_since(b.1).as_secs_f64();
                b.1 = now;
                b.0 = (b.0 + rate * dt).min(rate);
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

        let mut vars: HashMap<String, Value> = HashMap::with_capacity(2);
        vars.insert("t0".to_string(), json!(now_ms()));
        vars.insert("lane".to_string(), Value::String(lane.clone()));
        let instruction =
            ProcessInstanceCreationInstruction::ProcessInstanceCreationInstructionByKey(Box::new(
                ProcessInstanceCreationInstructionByKey {
                    process_definition_key: Box::new(ProcessDefinitionKey::assume_exists(
                        pdk.clone(),
                    )),
                    variables: Some(vars),
                    await_completion: Some(false),
                    ..Default::default()
                },
            ));

        // Count the create attempt; classify the result as accepted or shed.
        lane_ref(&shared, is_fast)
            .produced
            .fetch_add(1, Ordering::Relaxed);
        tokio::select! {
            _ = stop_rx.changed() => break,
            r = client.create_process_instance(instruction) => {
                match r {
                    Ok(_) => { lane_ref(&shared, is_fast).accepted.fetch_add(1, Ordering::Relaxed); }
                    Err(_) => {
                        // Rejected create — the dominant cause in this controlled
                        // scenario is the Tier-2 admission compressor shedding the
                        // sick definition. Brief backoff so a shed lane doesn't spin.
                        lane_ref(&shared, is_fast).shed.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_micros(500)).await;
                    }
                }
            }
        }
    }
}
