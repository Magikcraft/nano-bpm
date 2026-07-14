//! worker-unit — native Rust command-stream worker for the over-provisioning harness.
//!
//! Functional twin of `src/worker-unit.ts` (stream transport), but each "client" is
//! a real OS-thread-backed tokio task driving one WebSocket command-stream
//! subscription. Lets us measure the server's dispatch ceiling without the Node
//! event-loop confound on the client side.
//!
//! Honors the same file-barrier protocol as the TS unit so `scale-workers.ts` can
//! spawn it interchangeably:
//!   1. open CLIENTS command-stream subscriptions to `test-job`,
//!   2. touch READY_FILE once all are subscribed,
//!   3. record completion count + raw e2e latency for window jobs (vars.w === 1),
//!   4. on STOP_FILE, write {unit,clients,completed,w1Completed,latencies[]} to
//!      STATS_FILE and exit.
//!
//! Env: CLIENTS, BASE_URL, MAXPAR, READY_FILE, STOP_FILE, STATS_FILE, UNIT_ID

use std::fs;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Default)]
struct Metrics {
    completed: AtomicU64,
    w1_completed: AtomicU64,
    latencies: Mutex<Vec<i64>>,
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

/// Derive the command-stream WebSocket URL from the gateway base URL.
/// `http://host:port` -> `ws://host:port/command-stream` (https -> wss).
fn ws_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    let ws_base = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        // already ws:// / wss:// or bare host
        base.to_string()
    };
    format!("{ws_base}/command-stream")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let clients: usize = env_or("CLIENTS", "8").parse().unwrap_or(8);
    let base_url = env_or("BASE_URL", "http://localhost:8090");
    let maxpar: i64 = env_or("MAXPAR", "4").parse().unwrap_or(4);
    let ready_file = env_or("READY_FILE", "");
    let stop_file = env_or("STOP_FILE", "");
    let stats_file = env_or("STATS_FILE", "");
    let unit_id = env_or("UNIT_ID", "0");

    let url = ws_url(&base_url);
    let metrics = Arc::new(Metrics::default());
    let subscribed = Arc::new(AtomicUsize::new(0));
    let (stop_tx, stop_rx) = watch::channel(false);

    let mut handles = Vec::with_capacity(clients);
    for i in 0..clients {
        let url = url.clone();
        let unit_id = unit_id.clone();
        let metrics = metrics.clone();
        let subscribed = subscribed.clone();
        let stop_rx = stop_rx.clone();
        handles.push(tokio::spawn(async move {
            run_connection(url, maxpar, unit_id, i, metrics, subscribed, stop_rx).await;
        }));
    }

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
        "[unit {unit_id}] READY transport=rust-stream clients={clients} subscribed={}",
        subscribed.load(Ordering::Relaxed)
    );

    // Run until the coordinator drops STOP_FILE.
    loop {
        if !stop_file.is_empty() && std::path::Path::new(&stop_file).exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let _ = stop_tx.send(true);

    for h in handles {
        let _ = h.await;
    }

    let completed = metrics.completed.load(Ordering::Relaxed);
    let w1 = metrics.w1_completed.load(Ordering::Relaxed);
    let latencies = metrics.latencies.lock().unwrap().clone();
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

#[allow(clippy::too_many_arguments)]
async fn run_connection(
    url: String,
    maxpar: i64,
    unit_id: String,
    idx: usize,
    metrics: Arc<Metrics>,
    subscribed: Arc<AtomicUsize>,
    mut stop_rx: watch::Receiver<bool>,
) {
    // The coordinator starts the server before spawning units, but retry briefly
    // to absorb any startup race.
    let mut ws = None;
    for attempt in 0..50 {
        match connect_async(&url).await {
            Ok((stream, _)) => {
                ws = Some(stream);
                break;
            }
            Err(e) => {
                if attempt == 49 {
                    eprintln!("[unit {unit_id} c{idx}] connect failed: {e}");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    let (mut write, mut read) = ws.unwrap().split();

    let worker_name = format!("u{unit_id}-w{idx}");
    let subscribe = json!({
        "type": "subscribe",
        "jobType": "test-job",
        "jobCredits": maxpar,
        "timeout": 30_000,
        "worker": worker_name,
        "fetchVariable": ["t0", "w"],
    });
    if write
        .send(Message::Text(subscribe.to_string()))
        .await
        .is_err()
    {
        eprintln!("[unit {unit_id} c{idx}] subscribe send failed");
        return;
    }
    subscribed.fetch_add(1, Ordering::Relaxed);

    let mut corr: u64 = 0;
    loop {
        tokio::select! {
            _ = stop_rx.changed() => break,
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if let Err(()) = handle_text(&text, &mut write, &metrics, &mut corr).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = write.send(Message::Pong(p)).await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }
}

/// Process one server text frame. Returns Err(()) if the connection should close
/// (a send failure).
async fn handle_text<S>(
    text: &str,
    write: &mut S,
    metrics: &Metrics,
    corr: &mut u64,
) -> Result<(), ()>
where
    S: SinkExt<Message> + Unpin,
{
    let v: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return Ok(()),
    };
    if v.get("type").and_then(Value::as_str) != Some("job") {
        // welcome / heartbeat / submissionCredits / pressure / commandResult: ignored.
        return Ok(());
    }
    let job = &v["job"];
    let job_key = job.get("jobKey").and_then(Value::as_str).unwrap_or("");
    let vars = &job["variables"];

    metrics.completed.fetch_add(1, Ordering::Relaxed);
    if vars.get("w").and_then(Value::as_i64) == Some(1) {
        if let Some(t0) = vars.get("t0").and_then(Value::as_f64) {
            metrics.w1_completed.fetch_add(1, Ordering::Relaxed);
            metrics
                .latencies
                .lock()
                .unwrap()
                .push((now_ms() as f64 - t0) as i64);
        }
    }

    *corr += 1;
    let complete = json!({
        "type": "completeJob",
        "corr": *corr,
        "jobKey": job_key,
        "variables": { "done": true },
    });
    if write
        .send(Message::Text(complete.to_string()))
        .await
        .is_err()
    {
        return Err(());
    }
    // Replenish one job credit so demand stays at maxParallelJobs.
    let credits = json!({ "type": "jobCredits", "jobType": "test-job", "n": 1 });
    if write.send(Message::Text(credits.to_string())).await.is_err() {
        return Err(());
    }
    Ok(())
}
