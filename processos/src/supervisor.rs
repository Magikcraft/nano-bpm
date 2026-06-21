//! Own-Nano supervisor.
//!
//! The external Nano ProcessOS analyses is the **client's** production engine — a
//! read-only target. ProcessOS cannot run its meta-workloads (the `pilotSelfOptimize`
//! loop + workers) there, so it needs its **own** engine. Rather than embed the
//! engine, ProcessOS can *start one*: spawn the Nano gateway binary as a child
//! process pointed at a ProcessOS-owned data dir, wait for it to serve, and deploy
//! the pilot process onto it.
//!
//! This keeps the one-way contract intact (ProcessOS drives its own engine over the
//! same public v2 REST / console API any client uses) and the engine stays a real,
//! separate process — not embedded, not special-cased.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

use crate::contracts::NanoClient;

/// The pilot process body, embedded so the supervisor can deploy it regardless of
/// the working directory the binary is launched from.
const PILOT_BPMN: &str = include_str!("../pilot/pilot-self-optimize.bpmn");

/// How the supervisor should start the own engine. Built from the environment; the
/// feature is opt-in (absent `PROCESSOS_SPAWN_NANO` ⇒ `None`, ProcessOS points
/// `PROCESSOS_NANO_URL` at an externally-managed engine as before).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnConfig {
    /// Path to the Nano gateway binary (must be a `--features console` build so the
    /// cockpit can read traces back over `/console/api`).
    pub bin: PathBuf,
    /// `PORT` for the spawned engine; `0` lets the OS pick a free port (the actual
    /// port is learned from the child's `LISTENING_PORT=` line).
    pub port: u16,
    /// `NANOBPMN_DATA_DIR` for the spawned engine — a ProcessOS-owned data dir,
    /// distinct from the client's. Deleting it never touches the client.
    pub data_dir: PathBuf,
    /// Enable trace capture (`NANOBPMN_TRACE_STIMULI=1`) so the cockpit can read the
    /// pilot instances' traces back. On by default.
    pub capture: bool,
}

impl SpawnConfig {
    /// Build from the environment, or `None` when spawning is disabled.
    pub fn from_env() -> Option<Self> {
        let enabled = std::env::var("PROCESSOS_SPAWN_NANO")
            .ok()
            .map(|v| truthy(&v))
            .unwrap_or(false);
        if !enabled {
            return None;
        }
        let bin = std::env::var("PROCESSOS_NANO_BIN")
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .or_else(default_gateway_bin)?;
        let port = std::env::var("PROCESSOS_NANO_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let data_dir = std::env::var("PROCESSOS_NANO_DATA_DIR")
            .ok()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(".processos-nano-data"));
        let capture = std::env::var("PROCESSOS_NANO_CAPTURE")
            .ok()
            .map(|v| truthy(&v))
            .unwrap_or(true);
        Some(Self { bin, port, data_dir, capture })
    }
}

/// A handle to the spawned own engine. Killing the child on shutdown prevents
/// orphaned gateways from lingering across ProcessOS restarts.
pub struct OwnNano {
    child: Child,
    /// `http://localhost:<actual-port>` — what ProcessOS points its own client at.
    pub base_url: String,
}

impl OwnNano {
    /// Spawn the gateway, wait for it to bind + serve, and deploy the pilot process.
    pub async fn spawn(cfg: &SpawnConfig) -> Result<Self, String> {
        if !cfg.bin.exists() {
            return Err(format!(
                "own-Nano binary not found at {} (set PROCESSOS_NANO_BIN)",
                cfg.bin.display()
            ));
        }
        let mut cmd = Command::new(&cfg.bin);
        cmd.env("PORT", cfg.port.to_string())
            .env("NANOBPMN_DATA_DIR", &cfg.data_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if cfg.capture {
            // Implies variable capture; lets the cockpit read pilot traces back.
            cmd.env("NANOBPMN_TRACE_STIMULI", "1");
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", cfg.bin.display()))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "child stdout not captured".to_string())?;

        // Learn the actual bound port from the gateway's `LISTENING_PORT=` line, then
        // KEEP DRAINING stdout for the child's lifetime. The console gateway prints a
        // startup banner + logs AFTER the port line; if we stop reading, the OS pipe
        // buffer fills and the child blocks on write before it ever calls serve().
        let (tx, rx) = tokio::sync::oneshot::channel::<u16>();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            let mut tx = Some(tx);
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(p) = parse_listening_port(&line) {
                    if let Some(sender) = tx.take() {
                        let _ = sender.send(p);
                    }
                }
                // Keep draining after the port is found so the child never blocks.
            }
        });

        let port = match tokio::time::timeout(Duration::from_secs(20), rx).await {
            Ok(Ok(p)) => p,
            _ => return Err("timed out waiting for own Nano to report LISTENING_PORT".into()),
        };
        let base_url = format!("http://localhost:{port}");

        // Wait until it actually serves before handing the URL back.
        let client = NanoClient::new(&base_url);
        wait_until_healthy(&client, Duration::from_secs(30)).await?;

        // Install the pilot process so the cockpit can create experiments on it.
        // Idempotent: a byte-identical redeploy is a no-op on the engine.
        client
            .deploy_bpmn("pilot-self-optimize.bpmn", PILOT_BPMN)
            .await
            .map_err(|e| format!("deploy pilot process: {e}"))?;

        Ok(Self { child, base_url })
    }

    /// Stop the spawned engine. Best-effort; logs and continues on error.
    pub async fn shutdown(mut self) {
        match self.child.start_kill() {
            Ok(()) => {
                let _ = self.child.wait().await;
                tracing::info!("own Nano engine stopped");
            }
            Err(e) => tracing::warn!(error = %e, "failed to stop own Nano engine"),
        }
    }
}

/// Poll `GET /v2/topology` until it succeeds or the timeout elapses.
async fn wait_until_healthy(client: &NanoClient, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if client.health_ok().await {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("own Nano did not become healthy in time".into());
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// Parse `LISTENING_PORT=<n>` from a gateway stdout line.
fn parse_listening_port(line: &str) -> Option<u16> {
    line.trim().strip_prefix("LISTENING_PORT=")?.trim().parse().ok()
}

/// Best-effort default for the gateway binary: a release `--features console` build
/// under a sibling `server/` crate, relative to the current dir or its parent.
fn default_gateway_bin() -> Option<PathBuf> {
    const REL: &str = "server/target/release/nanobpm-gateway-rest-server";
    for root in [PathBuf::from("."), PathBuf::from("..")] {
        let p = root.join(REL);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

fn truthy(v: &str) -> bool {
    matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_listening_port_line() {
        assert_eq!(parse_listening_port("LISTENING_PORT=8081"), Some(8081));
        assert_eq!(parse_listening_port("  LISTENING_PORT=0 \n"), Some(0));
    }

    #[test]
    fn ignores_other_stdout_lines() {
        assert_eq!(parse_listening_port("starting up"), None);
        assert_eq!(parse_listening_port("PORT=8081"), None);
        assert_eq!(parse_listening_port("LISTENING_PORT=notaport"), None);
    }

    #[test]
    fn truthy_accepts_common_forms() {
        for v in ["1", "true", "TRUE", "yes", "on", " On "] {
            assert!(truthy(v), "{v:?} should be truthy");
        }
        for v in ["0", "false", "no", "off", ""] {
            assert!(!truthy(v), "{v:?} should be falsy");
        }
    }

    #[test]
    fn the_embedded_pilot_bpmn_is_the_pilot_process() {
        assert!(PILOT_BPMN.contains("pilotSelfOptimize"));
    }
}
