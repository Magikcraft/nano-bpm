//! **Local llama.cpp sidecar supervisor.**
//!
//! ProcessOS reaches its LLM over an OpenAI-compatible HTTP endpoint (see [`crate::settings`]).
//! Rather than make the operator download, configure and run `llama-server` separately, this
//! module lets ProcessOS *start and stop one itself* — a supervised child process, exactly like
//! [`crate::supervisor::OwnNano`] does for the Nano gateway. A profile flagged `sidecar:true`
//! describes which model to load ([`LlmProfile::model_file`]) and any extra launch args
//! ([`LlmProfile::sidecar_args`]); the manager builds the `llama-server` command, spawns it,
//! drains its combined stdout/stderr into a bounded ring buffer, and serves the model at the
//! profile's `base_url` port.
//!
//! It is intentionally optional and crash-isolated: starting the sidecar is a button, the model
//! weights are the real cost (downloaded on demand by `llama-server` into the shared models dir),
//! and a profile can still point at a remote endpoint instead.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

use crate::settings::LlmProfile;

/// Most recent log lines kept in memory for the streaming viewer.
const LOG_CAPACITY: usize = 2000;

/// The plan for launching `llama-server`: the resolved binary, argv, and the `LLAMA_CACHE`
/// (models) directory. Split out from the spawn so it can be unit-tested and shown to the operator
/// as the equivalent terminal command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchPlan {
    pub bin: String,
    pub args: Vec<String>,
    pub models_dir: PathBuf,
    /// Display fields echoed back in the status payload.
    pub profile_id: String,
    pub model: String,
    pub port: u16,
}

impl LaunchPlan {
    /// Build the launch plan for a sidecar profile. `llama_bin` is the operator-configured binary
    /// path (else `llama-server` on `PATH`); `models_dir` is exported as `LLAMA_CACHE` so HF
    /// downloads and local GGUFs are shared with a separately-run llama.cpp.
    pub fn build(
        profile: &LlmProfile,
        models_dir: &Path,
        llama_bin: Option<&str>,
    ) -> Result<Self, String> {
        let model = profile
            .model_file
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .or(profile.model.as_deref())
            .ok_or_else(|| {
                format!(
                    "profile '{}' has no model_file to load with the sidecar",
                    profile.id
                )
            })?
            .to_string();
        let port = profile.sidecar_port();
        let bin = llama_bin
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("llama-server")
            .to_string();

        let mut args: Vec<String> = Vec::new();
        // A `.gguf` is a local file (absolute, or relative to the models dir); anything else is a
        // Hugging Face `repo[:quant]` spec that llama-server downloads into LLAMA_CACHE.
        if model.to_ascii_lowercase().ends_with(".gguf") {
            let p = PathBuf::from(&model);
            let resolved = if p.is_absolute() {
                p
            } else {
                models_dir.join(&p)
            };
            args.push("-m".into());
            args.push(resolved.display().to_string());
        } else {
            args.push("-hf".into());
            args.push(model.clone());
        }
        args.push("--host".into());
        args.push("127.0.0.1".into());
        args.push("--port".into());
        args.push(port.to_string());
        // Append the operator's extra args verbatim (e.g. "-ngl 99 -c 32768 --jinja").
        if let Some(extra) = profile.sidecar_args.as_deref() {
            args.extend(split_args(extra));
        }

        Ok(Self {
            bin,
            args,
            models_dir: models_dir.to_path_buf(),
            profile_id: profile.id.clone(),
            model,
            port,
        })
    }

    /// The equivalent shell command, so the operator can run the same server in a terminal to
    /// watch its output directly. Includes the `LLAMA_CACHE` export.
    pub fn command_line(&self) -> String {
        let mut s = format!(
            "LLAMA_CACHE={} {}",
            shell_quote(&self.models_dir.display().to_string()),
            shell_quote(&self.bin)
        );
        for a in &self.args {
            s.push(' ');
            s.push_str(&shell_quote(a));
        }
        s
    }
}

/// Split a free-text args string into argv on whitespace, honouring simple single/double quotes
/// so a quoted value (e.g. a path with spaces) stays one argument.
fn split_args(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    let mut has = false;
    for c in s.chars() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                } else {
                    cur.push(c);
                }
            }
            None => {
                if c == '\'' || c == '"' {
                    quote = Some(c);
                    has = true;
                } else if c.is_whitespace() {
                    if has || !cur.is_empty() {
                        out.push(std::mem::take(&mut cur));
                        has = false;
                    }
                } else {
                    cur.push(c);
                }
            }
        }
    }
    if has || !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Minimal shell quoting for the display-only command line.
fn shell_quote(s: &str) -> String {
    if !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_./:=".contains(c))
    {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}

/// A bounded, shared log buffer; lines carry a monotonic offset so the viewer can poll for new
/// output with `?since=N`.
#[derive(Clone, Default)]
struct LogBuffer {
    inner: Arc<Mutex<LogInner>>,
}

#[derive(Default)]
struct LogInner {
    lines: VecDeque<String>,
    /// Offset of the first line still retained (older lines were evicted).
    base: u64,
}

impl LogBuffer {
    fn push(&self, line: String) {
        let mut g = self.inner.lock().unwrap();
        g.lines.push_back(line);
        while g.lines.len() > LOG_CAPACITY {
            g.lines.pop_front();
            g.base += 1;
        }
    }

    fn clear(&self) {
        let mut g = self.inner.lock().unwrap();
        let end = g.base + g.lines.len() as u64;
        g.lines.clear();
        g.base = end;
    }

    /// Lines with offset >= `since`, plus the next offset to poll from.
    fn since(&self, since: u64) -> (Vec<String>, u64) {
        let g = self.inner.lock().unwrap();
        let end = g.base + g.lines.len() as u64;
        let from = since.max(g.base);
        let start = (from - g.base) as usize;
        let lines = g.lines.iter().skip(start).cloned().collect();
        (lines, end)
    }
}

/// What's currently running (or last attempted), surfaced to the status endpoint.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LlamaStatus {
    pub running: bool,
    pub profile_id: Option<String>,
    pub model: Option<String>,
    pub port: Option<u16>,
    pub pid: Option<u32>,
    pub command: Option<String>,
    pub models_dir: Option<String>,
    pub started_at: Option<String>,
    /// Set when the last start/stop attempt failed.
    pub error: Option<String>,
}

struct Running {
    child: Child,
    profile_id: String,
    model: String,
    port: u16,
    pid: Option<u32>,
    command: String,
    models_dir: String,
    started_at: String,
}

#[derive(Default)]
struct ManagerInner {
    running: Option<Running>,
    last_error: Option<String>,
}

/// The supervised-sidecar handle held in `AppState`. Cloneable (shares the inner lock + log
/// buffer) so it can live in the axum state and be driven from handlers.
#[derive(Clone, Default)]
pub struct LlamaManager {
    inner: Arc<Mutex<ManagerInner>>,
    logs: LogBuffer,
}

impl LlamaManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start `llama-server` for `plan`. Errors if a sidecar is already running or the spawn fails.
    pub fn start(&self, plan: LaunchPlan) -> Result<LlamaStatus, String> {
        {
            let g = self.inner.lock().unwrap();
            if g.running.is_some() {
                return Err("a model server is already running; stop it first".into());
            }
        }
        let command = plan.command_line();
        self.logs.clear();
        self.logs.push(format!("$ {command}"));

        let mut cmd = Command::new(&plan.bin);
        cmd.args(&plan.args)
            .env("LLAMA_CACHE", &plan.models_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd.spawn().map_err(|e| {
            let msg = format!("failed to start {}: {e}", plan.bin);
            self.set_error(&msg);
            self.logs.push(msg.clone());
            msg
        })?;
        let pid = child.id();

        // Drain BOTH stdout and stderr into the ring buffer so the pipe never blocks the child and
        // the operator sees everything (llama-server logs mostly to stderr).
        if let Some(out) = child.stdout.take() {
            self.spawn_drain(out);
        }
        if let Some(err) = child.stderr.take() {
            self.spawn_drain(err);
        }

        let started_at = now_iso();
        let running = Running {
            child,
            profile_id: plan.profile_id.clone(),
            model: plan.model.clone(),
            port: plan.port,
            pid,
            command,
            models_dir: plan.models_dir.display().to_string(),
            started_at,
        };
        let mut g = self.inner.lock().unwrap();
        g.last_error = None;
        g.running = Some(running);
        Ok(status_of(&g))
    }

    /// Stop the running sidecar (best-effort). A no-op when nothing is running.
    pub async fn stop(&self) -> LlamaStatus {
        let mut child = {
            let mut g = self.inner.lock().unwrap();
            match g.running.take() {
                Some(r) => r.child,
                None => return status_of(&g),
            }
        };
        let _ = child.start_kill();
        let _ = child.wait().await;
        self.logs.push("[server stopped]".into());
        status_of(&self.inner.lock().unwrap())
    }

    /// Reap the child if it exited on its own, so status reflects reality.
    pub fn reap(&self) {
        let mut g = self.inner.lock().unwrap();
        let exited = g
            .running
            .as_mut()
            .map(|r| matches!(r.child.try_wait(), Ok(Some(_))))
            .unwrap_or(false);
        if exited {
            if let Some(r) = g.running.take() {
                self.logs.push("[server process exited]".into());
                g.last_error = Some(format!("model server for '{}' exited", r.profile_id));
            }
        }
    }

    pub fn status(&self) -> LlamaStatus {
        self.reap();
        status_of(&self.inner.lock().unwrap())
    }

    /// Log lines at or after `since`, plus the next offset and the running flag.
    pub fn logs_since(&self, since: u64) -> (Vec<String>, u64, bool) {
        let (lines, next) = self.logs.since(since);
        let running = self.inner.lock().unwrap().running.is_some();
        (lines, next, running)
    }

    fn spawn_drain<R>(&self, reader: R)
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let logs = self.logs.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                logs.push(line);
            }
        });
    }

    fn set_error(&self, msg: &str) {
        self.inner.lock().unwrap().last_error = Some(msg.to_string());
    }
}

fn status_of(g: &ManagerInner) -> LlamaStatus {
    match &g.running {
        Some(r) => LlamaStatus {
            running: true,
            profile_id: Some(r.profile_id.clone()),
            model: Some(r.model.clone()),
            port: Some(r.port),
            pid: r.pid,
            command: Some(r.command.clone()),
            models_dir: Some(r.models_dir.clone()),
            started_at: Some(r.started_at.clone()),
            error: g.last_error.clone(),
        },
        None => LlamaStatus {
            running: false,
            profile_id: None,
            model: None,
            port: None,
            pid: None,
            command: None,
            models_dir: None,
            started_at: None,
            error: g.last_error.clone(),
        },
    }
}

/// A coarse ISO-8601 UTC timestamp (seconds) without pulling in chrono.
fn now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // days-since-epoch → y/m/d (civil calendar, Howard Hinnant's algorithm).
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(model_file: &str, base_url: &str, args: Option<&str>) -> LlmProfile {
        LlmProfile {
            id: "p".into(),
            name: "p".into(),
            provider: Some("openai".into()),
            base_url: Some(base_url.into()),
            model: Some(model_file.into()),
            api_key: None,
            max_tokens: None,
            temperature: None,
            sidecar: true,
            model_file: Some(model_file.into()),
            sidecar_args: args.map(String::from),
        }
    }

    #[test]
    fn builds_hf_launch_with_port_from_base_url() {
        let p = profile(
            "unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M",
            "http://127.0.0.1:8888/v1",
            Some("-ngl 99 -c 32768"),
        );
        let plan = LaunchPlan::build(&p, Path::new("/models"), None).unwrap();
        assert_eq!(plan.bin, "llama-server");
        assert_eq!(plan.port, 8888);
        assert_eq!(
            plan.args,
            vec![
                "-hf",
                "unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M",
                "--host",
                "127.0.0.1",
                "--port",
                "8888",
                "-ngl",
                "99",
                "-c",
                "32768",
            ]
        );
        let cl = plan.command_line();
        assert!(cl.contains("LLAMA_CACHE=/models"));
        assert!(cl.contains("-hf unsloth/gemma-4-26B-A4B-it-GGUF:UD-Q4_K_M"));
    }

    #[test]
    fn builds_local_gguf_launch_resolving_against_models_dir() {
        let p = profile("loan.gguf", "http://127.0.0.1:9001/v1", None);
        let plan = LaunchPlan::build(&p, Path::new("/models"), Some("/opt/llama-server")).unwrap();
        assert_eq!(plan.bin, "/opt/llama-server");
        assert_eq!(plan.args[0], "-m");
        assert_eq!(plan.args[1], "/models/loan.gguf");
        assert_eq!(plan.port, 9001);
    }

    #[test]
    fn absolute_gguf_path_is_left_untouched() {
        let p = profile("/data/m.gguf", "http://127.0.0.1:8080/v1", None);
        let plan = LaunchPlan::build(&p, Path::new("/models"), None).unwrap();
        assert_eq!(plan.args[1], "/data/m.gguf");
    }

    #[test]
    fn split_args_honours_quotes() {
        assert_eq!(
            split_args("-ngl 99 -c 32768"),
            vec!["-ngl", "99", "-c", "32768"]
        );
        assert_eq!(
            split_args("--chat-template '/a b/t.jinja' -ngl 99"),
            vec!["--chat-template", "/a b/t.jinja", "-ngl", "99"]
        );
        assert!(split_args("   ").is_empty());
    }

    #[test]
    fn log_buffer_offsets_and_eviction() {
        let buf = LogBuffer::default();
        buf.push("a".into());
        buf.push("b".into());
        let (lines, next) = buf.since(0);
        assert_eq!(lines, vec!["a", "b"]);
        assert_eq!(next, 2);
        // Polling from the tip returns nothing new.
        let (lines, _) = buf.since(2);
        assert!(lines.is_empty());
    }

    #[test]
    fn now_iso_is_well_formed() {
        let s = now_iso();
        assert_eq!(s.len(), 20);
        assert!(s.ends_with('Z') && s.contains('T'));
    }
}
