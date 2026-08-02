//! Integrated-terminal WebSocket for the Nano IDE (issue #496).
//!
//! `GET /console/api/projects/{name}/pty` upgrades to a WebSocket that is glued
//! to a real PTY running the operator's shell in the project's directory. The
//! browser side is an xterm.js terminal.
//!
//! ## Wire protocol
//! * **Binary** frames, both directions, are raw terminal bytes (keystrokes
//!   client→server, program output server→client).
//! * **Text** frames client→server are JSON control messages; the only one so
//!   far is a resize: `{"type":"resize","cols":<u16>,"rows":<u16>}`. Anything
//!   unrecognised is ignored (forward-compatible).
//!
//! ## Security — a shell is arbitrary code execution
//! Two independent gates, both required, are checked **before** the upgrade:
//! 1. **Loopback only** — same [`super::request_is_loopback`] gate as the
//!    filesystem browser: local peer IP *and* a loopback `Host` header.
//! 2. **Enabled** — the [`super::terminal_settings`] gate. Off by default; the
//!    operator turns it on in the console (persisted), unless
//!    `NANO_CONSOLE_TERMINAL` explicitly disabled it (a hard lock). See #500.
//!
//! Un-gated this would be a trivial RCE for anyone who can reach the port; see
//! the issue for the full threat model. Hardening (session limits, audit) is
//! tracked as follow-up.

use std::io::{Read, Write};
use std::path::PathBuf;

use axum::{
    extract::{
        ConnectInfo, Path,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};

/// `true` when the integrated terminal is enabled — the console setting,
/// subject to the `NANO_CONSOLE_TERMINAL` hard-lock. Resolved and persisted by
/// [`super::terminal_settings`]; default-off, and `false` before boot init.
pub(super) fn terminal_enabled() -> bool {
    super::terminal_settings::gate()
        .map(super::terminal_settings::Gate::effective)
        .unwrap_or(false)
}

/// The shell to spawn for a fresh terminal. Honours `$SHELL` on Unix and
/// `%ComSpec%` on Windows, falling back to a sensible default per platform.
fn default_shell() -> String {
    if cfg!(windows) {
        std::env::var("ComSpec").unwrap_or_else(|_| "cmd.exe".to_string())
    } else {
        std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string())
    }
}

/// Upgrade handler. Enforces the loopback + opt-in gates, resolves the project
/// directory, then hands the socket to [`run_pty`].
pub(super) async fn pty_ws(
    ConnectInfo(peer): ConnectInfo<crate::PeerAddr>,
    headers: HeaderMap,
    Path(name): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    if !super::request_is_loopback(&peer, &headers) {
        return (
            StatusCode::FORBIDDEN,
            "the integrated terminal is available on localhost only",
        )
            .into_response();
    }
    if !terminal_enabled() {
        return (
            StatusCode::FORBIDDEN,
            "the integrated terminal is disabled; enable it in the console (Config) \
             unless NANO_CONSOLE_TERMINAL has locked it off",
        )
            .into_response();
    }
    let Some(dir) = super::projects::project_dir(&name) else {
        return (StatusCode::NOT_FOUND, "unknown project").into_response();
    };
    ws.on_upgrade(move |socket| run_pty(socket, dir))
}

/// A control frame parsed from a client Text message. Only resize for now.
#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Control {
    Resize { cols: u16, rows: u16 },
}

/// Bridges one WebSocket to one PTY for the life of the connection.
///
/// The PTY master exposes *blocking* `Read`/`Write`, so each direction gets a
/// dedicated OS thread that talks to the async side over channels; the async
/// `select!` loop owns the socket and the master (for resize). When either side
/// closes we drop the input channel (ending the writer thread) and kill the
/// child so no orphaned shell is left behind.
async fn run_pty(socket: WebSocket, dir: PathBuf) {
    let pty_system = native_pty_system();
    let pair = match pty_system.openpty(PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    }) {
        Ok(p) => p,
        Err(e) => {
            let mut socket = socket;
            let _ = socket
                .send(Message::Text(format!("failed to open pty: {e}").into()))
                .await;
            return;
        }
    };

    let mut cmd = CommandBuilder::new(default_shell());
    cmd.cwd(&dir);
    // A colour-capable, well-known terminal type so programs behave sanely.
    cmd.env("TERM", "xterm-256color");

    let mut child = match pair.slave.spawn_command(cmd) {
        Ok(c) => c,
        Err(e) => {
            let mut socket = socket;
            let _ = socket
                .send(Message::Text(format!("failed to spawn shell: {e}").into()))
                .await;
            return;
        }
    };
    // Drop the slave now that the child holds it, so the master read sees EOF
    // when the shell exits.
    drop(pair.slave);

    let mut reader = match pair.master.try_clone_reader() {
        Ok(r) => r,
        Err(_) => {
            let _ = child.kill();
            return;
        }
    };
    let mut writer = match pair.master.take_writer() {
        Ok(w) => w,
        Err(_) => {
            let _ = child.kill();
            return;
        }
    };
    let master = pair.master;

    // PTY output → async side.
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);
    std::thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if out_tx.blocking_send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Async side → PTY input.
    let (in_tx, in_rx) = std::sync::mpsc::channel::<Vec<u8>>();
    std::thread::spawn(move || {
        while let Ok(bytes) = in_rx.recv() {
            if writer.write_all(&bytes).is_err() || writer.flush().is_err() {
                break;
            }
        }
    });

    let (mut ws_tx, mut ws_rx) = socket.split();

    loop {
        tokio::select! {
            out = out_rx.recv() => {
                match out {
                    Some(chunk) => {
                        if ws_tx.send(Message::Binary(chunk.into())).await.is_err() {
                            break;
                        }
                    }
                    // Reader thread ended → shell exited.
                    None => break,
                }
            }
            msg = ws_rx.next() => {
                match msg {
                    Some(Ok(Message::Binary(b))) => {
                        if in_tx.send(b.to_vec()).is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Text(t))) => {
                        match serde_json::from_str::<Control>(&t) {
                            Ok(Control::Resize { cols, rows }) => {
                                let _ = master.resize(PtySize {
                                    rows,
                                    cols,
                                    pixel_width: 0,
                                    pixel_height: 0,
                                });
                            }
                            // Not a control frame: ignore (protocol is
                            // Binary-for-input, Text-for-control).
                            Err(_) => {}
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    // A receive error means the socket is broken: stop so the
                    // PTY and child get torn down instead of spinning.
                    Some(Err(_)) => break,
                    // Ping/Pong handled by axum; ignore the rest.
                    _ => {}
                }
            }
        }
    }

    // Tear down: end both bridge threads, signal the shell, then reap it
    // *off* the async runtime. Dropping the output receiver unblocks the
    // reader thread if it is parked in `blocking_send`. `kill()` is a
    // non-blocking signal so it stays inline, but `wait()` is a blocking
    // `waitpid`: run inline on a scheduler worker it blocks that worker until
    // the child is reaped, and enough concurrent PTY teardowns can starve
    // every worker and wedge the whole server (observed live: a worker stuck
    // in `__wait4` here, #500). `detach_wait` moves the reap to the blocking
    // pool so this async teardown returns immediately.
    drop(in_tx);
    drop(out_rx);
    let _ = child.kill();
    detach_wait(child);
}

/// Reap a spawned shell without blocking the async runtime.
///
/// [`portable_pty::Child::wait`] is a blocking `waitpid`. Calling it inline on
/// a Tokio worker holds that worker until the child exits; under enough
/// concurrent PTY teardowns this starves every worker and wedges the entire
/// server (observed live: a scheduler worker stuck in `__wait4` at teardown,
/// #500). Moving the wait to the blocking pool via [`tokio::task::spawn_blocking`]
/// lets the async teardown return immediately while the child is still reaped.
fn detach_wait(mut child: Box<dyn portable_pty::Child + Send + Sync>) {
    tokio::task::spawn_blocking(move || {
        let _ = child.wait();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resize_control_frame_parses() {
        let Control::Resize { cols, rows } =
            serde_json::from_str(r#"{"type":"resize","cols":120,"rows":40}"#).unwrap();
        assert_eq!((cols, rows), (120, 40));
    }

    #[test]
    fn non_control_text_is_rejected_as_control() {
        // Plain keystroke text must not accidentally parse as a control frame;
        // the protocol keeps input on Binary frames and control on Text.
        assert!(serde_json::from_str::<Control>("ls -la\n").is_err());
        assert!(serde_json::from_str::<Control>(r#"{"type":"bogus"}"#).is_err());
    }

    /// Regression guard for the runtime-wedge defect class (#500): a PTY
    /// teardown must never run the blocking `waitpid` on the async worker.
    ///
    /// A live worker was captured stuck in `__wait4` inside the teardown reap,
    /// which — repeated across sessions — wedged the whole tokio runtime (every
    /// HTTP route stopped responding). This test spawns a real PTY child that
    /// lingers and, crucially, does **not** kill it: an inline `child.wait()`
    /// would block the caller for the child's whole lifetime, whereas
    /// [`detach_wait`] must return effectively immediately.
    // Unix-gated: it spawns `sleep` to model a shell that outlives its socket,
    // and `sleep` isn't guaranteed on Windows. The defect and fix are
    // platform-agnostic; this reproduction just needs a portable "linger".
    #[cfg(unix)]
    #[tokio::test]
    async fn detach_wait_does_not_block_the_async_worker() {
        use std::time::{Duration, Instant};

        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            })
            .expect("openpty");
        // A long-lived child so a *blocking* inline `wait()` would hold the
        // worker for seconds, while the correct `spawn_blocking` enqueue
        // returns in sub-millisecond time — a gap far wider than any CI
        // scheduling jitter, so the bound can't flake.
        let mut cmd = CommandBuilder::new("sleep");
        cmd.arg("30");
        let child = pair.slave.spawn_command(cmd).expect("spawn sleep");
        drop(pair.slave);
        // Keep an independent killer so we can reap the lingering child once
        // the measurement is done (the detached `wait()` then returns).
        let mut killer = child.clone_killer();

        let start = Instant::now();
        detach_wait(child);
        let elapsed = start.elapsed();

        // Stop the lingering child regardless of the assertion outcome.
        let _ = killer.kill();

        assert!(
            elapsed < Duration::from_secs(1),
            "PTY reap blocked the async worker for {elapsed:?}; a blocking \
             waitpid on a scheduler worker can wedge the whole runtime (#500)"
        );
    }
}
