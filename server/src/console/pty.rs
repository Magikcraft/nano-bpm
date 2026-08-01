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
//! 2. **Explicit opt-in** — the `NANO_CONSOLE_TERMINAL` env var must be truthy
//!    (`1`/`true`/`yes`/`on`). Off by default, so a stock server never exposes a
//!    shell even to a local user.
//!
//! Un-gated this would be a trivial RCE for anyone who can reach the port; see
//! the issue for the full threat model. Hardening (session limits, audit,
//! opt-in UX) is tracked as follow-up — this is a happy-path prototype.

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

/// `true` when the operator has explicitly opted into the integrated terminal.
/// Default-off: a full shell must never be reachable by accident.
pub(super) fn terminal_enabled() -> bool {
    is_truthy(&std::env::var("NANO_CONSOLE_TERMINAL").unwrap_or_default())
}

/// Parses the opt-in env var. Accepts the usual truthy spellings, case- and
/// whitespace-insensitive; everything else (including unset/empty) is `false`.
fn is_truthy(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
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
            "the integrated terminal is disabled; set NANO_CONSOLE_TERMINAL=1 to enable it",
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
                    // Ping/Pong handled by axum; ignore the rest.
                    _ => {}
                }
            }
        }
    }

    // Tear down: end the writer thread and reap the shell.
    drop(in_tx);
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opt_in_accepts_truthy_spellings() {
        for s in ["1", "true", "TRUE", "yes", "On", "  on  "] {
            assert!(is_truthy(s), "{s:?} should enable the terminal");
        }
    }

    #[test]
    fn opt_in_rejects_everything_else() {
        // Default-off is the whole point: unset/empty/garbage must not open a shell.
        for s in ["", "0", "false", "no", "off", "enabled", "2", " "] {
            assert!(!is_truthy(s), "{s:?} must not enable the terminal");
        }
    }

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
}
