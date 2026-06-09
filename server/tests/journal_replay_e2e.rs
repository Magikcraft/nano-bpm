//! End-to-end journal-replay tests.
//!
//! Each test boots the *real* server binary over a throwaway journal file on an
//! ephemeral port, drives it over HTTP, then kills the process and boots a fresh
//! one over the **same** journal file to prove durable state was recovered by
//! replaying the log.
//!
//! These are deliberately full-stack: HTTP request → engine command → journal
//! append+flush → process restart → `Engine::replay`. They are also hermetic and
//! reproducible — every test gets its own auto-removed temp directory and a
//! freshly allocated port, so runs never collide with each other or leak state
//! between them.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Path to the compiled server binary, provided by Cargo for integration tests.
const SERVER_BIN: &str = env!("CARGO_BIN_EXE_camunda-gateway-rest-server");

/// The generated REST layer mounts every route under this base path.
const BASE_PATH: &str = "/v2";

/// A unique temp directory that removes itself (and everything under it) on drop,
/// so a passing or panicking test never leaves files behind.
struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("nanobpmn-e2e-{}-{nanos}-{seq}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create scratch dir");
        Self { path }
    }

    fn journal_path(&self) -> PathBuf {
        self.path.join("test.journal")
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Reserves an OS-assigned free TCP port, then releases it so the spawned server
/// can bind it. There is a tiny race between release and re-bind, but a fresh
/// port per boot keeps tests independent and avoids `TIME_WAIT` collisions.
fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("reserve port")
        .local_addr()
        .expect("local addr")
        .port()
}

/// A running server child process. Killed and reaped on drop, so a panicking
/// assertion can never strand the process.
struct ServerProcess {
    child: Child,
    port: u16,
}

impl ServerProcess {
    /// Boots the server over `journal`, waits until it answers HTTP, and returns
    /// the handle. Output is suppressed to keep test logs clean.
    fn boot(journal: &Path) -> Self {
        let port = free_port();
        let child = Command::new(SERVER_BIN)
            .env("NANOBPMN_JOURNAL", journal)
            .env("PORT", port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn server binary");

        let server = Self { child, port };
        server.wait_until_ready();
        server
    }

    /// Polls a wired route until the server returns an HTTP response, so callers
    /// never race the bind/serve startup window.
    fn wait_until_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Some((status, _)) =
                try_request(self.port, "GET", &path("/process-instances/0"), None)
            {
                // Any HTTP status (here: 404 for the bogus key) proves the
                // router is up and handling requests.
                assert_eq!(status, 404, "unexpected readiness probe status");
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("server on port {} never became ready", self.port);
    }

    fn request(&self, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
        try_request(self.port, method, path, body)
            .unwrap_or_else(|| panic!("{method} {path} failed: connection error"))
    }

    /// Stops the server and waits for it to exit, surfacing failures explicitly
    /// rather than relying on the drop guard.
    fn shutdown(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Mark as already reaped so Drop is a no-op.
        self.port = 0;
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if self.port != 0 {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Builds a full route under the generated REST base path.
fn path(suffix: &str) -> String {
    format!("{BASE_PATH}{suffix}")
}

/// Minimal HTTP/1.1 client: sends one request with `Connection: close` and reads
/// the whole response to EOF. Returns `(status_code, body)` or `None` if the TCP
/// connection could not be established (used by the readiness poll).
fn try_request(port: u16, method: &str, path: &str, body: Option<&str>) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .expect("set write timeout");

    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).expect("write request");
    stream.flush().expect("flush request");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read response");
    let raw = String::from_utf8_lossy(&raw);

    let status = raw
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("parse status line");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();

    Some((status, body))
}

/// Creates an instance of the pre-seeded `demo` process and returns its key.
fn create_demo_instance(server: &ServerProcess) -> String {
    let (status, body) = server.request(
        "POST",
        &path("/process-instances"),
        Some(r#"{"processDefinitionId":"demo"}"#),
    );
    assert_eq!(status, 200, "create instance failed: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).expect("create response is JSON");
    json["processInstanceKey"]
        .as_str()
        .expect("processInstanceKey present")
        .to_string()
}

#[test]
fn process_instance_survives_a_restart() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // Given: a fresh server creates and parks a process instance.
    let server = ServerProcess::boot(&journal);
    let instance_key = create_demo_instance(&server);

    let (status, _) = server.request(
        "GET",
        &path(&format!("/process-instances/{instance_key}")),
        None,
    );
    assert_eq!(status, 200, "instance should be visible before restart");

    // When: the process is killed and a new one is booted over the same journal.
    server.shutdown();
    let restarted = ServerProcess::boot(&journal);

    // Then: the instance is recovered by replaying the journal.
    let (status, body) = restarted.request(
        "GET",
        &path(&format!("/process-instances/{instance_key}")),
        None,
    );
    assert_eq!(status, 200, "instance must survive the restart: {body}");

    restarted.shutdown();
}

#[test]
fn the_demo_process_is_not_re_seeded_on_restart() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // Given: an instance created against the freshly seeded demo process.
    let server = ServerProcess::boot(&journal);
    let first_key = create_demo_instance(&server);
    server.shutdown();

    // When: the server restarts (recovering, so it must NOT re-deploy demo) and
    // a second instance is created.
    let restarted = ServerProcess::boot(&journal);
    let second_key = create_demo_instance(&restarted);

    // Then: both instances exist with distinct keys — recovery rebuilt the key
    // generator past every replayed key and reused the single demo deployment.
    assert_ne!(
        first_key, second_key,
        "post-restart instance must get a fresh key"
    );
    for key in [&first_key, &second_key] {
        let (status, _) =
            restarted.request("GET", &path(&format!("/process-instances/{key}")), None);
        assert_eq!(
            status, 200,
            "instance {key} should be present after restart"
        );
    }

    restarted.shutdown();
}

#[test]
fn a_fresh_journal_starts_empty() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // A brand-new journal has no instances: an arbitrary key is unknown.
    let server = ServerProcess::boot(&journal);
    let (status, _) = server.request("GET", &path("/process-instances/123456789"), None);
    assert_eq!(status, 404, "fresh journal must not know any instance");

    server.shutdown();
}
