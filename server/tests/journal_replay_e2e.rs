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

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Path to the compiled server binary, provided by Cargo for integration tests.
const SERVER_BIN: &str = env!("CARGO_BIN_EXE_nanobpm-gateway-rest-server");

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

/// Reads the `LISTENING_PORT=<n>` line the server prints on stdout once it has
/// bound its OS-assigned port. A background reader thread keeps the pipe drained
/// and bridges a `recv_timeout`, so a server that dies before binding surfaces as
/// a timeout rather than a hang.
fn read_listening_port(child: &mut Child) -> u16 {
    let stdout = child.stdout.take().expect("server stdout is piped");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break, // EOF (process exited) or read error.
                Ok(_) => {
                    if let Some(rest) = line.trim().strip_prefix("LISTENING_PORT=")
                        && let Ok(port) = rest.parse::<u16>()
                    {
                        let _ = tx.send(port);
                        return;
                    }
                }
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(30))
        .expect("server never reported its listening port")
}

/// A running server child process. Killed and reaped on drop, so a panicking
/// assertion can never strand the process.
struct ServerProcess {
    child: Child,
    port: u16,
}

impl ServerProcess {
    /// Boots the server over `journal`, waits until it answers HTTP, and returns
    /// the handle. The server binds an **OS-assigned** port (`PORT=0`) and
    /// reports it back on stdout, so each test gets a unique port with no
    /// reserve-then-rebind race. Output is otherwise suppressed to keep test
    /// logs clean.
    fn boot(journal: &Path) -> Self {
        let mut child = Command::new(SERVER_BIN)
            .env("NANOBPMN_JOURNAL", journal)
            .env("PORT", "0")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn server binary");

        let port = read_listening_port(&mut child);
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

    /// Polls `method path` until `accept(status, body)` holds or a short deadline
    /// elapses, returning the last response. All `search*`/`get*` queries are
    /// served from the asynchronously-updated read model, so a read issued
    /// immediately after a write may briefly not observe it; this bridges that
    /// eventual-consistency window deterministically.
    fn request_until(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        accept: impl Fn(u16, &str) -> bool,
    ) -> (u16, String) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (status, resp) = self.request(method, path, body);
            if accept(status, &resp) || Instant::now() >= deadline {
                return (status, resp);
            }
            std::thread::sleep(Duration::from_millis(25));
        }
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

/// Whether a process-definition search response body contains the `demo`
/// definition. Used to poll the eventually-consistent read model until the
/// seeded demo deployment has been projected.
fn body_has_demo_definition(body: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|j| {
            j["items"].as_array().map(|items| {
                items
                    .iter()
                    .any(|i| i["processDefinitionId"].as_str() == Some("demo"))
            })
        })
        .unwrap_or(false)
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

/// Deploys a single BPMN resource via the multipart `/deployments` endpoint,
/// returning `(status, body)`. A minimal hand-rolled `multipart/form-data`
/// request keeps the test client dependency-free.
fn deploy_bpmn(port: u16, xml: &str) -> (u16, String) {
    let boundary = "----nanobpmnE2EBoundary";
    let body = format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"resource\"; filename=\"process.bpmn\"\r\n\
         Content-Type: text/xml\r\n\
         \r\n\
         {xml}\r\n\
         --{boundary}--\r\n"
    );

    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect for deploy");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set read timeout");
    let request = format!(
        "POST {} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: multipart/form-data; boundary={boundary}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        path("/deployments"),
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .expect("write deploy request");
    stream.flush().expect("flush deploy request");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read deploy response");
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
    (status, body)
}

#[test]
fn process_instance_survives_a_restart() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // Given: a fresh server creates and parks a process instance.
    let server = ServerProcess::boot(&journal);
    let instance_key = create_demo_instance(&server);

    let (status, _) = server.request_until(
        "GET",
        &path(&format!("/process-instances/{instance_key}")),
        None,
        |status, _| status == 200,
    );
    assert_eq!(status, 200, "instance should be visible before restart");

    // When: the process is killed and a new one is booted over the same journal.
    server.shutdown();
    let restarted = ServerProcess::boot(&journal);

    // Then: the instance is recovered by replaying the journal.
    let (status, body) = restarted.request_until(
        "GET",
        &path(&format!("/process-instances/{instance_key}")),
        None,
        |status, _| status == 200,
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
        let (status, _) = restarted.request_until(
            "GET",
            &path(&format!("/process-instances/{key}")),
            None,
            |status, _| status == 200,
        );
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

#[test]
fn publishing_a_message_with_no_subscription_returns_a_key() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // nanobpmn does not buffer messages: publishing one nobody is waiting for
    // still mints a message key and succeeds (the message is simply dropped).
    let server = ServerProcess::boot(&journal);
    let (status, body) = server.request(
        "POST",
        &path("/messages/publication"),
        Some(r#"{"name":"nobody-home","correlationKey":"X"}"#),
    );
    assert_eq!(status, 200, "publish should succeed: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).expect("publish response is JSON");
    assert!(
        json["messageKey"].as_str().is_some_and(|k| !k.is_empty()),
        "publish must return a message key: {body}"
    );

    server.shutdown();
}

#[test]
fn correlating_a_message_with_no_subscription_returns_404() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // Unlike publish, correlate reports 404 when nothing matches, so callers can
    // distinguish "delivered" from "no open subscription".
    let server = ServerProcess::boot(&journal);
    let (status, body) = server.request(
        "POST",
        &path("/messages/correlation"),
        Some(r#"{"name":"nobody-home","correlationKey":"X"}"#),
    );
    assert_eq!(status, 404, "correlate with no match must be 404: {body}");

    server.shutdown();
}

#[test]
fn a_message_start_event_creates_and_replays_a_process_instance() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // A deployed message start event opens a process-level subscription; a
    // matching correlateMessage creates a brand-new instance (no prior
    // createProcessInstance call) and reports its key.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="on-order" isExecutable="true">
    <bpmn:startEvent id="start">
      <bpmn:messageEventDefinition messageRef="Message_1" />
    </bpmn:startEvent>
    <bpmn:endEvent id="end" />
    <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="end" />
  </bpmn:process>
  <bpmn:message id="Message_1" name="order-placed" />
</bpmn:definitions>"#;

    let server = ServerProcess::boot(&journal);
    let (status, body) = deploy_bpmn(server.port, xml);
    assert_eq!(status, 200, "deploy should succeed: {body}");

    // Correlating the start message creates an instance and returns its key.
    let (status, body) = server.request(
        "POST",
        &path("/messages/correlation"),
        Some(r#"{"name":"order-placed","correlationKey":""}"#),
    );
    assert_eq!(status, 200, "message start should correlate: {body}");
    let json: serde_json::Value =
        serde_json::from_str(&body).expect("correlation response is JSON");
    let instance_key = json["processInstanceKey"]
        .as_str()
        .expect("processInstanceKey present")
        .to_string();
    assert!(!instance_key.is_empty());
    server.shutdown();

    // After a restart the subscription is recovered from the journal, so a
    // second message creates another, distinct instance.
    let server = ServerProcess::boot(&journal);
    let (status, body) = server.request(
        "POST",
        &path("/messages/correlation"),
        Some(r#"{"name":"order-placed","correlationKey":""}"#),
    );
    assert_eq!(
        status, 200,
        "recovered subscription should still fire: {body}"
    );
    let json: serde_json::Value =
        serde_json::from_str(&body).expect("correlation response is JSON");
    let second_key = json["processInstanceKey"]
        .as_str()
        .expect("processInstanceKey present")
        .to_string();
    assert_ne!(
        instance_key, second_key,
        "each message starts a new instance"
    );

    server.shutdown();
}

#[test]
fn an_embedded_subprocess_error_boundary_deploys_and_routes() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path());

    // Regression: a model with an embedded sub-process and an interrupting error
    // boundary attached to it failed to parse (the sub-process element kind did
    // not exist), so the deployment was rejected. It must now deploy, and the
    // BUSINESS_ERROR thrown inside the sub-process must be caught by the boundary
    // and routed onto the sad-flow path.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="throw-bpmn-error" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:subProcess id="sub">
      <bpmn:startEvent id="sub_start" />
      <bpmn:serviceTask id="inner">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="inner-work" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:endEvent id="sub_end" />
      <bpmn:sequenceFlow id="i0" sourceRef="sub_start" targetRef="inner" />
      <bpmn:sequenceFlow id="i1" sourceRef="inner" targetRef="sub_end" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="boundary" attachedToRef="sub">
      <bpmn:errorEventDefinition errorRef="Error_1" />
    </bpmn:boundaryEvent>
    <bpmn:serviceTask id="sad">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="sad-flow" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="done" />
    <bpmn:endEvent id="sad_end" />
    <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="sub" />
    <bpmn:sequenceFlow id="f1" sourceRef="sub" targetRef="done" />
    <bpmn:sequenceFlow id="f2" sourceRef="boundary" targetRef="sad" />
    <bpmn:sequenceFlow id="f3" sourceRef="sad" targetRef="sad_end" />
  </bpmn:process>
  <bpmn:error id="Error_1" name="Business" errorCode="BUSINESS_ERROR" />
</bpmn:definitions>"#;

    let (status, body) = deploy_bpmn(server.port, xml);
    assert_eq!(status, 200, "sub-process model should deploy: {body}");

    // Start an instance; the token parks on the inner service task.
    let (status, body) = server.request(
        "POST",
        &path("/process-instances"),
        Some(r#"{"processDefinitionId":"throw-bpmn-error"}"#),
    );
    assert_eq!(status, 200, "create instance failed: {body}");

    // Activate the inner job and throw the business error the boundary catches.
    let job_key = activate_one_job(&server, "inner-work");
    let (status, body) = server.request(
        "POST",
        &path(&format!("/jobs/{job_key}/error")),
        Some(r#"{"errorCode":"BUSINESS_ERROR"}"#),
    );
    assert_eq!(status, 204, "throwing the caught error failed: {body}");

    // The interruption routed to the sad-flow task: its job is now activatable.
    let sad_key = activate_one_job(&server, "sad-flow");
    assert_ne!(sad_key, job_key, "a distinct sad-flow job was created");

    server.shutdown();
}

/// Activates exactly one job of `job_type` (no long-poll) and returns its key.
fn activate_one_job(server: &ServerProcess, job_type: &str) -> String {
    let body = format!(
        r#"{{"type":"{job_type}","maxJobsToActivate":1,"timeout":60000,"requestTimeout":-1}}"#
    );
    let (status, resp) = server.request("POST", &path("/jobs/activation"), Some(&body));
    assert_eq!(status, 200, "activation failed: {resp}");
    let json: serde_json::Value = serde_json::from_str(&resp).expect("activation response is JSON");
    let jobs = json["jobs"].as_array().expect("jobs array");
    assert_eq!(jobs.len(), 1, "exactly one {job_type} job should activate: {resp}");
    jobs[0]["jobKey"]
        .as_str()
        .expect("jobKey present")
        .to_string()
}

#[test]
fn a_non_interrupting_message_boundary_deploys_and_spawns_a_parallel_token() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path());

    // A non-interrupting message boundary (cancelActivity="false") must deploy
    // and, on a matching message, spawn a parallel token down its side path
    // WITHOUT cancelling the main service task — both jobs stay activatable.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="notifiable" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="charge">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="main-work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:boundaryEvent id="notify" attachedToRef="charge" cancelActivity="false">
      <bpmn:messageEventDefinition messageRef="Message_1" />
    </bpmn:boundaryEvent>
    <bpmn:serviceTask id="remind">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="notify-work" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="done" />
    <bpmn:endEvent id="notified" />
    <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="charge" />
    <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
    <bpmn:sequenceFlow id="f2" sourceRef="notify" targetRef="remind" />
    <bpmn:sequenceFlow id="f3" sourceRef="remind" targetRef="notified" />
  </bpmn:process>
  <bpmn:message id="Message_1" name="reminder">
    <bpmn:extensionElements>
      <zeebe:subscription correlationKey="=orderId" />
    </bpmn:extensionElements>
  </bpmn:message>
</bpmn:definitions>"#;

    let (status, body) = deploy_bpmn(server.port, xml);
    assert_eq!(
        status, 200,
        "non-interrupting boundary model should deploy: {body}"
    );

    // Start an instance; the token parks on the main service task and the
    // boundary subscription opens (orderId unset, so its correlation value is "").
    let (status, body) = server.request(
        "POST",
        &path("/process-instances"),
        Some(r#"{"processDefinitionId":"notifiable"}"#),
    );
    assert_eq!(status, 200, "create instance failed: {body}");

    // Correlate the boundary message: it spawns a parallel token down the side
    // path without interrupting the main task.
    let (status, body) = server.request(
        "POST",
        &path("/messages/correlation"),
        Some(r#"{"name":"reminder","correlationKey":""}"#),
    );
    assert_eq!(status, 200, "correlating the boundary message failed: {body}");

    // Both paths now have an activatable job: the spawned notify-work job AND the
    // still-running main-work job (the task was not cancelled).
    let notify_key = activate_one_job(&server, "notify-work");
    let main_key = activate_one_job(&server, "main-work");
    assert_ne!(notify_key, main_key, "distinct jobs for the parallel paths");

    server.shutdown();
}

#[test]
fn an_interrupting_message_boundary_on_a_subprocess_tears_down_the_inner_scope() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path());

    // An interrupting message boundary attached to an embedded sub-process must
    // deploy and, on a matching message, cancel the inner job, tear down the
    // whole inner scope, and route the token out the boundary onto the abort
    // path (the normal "done" flow is NOT taken).
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="cancellable-sub" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:subProcess id="sub">
      <bpmn:startEvent id="sub_start" />
      <bpmn:serviceTask id="inner">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="inner-work" />
        </bpmn:extensionElements>
      </bpmn:serviceTask>
      <bpmn:endEvent id="sub_end" />
      <bpmn:sequenceFlow id="i0" sourceRef="sub_start" targetRef="inner" />
      <bpmn:sequenceFlow id="i1" sourceRef="inner" targetRef="sub_end" />
    </bpmn:subProcess>
    <bpmn:boundaryEvent id="cancel" attachedToRef="sub">
      <bpmn:messageEventDefinition messageRef="Message_1" />
    </bpmn:boundaryEvent>
    <bpmn:serviceTask id="abort">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="abort-flow" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="done" />
    <bpmn:endEvent id="aborted" />
    <bpmn:sequenceFlow id="f0" sourceRef="start" targetRef="sub" />
    <bpmn:sequenceFlow id="f1" sourceRef="sub" targetRef="done" />
    <bpmn:sequenceFlow id="f2" sourceRef="cancel" targetRef="abort" />
    <bpmn:sequenceFlow id="f3" sourceRef="abort" targetRef="aborted" />
  </bpmn:process>
  <bpmn:message id="Message_1" name="order-cancelled">
    <bpmn:extensionElements>
      <zeebe:subscription correlationKey="=orderId" />
    </bpmn:extensionElements>
  </bpmn:message>
</bpmn:definitions>"#;

    let (status, body) = deploy_bpmn(server.port, xml);
    assert_eq!(
        status, 200,
        "sub-process message-boundary model should deploy: {body}"
    );

    // Start an instance; the token parks on the inner service task and the
    // boundary subscription opens on the sub-process.
    let (status, body) = server.request(
        "POST",
        &path("/process-instances"),
        Some(r#"{"processDefinitionId":"cancellable-sub"}"#),
    );
    assert_eq!(status, 200, "create instance failed: {body}");
    let inner_key = activate_one_job(&server, "inner-work");

    // Correlate the boundary message: it interrupts the whole sub-process and
    // routes onto the abort path.
    let (status, body) = server.request(
        "POST",
        &path("/messages/correlation"),
        Some(r#"{"name":"order-cancelled","correlationKey":""}"#),
    );
    assert_eq!(
        status, 200,
        "correlating the boundary message failed: {body}"
    );

    // The abort-flow job is now activatable; the cancelled inner job cannot be
    // re-activated (the inner scope was torn down), so no inner-work job remains.
    let abort_key = activate_one_job(&server, "abort-flow");
    assert_ne!(abort_key, inner_key, "a distinct abort-flow job was created");
    let (status, resp) = server.request(
        "POST",
        &path("/jobs/activation"),
        Some(r#"{"type":"inner-work","maxJobsToActivate":1,"timeout":60000,"requestTimeout":-1}"#),
    );
    assert_eq!(status, 200, "activation request failed: {resp}");
    let json: serde_json::Value = serde_json::from_str(&resp).expect("activation response is JSON");
    assert_eq!(
        json["jobs"].as_array().expect("jobs array").len(),
        0,
        "the inner job was cancelled by the interrupting boundary: {resp}"
    );

    server.shutdown();
}

#[test]
fn create_instance_accepts_the_default_tenant_id() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // Regression: the generated tenantId validation regex had HTML-escaped
    // delimiters (`&lt;default&gt;`), so the literal default-tenant alias
    // `<default>` failed validation with a 400. A request carrying it must now
    // be accepted (the demo process is pre-seeded).
    let server = ServerProcess::boot(&journal);
    let (status, body) = server.request(
        "POST",
        &path("/process-instances"),
        Some(r#"{"processDefinitionId":"demo","tenantId":"<default>"}"#),
    );
    assert_eq!(status, 200, "<default> tenant must be accepted: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("create response is JSON");
    assert!(json["processInstanceKey"].as_str().is_some());

    server.shutdown();
}

#[test]
fn variables_set_on_an_instance_are_searchable_and_fetchable() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path());

    // Seed an instance with three instance-scope variables {a:1, b:2, c:3}.
    let instance_key = create_demo_instance_with_vars(&server);

    // The read model is eventually consistent, so wait until all three variables
    // have been projected for the instance.
    let search_body = format!(r#"{{"filter":{{"processInstanceKey":"{instance_key}"}}}}"#);
    let (status, resp) = server.request_until(
        "POST",
        &path("/variables/search"),
        Some(&search_body),
        |status, body| {
            status == 200
                && serde_json::from_str::<serde_json::Value>(body)
                    .ok()
                    .and_then(|j| j["items"].as_array().map(|items| items.len() >= 3))
                    .unwrap_or(false)
        },
    );
    assert_eq!(status, 200, "variable search failed: {resp}");
    let json: serde_json::Value = serde_json::from_str(&resp).expect("search response is JSON");
    let items = json["items"].as_array().expect("items array");
    assert_eq!(items.len(), 3, "three variables for the instance: {resp}");

    // Each variable's scopeKey equals its processInstanceKey (nano keeps a single
    // instance-level scope), the tenant is the default, and short values are not
    // truncated. Integer values render bare in the serialized-JSON value string.
    let mut names: Vec<&str> = Vec::new();
    for it in items {
        assert_eq!(it["processInstanceKey"].as_str(), Some(instance_key.as_str()));
        assert_eq!(it["scopeKey"], it["processInstanceKey"]);
        assert_eq!(it["tenantId"].as_str(), Some("<default>"));
        assert_eq!(it["isTruncated"].as_bool(), Some(false));
        names.push(it["name"].as_str().expect("name"));
    }
    names.sort_unstable();
    assert_eq!(names, vec!["a", "b", "c"]);

    // Filtering by name returns just that variable with its serialized value.
    let (status, resp) = server.request(
        "POST",
        &path("/variables/search"),
        Some(&format!(
            r#"{{"filter":{{"processInstanceKey":"{instance_key}","name":"b"}}}}"#
        )),
    );
    assert_eq!(status, 200, "filtered search failed: {resp}");
    let json: serde_json::Value = serde_json::from_str(&resp).unwrap();
    let items = json["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "one variable named b: {resp}");
    assert_eq!(items[0]["value"].as_str(), Some("2"));

    // Fetch that variable by its (read-model) key; getVariable returns the full
    // value and the same identity.
    let var_key = items[0]["variableKey"].as_str().expect("variableKey");
    let (status, body) = server.request("GET", &path(&format!("/variables/{var_key}")), None);
    assert_eq!(status, 200, "get variable failed: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).expect("get response is JSON");
    assert_eq!(v["name"].as_str(), Some("b"));
    assert_eq!(v["value"].as_str(), Some("2"));
    assert_eq!(v["processInstanceKey"].as_str(), Some(instance_key.as_str()));
    assert_eq!(v["scopeKey"].as_str(), Some(instance_key.as_str()));

    // An unknown variable key 404s.
    let (status, _) = server.request("GET", &path("/variables/99999999999"), None);
    assert_eq!(status, 404, "unknown variable key should 404");

    server.shutdown();
}

#[test]
fn searched_variables_survive_a_restart() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // Seed variables, then confirm they were projected before stopping.
    let instance_key = {
        let server = ServerProcess::boot(&journal);
        let key = create_demo_instance_with_vars(&server);
        let body = format!(r#"{{"filter":{{"processInstanceKey":"{key}"}}}}"#);
        server.request_until(
            "POST",
            &path("/variables/search"),
            Some(&body),
            |status, body| {
                status == 200
                    && serde_json::from_str::<serde_json::Value>(body)
                        .ok()
                        .and_then(|j| j["items"].as_array().map(|items| items.len() >= 3))
                        .unwrap_or(false)
            },
        );
        server.shutdown();
        key
    };

    // Restart: the read model is rebuilt by replaying the journal, so the
    // variables remain searchable.
    let restarted = ServerProcess::boot(&journal);
    let body = format!(r#"{{"filter":{{"processInstanceKey":"{instance_key}"}}}}"#);
    let (status, resp) = restarted.request_until(
        "POST",
        &path("/variables/search"),
        Some(&body),
        |status, body| {
            status == 200
                && serde_json::from_str::<serde_json::Value>(body)
                    .ok()
                    .and_then(|j| j["items"].as_array().map(|items| items.len() >= 3))
                    .unwrap_or(false)
        },
    );
    assert_eq!(status, 200, "post-restart variable search failed: {resp}");
    let json: serde_json::Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(
        json["items"].as_array().unwrap().len(),
        3,
        "all three variables survive a restart: {resp}"
    );

    restarted.shutdown();
}

#[test]
fn a_created_instance_reports_a_real_start_date() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // Regression: the clock-free engine did not record a start time, so every
    // process instance was projected with the Unix epoch
    // (`1970-01-01T00:00:00Z`) as its start date. The server now stamps the
    // creating command's wall-clock instant onto the instance, so the search
    // projection must report a present-day timestamp.
    let server = ServerProcess::boot(&journal);
    let key = create_demo_instance(&server);

    let (status, body) = server.request_until(
        "POST",
        &path("/process-instances/search"),
        Some(r#"{}"#),
        |status, body| {
            status == 200
                && serde_json::from_str::<serde_json::Value>(body)
                    .ok()
                    .and_then(|j| {
                        j["items"].as_array().map(|items| {
                            items
                                .iter()
                                .any(|i| i["processInstanceKey"].as_str() == Some(key.as_str()))
                        })
                    })
                    .unwrap_or(false)
        },
    );
    assert_eq!(status, 200, "search failed: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).expect("search response is JSON");
    let item = json["items"]
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .find(|i| i["processInstanceKey"].as_str() == Some(key.as_str()))
        })
        .expect("created instance present in search results");

    let start_date = item["startDate"].as_str().expect("startDate is a string");
    assert!(
        !start_date.starts_with("1970"),
        "start date must be the real creation time, not the epoch: {start_date}"
    );

    server.shutdown();
}

#[test]
fn searching_process_definitions_returns_the_deployed_demo() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // Regression: SearchProcessDefinitions was an unimplemented stub that
    // returned 501. It now projects the engine's deployed definitions (the
    // demo process is pre-seeded).
    let server = ServerProcess::boot(&journal);
    let (status, body) = server.request_until(
        "POST",
        &path("/process-definitions/search"),
        Some(r#"{}"#),
        |status, body| status == 200 && body_has_demo_definition(body),
    );
    assert_eq!(status, 200, "definition search must succeed: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).expect("search response is JSON");
    let demo = json["items"]
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .find(|i| i["processDefinitionId"].as_str() == Some("demo"))
        })
        .expect("demo definition present in search results");

    assert_eq!(demo["version"].as_i64(), Some(1));
    assert_eq!(demo["tenantId"].as_str(), Some("<default>"));
    assert!(demo["processDefinitionKey"].as_str().is_some());

    server.shutdown();
}

#[test]
fn create_instance_accepts_a_process_definition_key() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // Starting by processDefinitionKey was previously rejected with a 400. The
    // server now resolves the key to its deployed definition and starts it. We
    // first discover the demo definition's key via the search endpoint, then
    // start an instance by that key.
    let server = ServerProcess::boot(&journal);

    let (status, body) = server.request_until(
        "POST",
        &path("/process-definitions/search"),
        Some(r#"{}"#),
        |status, body| status == 200 && body_has_demo_definition(body),
    );
    assert_eq!(status, 200, "definition search must succeed: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("search response is JSON");
    let demo_key = json["items"]
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .find(|i| i["processDefinitionId"].as_str() == Some("demo"))
        })
        .and_then(|i| i["processDefinitionKey"].as_str())
        .expect("demo definition key present")
        .to_string();

    let create_body = format!(r#"{{"processDefinitionKey":"{demo_key}"}}"#);
    let (status, body) = server.request("POST", &path("/process-instances"), Some(&create_body));
    assert_eq!(status, 200, "create by key must succeed: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).expect("create response is JSON");
    assert_eq!(json["processDefinitionId"].as_str(), Some("demo"));
    assert_eq!(
        json["processDefinitionKey"].as_str(),
        Some(demo_key.as_str())
    );
    assert!(json["processInstanceKey"].as_str().is_some());

    server.shutdown();
}

#[test]
fn create_instance_rejects_an_unknown_process_definition_key() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    let server = ServerProcess::boot(&journal);
    let (status, body) = server.request(
        "POST",
        &path("/process-instances"),
        Some(r#"{"processDefinitionKey":"999999999"}"#),
    );
    assert_eq!(status, 400, "unknown key must be rejected: {body}");

    server.shutdown();
}

#[test]
fn concurrent_reads_and_writes_stay_consistent() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // The engine sits behind a read/write lock: writes serialize, reads run in
    // parallel. Hammer the server from many threads with a mix of reads
    // (process-definition search) and writes (create instance) to prove the
    // locking neither deadlocks nor corrupts state, and that every request is
    // served. Each created instance must be retrievable afterwards.
    let server = ServerProcess::boot(&journal);

    const READERS: usize = 8;
    const WRITERS: usize = 4;
    const WRITES_PER_THREAD: usize = 10;

    let created = std::sync::Mutex::new(Vec::<String>::new());

    std::thread::scope(|scope| {
        for _ in 0..READERS {
            scope.spawn(|| {
                for _ in 0..25 {
                    let (status, body) =
                        server.request("POST", &path("/process-definitions/search"), Some(r#"{}"#));
                    assert_eq!(status, 200, "concurrent read must succeed: {body}");
                }
            });
        }
        for _ in 0..WRITERS {
            scope.spawn(|| {
                for _ in 0..WRITES_PER_THREAD {
                    let (status, body) = server.request(
                        "POST",
                        &path("/process-instances"),
                        Some(r#"{"processDefinitionId":"demo"}"#),
                    );
                    assert_eq!(status, 200, "concurrent write must succeed: {body}");
                    let json: serde_json::Value =
                        serde_json::from_str(&body).expect("create response is JSON");
                    let key = json["processInstanceKey"]
                        .as_str()
                        .expect("instance key present")
                        .to_string();
                    created.lock().unwrap().push(key);
                }
            });
        }
    });

    let keys = created.into_inner().unwrap();
    assert_eq!(
        keys.len(),
        WRITERS * WRITES_PER_THREAD,
        "every write produced an instance"
    );

    // Keys are minted by a single writer, so they must all be unique.
    let mut unique = keys.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), keys.len(), "instance keys must be unique");

    // Every created instance is individually retrievable (state is consistent).
    // Reads are served from the asynchronously-updated read model, so poll each
    // until the exporter has projected it.
    for key in &keys {
        let (status, body) = server.request_until(
            "GET",
            &path(&format!("/process-instances/{key}")),
            None,
            |status, _| status == 200,
        );
        assert_eq!(status, 200, "instance {key} must be retrievable: {body}");
    }

    server.shutdown();
}

#[test]
fn topology_reports_a_single_broker_cluster() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // nanobpmn is a single-writer, single-partition embedded engine, so the
    // topology endpoint advertises a one-broker, one-partition cluster with this
    // gateway as the healthy leader of partition 1.
    let server = ServerProcess::boot(&journal);
    let (status, body) = server.request("GET", &path("/topology"), None);
    assert_eq!(status, 200, "topology must succeed: {body}");

    let json: serde_json::Value = serde_json::from_str(&body).expect("topology response is JSON");
    assert_eq!(json["clusterSize"].as_i64(), Some(1));
    assert_eq!(json["partitionsCount"].as_i64(), Some(1));
    assert_eq!(json["replicationFactor"].as_i64(), Some(1));
    assert!(
        json["gatewayVersion"]
            .as_str()
            .is_some_and(|v| !v.is_empty()),
        "gatewayVersion must be reported"
    );

    let brokers = json["brokers"].as_array().expect("brokers is an array");
    assert_eq!(brokers.len(), 1, "exactly one broker");
    let broker = &brokers[0];
    assert_eq!(broker["nodeId"].as_i64(), Some(0));

    let partitions = broker["partitions"]
        .as_array()
        .expect("partitions is an array");
    assert_eq!(partitions.len(), 1, "exactly one partition");
    assert_eq!(partitions[0]["partitionId"].as_i64(), Some(1));
    assert_eq!(partitions[0]["role"].as_str(), Some("leader"));
    assert_eq!(partitions[0]["health"].as_str(), Some("healthy"));

    server.shutdown();
}

#[test]
fn canceling_a_process_instance_terminates_it() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // Given: a parked demo instance (waiting on its service-task job), visible in
    // the eventually-consistent read model as ACTIVE.
    let server = ServerProcess::boot(&journal);
    let instance_key = create_demo_instance(&server);

    let (status, _) = server.request_until(
        "GET",
        &path(&format!("/process-instances/{instance_key}")),
        None,
        |status, body| {
            status == 200
                && serde_json::from_str::<serde_json::Value>(body)
                    .ok()
                    .and_then(|j| j["state"].as_str().map(|s| s == "ACTIVE"))
                    .unwrap_or(false)
        },
    );
    assert_eq!(status, 200, "instance should be ACTIVE before cancellation");

    // When: the instance is cancelled.
    let (status, body) = server.request(
        "POST",
        &path(&format!("/process-instances/{instance_key}/cancellation")),
        Some("{}"),
    );
    assert_eq!(status, 204, "cancellation must return 204: {body}");

    // Then: the read model eventually reports it TERMINATED.
    let (status, body) = server.request_until(
        "GET",
        &path(&format!("/process-instances/{instance_key}")),
        None,
        |status, body| {
            status == 200
                && serde_json::from_str::<serde_json::Value>(body)
                    .ok()
                    .and_then(|j| j["state"].as_str().map(|s| s == "TERMINATED"))
                    .unwrap_or(false)
        },
    );
    assert_eq!(status, 200, "instance must be TERMINATED: {body}");

    // And: cancelling it again is a 404 (no longer an active instance).
    let (status, _) = server.request(
        "POST",
        &path(&format!("/process-instances/{instance_key}/cancellation")),
        Some("{}"),
    );
    assert_eq!(status, 404, "cancelling a terminated instance must 404");

    // And: cancelling an unknown key is a 404.
    let (status, _) = server.request(
        "POST",
        &path("/process-instances/999999/cancellation"),
        Some("{}"),
    );
    assert_eq!(status, 404, "cancelling an unknown instance must 404");

    server.shutdown();
}

#[test]
fn a_canceled_process_instance_stays_terminated_after_restart() {
    let scratch = ScratchDir::new();
    let journal = scratch.journal_path();

    // Given: a created-then-cancelled instance.
    let server = ServerProcess::boot(&journal);
    let instance_key = create_demo_instance(&server);
    let (status, _) = server.request(
        "POST",
        &path(&format!("/process-instances/{instance_key}/cancellation")),
        Some("{}"),
    );
    assert_eq!(status, 204, "cancellation must return 204");

    // When: the server is restarted over the same journal (replaying the
    // CancelInstance command).
    server.shutdown();
    let restarted = ServerProcess::boot(&journal);

    // Then: the recovered instance is still TERMINATED.
    let (status, body) = restarted.request_until(
        "GET",
        &path(&format!("/process-instances/{instance_key}")),
        None,
        |status, body| {
            status == 200
                && serde_json::from_str::<serde_json::Value>(body)
                    .ok()
                    .and_then(|j| j["state"].as_str().map(|s| s == "TERMINATED"))
                    .unwrap_or(false)
        },
    );
    assert_eq!(status, 200, "cancellation must survive the restart: {body}");

    restarted.shutdown();
}

/// Activates one `demo-work` job, returning its `variables` object as JSON.
/// `fetch` is the optional `fetchVariable` projection list.
fn activate_demo_job_variables(
    server: &ServerProcess,
    fetch: Option<&[&str]>,
) -> serde_json::Value {
    let fetch_field = match fetch {
        Some(names) => {
            let list = names
                .iter()
                .map(|n| format!("\"{n}\""))
                .collect::<Vec<_>>()
                .join(",");
            format!(r#","fetchVariable":[{list}]"#)
        }
        None => String::new(),
    };
    let body = format!(
        r#"{{"type":"demo-work","maxJobsToActivate":1,"timeout":60000,"requestTimeout":-1{fetch_field}}}"#
    );
    let (status, resp) = server.request("POST", &path("/jobs/activation"), Some(&body));
    assert_eq!(status, 200, "activation failed: {resp}");
    let json: serde_json::Value = serde_json::from_str(&resp).expect("activation response is JSON");
    let jobs = json["jobs"].as_array().expect("jobs array");
    assert_eq!(jobs.len(), 1, "exactly one job should activate: {resp}");
    jobs[0]["variables"].clone()
}

/// Creates a demo instance and seeds three instance-scope variables on it, so an
/// activated `demo-work` job carries `{a, b, c}`.
fn create_demo_instance_with_vars(server: &ServerProcess) -> String {
    let instance_key = create_demo_instance(server);
    let (status, body) = server.request(
        "PUT",
        &path(&format!("/element-instances/{instance_key}/variables")),
        Some(r#"{"variables":{"a":1,"b":2,"c":3}}"#),
    );
    assert_eq!(status, 204, "setting variables failed: {body}");
    instance_key
}

#[test]
fn activation_fetch_variable_returns_only_named_variables() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path());
    create_demo_instance_with_vars(&server);

    // fetchVariable = [a, c] projects only those two; b is omitted.
    let vars = activate_demo_job_variables(&server, Some(&["a", "c"]));
    let obj = vars.as_object().expect("variables is an object");
    let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["a", "c"], "only fetched variables returned");
    assert_eq!(obj["a"].as_i64(), Some(1));
    assert_eq!(obj["c"].as_i64(), Some(3));

    server.shutdown();
}

#[test]
fn activation_without_fetch_variable_returns_all_variables() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path());
    create_demo_instance_with_vars(&server);

    // No fetchVariable: every visible variable is returned.
    let vars = activate_demo_job_variables(&server, None);
    let obj = vars.as_object().expect("variables is an object");
    let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["a", "b", "c"], "all variables returned");

    server.shutdown();
}

#[test]
fn activation_empty_fetch_variable_returns_all_variables() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path());
    create_demo_instance_with_vars(&server);

    // An empty fetchVariable list behaves like "fetch all" per the API spec.
    let vars = activate_demo_job_variables(&server, Some(&[]));
    let obj = vars.as_object().expect("variables is an object");
    assert_eq!(obj.len(), 3, "empty list returns all variables: {vars}");

    server.shutdown();
}

#[test]
fn create_instance_variables_flow_through_to_activated_jobs() {
    let scratch = ScratchDir::new();
    let server = ServerProcess::boot(&scratch.journal_path());

    // Variables supplied on the create request seed the root scope, so an
    // activated demo-work job carries them.
    let (status, body) = server.request(
        "POST",
        &path("/process-instances"),
        Some(r#"{"processDefinitionId":"demo","variables":{"a":1,"b":2,"c":3}}"#),
    );
    assert_eq!(status, 200, "create with variables failed: {body}");

    let vars = activate_demo_job_variables(&server, None);
    let obj = vars.as_object().expect("variables is an object");
    let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["a", "b", "c"], "created variables present on job");
    assert_eq!(obj["a"].as_i64(), Some(1));
    assert_eq!(obj["b"].as_i64(), Some(2));
    assert_eq!(obj["c"].as_i64(), Some(3));

    server.shutdown();
}
