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

    let (status, body) = server.request("POST", &path("/process-instances/search"), Some(r#"{}"#));
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
    let (status, body) =
        server.request("POST", &path("/process-definitions/search"), Some(r#"{}"#));
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

    let (status, body) =
        server.request("POST", &path("/process-definitions/search"), Some(r#"{}"#));
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
    for key in &keys {
        let (status, body) =
            server.request("GET", &path(&format!("/process-instances/{key}")), None);
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
