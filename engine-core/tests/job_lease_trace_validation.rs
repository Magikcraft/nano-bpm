//! Trace-validation conformance for the **JobLease** spec (#1227, epic #1224):
//! nano's job lease / activation semantics (ADR 0002; ADR 0001/0017) anchored to
//! the real engine, so the TLA+ model in `formal/tla/joblease/JobLease.tla`
//! cannot silently drift from `engine-core`.
//!
//! This reuses the spec-agnostic #1226 replay harness
//! (`engine-core/tests/trace_validation/harness.rs`) rather than copying its
//! deploy/drive/compare loop:
//!
//!   * The **activation lifecycle** (create -> activate -> complete) is anchored
//!     through the harness `TraceMapping` + `validate` entry point, exactly as
//!     `token_flow.rs` does: a one-job JobLease model is a single-service-task
//!     process, and the engine's observable milestone multiset must equal the
//!     spec's.
//!   * The **lease safety** the token-flow milestone vocabulary cannot express
//!     — exclusivity (at-most-one live holder), reclaim after expiry, and
//!     done-is-terminal — is anchored by replaying committed JobLease behaviours
//!     (`trace_validation/joblease/*.json`, each an admitted `JobLease.tla`
//!     behaviour) against `Engine::apply_command`, driving the engine's
//!     `ActivateJobs` / `ExpireJobs` / `CompleteJob` surface and asserting its
//!     job-lease `Event`s and the two spec invariants at every step. A
//!     divergence fails the test — no tolerated mismatch, no retries.
//!
//! The lease pipeline deliberately does **not** go through the TokenFlow
//! `gen-traces.sh` / `parse.mjs` fixtures (whose `pending`/`waiting`/`fireCount`
//! observable vocabulary is token-flow specific and owned by #1240): a
//! lease/expiry behaviour is a per-state safety trace, not an
//! interleaving-invariant milestone multiset, so the JobLease descriptor
//! declares no `SPEC_TRACE_MODELS` and this test carries its own behaviours.

#[path = "trace_validation/harness.rs"]
mod harness;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use nanobpmn_engine_core::{Command, Engine, Event, ProcessBuilder, ProcessDefinition};
use serde_json::Value;

use harness::{Fixture, Graph, Milestone, TraceMapping};

// ---------------------------------------------------------------------------
// Activation-lifecycle anchor: reuse the #1226 harness driver.

/// A one-job JobLease model as a single-service-task process: the job's
/// create -> activate -> complete lifecycle is the engine's job lifecycle. This
/// is the JobLease binding of the shared `TraceMapping` seam.
struct JobLeaseLifecycleMapping;

impl TraceMapping for JobLeaseLifecycleMapping {
    fn process_id(&self, fixture: &Fixture) -> String {
        fixture.model.clone()
    }

    fn build_process(&self, fixture: &Fixture) -> ProcessDefinition {
        let g = &fixture.graph;
        let mut b = ProcessBuilder::new(fixture.model.clone());
        for (id, kind) in &g.nodes {
            b = match kind.as_str() {
                "start" => b.start_event(id.clone()),
                "end" => b.end_event(id.clone()),
                "task" => b.service_task(id.clone(), id.clone()),
                other => panic!("unmapped JobLease node kind {other:?}"),
            };
        }
        for (_id, from, to) in &g.flows {
            b = b.connect(from.clone(), to.clone());
        }
        b.build()
            .expect("JobLease lifecycle graph builds a valid process")
    }

    fn engine_milestones(&self, events: &[Event]) -> Vec<Milestone> {
        let mut out = Vec::new();
        for e in events {
            match e {
                Event::SequenceFlowTaken { from, to, .. } => out.push(Milestone::Flow {
                    from: from.to_string(),
                    to: to.to_string(),
                }),
                Event::JobCreated { element_id, .. } => out.push(Milestone::Task {
                    node: element_id.to_string(),
                }),
                Event::ProcessInstanceCompleted { .. } => out.push(Milestone::Completed),
                _ => {}
            }
        }
        out
    }
}

/// The activation lifecycle of a single leased job conforms to the engine: a
/// created job is activated to exactly one holder and driven to completion,
/// producing the spec's milestone multiset. Reuses `harness::validate`.
#[test]
fn job_lease_lifecycle_conforms_to_engine() {
    let mut nodes = BTreeMap::new();
    nodes.insert("start".to_string(), "start".to_string());
    nodes.insert("work".to_string(), "task".to_string());
    nodes.insert("end".to_string(), "end".to_string());
    let fixture = Fixture {
        spec: "JobLease".to_string(),
        model: "lease-lifecycle".to_string(),
        graph: Graph {
            start: "start".to_string(),
            nodes,
            flows: vec![
                ("f1".to_string(), "start".to_string(), "work".to_string()),
                ("f2".to_string(), "work".to_string(), "end".to_string()),
            ],
        },
        milestones: vec![
            Milestone::Flow {
                from: "start".to_string(),
                to: "work".to_string(),
            },
            Milestone::Task {
                node: "work".to_string(),
            },
            Milestone::Flow {
                from: "work".to_string(),
                to: "end".to_string(),
            },
            Milestone::Completed,
        ],
    };
    harness::validate(&JobLeaseLifecycleMapping, &fixture)
        .unwrap_or_else(|e| panic!("JobLease lifecycle anchor: {e}"));
}

// ---------------------------------------------------------------------------
// Lease-safety anchor: replay committed JobLease behaviours.

fn joblease_traces_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/trace_validation/joblease")
}

fn load_lease_fixtures() -> Vec<(String, Value)> {
    let dir = joblease_traces_dir();
    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|r| r.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    entries.sort();
    let mut out = Vec::new();
    for path in entries {
        let text = std::fs::read_to_string(&path).expect("read fixture");
        let v: Value =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
        out.push((path.display().to_string(), v));
    }
    assert!(
        !out.is_empty(),
        "no committed JobLease trace fixtures under {}",
        dir.display()
    );
    out
}

/// Deploy one single-service-task process per abstract job (job_type == job id)
/// and return the engine plus the stable `job id -> job key` map.
fn setup_jobs(jobs: &[String]) -> Result<(Engine, BTreeMap<String, u64>), String> {
    let mut engine = Engine::new();
    let mut keys = BTreeMap::new();
    for job in jobs {
        let pid = format!("lease-{job}");
        let def = ProcessBuilder::new(pid.clone())
            .start_event("start")
            .service_task("work", job.clone())
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .map_err(|e| format!("build process for {job}: {e:?}"))?;
        engine
            .apply_command(Command::DeployProcess(def))
            .map_err(|e| format!("deploy {job}: {e:?}"))?;
        let events = engine
            .apply_command(Command::create_instance(pid))
            .map_err(|e| format!("create instance {job}: {e:?}"))?;
        let key = events
            .iter()
            .find_map(|e| match e {
                Event::JobCreated {
                    job_key, job_type, ..
                } if job_type == job => Some(*job_key),
                _ => None,
            })
            .ok_or_else(|| format!("no JobCreated for job {job}"))?;
        keys.insert(job.clone(), key);
    }
    Ok((engine, keys))
}

/// The `activated_leases()` invariant: a job never appears twice, i.e. no job
/// ever has two concurrent live holders (the engine-observable counterpart of
/// `AtMostOneLiveHolder`).
fn assert_at_most_one_live_holder(engine: &Engine) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for (key, _deadline) in engine.activated_leases() {
        if !seen.insert(key) {
            return Err(format!(
                "AtMostOneLiveHolder violated: job {key} has two concurrent live leases"
            ));
        }
    }
    Ok(())
}

fn str_at(v: &Value, key: &str) -> Result<String, String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("step missing/!string `{key}`"))
}

fn u64_at(v: &Value, key: &str) -> Result<u64, String> {
    v.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("step missing/!u64 `{key}`"))
}

/// Replay one committed JobLease behaviour against the engine, asserting the
/// job-lease `Event`s and the spec invariants at every step. Returns `Err`
/// (never panics) on divergence so the detector test can prove the replay is not
/// vacuous.
fn replay_lease_behaviour(fixture: &Value) -> Result<(), String> {
    let model = str_at(fixture, "model")?;
    let timeout = u64_at(fixture, "timeout")?;
    let jobs: Vec<String> = fixture
        .get("jobs")
        .and_then(Value::as_array)
        .ok_or("fixture missing `jobs`")?
        .iter()
        .map(|j| {
            j.as_str()
                .map(str::to_string)
                .ok_or("job id must be a string".to_string())
        })
        .collect::<Result<_, _>>()?;

    let (mut engine, keys) = setup_jobs(&jobs)?;
    let by_key: BTreeMap<u64, String> = keys.iter().map(|(j, k)| (*k, j.clone())).collect();
    let mut done: BTreeSet<String> = BTreeSet::new();

    let steps = fixture
        .get("steps")
        .and_then(Value::as_array)
        .ok_or("fixture missing `steps`")?;

    for (i, step) in steps.iter().enumerate() {
        let ctx = |msg: String| {
            format!(
                "{model} step {i} ({}): {msg}",
                str_at(step, "action").unwrap_or_default()
            )
        };
        match str_at(step, "action")?.as_str() {
            "activate" => {
                let job = str_at(step, "job")?;
                let worker = str_at(step, "worker")?;
                let now = u64_at(step, "now")?;
                let expect_leased = step
                    .get("expect_leased")
                    .and_then(Value::as_bool)
                    .ok_or_else(|| ctx("missing/!bool `expect_leased`".into()))?;
                let activated = engine.activate_jobs(job.clone(), worker.clone(), 1, timeout, now);
                if expect_leased {
                    if activated.len() != 1 {
                        return Err(ctx(format!(
                            "expected {worker} to lease {job}, but {} jobs activated",
                            activated.len()
                        )));
                    }
                    let a = &activated[0];
                    let want_key = *keys.get(&job).ok_or_else(|| ctx("unknown job".into()))?;
                    if a.key != want_key {
                        return Err(ctx(format!(
                            "activated key {} != job key {want_key}",
                            a.key
                        )));
                    }
                    if a.worker != worker {
                        return Err(ctx(format!("holder {} != expected {worker}", a.worker)));
                    }
                    if a.deadline != now + timeout {
                        return Err(ctx(format!(
                            "deadline {} != now+timeout {}",
                            a.deadline,
                            now + timeout
                        )));
                    }
                } else if !activated.is_empty() {
                    return Err(ctx(format!(
                        "expected {job} to be unavailable to {worker} (live lock held), but it activated"
                    )));
                }
                assert_at_most_one_live_holder(&engine).map_err(ctx)?;
            }
            "expire" => {
                let now = u64_at(step, "now")?;
                let expect: BTreeSet<String> = step
                    .get("expect_expired")
                    .and_then(Value::as_array)
                    .ok_or_else(|| ctx("missing `expect_expired` array".into()))?
                    .iter()
                    .map(|j| {
                        j.as_str()
                            .map(str::to_string)
                            .ok_or_else(|| ctx("job id must be a string".into()))
                    })
                    .collect::<Result<_, _>>()?;
                let events = engine.expire_jobs(now);
                let got: BTreeSet<String> = events
                    .iter()
                    .filter_map(|e| match e {
                        Event::JobLockExpired { job_key, .. } => by_key.get(job_key).cloned(),
                        _ => None,
                    })
                    .collect();
                if got != expect {
                    return Err(ctx(format!("reclaimed {got:?} but expected {expect:?}")));
                }
                assert_at_most_one_live_holder(&engine).map_err(ctx)?;
            }
            "complete" => {
                let job = str_at(step, "job")?;
                let key = *keys.get(&job).ok_or_else(|| ctx("unknown job".into()))?;
                let events = engine
                    .apply_command(Command::complete_job(key))
                    .map_err(|e| ctx(format!("complete {job}: {e:?}")))?;
                if !events
                    .iter()
                    .any(|e| matches!(e, Event::JobCompleted { job_key, .. } if *job_key == key))
                {
                    return Err(ctx(format!("no JobCompleted for {job}")));
                }
                done.insert(job.clone());
                // DoneIsTerminal: a completed job is never redelivered — reclaim
                // can never resurrect it, so it is not activatable again.
                let re = engine.activate_jobs(job.clone(), "done-check", 1, timeout, 0);
                if !re.is_empty() {
                    return Err(ctx(format!(
                        "DoneIsTerminal violated: completed job {job} was re-activated"
                    )));
                }
                assert_at_most_one_live_holder(&engine).map_err(ctx)?;
            }
            other => return Err(ctx(format!("unknown action {other:?}"))),
        }
    }
    Ok(())
}

/// Every committed JobLease behaviour replays against the engine with matching
/// lease events and no invariant violation.
#[test]
fn job_lease_traces_conform_to_engine() {
    for (path, fixture) in load_lease_fixtures() {
        replay_lease_behaviour(&fixture).unwrap_or_else(|e| panic!("{path}: {e}"));
    }
}

/// The replay must actually *catch* a divergence, or the conformance test above
/// would be vacuous. Tamper with the `exclusive-lease` behaviour so it expects
/// the second, concurrent activation to succeed, and assert the replay reports
/// the divergence (Red/Green: the detector is proven to fire).
#[test]
fn replay_detects_lease_divergence() {
    let (_, mut fixture) = load_lease_fixtures()
        .into_iter()
        .find(|(_, v)| v.get("model").and_then(Value::as_str) == Some("exclusive-lease"))
        .expect("the exclusive-lease behaviour is committed");
    // Flip the exclusivity expectation: claim the second worker CAN lease the
    // already-live job. The engine refuses, so the replay must diverge.
    fixture["steps"][1]["expect_leased"] = Value::Bool(true);
    let result = replay_lease_behaviour(&fixture);
    assert!(
        result.is_err(),
        "replay must fail when the engine diverges from the (tampered) behaviour"
    );
    assert!(
        result.unwrap_err().contains("to lease"),
        "the divergence report should name the exclusivity mismatch"
    );
}
