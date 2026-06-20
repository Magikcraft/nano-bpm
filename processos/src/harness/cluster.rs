//! M3 — the **at-scale measurement** half of the ClusterRunner.
//!
//! The ClusterRunner has two halves: (a) *drive* concurrent load against a real
//! Nano gateway (producer over the v2 REST API + workers over `/command-stream`,
//! reusing the perf-matrix load path), and (b) *measure* the at-scale result. This
//! module is (b): given the live traces a real run produced (read over the T1
//! contract), it computes the signals the per-instance, sequential [`super::sim`]
//! **structurally cannot** — **throughput**, **tail latency (p50/p95/p99)**, and the
//! **queue-vs-service split under contention** — which the latent-exploration
//! analysis (`docs/processos-latent-process-exploration.md` §2) flags as exactly
//! the data means alone hide.
//!
//! It is pure (no I/O): point it at the traces from any loaded cluster (e.g. one
//! the perf-matrix is driving) to get the at-scale view. Driving the load itself
//! is the documented integration boundary; the science lives here and is tested.

use serde::Serialize;

use crate::contracts::{InstanceTrace, NanoClient, TraceSummary};

/// The at-scale result of a cluster run for one process: throughput + latency
/// distribution + the queue/service decomposition under load. This is the
/// ClusterRunner's analogue of [`super::sim::InstanceRun`], but *aggregate and
/// contention-aware* rather than per-instance and isolated.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRunSummary {
    pub process_id: String,
    /// Instances observed in the window.
    pub instances: usize,
    pub completed: usize,
    pub terminated: usize,
    /// Instances still running at read time.
    pub active: usize,
    /// Instances carrying at least one incident.
    pub with_incidents: usize,
    /// Wall-clock span from the earliest start to the latest end, in ms. `None`
    /// when fewer than one ended instance pins the window.
    pub window_ms: Option<u64>,
    /// Completed instances per second across the window — the throughput signal
    /// the isolated SimRunner cannot produce. `None` when the window is undefined
    /// or zero-length.
    pub throughput_per_sec: Option<f64>,
    /// End-to-end duration percentiles over instances that reported a duration.
    pub e2e_p50_ms: Option<u64>,
    pub e2e_p95_ms: Option<u64>,
    pub e2e_p99_ms: Option<u64>,
    /// Mean broker-wait (queue) and worker (service) ms across sampled jobs — the
    /// decomposition that tells "fixable by scaling" from "needs a structural
    /// change". Populated only when trace *details* are supplied.
    pub avg_queue_ms: Option<u64>,
    pub avg_service_ms: Option<u64>,
    /// Per-job-type resource breakdown — the granularity the worker-count question
    /// ("how many workers does *this job type* need to hold p99 < X?") and the
    /// regime-(b) queueing simulator are fitted from (see
    /// `processos-deployment-cooptimization.md` §4.2). Empty when no details.
    pub by_job_type: Vec<JobTypeStat>,
}

/// One job type's contention profile across the sampled run: how many jobs ran,
/// and the queue (broker-wait) vs service (worker) split. A high `avg_queue_ms`
/// with low `avg_service_ms` points at *too few workers* (scale); the reverse
/// points at a *slow worker* (bind/substitute) — the lever the split discriminates.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobTypeStat {
    pub job_type: String,
    /// Number of sampled job occurrences of this type.
    pub samples: usize,
    pub avg_queue_ms: Option<u64>,
    pub avg_service_ms: Option<u64>,
    /// Total job failures observed for this type.
    pub failures: u32,
}

/// Summarize a cluster run for `process_id` from its live traces. `summaries`
/// (cheap list rows) drive throughput + the e2e distribution; `details` (one
/// request each, optional) add the queue/service split. Pure and deterministic.
pub fn summarize_run(
    process_id: &str,
    summaries: &[TraceSummary],
    details: &[InstanceTrace],
) -> ClusterRunSummary {
    let mine: Vec<&TraceSummary> = summaries
        .iter()
        .filter(|s| s.process_id == process_id)
        .collect();

    let mut completed = 0usize;
    let mut terminated = 0usize;
    let mut active = 0usize;
    let mut with_incidents = 0usize;
    let mut durations: Vec<u64> = Vec::new();
    let mut min_start: Option<u64> = None;
    let mut max_end: Option<u64> = None;

    for s in &mine {
        match s.outcome.as_str() {
            "completed" => completed += 1,
            "terminated" => terminated += 1,
            _ => active += 1,
        }
        if s.incident_count > 0 {
            with_incidents += 1;
        }
        if let Some(d) = s.duration_ms {
            durations.push(d);
        }
        min_start = Some(min_start.map_or(s.started_at, |m| m.min(s.started_at)));
        if let Some(e) = s.ended_at {
            max_end = Some(max_end.map_or(e, |m| m.max(e)));
        }
    }

    durations.sort_unstable();

    // Throughput: completed instances over the wall-clock window they spanned.
    let window_ms = match (min_start, max_end) {
        (Some(a), Some(b)) if b > a => Some(b - a),
        _ => None,
    };
    let throughput_per_sec = window_ms
        .filter(|w| *w > 0)
        .map(|w| completed as f64 / (w as f64 / 1000.0));

    // Queue/service split from details (optional, more expensive to fetch), kept
    // both in aggregate and broken down per job type.
    let mut queue_sum = 0u64;
    let mut queue_n = 0usize;
    let mut service_sum = 0u64;
    let mut service_n = 0usize;
    // job_type -> (queue_sum, queue_n, service_sum, service_n, samples, failures)
    let mut by_type: std::collections::BTreeMap<String, JobTypeAcc> =
        std::collections::BTreeMap::new();
    for t in details.iter().filter(|t| t.process_id == process_id) {
        for e in &t.elements {
            if let Some(j) = &e.job {
                let acc = by_type.entry(j.job_type.clone()).or_default();
                acc.samples += 1;
                acc.failures += j.failures;
                if let Some(q) = j.queue_ms {
                    queue_sum += q;
                    queue_n += 1;
                    acc.queue_sum += q;
                    acc.queue_n += 1;
                }
                if let Some(sv) = j.service_ms {
                    service_sum += sv;
                    service_n += 1;
                    acc.service_sum += sv;
                    acc.service_n += 1;
                }
            }
        }
    }

    let by_job_type: Vec<JobTypeStat> = by_type
        .into_iter()
        .map(|(job_type, a)| JobTypeStat {
            job_type,
            samples: a.samples,
            avg_queue_ms: mean(a.queue_sum, a.queue_n),
            avg_service_ms: mean(a.service_sum, a.service_n),
            failures: a.failures,
        })
        .collect();

    ClusterRunSummary {
        process_id: process_id.to_string(),
        instances: mine.len(),
        completed,
        terminated,
        active,
        with_incidents,
        window_ms,
        throughput_per_sec,
        e2e_p50_ms: percentile(&durations, 50.0),
        e2e_p95_ms: percentile(&durations, 95.0),
        e2e_p99_ms: percentile(&durations, 99.0),
        avg_queue_ms: mean(queue_sum, queue_n),
        avg_service_ms: mean(service_sum, service_n),
        by_job_type,
    }
}

/// Per-job-type running aggregate while folding trace details.
#[derive(Default)]
struct JobTypeAcc {
    samples: usize,
    failures: u32,
    queue_sum: u64,
    queue_n: usize,
    service_sum: u64,
    service_n: usize,
}

/// Read the live dataset from Nano and summarize the at-scale run for `process_id`
/// (or the busiest sampled process when `None`). Network I/O is confined here; the
/// aggregation is the pure [`summarize_run`]. Errors only on a read-contract
/// failure or when no live traces exist.
pub async fn build_cluster_summary(
    nano: &NanoClient,
    process_id: Option<&str>,
    limit: usize,
    sample: usize,
) -> Result<ClusterRunSummary, String> {
    let summaries = nano.list_traces(limit).await?;
    if summaries.is_empty() {
        return Err(
            "no live traces to summarize; load the cluster (e.g. via the perf-matrix) first"
                .to_string(),
        );
    }

    let target = match process_id {
        Some(id) => id.to_string(),
        None => busiest_process(&summaries)
            .ok_or_else(|| "no process could be selected from live traces".to_string())?,
    };
    if !summaries.iter().any(|s| s.process_id == target) {
        return Err(format!("process id '{target}' has no sampled live traces"));
    }

    // Pull a bounded sample of details (for the queue/service split).
    let take = sample.min(summaries.len());
    let mut details: Vec<InstanceTrace> = Vec::with_capacity(take);
    for s in summaries.iter().filter(|s| s.process_id == target).take(take) {
        if let Ok(t) = nano.trace(&s.instance_key).await {
            details.push(t);
        }
    }

    Ok(summarize_run(&target, &summaries, &details))
}

/// The process id with the most rows in `summaries`.
fn busiest_process(summaries: &[TraceSummary]) -> Option<String> {
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for s in summaries {
        *counts.entry(s.process_id.as_str()).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by_key(|(_, n)| *n)
        .map(|(p, _)| p.to_string())
}

fn mean(sum: u64, n: usize) -> Option<u64> {
    if n == 0 {
        None
    } else {
        Some(sum / n as u64)
    }
}

/// Nearest-rank percentile over an already-sorted slice.
fn percentile(sorted: &[u64], pct: f64) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (pct / 100.0 * sorted.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted.len() - 1);
    Some(sorted[idx])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(key: &str, proc: &str, outcome: &str, start: u64, end: u64, dur: u64) -> TraceSummary {
        TraceSummary {
            instance_key: key.into(),
            process_id: proc.into(),
            version: Some(1),
            business_id: None,
            outcome: outcome.into(),
            started_at: start,
            ended_at: Some(end),
            duration_ms: Some(dur),
            element_count: 2,
            incident_count: 0,
        }
    }

    #[test]
    fn throughput_and_tail_over_the_window() {
        // 4 completed `order` instances spanning [1000, 3000] => 2s window => 2/s.
        let summaries = vec![
            summary("1", "order", "completed", 1000, 1500, 500),
            summary("2", "order", "completed", 1200, 1900, 700),
            summary("3", "order", "completed", 2000, 2600, 600),
            summary("4", "order", "completed", 2500, 3000, 900),
            // a different process is ignored
            summary("9", "other", "completed", 1000, 5000, 4000),
        ];
        let s = summarize_run("order", &summaries, &[]);
        assert_eq!(s.instances, 4);
        assert_eq!(s.completed, 4);
        assert_eq!(s.window_ms, Some(2000));
        assert_eq!(s.throughput_per_sec, Some(2.0));
        // durations sorted: 500,600,700,900 ; nearest-rank p95/p99 => 900.
        assert_eq!(s.e2e_p50_ms, Some(600));
        assert_eq!(s.e2e_p99_ms, Some(900));
    }

    #[test]
    fn counts_outcomes_and_incidents() {
        let mut a = summary("1", "p", "completed", 0, 100, 100);
        a.incident_count = 1;
        let summaries = vec![
            a,
            summary("2", "p", "terminated", 0, 50, 50),
            {
                let mut act = summary("3", "p", "active", 10, 10, 0);
                act.ended_at = None;
                act.duration_ms = None;
                act
            },
        ];
        let s = summarize_run("p", &summaries, &[]);
        assert_eq!(s.completed, 1);
        assert_eq!(s.terminated, 1);
        assert_eq!(s.active, 1);
        assert_eq!(s.with_incidents, 1);
    }

    #[test]
    fn queue_service_split_from_details() {
        use crate::contracts::{Element, InstanceTrace, Job};
        let detail = InstanceTrace {
            instance_key: "1".into(),
            process_id: "order".into(),
            version: Some(1),
            outcome: "completed".into(),
            duration_ms: Some(100),
            elements: vec![Element {
                element_id: "charge".into(),
                duration_ms: Some(80),
                incidents: 0,
                job: Some(Job {
                    job_type: "payment".into(),
                    queue_ms: Some(60),
                    service_ms: Some(20),
                    failures: 0,
                }),
            }],
            incidents: vec![],
        };
        let summaries = vec![summary("1", "order", "completed", 0, 100, 100)];
        let s = summarize_run("order", &summaries, &[detail]);
        assert_eq!(s.avg_queue_ms, Some(60));
        assert_eq!(s.avg_service_ms, Some(20));
        // The same split appears per job type.
        assert_eq!(s.by_job_type.len(), 1);
        let jt = &s.by_job_type[0];
        assert_eq!(jt.job_type, "payment");
        assert_eq!(jt.samples, 1);
        assert_eq!(jt.avg_queue_ms, Some(60));
        assert_eq!(jt.avg_service_ms, Some(20));
    }

    #[test]
    fn per_job_type_breakdown_discriminates_the_lever() {
        use crate::contracts::{Element, InstanceTrace, Job};
        // `classify` is queue-bound (60 queue / 5 service => scale workers);
        // `summarize` is service-bound (5 queue / 90 service => slow worker).
        let job = |ty: &str, q: u64, sv: u64, f: u32| Element {
            element_id: ty.into(),
            duration_ms: Some(q + sv),
            incidents: 0,
            job: Some(Job {
                job_type: ty.into(),
                queue_ms: Some(q),
                service_ms: Some(sv),
                failures: f,
            }),
        };
        let detail = InstanceTrace {
            instance_key: "1".into(),
            process_id: "order".into(),
            version: Some(1),
            outcome: "completed".into(),
            duration_ms: Some(160),
            elements: vec![job("classify", 60, 5, 0), job("summarize", 5, 90, 2)],
            incidents: vec![],
        };
        let summaries = vec![summary("1", "order", "completed", 0, 160, 160)];
        let s = summarize_run("order", &summaries, &[detail]);
        let by: std::collections::HashMap<_, _> =
            s.by_job_type.iter().map(|j| (j.job_type.as_str(), j)).collect();
        let classify = by["classify"];
        let summarize = by["summarize"];
        // queue-bound: queue >> service
        assert!(classify.avg_queue_ms.unwrap() > classify.avg_service_ms.unwrap());
        // service-bound: service >> queue, and its failures are attributed here
        assert!(summarize.avg_service_ms.unwrap() > summarize.avg_queue_ms.unwrap());
        assert_eq!(summarize.failures, 2);
    }

    #[test]
    fn undefined_window_yields_no_throughput() {
        // All instances share a single instant => zero-length window.
        let summaries = vec![summary("1", "p", "completed", 5, 5, 0)];
        let s = summarize_run("p", &summaries, &[]);
        assert_eq!(s.window_ms, None);
        assert_eq!(s.throughput_per_sec, None);
    }
}
