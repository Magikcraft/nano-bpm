//! Stage T1 **Insights** — fold Nano's exported traces into a per-process /
//! per-element performance report. This is the read-only foundation every later
//! ProcessOS capability (cost model, canary, reasoning) builds on: faithfully
//! surface "what happened" before anything counterfactual or autonomous is added.
//!
//! Pure aggregation over the read-contract DTOs — no engine, no LLM, no writes.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::contracts::{InstanceTrace, Metrics, NanoClient, TraceSummary};

/// The full Insights document served at `GET /api/insights`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Insights {
    pub generated_at_ms: u64,
    pub nano_base_url: String,
    /// Number of trace details sampled to build the per-element aggregates.
    pub sampled_instances: usize,
    pub totals: Totals,
    pub live: Option<Metrics>,
    pub processes: Vec<ProcessInsight>,
    pub incident_clusters: Vec<IncidentCluster>,
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Totals {
    pub instances: usize,
    pub completed: usize,
    pub terminated: usize,
    pub active: usize,
    pub incidents: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessInsight {
    pub process_id: String,
    pub instances: usize,
    pub completed: usize,
    pub incident_instances: usize,
    pub avg_duration_ms: Option<u64>,
    pub p95_duration_ms: Option<u64>,
    /// The element with the highest average self-duration — the headline bottleneck.
    pub bottleneck: Option<ElementInsight>,
    pub elements: Vec<ElementInsight>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ElementInsight {
    pub element_id: String,
    pub count: usize,
    pub avg_duration_ms: Option<u64>,
    /// For service tasks: average time a job waited in queue before activation.
    pub avg_queue_ms: Option<u64>,
    /// For service tasks: average worker service time once activated.
    pub avg_service_ms: Option<u64>,
    pub incidents: u32,
    pub job_failures: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IncidentCluster {
    pub element_id: String,
    pub kind: String,
    pub count: usize,
    pub top_reason: String,
}

/// Fetch recent traces from Nano and fold them into an Insights report. `sample`
/// bounds how many trace *details* we pull for per-element aggregation (the list
/// gives totals cheaply; details cost one request each).
pub async fn build(nano: &NanoClient, limit: usize, sample: usize) -> Result<Insights, String> {
    let summaries = nano.list_traces(limit).await?;
    let live = nano.metrics().await.ok();

    let totals = totals_of(&summaries);

    // Pull details for a bounded sample (most recent first) for element-level folds.
    let take = sample.min(summaries.len());
    let mut details: Vec<InstanceTrace> = Vec::with_capacity(take);
    for s in summaries.iter().take(take) {
        if let Ok(t) = nano.trace(&s.instance_key).await {
            details.push(t);
        }
    }

    let processes = fold_processes(&details);
    let incident_clusters = fold_incident_clusters(&details);

    Ok(Insights {
        generated_at_ms: now_ms(),
        nano_base_url: nano.base_url().to_string(),
        sampled_instances: details.len(),
        totals,
        live,
        processes,
        incident_clusters,
    })
}

fn totals_of(summaries: &[TraceSummary]) -> Totals {
    let mut t = Totals {
        instances: summaries.len(),
        ..Default::default()
    };
    for s in summaries {
        match s.outcome.as_str() {
            "completed" => t.completed += 1,
            "terminated" => t.terminated += 1,
            _ => t.active += 1,
        }
        t.incidents += s.incident_count;
    }
    t
}

/// Per-element running aggregate while folding instance traces.
#[derive(Default)]
struct ElemAcc {
    count: usize,
    dur_sum: u64,
    dur_n: usize,
    queue_sum: u64,
    queue_n: usize,
    service_sum: u64,
    service_n: usize,
    incidents: u32,
    job_failures: u32,
}

#[derive(Default)]
struct ProcAcc {
    instances: usize,
    completed: usize,
    incident_instances: usize,
    durations: Vec<u64>,
    elements: BTreeMap<String, ElemAcc>,
}

fn fold_processes(details: &[InstanceTrace]) -> Vec<ProcessInsight> {
    let mut by_proc: BTreeMap<String, ProcAcc> = BTreeMap::new();

    for t in details {
        let p = by_proc.entry(t.process_id.clone()).or_default();
        p.instances += 1;
        if t.outcome == "completed" {
            p.completed += 1;
        }
        if !t.incidents.is_empty() {
            p.incident_instances += 1;
        }
        if let Some(d) = t.duration_ms {
            p.durations.push(d);
        }
        for e in &t.elements {
            let acc = p.elements.entry(e.element_id.clone()).or_default();
            acc.count += 1;
            if let Some(d) = e.duration_ms {
                acc.dur_sum += d;
                acc.dur_n += 1;
            }
            acc.incidents += e.incidents;
            if let Some(j) = &e.job {
                acc.job_failures += j.failures;
                if let Some(q) = j.queue_ms {
                    acc.queue_sum += q;
                    acc.queue_n += 1;
                }
                if let Some(s) = j.service_ms {
                    acc.service_sum += s;
                    acc.service_n += 1;
                }
            }
        }
    }

    by_proc
        .into_iter()
        .map(|(process_id, mut p)| {
            let elements: Vec<ElementInsight> = p
                .elements
                .iter()
                .map(|(id, a)| ElementInsight {
                    element_id: id.clone(),
                    count: a.count,
                    avg_duration_ms: mean(a.dur_sum, a.dur_n),
                    avg_queue_ms: mean(a.queue_sum, a.queue_n),
                    avg_service_ms: mean(a.service_sum, a.service_n),
                    incidents: a.incidents,
                    job_failures: a.job_failures,
                })
                .collect();

            // Bottleneck = element with the greatest average self-duration.
            let bottleneck = elements
                .iter()
                .filter(|e| e.avg_duration_ms.is_some())
                .max_by_key(|e| e.avg_duration_ms.unwrap_or(0))
                .cloned();

            p.durations.sort_unstable();
            ProcessInsight {
                process_id,
                instances: p.instances,
                completed: p.completed,
                incident_instances: p.incident_instances,
                avg_duration_ms: mean(p.durations.iter().sum(), p.durations.len()),
                p95_duration_ms: percentile(&p.durations, 95.0),
                bottleneck,
                elements,
            }
        })
        .collect()
}

fn fold_incident_clusters(details: &[InstanceTrace]) -> Vec<IncidentCluster> {
    // (element_id, kind) -> (count, reason histogram)
    let mut clusters: BTreeMap<(String, String), (usize, BTreeMap<String, usize>)> =
        BTreeMap::new();
    for t in details {
        for inc in &t.incidents {
            let entry = clusters
                .entry((inc.element_id.clone(), inc.kind.clone()))
                .or_default();
            entry.0 += 1;
            *entry.1.entry(inc.reason.clone()).or_default() += 1;
        }
    }

    let mut out: Vec<IncidentCluster> = clusters
        .into_iter()
        .map(|((element_id, kind), (count, reasons))| {
            let top_reason = reasons
                .into_iter()
                .max_by_key(|(_, n)| *n)
                .map(|(r, _)| r)
                .unwrap_or_default();
            IncidentCluster {
                element_id,
                kind,
                count,
                top_reason,
            }
        })
        .collect();
    out.sort_by(|a, b| b.count.cmp(&a.count));
    out
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_nearest_rank() {
        let v = vec![10u64, 20, 30, 40, 50];
        assert_eq!(percentile(&v, 95.0), Some(50));
        assert_eq!(percentile(&v, 50.0), Some(30));
        assert_eq!(percentile(&[], 95.0), None);
    }

    #[test]
    fn mean_guards_zero() {
        assert_eq!(mean(0, 0), None);
        assert_eq!(mean(100, 4), Some(25));
    }

    #[test]
    fn folds_elements_and_bottleneck() {
        use crate::contracts::{Element, Incident, InstanceTrace, Job};
        let t = InstanceTrace {
            instance_key: "1".into(),
            process_id: "p".into(),
            version: Some(1),
            outcome: "completed".into(),
            duration_ms: Some(100),
            elements: vec![
                Element {
                    element_id: "fast".into(),
                    duration_ms: Some(5),
                    incidents: 0,
                    job: None,
                },
                Element {
                    element_id: "slow".into(),
                    duration_ms: Some(80),
                    incidents: 0,
                    job: Some(Job {
                        job_type: "t".into(),
                        queue_ms: Some(60),
                        service_ms: Some(20),
                        failures: 2,
                    }),
                },
            ],
            incidents: vec![Incident {
                element_id: "slow".into(),
                kind: "JOB_NO_RETRIES".into(),
                reason: "boom".into(),
            }],
        };
        let procs = fold_processes(&[t.clone()]);
        assert_eq!(procs.len(), 1);
        let p = &procs[0];
        assert_eq!(p.instances, 1);
        assert_eq!(p.bottleneck.as_ref().unwrap().element_id, "slow");
        assert_eq!(p.bottleneck.as_ref().unwrap().avg_service_ms, Some(20));

        let clusters = fold_incident_clusters(&[t]);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].element_id, "slow");
        assert_eq!(clusters[0].top_reason, "boom");
    }
}
