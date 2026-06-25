//! Regime-(b) **queueing simulator** — the model-simulable middle the
//! `processos-deployment-cooptimization.md` §4.2 calls for, sitting between the
//! logic-faithful offline `SimRunner` (regime a) and the live act-and-measure of the
//! `ClusterRunner` (regime c).
//!
//! It does **not** replay the engine. It is an `M/M/c` (Erlang-C) worker-pool model
//! *fitted from the ClusterRunner's measured distributions* — per-job-type arrival
//! rate (λ, from `samples` over the run `window`) and mean service time (S, from
//! `avgServiceMs`) — and it answers the one question the isolated engine structurally
//! cannot: **how many workers does this job type need to hold p99 queue-wait < X?**
//!
//! It ranks scaling candidates *before* the live act-and-measure of regime (c), so a
//! recommendation is grounded in measured load yet costs no cluster time to obtain.
//! Pure and deterministic; the math is unit-tested against known M/M/c values.
//!
//! Scope discipline: this is a *recommendation* (advisory). It actuates nothing —
//! turning a recommendation into an applied scaling action is the deferred third
//! "scaling" control verb with its own public-API design, not part of this model.

use serde::Serialize;

use super::cluster::ClusterRunSummary;

/// A per-job-type staffing recommendation derived from the measured run: the offered
/// load, the minimum worker count that holds the p99 queue-wait target, and what that
/// staffing is predicted to deliver. `recommendedWorkers` is the actionable number;
/// the rest shows the working so the trade ("p99 < X costs Y more workers") is legible.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStaffing {
    pub job_type: String,
    /// Mean arrival rate fitted from the run: `samples / (window_seconds)`.
    pub arrival_per_sec: f64,
    /// Mean service time per job (the worker's busy time), from `avgServiceMs`.
    pub service_ms: u64,
    /// Offered load in Erlangs (`λ·S`) — the *minimum* workers needed merely for
    /// stability is `ceil(offeredLoadErlangs)`; holding a tail target needs more.
    pub offered_load_erlangs: f64,
    /// Minimum worker count that holds the p99 queue-wait target (and keeps ρ < 1).
    pub recommended_workers: u32,
    /// Predicted p99 *queue* wait (excludes service) at `recommendedWorkers`, ms.
    pub predicted_p99_wait_ms: u64,
    /// Predicted utilization (ρ = load / workers) at `recommendedWorkers`.
    pub predicted_utilization: f64,
    /// Why a recommendation could not be computed (missing service time, no arrivals,
    /// or no run window). When set, the numeric fields are best-effort defaults.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Derive per-job-type staffing recommendations from a measured cluster run, holding
/// the given p99 queue-wait target (ms). One entry per `byJobType` row, in the same
/// order. Pure: it reads only the already-measured summary.
pub fn staff_for_summary(
    summary: &ClusterRunSummary,
    target_p99_wait_ms: u64,
) -> Vec<WorkerStaffing> {
    let window_s = summary
        .window_ms
        .map(|w| w as f64 / 1000.0)
        .filter(|w| *w > 0.0);

    summary
        .by_job_type
        .iter()
        .map(|jt| {
            let service_ms = jt.avg_service_ms.unwrap_or(0);
            match (window_s, jt.avg_service_ms, jt.samples) {
                (Some(win), Some(svc_ms), n) if n > 0 && svc_ms > 0 => {
                    let lambda = n as f64 / win;
                    let plan = min_workers_for_p99(lambda, svc_ms, target_p99_wait_ms);
                    WorkerStaffing {
                        job_type: jt.job_type.clone(),
                        arrival_per_sec: lambda,
                        service_ms: svc_ms,
                        offered_load_erlangs: lambda * (svc_ms as f64 / 1000.0),
                        recommended_workers: plan.workers,
                        predicted_p99_wait_ms: plan.p99_wait_ms,
                        predicted_utilization: plan.utilization,
                        note: None,
                    }
                }
                _ => WorkerStaffing {
                    job_type: jt.job_type.clone(),
                    arrival_per_sec: 0.0,
                    service_ms,
                    offered_load_erlangs: 0.0,
                    recommended_workers: 1,
                    predicted_p99_wait_ms: 0,
                    predicted_utilization: 0.0,
                    note: Some(
                        "insufficient signal to fit the queueing model (need a run window, \
                         arrivals, and a non-zero service time)"
                            .to_string(),
                    ),
                },
            }
        })
        .collect()
}

/// The outcome of sizing a worker pool for one job type.
#[derive(Debug, Clone, Copy)]
pub struct StaffingPlan {
    pub workers: u32,
    pub p99_wait_ms: u64,
    pub utilization: f64,
}

/// Minimum number of M/M/c servers that holds the p99 queue-wait at or below
/// `target_p99_wait_ms`, given arrival rate (per sec) and mean service time (ms).
/// Always returns a stable pool (ρ < 1). A search cap guards against runaway, though
/// in practice the wait tail collapses to zero a few servers past the stability point.
pub fn min_workers_for_p99(
    arrival_per_sec: f64,
    service_ms: u64,
    target_p99_wait_ms: u64,
) -> StaffingPlan {
    let s = service_ms as f64 / 1000.0;
    let a = arrival_per_sec * s; // offered load (Erlangs)

    if a <= 0.0 {
        return StaffingPlan {
            workers: 1,
            p99_wait_ms: 0,
            utilization: 0.0,
        };
    }

    // The smallest integer worker count that is stable (ρ < 1).
    let mut c = (a.floor() as u32) + 1;
    let cap = c + 100_000;
    loop {
        let p99 = p99_wait_ms(c, arrival_per_sec, s);
        if let Some(p99) = p99 {
            if p99 <= target_p99_wait_ms as f64 || c >= cap {
                return StaffingPlan {
                    workers: c,
                    p99_wait_ms: p99.round() as u64,
                    utilization: a / c as f64,
                };
            }
        }
        c += 1;
    }
}

/// Predicted p99 *queue* wait (ms) for `c` M/M/c servers at arrival rate `lambda`
/// (per sec) and service time `s` (sec). `None` when the pool is unstable (c ≤ a).
///
/// In M/M/c the waiting time is `P(Wq > t) = C·exp(-(c−a)/s · t)`, where `C` is the
/// Erlang-C probability of waiting. The p99 wait is the `t` solving `P = 0.01`; when
/// `C ≤ 0.01` (≥99% of jobs never queue) the p99 wait is zero.
pub fn p99_wait_ms(c: u32, lambda: f64, s: f64) -> Option<f64> {
    let a = lambda * s; // Erlangs
    if (c as f64) <= a {
        return None; // unstable
    }
    let cc = erlang_c(c, a);
    if cc <= 0.01 {
        return Some(0.0);
    }
    // t99 in seconds, then to ms.
    let decay = (c as f64 - a) / s; // per second
    let t99 = (cc / 0.01).ln() / decay;
    Some((t99 * 1000.0).max(0.0))
}

/// Erlang-C — the probability that an arriving job finds all `c` servers busy and
/// must wait — for offered load `a` (Erlangs). Derived from Erlang-B for numerical
/// stability: `C = B / (1 − ρ·(1 − B))`, with `ρ = a/c`. Returns 1.0 when unstable.
pub fn erlang_c(c: u32, a: f64) -> f64 {
    let rho = a / c as f64;
    if rho >= 1.0 {
        return 1.0;
    }
    let b = erlang_b(c, a);
    b / (1.0 - rho * (1.0 - b))
}

/// Erlang-B — blocking probability for `c` servers at offered load `a` (Erlangs) —
/// via the numerically stable recurrence `B(0)=1`, `B(k)=a·B(k−1)/(k + a·B(k−1))`.
pub fn erlang_b(c: u32, a: f64) -> f64 {
    let mut b = 1.0_f64;
    for k in 1..=c {
        b = (a * b) / (k as f64 + a * b);
    }
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(x: f64, y: f64, eps: f64) -> bool {
        (x - y).abs() < eps
    }

    #[test]
    fn erlang_c_matches_mm1() {
        // For a single server, Erlang-C reduces to ρ: P(wait) = a.
        assert!(approx(erlang_c(1, 0.5), 0.5, 1e-9));
        assert!(approx(erlang_c(1, 0.25), 0.25, 1e-9));
    }

    #[test]
    fn erlang_c_is_one_when_unstable() {
        assert_eq!(erlang_c(2, 2.0), 1.0);
        assert_eq!(erlang_c(2, 3.0), 1.0);
    }

    #[test]
    fn more_workers_monotonically_cut_the_p99_wait() {
        // Fixed offered load a = 5 Erlangs (λ=10/s, S=0.5s); adding servers can only
        // reduce the predicted p99 wait.
        let (lambda, s) = (10.0, 0.5);
        let mut prev = f64::INFINITY;
        for c in 6..=20 {
            let w = p99_wait_ms(c, lambda, s).unwrap();
            assert!(w <= prev + 1e-9, "c={c} w={w} prev={prev}");
            prev = w;
        }
    }

    #[test]
    fn recommendation_is_stable_and_meets_the_target() {
        // λ=10/s, S=0.5s => a=5 Erlangs; hold p99 queue-wait <= 100ms.
        let plan = min_workers_for_p99(10.0, 500, 100);
        // Must be stable (more servers than the offered load).
        assert!(plan.workers as f64 > 5.0);
        assert!(plan.utilization < 1.0);
        // And it must actually hold the target.
        assert!(plan.p99_wait_ms <= 100);
        // Sanity: it shouldn't massively over-provision for this modest target.
        assert!(plan.workers <= 12, "workers={}", plan.workers);
    }

    #[test]
    fn tighter_targets_need_at_least_as_many_workers() {
        let loose = min_workers_for_p99(10.0, 500, 500).workers;
        let tight = min_workers_for_p99(10.0, 500, 10).workers;
        assert!(tight >= loose, "tight={tight} loose={loose}");
    }

    #[test]
    fn heavier_load_needs_more_workers() {
        let light = min_workers_for_p99(5.0, 500, 100).workers; // a=2.5
        let heavy = min_workers_for_p99(20.0, 500, 100).workers; // a=10
        assert!(heavy > light, "heavy={heavy} light={light}");
    }

    #[test]
    fn no_load_recommends_a_single_worker() {
        let plan = min_workers_for_p99(0.0, 500, 100);
        assert_eq!(plan.workers, 1);
        assert_eq!(plan.p99_wait_ms, 0);
    }

    #[test]
    fn staffing_skips_job_types_without_a_fittable_signal() {
        use super::super::cluster::{ClusterRunSummary, JobTypeStat};
        let summary = ClusterRunSummary {
            process_id: "p".into(),
            instances: 100,
            completed: 100,
            terminated: 0,
            active: 0,
            with_incidents: 0,
            window_ms: Some(10_000), // 10s window
            throughput_per_sec: Some(10.0),
            e2e_p50_ms: None,
            e2e_p95_ms: None,
            e2e_p99_ms: None,
            avg_queue_ms: None,
            avg_service_ms: None,
            by_job_type: vec![
                // fittable: 100 jobs / 10s = 10/s, S=500ms
                JobTypeStat {
                    job_type: "charge".into(),
                    samples: 100,
                    avg_queue_ms: Some(900),
                    avg_service_ms: Some(500),
                    failures: 0,
                },
                // unfittable: no service time
                JobTypeStat {
                    job_type: "notify".into(),
                    samples: 50,
                    avg_queue_ms: Some(5),
                    avg_service_ms: None,
                    failures: 0,
                },
            ],
            by_node: vec![],
        };
        let staffing = staff_for_summary(&summary, 100);
        assert_eq!(staffing.len(), 2);
        // charge is fittable and recommends a stable, target-meeting pool.
        assert!(staffing[0].note.is_none());
        assert!(staffing[0].recommended_workers as f64 > staffing[0].offered_load_erlangs);
        assert!(staffing[0].predicted_p99_wait_ms <= 100);
        // notify cannot be fitted.
        assert!(staffing[1].note.is_some());
    }
}
