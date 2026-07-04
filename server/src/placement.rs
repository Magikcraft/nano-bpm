//! Cluster create-placement protection & load-awareness (ADR 0014).
//!
//! By default nanobpmn places `createProcessInstance` by **blind round-robin**
//! over every partition ([`crate::partition::PartitionRouter::next_create_placement`]):
//! it ignores whether the chosen owner is saturated, and a forwarded create
//! bypasses the owner's own admission gates. That is fine when every node has
//! equal capacity and load arrives evenly, but under **heterogeneous resources**
//! or **skewed ingress** it lets one node be overrun while peers sit idle.
//!
//! This module adds two opt-in, staged behaviours, selected by
//! `NANOBPMN_CREATE_PLACEMENT`:
//!
//! - **`protect`** — every node self-protects: a forwarded create is subjected to
//!   the owner's admission gates and, if the owner is saturated, *shed* back to the
//!   ingress node, which **reroutes** to another owner (and only 503s the client
//!   when the whole cluster is saturated — true global backpressure). This closes
//!   the "forwarded creates skip admission" hole so no node is overrun by work
//!   placed on it.
//! - **`balanced`** — implies `protect`, and additionally makes placement
//!   **load-aware**: each node periodically gossips a composite load index to its
//!   peers, and placement weights owners inversely to load so new creates flow to
//!   nodes with real headroom (work-conserving balance) instead of piling onto a
//!   saturated owner. A stale/absent peer hint is treated as full headroom, so an
//!   unprobed peer still receives traffic; the reactive `protect` shed is the
//!   backstop when the hint is wrong.
//!
//! `off` (default) keeps placement byte-identical to blind round-robin — single
//! node and every existing benchmark are unaffected.

/// Runtime mode for cluster create placement, from `NANOBPMN_CREATE_PLACEMENT`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PlacementMode {
    /// Blind round-robin, forwarded creates ungated (historical behaviour).
    Off,
    /// Forwarded creates honour the owner's admission gates; ingress reroutes
    /// around a shedding owner.
    Protect,
    /// `Protect` plus load-aware, load-gossiped weighted placement.
    Balanced,
}

impl PlacementMode {
    /// Whether forwarded creates are gated on the owner and rerouted on shed.
    /// True for `Protect` and `Balanced`.
    pub fn protects(self) -> bool {
        matches!(self, PlacementMode::Protect | PlacementMode::Balanced)
    }

    /// Whether placement is load-aware (weighted) and load is gossiped between
    /// peers. True only for `Balanced`.
    pub fn balances(self) -> bool {
        matches!(self, PlacementMode::Balanced)
    }

    /// Human-readable description for the startup log.
    pub fn describe(self) -> &'static str {
        match self {
            PlacementMode::Off => "off (blind round-robin; forwarded creates ungated)",
            PlacementMode::Protect => {
                "protect (forwarded creates honour the owner's admission gates; reroute on shed)"
            }
            PlacementMode::Balanced => {
                "balanced (protect + load-aware weighted placement with peer load gossip)"
            }
        }
    }
}

/// Parse `NANOBPMN_CREATE_PLACEMENT`. Fail-safe to `Off` (historical behaviour)
/// for anything unrecognised. Accepts a few friendly aliases.
pub fn parse_placement_mode(raw: Option<&str>) -> PlacementMode {
    match raw.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
        Some("balanced") | Some("2") | Some("weighted") | Some("load-aware") => {
            PlacementMode::Balanced
        }
        Some("protect") | Some("1") | Some("on") | Some("true") | Some("yes") => {
            PlacementMode::Protect
        }
        _ => PlacementMode::Off,
    }
}

/// Sentinel load index meaning "this node is shedding creates right now" — it is
/// never chosen by weighted placement (weight 0). Larger than any realistic
/// backlog and safely below `i64::MAX` so arithmetic on it cannot overflow.
pub const SHED_LOAD: i64 = i64::MAX / 2;

/// Numerator for the inverse-load weight. A node with load 0 gets this weight; a
/// node with load `l` gets `WEIGHT_SCALE / (l + 1)`, so weight decreases with
/// load and differentiates strongly across an order-of-magnitude backlog gap
/// (e.g. load 100 → ~9901, load 4000 → ~250, a ~40× steer) while never dividing
/// by zero.
const WEIGHT_SCALE: u128 = 1_000_000;

/// Inverse-load placement weight for an owner with composite load index `load`.
/// A shedding owner ([`SHED_LOAD`] or above) gets weight 0 — never placed on.
/// Otherwise the weight is `WEIGHT_SCALE / (load + 1)`, monotonically decreasing
/// in load with a floor at load 0.
pub fn placement_weight(load: i64) -> u128 {
    if load >= SHED_LOAD {
        return 0;
    }
    let l = load.max(0) as u128;
    WEIGHT_SCALE / (l + 1)
}

/// Interleaved weighted round-robin selection over `weights`, advancing `counter`
/// by one per call. Returns the chosen index, or `None` when every weight is 0
/// (all owners shedding) — the caller falls back to plain rotation, letting the
/// reactive shed/reroute layer handle protection.
///
/// The raw weights span many orders of magnitude (`WEIGHT_SCALE / (load+1)`), so
/// they are first normalised onto a bounded resolution ([`WRR_RESOLUTION`]) — a
/// unit-stepped counter would otherwise stay stuck in the first (enormous) band
/// forever. After normalisation every non-shedding owner keeps at least a
/// `1/WRR_RESOLUTION` share (so an unprobed/healthy peer always receives some
/// traffic), and the counter sweeps `[0, WRR_RESOLUTION)` deterministically:
/// over a full window each index is chosen in proportion to its (normalised)
/// weight, and the distribution self-adjusts as the weights drift between calls.
pub fn weighted_pick(weights: &[u128], counter: usize) -> Option<usize> {
    let total: u128 = weights.iter().copied().sum();
    if total == 0 {
        return None;
    }
    // Normalise onto a bounded resolution so a unit-stepped counter actually
    // sweeps the distribution; every nonzero weight keeps at least a 1-unit band.
    let scaled: Vec<u128> = weights
        .iter()
        .map(|&w| {
            if w == 0 {
                0
            } else {
                (w * WRR_RESOLUTION / total).max(1)
            }
        })
        .collect();
    let stotal: u128 = scaled.iter().sum();
    let target = (counter as u128) % stotal;
    let mut acc = 0u128;
    for (i, w) in scaled.iter().enumerate() {
        acc += *w;
        if target < acc {
            return Some(i);
        }
    }
    // Unreachable (target < stotal ≤ acc), but return the last index defensively.
    Some(scaled.len() - 1)
}

/// Resolution the raw inverse-load weights are normalised onto before the
/// weighted round-robin sweep (see [`weighted_pick`]).
const WRR_RESOLUTION: u128 = 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_defaults_to_off_and_reads_stages() {
        assert_eq!(parse_placement_mode(None), PlacementMode::Off);
        assert_eq!(parse_placement_mode(Some("nonsense")), PlacementMode::Off);
        assert_eq!(parse_placement_mode(Some("off")), PlacementMode::Off);
        assert_eq!(parse_placement_mode(Some("protect")), PlacementMode::Protect);
        assert_eq!(parse_placement_mode(Some(" PROTECT ")), PlacementMode::Protect);
        assert_eq!(parse_placement_mode(Some("1")), PlacementMode::Protect);
        assert_eq!(parse_placement_mode(Some("balanced")), PlacementMode::Balanced);
        assert_eq!(parse_placement_mode(Some("2")), PlacementMode::Balanced);
    }

    #[test]
    fn mode_capability_flags() {
        assert!(!PlacementMode::Off.protects());
        assert!(!PlacementMode::Off.balances());
        assert!(PlacementMode::Protect.protects());
        assert!(!PlacementMode::Protect.balances());
        assert!(PlacementMode::Balanced.protects());
        assert!(PlacementMode::Balanced.balances());
    }

    #[test]
    fn weight_decreases_with_load_and_zeroes_when_shedding() {
        assert_eq!(placement_weight(SHED_LOAD), 0);
        assert_eq!(placement_weight(i64::MAX), 0);
        assert_eq!(placement_weight(0), WEIGHT_SCALE);
        // Monotonically decreasing; a deep node gets far less than a shallow one.
        assert!(placement_weight(100) > placement_weight(4000));
        assert!(placement_weight(4000) > 0);
        // Negative (shouldn't happen) clamps to the load-0 weight, never panics.
        assert_eq!(placement_weight(-5), WEIGHT_SCALE);
    }

    #[test]
    fn weighted_pick_is_proportional_and_rotates() {
        // Two owners, 3:1 weight. Over a full normalised window the picks land
        // ~3:1 in favour of owner 0.
        let w = [3u128, 1u128];
        let picks: Vec<usize> = (0..1024).map(|c| weighted_pick(&w, c).unwrap()).collect();
        let zeros = picks.iter().filter(|&&i| i == 0).count();
        let ones = picks.iter().filter(|&&i| i == 1).count();
        assert!(ones > 0, "the lighter owner still gets a share");
        assert!(
            zeros > ones * 2,
            "the 3× owner dominates ({zeros} vs {ones})"
        );
    }

    #[test]
    fn weighted_pick_none_when_all_shedding() {
        assert_eq!(weighted_pick(&[0, 0, 0], 7), None);
    }
}
