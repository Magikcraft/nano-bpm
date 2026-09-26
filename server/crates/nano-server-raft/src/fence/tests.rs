//! Conformance anchor for the `RaftHandoff` TLA+ spec
//! (`formal/tla/raft/RaftHandoff.tla`, registered via
//! `formal/tla/specs/RaftHandoff.spec`).
//!
//! Anti-drift rule (#1228): the spec must be tied to the Rust implementation,
//! not left unanchored. The spec formalises the fence-epoch register whose two
//! decisions — [`wins`] and [`next_epoch`] — are the canonical functions the
//! gateway binary uses (`server/src/main.rs`: `handle_promotion`,
//! `next_promotion_epoch`). This test replays the spec's transition system
//! against those exact functions, so the model and the production fence rule
//! cannot silently diverge:
//!
//! * [`wins_matches_adr_truth_table`] / [`next_epoch_matches_adr`] pin the two
//!   functions to the concrete examples ADR 0019 and the `main.rs` doc comments
//!   state.
//! * [`spec_scenarios`] drives the exact behaviours the TLA+ counterexamples
//!   and completing runs walk (concurrent same-epoch split resolved by the
//!   lowest-id tie-break, fencing step-down, incumbent -> Owner handoff).
//! * [`exhaustive_safety_matches_spec`] is a bounded breadth-first exploration
//!   of the same transition system the model-checker explores, built on the
//!   production fence functions, asserting the same safety invariants
//!   (`LeaderOwnsFence`, `NoSplitWhenConverged`). It runs the multi-node (N=3)
//!   case the TLA+ liveness model-check is too expensive to cover, and confirms
//!   that disabling the fencing step-down reproduces the `RHNoFencing2`
//!   violation — so the anchor has teeth on both sides.

use std::collections::{BTreeSet, VecDeque};

use super::{NO_LEADER, next_epoch, wins};

// --- Direct truth-table anchors (ADR 0019 / main.rs doc comments) ------------

#[test]
fn wins_matches_adr_truth_table() {
    // Any real promotion (epoch >= 1) beats the implicit (0, NO_LEADER) state.
    assert!(wins(0, NO_LEADER, 1, 7));
    // Strictly higher epoch wins regardless of leader id.
    assert!(wins(3, 2, 4, 9));
    assert!(!wins(4, 9, 3, 2));
    // Equal epoch: the LOWER node id wins (the deterministic tie-break that
    // collapses a symmetric split back to one leader).
    assert!(wins(5, 8, 5, 3));
    assert!(!wins(5, 3, 5, 8));
    // Equal epoch and equal leader is not a win (idempotent re-announcement).
    assert!(!wins(5, 3, 5, 3));
    // A lower epoch never wins even from a lower id.
    assert!(!wins(6, 8, 5, 1));
}

#[test]
fn next_epoch_matches_adr() {
    // Overtaking a peer (or the implicit incumbent) climbs to cur + 1.
    assert_eq!(next_epoch(0, NO_LEADER, 2), 1);
    assert_eq!(next_epoch(4, 7, 2), 5);
    // Re-asserting our own hold is idempotent — the reclaim re-entry must not
    // climb (the unbounded-epoch / leader_reject storm this guards against).
    assert_eq!(next_epoch(4, 2, 2), 4);
}

// --- A faithful in-Rust mirror of RaftHandoff.tla ----------------------------
//
// The state, actions and invariants below correspond one-to-one to the TLA+
// module. The fence decisions route through the production `wins`/`next_epoch`,
// so a change to either function is exercised here exactly as the model-checker
// exercises the spec.

const NODES: &[u64] = &[1, 2, 3];
const OWNER: u64 = 1;

/// Epoch ceiling for the bounded exploration. The reachable epoch is small (a
/// promotion, a bounded number of crash-failovers, and a handoff each advance it
/// by one), but capping it keeps the breadth-first search provably finite and
/// fast regardless — the same bounded-model-checking discipline TLC applies.
/// Successors that would advance the fence past the cap are pruned.
const MAX_EPOCH: u64 = 4;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum Role {
    Down,
    Follower,
    Leader,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct Reg {
    epoch: u64,
    leader: u64,
}

/// One node's `(role, reg)`; the global state is a `Vec` indexed by node order.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct State {
    role: Vec<Role>,
    reg: Vec<Reg>,
    crashes: u64,
}

fn idx(n: u64) -> usize {
    NODES.iter().position(|&x| x == n).unwrap()
}

fn init() -> State {
    State {
        role: vec![Role::Follower; NODES.len()],
        reg: vec![
            Reg {
                epoch: 0,
                leader: NO_LEADER
            };
            NODES.len()
        ],
        crashes: 0,
    }
}

/// TLA+ `BelievedLeaderAlive(n)`: n's believed leader actually leads at the
/// fence n recorded — when false, the partition looks leaderless to n.
fn believed_leader_alive(s: &State, n: u64) -> bool {
    let r = s.reg[idx(n)];
    r.leader != NO_LEADER
        && NODES.contains(&r.leader)
        && s.role[idx(r.leader)] == Role::Leader
        && s.reg[idx(r.leader)] == r
}

/// TLA+ `ProbeResult(n)`: the winning register among self-leading nodes, or n's
/// own if none lead (a returning node probes the incumbents, no resurrection).
fn probe_result(s: &State, n: u64) -> Reg {
    let mut best: Option<Reg> = None;
    for &m in NODES {
        if s.role[idx(m)] == Role::Leader && s.reg[idx(m)].leader == m {
            let cand = s.reg[idx(m)];
            best = Some(match best {
                Some(b) if wins(b.epoch, b.leader, cand.epoch, cand.leader) => cand,
                Some(b) => b,
                None => cand,
            });
        }
    }
    best.unwrap_or(s.reg[idx(n)])
}

/// Enumerate every successor of `s` under the spec's actions. `fencing` toggles
/// the step-down on adopting a foreign fence (the `Fencing` constant), and
/// `max_crashes` bounds the adversary (`MaxCrashes`).
fn successors(s: &State, fencing: bool, max_crashes: u64) -> Vec<State> {
    let mut out = Vec::new();
    for &n in NODES {
        // Promote(n): leader-durable failover / self-promotion.
        if s.role[idx(n)] != Role::Down && !believed_leader_alive(s, n) {
            let cur = s.reg[idx(n)];
            let e = next_epoch(cur.epoch, cur.leader, n);
            if e <= MAX_EPOCH {
                let mut t = s.clone();
                t.reg[idx(n)] = Reg {
                    epoch: e,
                    leader: n,
                };
                t.role[idx(n)] = Role::Leader;
                out.push(t);
            }
        }
        // Crash(n): the bounded adversary.
        if s.role[idx(n)] != Role::Down && s.crashes < max_crashes {
            let mut t = s.clone();
            t.role[idx(n)] = Role::Down;
            t.crashes += 1;
            out.push(t);
        }
        // Restart(n): return as a fresh receiver (probe, no self-promote).
        if s.role[idx(n)] == Role::Down {
            let mut t = s.clone();
            t.role[idx(n)] = Role::Follower;
            t.reg[idx(n)] = probe_result(s, n);
            out.push(t);
        }
        // Handoff(n): incumbent hands the lock back to Owner.
        if s.role[idx(n)] == Role::Leader
            && s.reg[idx(n)].leader == n
            && n != OWNER
            && s.role[idx(OWNER)] != Role::Down
            && s.reg[idx(n)].epoch < MAX_EPOCH
        {
            let e = s.reg[idx(n)].epoch + 1;
            let mut t = s.clone();
            let v = Reg {
                epoch: e,
                leader: OWNER,
            };
            t.reg[idx(n)] = v;
            t.reg[idx(OWNER)] = v;
            t.role[idx(n)] = Role::Follower;
            t.role[idx(OWNER)] = Role::Leader;
            out.push(t);
        }
        // Gossip(n, m): adopt m's register iff it wins; step down when fencing.
        for &m in NODES {
            let (rn, rm) = (s.reg[idx(n)], s.reg[idx(m)]);
            if s.role[idx(n)] != Role::Down && wins(rn.epoch, rn.leader, rm.epoch, rm.leader) {
                let mut t = s.clone();
                t.reg[idx(n)] = rm;
                if rm.leader != n && fencing {
                    t.role[idx(n)] = Role::Follower;
                }
                out.push(t);
            }
        }
    }
    out
}

fn leaders(s: &State) -> Vec<u64> {
    NODES
        .iter()
        .copied()
        .filter(|&n| s.role[idx(n)] == Role::Leader)
        .collect()
}

/// TLA+ `LeaderOwnsFence`: a node only leads at a fence that names it.
fn leader_owns_fence(s: &State) -> bool {
    leaders(s).into_iter().all(|n| s.reg[idx(n)].leader == n)
}

/// TLA+ `NoSplitWhenConverged`: once every live node shares the register, at
/// most one node leads.
fn no_split_when_converged(s: &State) -> bool {
    let up: Vec<u64> = NODES
        .iter()
        .copied()
        .filter(|&n| s.role[idx(n)] != Role::Down)
        .collect();
    let converged = up.iter().all(|&n| s.reg[idx(n)] == s.reg[idx(up[0])]);
    !converged || leaders(s).len() <= 1
}

/// Bounded BFS over the whole reachable state space. Returns whether both safety
/// invariants held at every reachable state.
fn explore(fencing: bool, max_crashes: u64) -> bool {
    let mut seen: BTreeSet<State> = BTreeSet::new();
    let mut queue: VecDeque<State> = VecDeque::new();
    let start = init();
    seen.insert(start.clone());
    queue.push_back(start);
    let mut safe = true;
    while let Some(s) = queue.pop_front() {
        if !leader_owns_fence(&s) || !no_split_when_converged(&s) {
            safe = false;
        }
        for t in successors(&s, fencing, max_crashes) {
            if seen.insert(t.clone()) {
                queue.push_back(t);
            }
        }
    }
    safe
}

#[test]
fn exhaustive_safety_matches_spec() {
    // Fencing on: the real fence rule keeps leadership single-valued across the
    // full multi-node reachable space (RHHandoff2's invariants, one node wider
    // than the TLA+ liveness model-check affordably reaches).
    assert!(
        explore(true, 1),
        "fenced fence rule must preserve LeaderOwnsFence + NoSplitWhenConverged"
    );
    // Fencing off: the anchor has teeth — dropping the step-down reproduces the
    // RHNoFencing2 split-brain (a leader keeps serving after adopting a foreign
    // fence), so the safety invariants are reachable-violated.
    assert!(
        !explore(false, 0),
        "without the fencing step-down the split-brain must be reachable"
    );
}

#[test]
fn spec_scenarios() {
    // Concurrent same-epoch split (two survivors promote at epoch 1) resolves to
    // the lowest-id leader once the announcements gossip: node 3 adopts node 2's
    // (1, 2) — 2 < 3 wins — and steps down; node 2 keeps (1, 2) — 3 !< 2.
    let mut s = init();
    for n in [2u64, 3] {
        let cur = s.reg[idx(n)];
        let e = next_epoch(cur.epoch, cur.leader, n);
        s.reg[idx(n)] = Reg {
            epoch: e,
            leader: n,
        };
        s.role[idx(n)] = Role::Leader;
    }
    assert_eq!(leaders(&s), vec![2, 3]); // transient split, not yet gossiped
    // node 3 adopts node 2's register (fencing).
    let (r3, r2) = (s.reg[idx(3)], s.reg[idx(2)]);
    assert!(wins(r3.epoch, r3.leader, r2.epoch, r2.leader));
    s.reg[idx(3)] = r2;
    s.role[idx(3)] = Role::Follower;
    // node 2 does not adopt node 3's register (3 !< 2).
    assert!(!wins(r2.epoch, r2.leader, r3.epoch, r3.leader));
    assert_eq!(leaders(&s), vec![2]);
    assert!(leader_owns_fence(&s));

    // Handoff reclaim: incumbent (node 2) hands the lock back to the Owner at a
    // strictly higher epoch; Owner leads, incumbent steps down, and the fence
    // names the Owner — the returning owner reclaims without resurrecting.
    let inc_epoch = s.reg[idx(2)].epoch;
    let e = inc_epoch + 1;
    let v = Reg {
        epoch: e,
        leader: OWNER,
    };
    s.reg[idx(2)] = v;
    s.reg[idx(OWNER)] = v;
    s.role[idx(2)] = Role::Follower;
    s.role[idx(OWNER)] = Role::Leader;
    assert_eq!(leaders(&s), vec![OWNER]);
    assert!(e > inc_epoch);
    assert!(leader_owns_fence(&s));

    // A stale leader at a lower epoch steps down on learning the higher fence.
    let mut stale = init();
    stale.reg[idx(3)] = Reg {
        epoch: 1,
        leader: 3,
    };
    stale.role[idx(3)] = Role::Leader;
    let higher = Reg {
        epoch: e,
        leader: OWNER,
    };
    assert!(wins(1, 3, higher.epoch, higher.leader));
    stale.reg[idx(3)] = higher;
    stale.role[idx(3)] = Role::Follower; // rebuild_as_receiver / fencing
    assert!(leader_owns_fence(&stale));
}
