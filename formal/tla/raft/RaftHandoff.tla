-------------------------------- MODULE RaftHandoff --------------------------------
(***************************************************************************)
(* Leader-durable leadership handoff + reclaim (ADR 0019), as implemented  *)
(* by `nano-server-raft` and the gateway binary's fence-epoch logic        *)
(* (`server/src/main.rs`: `next_promotion_epoch`, `handle_promotion`;      *)
(* the canonical rule lives in `nano-server-raft::fence`).                 *)
(*                                                                         *)
(* One partition (the "lock") owned by a static `Owner`. Every node keeps a *)
(* fence register `reg[n] = (epoch, leader)` — its view of the "term/lock".*)
(* A node leads by promoting itself into the register at a fresh epoch;    *)
(* peers ADOPT the winning register value (epidemic propagation of the     *)
(* promotion announcement, `handle_promotion`) and a leader that adopts a  *)
(* value naming someone else STEPS DOWN (fencing / `rebuild_as_receiver`). *)
(*                                                                         *)
(*   - Promote(n): a surviving node whose believed leader is not actually  *)
(*     leading self-promotes (leader-durable failover). Its epoch is        *)
(*     `fence.next_epoch` (re-assert own epoch, else overtake at +1).       *)
(*   - Gossip(n, m): n adopts m's register iff it WINS the fence            *)
(*     (`fence.wins`: strictly newer, or equal epoch with a lower node id — *)
(*     the deterministic tie-break). On adopting a value that names another *)
(*     node, n steps down when `Fencing = "on"`.                            *)
(*   - Handoff(inc): the incumbent hands the lock back to `Owner`,          *)
(*     atomically advancing the fence to `(inc.epoch + 1, Owner)` and       *)
(*     stepping itself down (the ADR reclaim: add_learner -> catch up ->    *)
(*     change_voters_to([owner]) -> step down -> advance fence). A returning*)
(*     Owner does NOT self-promote under a live incumbent (Promote is       *)
(*     disabled once it has learned the incumbent's fence — "defer, don't   *)
(*     resurrect"); the incumbent hands off instead.                        *)
(*   - Crash(n) / Restart(n): a leader loss triggers failover; a returning  *)
(*     node boots as a fresh receiver (probes the live leaders' fence, no   *)
(*     self-promote). Crashes are bounded by `MaxCrashes` so reclaim        *)
(*     liveness is well-founded (the adversary cannot crash forever).       *)
(*                                                                         *)
(* Safety: `LeaderOwnsFence` (a leader always names itself in the fence —   *)
(* fencing correctness) and `NoSplitWhenConverged` (once every live node    *)
(* shares the register, at most one leader — no duplicated leadership).     *)
(* Reclaim: `ReclaimConverges` (eventually exactly one stable leader — no   *)
(* lost leadership, correct reclaim after a crashed/partitioned leader).    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Nodes,       \* the set of node ids (naturals); `<` orders them for the tie-break
    Owner,       \* the static owner of the partition (the handoff target)
    Fencing,     \* "on" (adopting a foreign fence steps a leader down) or "off"
    MaxCrashes   \* bound on total crashes, keeping reclaim liveness well-founded

\* Sentinel leader for "no known leader" (u64::MAX in `fence::NO_LEADER`): every
\* real node id ranks below it, so the first promotion always wins the fence.
NoLeader == 1000
ASSUME Owner \in Nodes
ASSUME Fencing \in {"on", "off"}
ASSUME MaxCrashes \in Nat
ASSUME \A n \in Nodes : n \in Nat /\ n < NoLeader

VARIABLES
    role,      \* [Nodes -> {"down", "follower", "leader"}]
    reg,       \* [Nodes -> [epoch: Nat, leader: Nodes \cup {NoLeader}]]
    crashes    \* Nat: total crashes so far (bounds the adversary)

vars == <<role, reg, crashes>>

Leaders == {n \in Nodes : role[n] = "leader"}
UpNodes == {n \in Nodes : role[n] # "down"}

RegVal(e, l) == [epoch |-> e, leader |-> l]

\* fence::wins — does (epoch, leader) beat register value `cur`? Strictly newer,
\* or an equal epoch broken by the lowest node id (the deterministic tie-break).
Wins(cur, epoch, leader) ==
    \/ epoch > cur.epoch
    \/ (epoch = cur.epoch /\ leader < cur.leader)

\* fence::next_epoch — re-assert own epoch (idempotent reclaim), else overtake +1.
NextEpoch(cur, me) == IF cur.leader = me THEN cur.epoch ELSE cur.epoch + 1

\* Node n's believed leader is actually leading at the fence n recorded for it.
\* When false, the partition looks leaderless to n and it may self-promote.
BelievedLeaderAlive(n) ==
    /\ reg[n].leader # NoLeader
    /\ role[reg[n].leader] = "leader"
    /\ reg[reg[n].leader] = reg[n]

\* The fence value a returning node adopts by probing the current live leaders
\* (`probe_incumbents_for_owned`): the winning register among self-leading nodes,
\* or its own if none lead. Keeps a returning Owner from resurrecting a lineage.
Beats(a, b) == Wins(b, a.epoch, a.leader)      \* a wins the fence over b
LiveLeaderRegs == {reg[n] : n \in {m \in Nodes : role[m] = "leader" /\ reg[m].leader = m}}
ProbeResult(n) ==
    IF LiveLeaderRegs = {} THEN reg[n]
    ELSE CHOOSE w \in LiveLeaderRegs : \A o \in LiveLeaderRegs : (o = w) \/ Beats(w, o)

------------------------------------------------------------------------------

Init ==
    /\ role = [n \in Nodes |-> "follower"]
    /\ reg  = [n \in Nodes |-> RegVal(0, NoLeader)]
    /\ crashes = 0

\* Leader-durable failover: a surviving node whose believed leader is not
\* actually leading promotes itself at a fresh fence epoch. The guard is exactly
\* the "defer under a live incumbent" rule — once n has learned the incumbent's
\* fence, BelievedLeaderAlive(n) holds and Promote is disabled.
Promote(n) ==
    /\ role[n] # "down"
    /\ ~BelievedLeaderAlive(n)
    /\ LET e == NextEpoch(reg[n], n) IN
         /\ reg' = [reg EXCEPT ![n] = RegVal(e, n)]
         /\ role' = [role EXCEPT ![n] = "leader"]
    /\ UNCHANGED crashes

\* handle_promotion: n adopts m's register iff it wins the fence; on adopting a
\* value naming another node, n steps down (fencing) when enabled.
Gossip(n, m) ==
    /\ role[n] # "down"
    /\ Wins(reg[n], reg[m].epoch, reg[m].leader)
    /\ reg' = [reg EXCEPT ![n] = reg[m]]
    /\ role' = [role EXCEPT ![n] =
         IF reg[m].leader = n \/ Fencing = "off" THEN role[n] ELSE "follower"]
    /\ UNCHANGED crashes

\* Reclaim handoff: the incumbent hands the lock back to Owner, atomically
\* advancing the fence to (inc.epoch + 1, Owner) and stepping itself down.
Handoff(inc) ==
    /\ role[inc] = "leader"
    /\ reg[inc].leader = inc
    /\ inc # Owner
    /\ role[Owner] # "down"
    /\ LET e == reg[inc].epoch + 1 IN
         /\ reg' = [reg EXCEPT ![inc] = RegVal(e, Owner), ![Owner] = RegVal(e, Owner)]
         /\ role' = [role EXCEPT ![inc] = "follower", ![Owner] = "leader"]
    /\ UNCHANGED crashes

\* A live node crashes (the adversary; bounded so reclaim stays live). A crashed
\* leader is a leader loss that failover must reclaim.
Crash(n) ==
    /\ role[n] # "down"
    /\ crashes < MaxCrashes
    /\ role' = [role EXCEPT ![n] = "down"]
    /\ crashes' = crashes + 1
    /\ UNCHANGED reg

\* A crashed node returns as a fresh receiver: it probes the live leaders' fence
\* (no lineage resurrection) and rejoins as a follower — it never self-promotes
\* under a live incumbent (that is Promote's job, and its guard defers).
Restart(n) ==
    /\ role[n] = "down"
    /\ role' = [role EXCEPT ![n] = "follower"]
    /\ reg' = [reg EXCEPT ![n] = ProbeResult(n)]
    /\ UNCHANGED crashes

Next ==
    \/ \E n \in Nodes : Promote(n)
    \/ \E n \in Nodes : Crash(n)
    \/ \E n \in Nodes : Restart(n)
    \/ \E n \in Nodes : Handoff(n)
    \/ \E n, m \in Nodes : Gossip(n, m)

\* Stutter at a quiescent state (no real action enabled — the converged single-
\* leader terminal) so it is not reported as a deadlock. A quiescent state that
\* is NOT a single stable leader is caught by ReclaimConverges instead.
Moves == Next
Full == Moves \/ (~ENABLED Moves /\ UNCHANGED vars)

\* Crash is the adversary (not fair); every other action is weakly fair, so a
\* leaderless partition is eventually promoted, a split is eventually resolved by
\* the tie-break, and an incumbent eventually hands the lock back to the Owner.
Fairness ==
    /\ \A n \in Nodes : WF_vars(Promote(n))
    /\ \A n \in Nodes : WF_vars(Restart(n))
    /\ \A n \in Nodes : WF_vars(Handoff(n))
    /\ \A n, m \in Nodes : WF_vars(Gossip(n, m))

Spec == Init /\ [][Full]_vars /\ Fairness

------------------------------------------------------------------------------
\* Safety.

TypeOK ==
    /\ \A n \in Nodes : role[n] \in {"down", "follower", "leader"}
    /\ \A n \in Nodes : reg[n].epoch \in Nat
    /\ \A n \in Nodes : reg[n].leader \in (Nodes \cup {NoLeader})
    /\ crashes \in 0..MaxCrashes

\* Fencing correctness: a node only leads at a fence that names it. A stale
\* leader that learned of a beating fence must have stepped down.
LeaderOwnsFence == \A n \in Nodes : role[n] = "leader" => reg[n].leader = n

\* No duplicated leadership: once every live node shares the register (all
\* announcements delivered), at most one node leads.
Converged == \A n, m \in UpNodes : reg[n] = reg[m]
NoSplitWhenConverged == Converged => Cardinality(Leaders) <= 1

------------------------------------------------------------------------------
\* Liveness: correct reclaim / no lost leadership. Under bounded crashes and
\* weak fairness, the partition converges to exactly one stable leader.
ReclaimConverges == <>[](Cardinality(Leaders) = 1)

==============================================================================
