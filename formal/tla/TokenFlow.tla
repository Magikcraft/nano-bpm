----------------------------- MODULE TokenFlow -----------------------------
(***************************************************************************)
(* Token flow of a single nanobpm process instance in a single scope.      *)
(*                                                                         *)
(* This models the engine-core drain loop (`engine/mod.rs`):               *)
(*                                                                         *)
(*   - A command enqueues `Step::Activate` work; the engine drains the     *)
(*     queue to empty before the command settles. `pending` is that queue, *)
(*     held as a bag of the sequence flows being taken: the engine drains  *)
(*     FIFO, but the model lets any pending step go next. That covers every*)
(*     FIFO order, so a safety property proved here also holds for FIFO.   *)
(*   - Tasks are wait states: activation parks a token in `waiting` until  *)
(*     an external `CompleteTask` command (a job completion).              *)
(*   - A parallel gateway with >1 incoming flow is a join                  *)
(*     (`arrive_at_parallel_join`): it opens on the first arrival, counts  *)
(*     arrivals (`ParallelJoinOpened` / `ParallelJoinTokenArrived`), and   *)
(*     fires once `count >= |incoming|` (`ParallelJoinReset`).             *)
(*   - An inclusive gateway with >1 incoming flow is a join                *)
(*     (`arrive_at_inclusive_join`): arrivals only count. It fires in the  *)
(*     quiescence sweep (`fire_ready_inclusive_joins`) once the queue is   *)
(*     empty and no live token rests on an element that can reach it      *)
(*     (`elements_reaching`). The engine fires the lowest-id ready join per*)
(*     sweep; the model may fire any ready join, which covers that choice. *)
(*   - Conditions are abstracted: an exclusive gateway takes some single   *)
(*     outgoing flow, an inclusive split or join takes some non-empty      *)
(*     subset (condition/incident paths are out of scope for this slice). *)
(*   - An instance completes once it settles with no live tokens           *)
(*     (`complete_finished_instances`).                                    *)
(*                                                                         *)
(* `arrived`, `fireCount` and `premature` are ghost variables: the engine  *)
(* keeps none of them, and they exist only to state properties.            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Nodes,        \* element ids
    Kind,         \* [Nodes -> ElementKinds]
    Flows,        \* sequence-flow ids (distinct ids may share endpoints, as in the engine)
    Src,          \* [Flows -> Nodes] source element of each flow
    Tgt,          \* [Flows -> Nodes] target element of each flow
    Start         \* the (single) none start event

ElementKinds == {"start", "end", "task", "xor", "and", "or"}

ASSUME Kind \in [Nodes -> ElementKinds]
ASSUME Src \in [Flows -> Nodes] /\ Tgt \in [Flows -> Nodes]
ASSUME Start \in Nodes /\ Kind[Start] = "start"

\* The pseudo-flow that carries the instance-creation activation of Start.
InitFlow == "<create>"
ASSUME InitFlow \notin Flows
AllFlows == Flows \cup {InitFlow}

\* The element a queued activation targets.
Target(f) == IF f = InitFlow THEN Start ELSE Tgt[f]

In(n)  == {f \in Flows : Tgt[f] = n}
Out(n) == {f \in Flows : Src[f] = n}

IsJoin(n) == Kind[n] \in {"and", "or"} /\ Cardinality(In(n)) > 1
ParJoins  == {n \in Nodes : Kind[n] = "and" /\ IsJoin(n)}
IncJoins  == {n \in Nodes : Kind[n] = "or" /\ IsJoin(n)}
Tasks     == {n \in Nodes : Kind[n] = "task"}

NonEmptySubsets(S) == (SUBSET S) \ {{}}

\* Mirrors `elements_reaching`: every element from which `t` is reachable
\* over directed sequence flows (a fixpoint bounded by |Nodes| rounds).
ReachingFrom(t) ==
    LET R[k \in 0..Cardinality(Nodes)] ==
            IF k = 0 THEN {Src[f] : f \in In(t)}
            ELSE LET prev == R[k-1]
                 IN  prev \cup {Src[f] : f \in {g \in Flows : Tgt[g] \in prev}}
    IN  R[Cardinality(Nodes)]

\* A constant-level map, so TLC evaluates it once instead of in every guard.
ReachingMap == [t \in Nodes |-> ReachingFrom(t)]
Reaching(t) == ReachingMap[t]

VARIABLES
    pending,      \* [AllFlows -> Nat]  queued Step::Activate, keyed by flow taken
    waiting,      \* [Nodes -> Nat]     tokens parked on wait-state tasks
    joinOpen,     \* [Nodes -> BOOLEAN] join_instances has an entry
    joinCount,    \* [Nodes -> Nat]     join_counts
    arrived,      \* ghost: [Nodes -> [Flows -> Nat]] arrivals per incoming flow since open
    fireCount,    \* ghost: [Nodes -> Nat] how often each join fired
    premature,    \* ghost: parallel joins that fired before every incoming flow delivered
    completed     \* ProcessInstanceCompleted emitted

vars == <<pending, waiting, joinOpen, joinCount, arrived, fireCount, premature, completed>>

Add(p, S)    == [g \in DOMAIN p |-> IF g \in S THEN p[g] + 1 ELSE p[g]]
Take(p, f)   == [p EXCEPT ![f] = @ - 1]
NoArrivals   == [g \in Flows |-> 0]

TypeOK ==
    /\ pending   \in [AllFlows -> Nat]
    /\ waiting   \in [Nodes -> Nat]
    /\ joinOpen  \in [Nodes -> BOOLEAN]
    /\ joinCount \in [Nodes -> Nat]
    /\ arrived   \in [Nodes -> [Flows -> Nat]]
    /\ fireCount \in [Nodes -> Nat]
    /\ premature \subseteq ParJoins
    /\ completed \in BOOLEAN

Init ==
    /\ pending   = [g \in AllFlows |-> IF g = InitFlow THEN 1 ELSE 0]
    /\ waiting   = [n \in Nodes |-> 0]
    /\ joinOpen  = [n \in Nodes |-> FALSE]
    /\ joinCount = [n \in Nodes |-> 0]
    /\ arrived   = [n \in Nodes |-> NoArrivals]
    /\ fireCount = [n \in Nodes |-> 0]
    /\ premature = {}
    /\ completed = FALSE

Quiescent == \A g \in AllFlows : pending[g] = 0

\* Mirrors the `still_waiting` guard in `fire_ready_inclusive_joins`.
InclusiveReady(j) ==
    /\ joinOpen[j]
    /\ \A m \in Reaching(j) \ {j} : waiting[m] = 0 /\ ~joinOpen[m]

Settled == Quiescent /\ \A j \in IncJoins : ~InclusiveReady(j)

-----------------------------------------------------------------------------
(* Draining one queued activation (`activate`). *)

\* Pass-through elements: complete immediately and route to `next`.
PassThrough(f, next) ==
    /\ pending' = Add(Take(pending, f), next)
    /\ UNCHANGED <<waiting, joinOpen, joinCount, arrived, fireCount, premature>>

ActivateTask(f) ==
    /\ pending' = Take(pending, f)
    /\ waiting' = [waiting EXCEPT ![Target(f)] = @ + 1]
    /\ UNCHANGED <<joinOpen, joinCount, arrived, fireCount, premature>>

ActivateEnd(f) ==
    /\ pending' = Take(pending, f)
    /\ UNCHANGED <<waiting, joinOpen, joinCount, arrived, fireCount, premature>>

ArriveParallelJoin(f) ==
    LET j       == Target(f)
        arr     == [arrived[j] EXCEPT ![f] = @ + 1]
        fires   == joinCount[j] + 1 >= Cardinality(In(j))
    IN  /\ IF fires
             THEN /\ pending'   = Add(Take(pending, f), Out(j))
                  /\ joinOpen'  = [joinOpen  EXCEPT ![j] = FALSE]
                  /\ joinCount' = [joinCount EXCEPT ![j] = 0]
                  /\ arrived'   = [arrived   EXCEPT ![j] = NoArrivals]
                  /\ fireCount' = [fireCount EXCEPT ![j] = @ + 1]
                  /\ premature' = IF \E g \in In(j) : arr[g] = 0
                                    THEN premature \cup {j} ELSE premature
             ELSE /\ pending'   = Take(pending, f)
                  /\ joinOpen'  = [joinOpen  EXCEPT ![j] = TRUE]
                  /\ joinCount' = [joinCount EXCEPT ![j] = @ + 1]
                  /\ arrived'   = [arrived   EXCEPT ![j] = arr]
                  /\ UNCHANGED <<fireCount, premature>>
        /\ UNCHANGED waiting

ArriveInclusiveJoin(f) ==
    LET j == Target(f) IN
    /\ pending'   = Take(pending, f)
    /\ joinOpen'  = [joinOpen  EXCEPT ![j] = TRUE]
    /\ joinCount' = [joinCount EXCEPT ![j] = @ + 1]
    /\ arrived'   = [arrived   EXCEPT ![j][f] = @ + 1]
    /\ UNCHANGED <<waiting, fireCount, premature>>

\* The routing choices available when an activation of `n` completes: an
\* exclusive gateway takes one outgoing flow, an inclusive split takes a
\* non-empty subset, and every other pass-through element takes all of them.
\* Each choice is its own action, so fairness can range over the choices.
Choices(n) ==
    CASE Kind[n] = "xor"                    -> {{g} : g \in Out(n)}
      [] Kind[n] = "or" /\ n \notin IncJoins -> NonEmptySubsets(Out(n))
      [] OTHER                              -> {Out(n)}

DrainVia(f, S) ==
    LET n == Target(f) IN
    /\ pending[f] > 0
    /\ S \in Choices(n)
    /\ CASE Kind[n] = "task"             -> ActivateTask(f)
         [] Kind[n] = "end"              -> ActivateEnd(f)
         [] n \in ParJoins               -> ArriveParallelJoin(f)
         [] n \in IncJoins               -> ArriveInclusiveJoin(f)
         [] OTHER                        -> PassThrough(f, S)
    /\ UNCHANGED completed

Drain(f) == \E S \in Choices(Target(f)) : DrainVia(f, S)

(* The quiescence sweep: fire one ready inclusive join, routing to S. *)
FireInclusiveJoinVia(j, S) ==
    /\ Quiescent
    /\ InclusiveReady(j)
    /\ S \in NonEmptySubsets(Out(j))
    /\ pending'   = Add(pending, S)
    /\ joinOpen'  = [joinOpen  EXCEPT ![j] = FALSE]
    /\ joinCount' = [joinCount EXCEPT ![j] = 0]
    /\ arrived'   = [arrived   EXCEPT ![j] = NoArrivals]
    /\ fireCount' = [fireCount EXCEPT ![j] = @ + 1]
    /\ UNCHANGED <<waiting, premature, completed>>

FireInclusiveJoin(j) == \E S \in NonEmptySubsets(Out(j)) : FireInclusiveJoinVia(j, S)

(* External command: a job worker completes a task. *)
CompleteTask(t) ==
    /\ Settled
    /\ ~completed
    /\ waiting[t] > 0
    /\ waiting' = [waiting EXCEPT ![t] = @ - 1]
    /\ pending' = Add(pending, Out(t))
    /\ UNCHANGED <<joinOpen, joinCount, arrived, fireCount, premature, completed>>

(* `complete_finished_instances`. *)
CompleteInstance ==
    /\ Settled
    /\ ~completed
    /\ \A n \in Nodes : waiting[n] = 0 /\ ~joinOpen[n]
    /\ completed' = TRUE
    /\ UNCHANGED <<pending, waiting, joinOpen, joinCount, arrived, fireCount, premature>>

\* Stutter after completion so a completed instance is not reported as a deadlock.
\* Any other state with no enabled action is a stuck instance, which TLC reports.
Done == completed /\ UNCHANGED vars

Next ==
    \/ \E f \in AllFlows : Drain(f)
    \/ \E j \in IncJoins : FireInclusiveJoin(j)
    \/ \E t \in Tasks    : CompleteTask(t)
    \/ CompleteInstance
    \/ Done

\* Fairness. The engine always drains each queued activation and fires each
\* ready inclusive join, and a job worker eventually completes every
\* activatable task (weak fairness, per action). Routing choices are
\* *strongly* fair: a gateway that is reached infinitely often eventually takes
\* each of its branches. This is the "fair data" assumption of classic
\* workflow-net soundness, and it lets a loop with an exit terminate.
\* Conditions are abstracted, so this states what a graph *allows*, not that
\* particular data exits a loop. (Fairness on Next as a whole would only
\* guarantee that *some* step happens, so one task could starve.)
Fairness ==
    /\ \A f \in AllFlows : \A S \in Choices(Target(f)) : SF_vars(DrainVia(f, S))
    /\ \A j \in IncJoins : \A S \in NonEmptySubsets(Out(j)) : SF_vars(FireInclusiveJoinVia(j, S))
    /\ \A t \in Tasks    : WF_vars(CompleteTask(t))
    /\ WF_vars(CompleteInstance)

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Properties *)

\* Join bookkeeping stays coherent (the class behind the missing
\* `ParallelJoinReset` bugs): a join is open iff it has counted arrivals, only
\* joins are ever open, and a parallel join never rests at or over its threshold.
JoinBookkeepingCoherent ==
    /\ \A n \in Nodes : joinOpen[n] <=> joinCount[n] > 0
    /\ \A n \in Nodes \ (ParJoins \cup IncJoins) : ~joinOpen[n]
    /\ \A j \in ParJoins : joinCount[j] < Cardinality(In(j))

\* BPMN / Zeebe parity: a parallel join activates only once *every* incoming
\* sequence flow has been taken (Zeebe counts distinct taken flows,
\* `ProcessInstanceStateTransitionGuard.canActivateParallelGateway`).
ParallelJoinWaitsForEveryFlow == premature = {}

\* Once a settled instance has no task left that could drive it on, no join may
\* still be open. Otherwise the instance can never complete: it is stuck
\* forever on a join that no token will ever reach.
NoStuckInstance ==
    (Settled /\ ~completed /\ \A t \in Tasks : waiting[t] = 0)
        => \A n \in Nodes : ~joinOpen[n]

\* Whether the process graph has no cycle. This is derived from the graph, never
\* declared, so a model cannot be mislabelled and silently skip the property
\* below.
Acyclic == \A n \in Nodes : n \notin Reaching(n)

\* Without cycles, every join fires at most once per instance. A rework loop
\* legitimately re-fires a join.
JoinFiresAtMostOnce == Acyclic => \A j \in ParJoins \cup IncJoins : fireCount[j] <= 1

\* Every instance eventually completes, under the fairness above.
Termination == <>completed
=============================================================================
