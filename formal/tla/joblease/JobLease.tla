------------------------------- MODULE JobLease -------------------------------
(***************************************************************************)
(* Job lease / activation semantics of a single nanobpm engine partition,   *)
(* per ADR 0002 (job activation, leases, expiry/reclaim, at-least-once) and  *)
(* ADR 0001/0017 (single-writer partition actor, worker-concurrency         *)
(* governor). This is epic #1224's JobLease spec (#1227).                    *)
(*                                                                           *)
(* This models the engine-core job lifecycle (`engine/api.rs`,              *)
(* `engine/mod.rs`) for the set of activatable jobs of one partition:        *)
(*                                                                           *)
(*   - A job is created `available` and is activatable. `ActivateJobs`       *)
(*     (`Engine::activate_jobs`) hands it to exactly one worker with a lock  *)
(*     whose `deadline = now + timeout` (`Event::JobActivated{worker,        *)
(*     deadline}`). Activation only ever picks an activatable job — one with *)
(*     no *live* lock (`select_activatable_job_keys` skips a job whose       *)
(*     `deadline > now`), which is the exclusive-lease rule.                  *)
(*   - `ExpireJobs{now}` releases every lock at or past its deadline         *)
(*     (`Event::JobLockExpired`), making the job activatable again. A lost   *)
(*     worker (crash / partition) is indistinguishable from an overrun and   *)
(*     is reclaimed the same way — this is the *reclaim* path.               *)
(*   - `CompleteJob{job_key}` completes an *activated* job by key alone      *)
(*     (`Event::JobCompleted`); the lock holder is irrelevant, but a job     *)
(*     whose lock already expired is no longer activated, so completing it   *)
(*     fails (`JobNotActivated`) until it is re-activated. That is the       *)
(*     at-least-once contract: an expired lease costs at most a redelivery,  *)
(*     never a lost job and never two live holders.                          *)
(*                                                                           *)
(* `clock` is the host-driven logical time the engine keeps out of its own   *)
(* clock (a periodic "tick"): `ActivateJobs`/`ExpireJobs` carry `now`.       *)
(*                                                                           *)
(* The model deliberately covers more behaviours than a leader produces: it  *)
(* lets time advance, leases expire and jobs be re-activated by any worker   *)
(* in any order. Each safety property checked against this superset          *)
(* therefore also holds for the engine's deterministic single-writer order.  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Jobs,        \* the set of activatable job keys of the partition
    Workers,     \* the set of worker identities that may activate jobs
    Timeout,     \* the lease duration a worker requests (now + Timeout = deadline)
    MaxClock     \* bound on the logical clock (keeps the model finite)

ASSUME Timeout \in Nat /\ Timeout > 0
ASSUME MaxClock \in Nat

\* Deadlines never exceed the last instant a lock could be taken plus its
\* duration, which bounds the (finite) lease domain TLC must enumerate.
Deadlines == 0..(MaxClock + Timeout)

\* A lock: the worker that holds it and the instant it expires. A job holds a
\* set of locks so that "at most one *live* holder" is a real invariant over the
\* reachable state space (a weaker activation guard could add a second one),
\* rather than a fact made true by representing the holder as a single value.
Lock == [worker: Workers, deadline: Deadlines]

VARIABLES
    locks,       \* [Jobs -> SUBSET Lock]  every lock currently recorded on a job
    done,        \* [Jobs -> BOOLEAN]      the job has been completed
    clock        \* Nat                    host-driven logical time

vars == <<locks, done, clock>>

\* The live locks on `j`: those whose deadline is still in the future. A lock at
\* or before `clock` is expired and no longer confers the lease.
LiveLocks(j) == {l \in locks[j] : l.deadline > clock}
HasLiveLock(j) == LiveLocks(j) # {}

TypeOK ==
    /\ locks \in [Jobs -> SUBSET Lock]
    /\ done  \in [Jobs -> BOOLEAN]
    /\ clock \in 0..MaxClock

Init ==
    /\ locks = [j \in Jobs |-> {}]
    /\ done  = [j \in Jobs |-> FALSE]
    /\ clock = 0

-----------------------------------------------------------------------------
(* Actions *)

\* `ActivateJobs` / `Engine::activate_jobs`: worker `w` acquires the lease on an
\* activatable job (created or reclaimed after expiry) and holds it until
\* `clock + Timeout`. The guard `~HasLiveLock(j)` is the engine's exclusive-lease
\* rule (`select_activatable_job_keys` skips a job with a live lock). Reclaiming
\* an expired/lost lease is simply this action firing again once the old lock is
\* no longer live.
Activate(j, w) ==
    /\ ~done[j]
    /\ ~HasLiveLock(j)
    /\ locks' = [locks EXCEPT ![j] = @ \cup {[worker |-> w, deadline |-> clock + Timeout]}]
    /\ UNCHANGED <<done, clock>>

\* `ExpireJobs{now}` / `Engine::expire_jobs`: release every lock on `j` that is
\* at or past its deadline, making the job activatable again (`JobLockExpired`).
\* Enabled only when there is something to reclaim.
Expire(j) ==
    /\ \E l \in locks[j] : l.deadline <= clock
    /\ locks' = [locks EXCEPT ![j] = LiveLocks(j)]
    /\ UNCHANGED <<done, clock>>

\* `CompleteJob{job_key}` / `Engine::complete_job`: an *activated* job (one with
\* a live lock) completes by key. Completion clears the job's locks: a completed
\* job can never be redelivered, so reclaim can never resurrect it.
Complete(j) ==
    /\ ~done[j]
    /\ HasLiveLock(j)
    /\ done'  = [done EXCEPT ![j] = TRUE]
    /\ locks' = [locks EXCEPT ![j] = {}]
    /\ UNCHANGED clock

\* The host advances logical time (a periodic tick). Time only ever ends leases
\* (turns a live lock into an expired one); it never creates a holder.
Tick ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<locks, done>>

\* Stutter once every job is completed, so the terminal state is not reported as
\* a deadlock. Any other state with no enabled action is a genuinely stuck job,
\* which TLC's deadlock check reports.
AllDone == \A j \in Jobs : done[j]
Terminal == AllDone /\ UNCHANGED vars

Next ==
    \/ \E j \in Jobs : \E w \in Workers : Activate(j, w)
    \/ \E j \in Jobs : Expire(j)
    \/ \E j \in Jobs : Complete(j)
    \/ Tick
    \/ Terminal

\* Fairness that makes redelivery (at-least-once) live:
\*   - SF on each `Activate(j, w)`: an activatable job is eventually leased, so a
\*     reclaimed job is re-handed to a worker rather than starving.
\*   - WF on each `Expire(j)`: a job stuck behind an expired lock is eventually
\*     reclaimed (the host keeps ticking expiry).
\*   - SF on each `Complete(j)`: a job that is leased infinitely often eventually
\*     completes rather than being re-leased forever. This is what rules out the
\*     "lease/expire/lease/expire forever, never complete" behaviour.
Fairness ==
    /\ \A j \in Jobs : \A w \in Workers : SF_vars(Activate(j, w))
    /\ \A j \in Jobs : WF_vars(Expire(j))
    /\ \A j \in Jobs : SF_vars(Complete(j))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Properties *)

\* SAFETY 1 — at-most-one live lease holder for a given job at any time. This is
\* the exclusive-lease guarantee: no two workers ever concurrently hold a job.
AtMostOneLiveHolder == \A j \in Jobs : Cardinality(LiveLocks(j)) <= 1

\* SAFETY 2a — a completed job holds no lock, so reclaim can never produce a
\* second delivery of a job that already finished (never double-delivers, never
\* "un-completes" a job).
DoneIsTerminal == \A j \in Jobs : done[j] => locks[j] = {}

\* SAFETY 2b — reclaim never drops a job: a job that is neither done nor live is
\* activatable (there is a worker whose `Activate` is enabled), so an expired or
\* lost lease is always reclaimable rather than a dead end. (`Workers` is
\* non-empty by construction of the models.)
ExpiredIsReclaimable ==
    \A j \in Jobs : (~done[j] /\ ~HasLiveLock(j)) => \E w \in Workers : ENABLED Activate(j, w)

\* LIVENESS — every job eventually completes despite arbitrary expiry/redelivery
\* (the at-least-once contract: reclaim never drops a job). Under `Fairness`.
AllEventuallyComplete == <>(\A j \in Jobs : done[j])
=============================================================================
