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
(*     (`Event::JobCompleted`); the lock holder is irrelevant. The engine    *)
(*     gates completion on a *persistent* `activated` latch (`Job::activated`,*)
(*     `engine/mod.rs` `CompleteJob`), *not* on a still-live lock. That latch *)
(*     is set on the first activation and is never cleared while the job      *)
(*     lives — in particular the `JobLockExpired` reducer returns the job to  *)
(*     the activatable pool but leaves `activated` set — so a job whose       *)
(*     deadline has passed completes whether or not `ExpireJobs` has already  *)
(*     swept its lock, and completion never requires re-activation            *)
(*     (finding #1257). That is the at-least-once contract: an expired lease  *)
(*     costs at most a redelivery, never a lost job and never two live        *)
(*     holders.                                                              *)
(*                                                                           *)
(* `clock` is the host-driven logical time the engine keeps out of its own   *)
(* clock (a periodic "tick"): `ActivateJobs`/`ExpireJobs` carry `now`.       *)
(*                                                                           *)
(* The model deliberately covers more behaviours than a leader produces: it  *)
(* lets time advance, leases expire and jobs be re-activated by any worker   *)
(* in any order. Each safety property checked against this superset          *)
(* therefore also holds for the engine's deterministic single-writer order.  *)
(*                                                                           *)
(* Scope. This models the *lease-deadline* mechanism of the standard         *)
(* activation path: a lock carrying a `deadline`, its expiry/reclaim, and    *)
(* completion by key. It does NOT model the `with_lease = true` worker mode  *)
(* (`Engine::activate_jobs`' opaque per-activation lease *token*), which adds *)
(* token fencing — an expired leased job cannot be re-activated by an        *)
(* unfenced worker, and completion is fenced on the matching token. Those    *)
(* are additional *preconditions*, so the fenced engine's behaviours are a   *)
(* subset of this unfenced model's; every safety property proved here        *)
(* therefore also holds under `with_lease`. Token-fencing safety is out of   *)
(* scope for this spec and belongs to a dedicated lease-token model.         *)
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
    activated,   \* [Jobs -> BOOLEAN]      the persistent activation latch
    clock        \* Nat                    host-driven logical time

vars == <<locks, done, activated, clock>>

\* The live locks on `j`: those whose deadline is still in the future. A lock at
\* or before `clock` is expired and no longer confers the lease.
LiveLocks(j) == {l \in locks[j] : l.deadline > clock}
HasLiveLock(j) == LiveLocks(j) # {}

TypeOK ==
    /\ locks \in [Jobs -> SUBSET Lock]
    /\ done  \in [Jobs -> BOOLEAN]
    /\ activated \in [Jobs -> BOOLEAN]
    /\ clock \in 0..MaxClock

Init ==
    /\ locks = [j \in Jobs |-> {}]
    /\ done  = [j \in Jobs |-> FALSE]
    /\ activated = [j \in Jobs |-> FALSE]
    /\ clock = 0

-----------------------------------------------------------------------------
(* Actions *)

\* `ActivateJobs` / `Engine::activate_jobs`: worker `w` acquires the lease on an
\* activatable job (created or reclaimed after expiry) and holds it until
\* `clock + Timeout`. The guard `~HasLiveLock(j)` is the engine's exclusive-lease
\* rule (`select_activatable_job_keys` only ever yields a job with no live lock).
\* This guard is a deliberate *over-approximation* of the engine's re-activation
\* path: the engine does not re-select an expired-but-`Activated` job directly.
\* `resync_job_index` drops an `Activated` job from the `activatable_jobs` index,
\* and only the `ExpireJobs` sweep (`JobLockExpired`) returns it to `Created` and
\* hence back into that index; `job_activatable`'s `Activated => deadline <= now`
\* arm is a defensive guard the ordinary pull path never reaches. This model
\* folds that `ExpireJobs -> Created -> re-activate` transition into a single
\* `Activate` firing, enabled the instant the lock stops being live, so the
\* model's reachable states are a *superset* of the engine's and every safety
\* property checked here also holds for the narrower engine order (an explicit
\* `Expire` may still fire first — the JSON fixtures exercise both paths). When
\* the engine does re-activate it records a *single* current lock (`job.deadline`
\* is replaced on `JobActivated`), dropping the stale expired lock rather than
\* retaining it. Modelling the effect with `LiveLocks(j) \cup {new}` (not
\* `locks[j] \cup {new}`) keeps that faithful: under the guard `LiveLocks(j) =
\* {}`, so the result is the singleton `{new}`, yet the union still yields two
\* *live* locks (tripping `AtMostOneLiveHolder`) if a weaker guard ever admitted
\* a concurrent holder — the reason locks are a set at all.
Activate(j, w) ==
    /\ ~done[j]
    /\ ~HasLiveLock(j)
    /\ locks' = [locks EXCEPT ![j] = LiveLocks(j) \cup {[worker |-> w, deadline |-> clock + Timeout]}]
    /\ activated' = [activated EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<done, clock>>

\* `ExpireJobs{now}` / `Engine::expire_jobs`: release every lock on `j` that is
\* at or past its deadline, making the job activatable again (`JobLockExpired`).
\* Enabled only when there is something to reclaim. The `JobLockExpired` reducer
\* returns the job to the activatable pool but does NOT clear `Job::activated`,
\* so `Expire` leaves the `activated` latch untouched — a reclaimed job stays
\* completable without re-activation (see `Complete`, finding #1257).
Expire(j) ==
    /\ \E l \in locks[j] : l.deadline <= clock
    /\ locks' = [locks EXCEPT ![j] = LiveLocks(j)]
    /\ UNCHANGED <<done, activated, clock>>

\* `CompleteJob{job_key}` / `Engine::complete_job`: an *activated* job completes
\* by key. The engine gates completion on the persistent `activated` latch, not
\* on a still-live lock and not on a lock still being recorded: `Job::activated`
\* is set on first activation and the `JobLockExpired` reducer does NOT clear it,
\* so a job completes even after `ExpireJobs` has reclaimed its lock, without
\* re-activation (finding #1257). We model that latch as a dedicated `activated`
\* variable (set by `Activate`, untouched by `Expire`) and guard `Complete` on
\* it — `locks[j] # {}` would wrongly disable completion once `Expire` swept the
\* lock, rejecting an engine-valid trace. Completion clears the job's locks: a
\* completed job can never be redelivered, so reclaim can never resurrect it.
Complete(j) ==
    /\ ~done[j]
    /\ activated[j]
    /\ done'  = [done EXCEPT ![j] = TRUE]
    /\ locks' = [locks EXCEPT ![j] = {}]
    /\ UNCHANGED <<activated, clock>>

\* The host advances logical time (a periodic tick). Time only ever ends leases
\* (turns a live lock into an expired one); it never creates a holder.
Tick ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<locks, done, activated>>

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

\* SAFETY 2c — completion requires the activation latch: a completed job was
\* activated at least once (the engine's `CompleteJob` rejects `activated ==
\* false` with `JobNotActivated`). This is what makes the persistent `activated`
\* latch — not a live/recorded lock — the completion precondition (finding
\* #1257): a job reclaimed by `ExpireJobs` keeps the latch and stays completable.
CompletedWasActivated == \A j \in Jobs : done[j] => activated[j]

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
