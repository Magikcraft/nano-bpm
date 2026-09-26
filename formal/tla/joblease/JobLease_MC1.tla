------------------------------ MODULE JobLease_MC1 ------------------------------
(* One job, two workers. The minimal model that exercises the full lease      *)
(* lifecycle: activate -> (expire | complete), reclaim after expiry, and      *)
(* redelivery to the *other* worker. This is where at-most-one-live-holder    *)
(* and reclaim correctness are exercised for a single contended job.          *)
EXTENDS JobLease

MCJobs     == {"j1"}
MCWorkers  == {"w1", "w2"}
MCTimeout  == 1
MCMaxClock == 2
================================================================================
