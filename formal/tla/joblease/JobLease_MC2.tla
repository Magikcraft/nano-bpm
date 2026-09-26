------------------------------ MODULE JobLease_MC2 ------------------------------
(* Two independent jobs, two workers. Adds concurrency across jobs: the two    *)
(* jobs' leases evolve independently under a shared clock, so at-most-one-live *)
(* holder and reclaim correctness are checked over interleaved per-job         *)
(* lifecycles (a worker leasing one job while another job is reclaimed).       *)
EXTENDS JobLease

MCJobs     == {"j1", "j2"}
MCWorkers  == {"w1", "w2"}
MCTimeout  == 1
MCMaxClock == 2
================================================================================
