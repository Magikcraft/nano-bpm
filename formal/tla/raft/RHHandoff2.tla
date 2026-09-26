------------------------------ MODULE RHHandoff2 ------------------------------
(* Two nodes, Owner = 1, fencing on, one crash. Exercises failover self-
   promotion, the fresh-receiver rejoin, the lowest-id tie-break on a
   concurrent same-epoch split, and the incumbent -> Owner handoff. *)
EXTENDS RaftHandoff

MCNodes      == {1, 2}
MCOwner      == 1
MCFencing    == "on"
MCMaxCrashes == 1
==============================================================================
