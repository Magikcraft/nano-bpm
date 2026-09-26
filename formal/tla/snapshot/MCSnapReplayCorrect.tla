---- MODULE MCSnapReplayCorrect ----
(***************************************************************************)
(* The shipped, fail-closed recovery. Both safety invariants hold across   *)
(* every reachable durable world: this model PASSES. It is the ratchet     *)
(* that guards seglog::recover against a future regression of the #1065 /  *)
(* #1066 safety properties.                                                *)
(***************************************************************************)
EXTENDS SnapshotReplay

MCMaxEvents == 3
MCCurVersion == 2
MCFailClosed == TRUE
====
