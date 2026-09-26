---- MODULE MCSnapReplayNaive ----
(***************************************************************************)
(* The historical pre-#1066 recovery, which silently stitched whatever     *)
(* base was present and never failed closed. It reproduces BOTH defects:   *)
(*                                                                         *)
(*   - NoSilentRewind is violated (#1065): a stale-below-floor snapshot    *)
(*     (or a compacted-away tail) is trusted, reconstructing a strictly-   *)
(*     older history than existed.                                         *)
(*   - FailClosed is violated (#1066): a pruned gap / undecodable cold     *)
(*     frame is not detected, so recovery proceeds on a partial history    *)
(*     instead of rejecting.                                               *)
(*                                                                         *)
(* Both invariants are reported violated (each has a state that violates   *)
(* only it: a stale snapshot with an intact cold prefix violates only      *)
(* NoSilentRewind; an unreadable snapshot whose cold prefix meets the      *)
(* floor but no longer decodes violates only FailClosed). The fix that     *)
(* landed the fail-closed recovery is what makes MCSnapReplayCorrect pass;  *)
(* this model keeps the defect from being silently reintroduced.           *)
(***************************************************************************)
EXTENDS SnapshotReplay

MCMaxEvents == 3
MCCurVersion == 2
MCFailClosed == FALSE
====
