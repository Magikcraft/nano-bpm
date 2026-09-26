------------------------------ MODULE RHNoFencing2 ------------------------------
(* Fencing DISABLED: a leader that adopts a beating fence keeps serving instead
   of stepping down. This reproduces the split-brain the ADR 0019 fencing step
   ("a stale leader steps down") prevents — two survivors that concurrently
   promote at the same epoch both keep leading after one adopts the other's
   register, so the fence no longer names a single leader. The violation
   documents WHY the step-down is required; the fenced models above prove it
   holds. No crashes are needed to reach it. *)
EXTENDS RaftHandoff

MCNodes      == {1, 2}
MCOwner      == 1
MCFencing    == "off"
MCMaxCrashes == 0
==============================================================================
