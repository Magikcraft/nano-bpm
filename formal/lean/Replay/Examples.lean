import Replay.Determinism

/-!
# Executable replay-determinism check (issue #1231)

`Replay.Determinism` proves determinism for *any* applier; this file instantiates
the model with a concrete, order-sensitive applier and checks by **kernel
computation** (`decide`, no `native_decide`/no extra axioms) that snapshot +
tail-replay reproduces the full replay — at **every** compaction split point.
That makes the theorem non-vacuous and gives a differential-style guard that the
fold actually behaves as the Rust loop does. -/

namespace Replay.Examples

open Replay

/-- A concrete domain state: an append log of `(maxKey, payload)` records. It is
order-sensitive and content-sensitive, so any misplaced/dropped/duplicated event
during recovery would change it — making the equality checks below meaningful. -/
abbrev St := List (Key × Nat)

/-- A concrete applier standing in for `state::apply`: prepend the event's key
and payload onto the log. -/
def applyEx (s : St) (ev : Event) : St := (ev.maxKey, ev.payload) :: s

/-- Convenience event builder. -/
def ev (k p : Nat) : Event := { maxKey := k, payload := p }

/-- A sample journal minted by partition `0` (keys `1..5 < 2^51`). -/
def journal : List Event :=
  [ev 1 100, ev 2 200, ev 3 300, ev 4 400, ev 5 500]

/-- Determinism holds by computation at the concrete split `k = 2`. -/
example :
    recover 0 applyEx (snapshotAt 0 applyEx [] journal 2) (journal.drop 2)
      = replay 0 applyEx [] journal := by decide

/-- Determinism holds by computation at **every** compaction boundary
`0 ≤ k ≤ journal.length` — snapshot-then-tail-replay always reproduces the full
replay. This is the executable no-silent-rewind guard. -/
example :
    (List.range (journal.length + 1)).all
        (fun k =>
          decide
            (recover 0 applyEx (snapshotAt 0 applyEx [] journal k) (journal.drop k)
              = replay 0 applyEx [] journal))
      = true := by decide

/-- The reconstructed local counter advances to the largest local id minted by
partition `0`: here `local_of(5) = 5`. -/
example : (replay 0 applyEx [] journal).nextLocal = 5 := by decide

/-- Faithful partition guard: an event whose `maxKey` was minted by a *different*
partition is applied to state but does **not** advance partition `0`'s local
counter. -/
example : (replay 0 applyEx [] [ev (composeKey 1 9) 0]).nextLocal = 0 := by decide

/-- …while the same foreign event still mutates the domain state (it is not
dropped — it just does not touch the counter). -/
example : (replay 0 applyEx [] [ev (composeKey 1 9) 0]).state = [(composeKey 1 9, 0)] := by
  decide

end Replay.Examples
