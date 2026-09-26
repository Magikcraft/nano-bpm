/-!
# Replay journal model (issue #1231)

A Lean 4 model of the engine's event-journal replay, faithful to the Rust
`engine-core`'s reconstruction path so the determinism proof in
`Replay.Determinism` formalises the *real* fold rather than an abstract one.

## Correspondence to the Rust implementation

The reconstruction primitive is `Engine::replay_partition`
(`engine-core/src/engine/mod.rs`):

```rust
let mut state = State::new();
let mut next_local: u64 = 0;
for event in events {
    let max_key = event.max_key();
    if state::partition_of(max_key) == partition_id {
        next_local = next_local.max(state::local_of(max_key));
    }
    state::apply(&mut state, &event);
}
```

That is a **left fold** over the journal that threads two things:

* the domain `state`, rebuilt by `state::apply` (the same applier used live), and
* `next_local`, the per-partition key counter, advanced to the maximum
  `local_of(max_key)` over the events **minted by this partition**
  (`partition_of(max_key) == partition_id`); keys minted elsewhere are applied
  to state but never advance the local counter.

Everything else on the rebuilt engine (`now`, round-robin cursors, dirty-var
tracking, …) is defaulted and, per the Rust doc comments, "never affects
replay/snapshot determinism", so the model omits it.

The domain `state` and its applier `apply` are kept as *parameters*: the
determinism property is structural in the fold, so the proof holds for **any**
deterministic applier — exactly matching that `state::apply` is a pure function
of `(state, event)`. `Replay.Examples` instantiates them with a concrete applier
to exercise the model by computation.

## Key layout (mirrors `engine-core/src/state/types.rs`)

`PARTITION_BITS = 13`, `LOCAL_BITS = 51`; a `Key`'s high 13 bits are the minting
partition and its low 51 bits are that partition's monotonic counter.
-/

namespace Replay

/-- A globally-unique key: `partition_of` in the high bits, `local_of` in the
low `localBits`. Mirrors `engine-core`'s `pub type Key = u64`. -/
abbrev Key := Nat

/-- Low bits of a `Key` holding the per-partition counter
(`engine-core`'s `LOCAL_BITS = 64 - PARTITION_BITS = 51`). -/
def localBits : Nat := 51

/-- Mask selecting the local-counter portion of a key
(`engine-core`'s `LOCAL_MASK = (1 << LOCAL_BITS) - 1`). -/
def localMask : Nat := (1 <<< localBits) - 1

/-- Largest representable partition id
(`engine-core`'s `MAX_PARTITION_ID = (1 << PARTITION_BITS) - 1`). -/
def maxPartitionId : Nat := (1 <<< 13) - 1

/-- The partition that minted `key` (its high bits). Mirrors `partition_of`. -/
def partitionOf (key : Key) : Nat := key >>> localBits

/-- The per-partition local counter portion of `key`. Mirrors `local_of`. -/
def localOf (key : Key) : Nat := key &&& localMask

/-- Compose a key from a partition id and a local counter. Mirrors `compose_key`. -/
def composeKey (partitionId localId : Nat) : Key :=
  (partitionId <<< localBits) ||| (localId &&& localMask)

/-- A journal record. `maxKey` is `Event::max_key()` — the largest key the event
mints, which drives the local-counter reconstruction; `payload` is an opaque
scalar the (parametric) applier interprets, standing in for the rest of the
event body that `state::apply` reads. -/
structure Event where
  maxKey : Key
  payload : Nat
  deriving Repr, DecidableEq, Inhabited

/-- The replay-relevant portion of a reconstructed engine: the rebuilt domain
`state` plus the `nextLocal` key counter. Mirrors the two fields of
`Engine::replay_partition`'s result that participate in determinism. -/
structure Engine (σ : Type) where
  state : σ
  nextLocal : Nat
  deriving Repr, DecidableEq

variable {σ : Type}

/-- The amount by which `ev` advances this partition's local counter: its
`local_of(max_key)` when it belongs to `pid`, otherwise `0` (so `max` leaves the
counter unchanged — exactly the `if partition_of == partition_id` guard). -/
def localContribution (pid : Nat) (ev : Event) : Nat :=
  if partitionOf ev.maxKey = pid then localOf ev.maxKey else 0

/-- One replay step: apply the event to the domain state and advance the local
counter by `localContribution`. This is the body of `replay_partition`'s loop. -/
def step (pid : Nat) (apply : σ → Event → σ) (e : Engine σ) (ev : Event) : Engine σ :=
  { state := apply e.state ev, nextLocal := max e.nextLocal (localContribution pid ev) }

/-- The engine before any event is replayed: `State::new()` and `next_local = 0`. -/
def initialEngine (init : σ) : Engine σ := { state := init, nextLocal := 0 }

/-- Replay `evs` on top of an existing engine `e` (a left fold of `step`). -/
def replayFrom (pid : Nat) (apply : σ → Event → σ) (e : Engine σ) (evs : List Event) : Engine σ :=
  evs.foldl (step pid apply) e

/-- Full replay of a journal from the empty engine. Mirrors `replay_partition`. -/
def replay (pid : Nat) (apply : σ → Event → σ) (init : σ) (evs : List Event) : Engine σ :=
  replayFrom pid apply (initialEngine init) evs

/-!
## Snapshot serialisation

A snapshot persists the reconstructed `state` and `nextLocal`, plus the
`totalEvents` count of the `[0, totalEvents)` history it certifies — the field
whose meaning `SNAPSHOT_FORMAT_VERSION` guards and whose *rewind* is the #1065
incident. `deserialize ∘ serialize = id` on the replay-relevant fields models a
format-faithful round-trip. -/
structure Snapshot (σ : Type) where
  state : σ
  nextLocal : Nat
  totalEvents : Nat
  deriving Repr

/-- Persist an engine as a snapshot certifying `total` events of history. -/
def serialize (e : Engine σ) (total : Nat) : Snapshot σ :=
  { state := e.state, nextLocal := e.nextLocal, totalEvents := total }

/-- Reconstruct the replay-relevant engine from a snapshot. -/
def deserialize (s : Snapshot σ) : Engine σ :=
  { state := s.state, nextLocal := s.nextLocal }

/-- Snapshot serialisation round-trips on the replay-relevant fields: restoring
a persisted engine recovers it exactly. -/
theorem deserialize_serialize (e : Engine σ) (total : Nat) :
    deserialize (serialize e total) = e := rfl

/-- The snapshot taken at the prefix boundary after `k` events: the compaction
point whose surviving tail is `evs.drop k`. -/
def snapshotAt (pid : Nat) (apply : σ → Event → σ) (init : σ)
    (evs : List Event) (k : Nat) : Snapshot σ :=
  serialize (replay pid apply init (evs.take k)) k

/-- Recovery: restore a snapshot and replay the surviving tail on top of it.
Mirrors `seglog.rs` `recover`/`recover_multi` rebuilding from the compacted
snapshot plus the hot tail. -/
def recover (pid : Nat) (apply : σ → Event → σ) (snap : Snapshot σ)
    (tail : List Event) : Engine σ :=
  replayFrom pid apply (deserialize snap) tail

end Replay
