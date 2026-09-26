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

Everything else on the rebuilt engine falls into two groups, and this model
deliberately covers **neither** — it scopes its theorems to the fold-reconstructed
projection `(state, nextLocal)` above, **not** to a bitwise-equal engine:

* **Snapshot-persisted scalar metadata** — `partition_id`, `num_partitions`,
  `now`, and `start_dispatch_rr` (`EngineSnapshot`,
  `engine-core/src/engine/mod.rs`), which `from_snapshot`
  (`engine-core/src/engine/memory.rs`) restores *verbatim* while a *full*
  `replay_partition` re-defaults them (`num_partitions = 1`, `now = 0`,
  `start_dispatch_rr = 0`). They are carried across the snapshot boundary rather
  than being derived by the replay fold, so `snapshot ∘ replay(tail)` and a full
  replay need not — and in general do not — agree on them. They are precisely the
  fields the determinism theorem does *not* assert equality of; capturing them
  would be modelling snapshot round-trip fidelity, a property distinct from the
  fold-determinism proved here.
* **Transient, non-snapshotted host bookkeeping** — `lenient_completion`,
  dirty-var tracking, and the like, which per the Rust doc comments are "never
  part of the snapshot" and "never affect replay/snapshot determinism".

So `Replay.Determinism`'s results are stated over the replay-relevant projection
`(state, nextLocal)` — the two fold-threaded values a silent rewind (#1065) would
actually corrupt — and must not be read as engine-wide bitwise equality.

The domain `state` and its applier `apply` are kept as *parameters*: the
determinism property is structural in the fold, so the proof holds for **any**
deterministic applier — exactly matching that `state::apply` is a pure function
of `(state, event)`. `Replay.Examples` instantiates them with a concrete applier
to exercise the model by computation.

## Key layout (mirrors `engine-core/src/state/types.rs`)

`PARTITION_BITS = 13`, `LOCAL_BITS = 51`; a `Key`'s high 13 bits are the minting
partition and its low 51 bits are that partition's monotonic counter.

### Domain: `Key := Nat` is a deliberate over-approximation

Rust's `Key` is a bounded `u64` (so `local_of < 2^51`, `partition_of ≤
maxPartitionId = 2^13 - 1`, and `replay_partition` *rejects* an out-of-range
partition id). This model uses unbounded `Nat` and does **not** constrain inputs
to that valid domain — `maxPartitionId` is provided to *state* the bound (and for
callers/`Replay.Examples` to assert validity) rather than to enforce it here. This
is sound for what is proved: the determinism property is purely structural in the
fold (see below), so it holds for **every** `Nat` key and therefore *a fortiori*
on the valid bounded `u64` subset the Rust engine actually admits. The price is
that this model is an over-approximation — it can *also* certify fold behaviour on
keys the Rust implementation would reject up front; capturing that rejection is a
domain-validity property distinct from the fold-determinism proved here, and is
deliberately out of scope. -/

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
incident. (The real `EngineSnapshot` also persists the scalar metadata
`partition_id`, `num_partitions`, `now`, and `start_dispatch_rr`; those are the
snapshot-carried fields deliberately outside this model's replay-relevant
projection — see the module header — so `Snapshot` mirrors only the fold-derived
subset.) `deserialize ∘ serialize = id` on those replay-relevant fields models a
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
point whose surviving tail is `evs.drop k`.

The certified `totalEvents` is the **actual** covered-prefix length
`(evs.take k).length`, not `k` itself: when `k ≤ evs.length` these coincide
(the intended case), but when `k > evs.length` the prefix is only `evs.length`
events, so recording `k` would overstate coverage for events that were never
replayed. Storing the real prefix length keeps `totalEvents` an honest witness
of what the snapshot certifies. This does not affect the determinism theorems,
which pair the snapshot with `evs.drop k` and never read `totalEvents`
(`recover` ignores it — see below). -/
def snapshotAt (pid : Nat) (apply : σ → Event → σ) (init : σ)
    (evs : List Event) (k : Nat) : Snapshot σ :=
  serialize (replay pid apply init (evs.take k)) (evs.take k).length

/-- Recovery: restore a snapshot and replay the surviving tail on top of it.
Mirrors `seglog.rs` `recover`/`recover_multi` rebuilding from the compacted
snapshot plus the hot tail.

**Boundary invariant is an assumption, not a check.** This function ignores
`snap.totalEvents` and replays *whatever* `tail` it is handed; the determinism
theorem holds only because `snapshotAt` and the caller separately supply the
matching boundary — a snapshot certifying `k` events (`snap.totalEvents = k`)
paired with exactly `tail = evs.drop k`. Unlike `seglog.rs`, this model therefore
does **not** represent a stale or missing snapshot, a coverage gap between
`totalEvents` and the tail, or the tail-only #1065 rewind, and it has no
fail-closed validation path. The `[snap.totalEvents, snap.totalEvents + tail]`
contiguity is taken as a caller-guaranteed precondition; validating it (and the
fail-closed behaviour when it is violated) is a separate property outside this
model's scope. -/
def recover (pid : Nat) (apply : σ → Event → σ) (snap : Snapshot σ)
    (tail : List Event) : Engine σ :=
  replayFrom pid apply (deserialize snap) tail

end Replay
