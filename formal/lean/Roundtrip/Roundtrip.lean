import Roundtrip.Bpmn

/-!
# `Roundtrip.Roundtrip` — the IR ⇄ BPMN round-trip theorems

This ties the pieces together into the theorem the slice exists to prove: on the
supported element-kind set, converting the reversible IR to BPMN and back is the
identity, and converting a canonical BPMN element to IR and back is the identity.
This is the Lean counterpart of the Rust `specs_match_pretty_printer` parity
test — structural round-trip fidelity, proved rather than sampled.

A full element is modelled as its `kind` plus the payload BPMN carries verbatim
across the mapping (its `id` and rendered `attrs`). The kind discriminator is
the interesting part — it is re-encoded structurally in BPMN and must be
recovered exactly (`Roundtrip.Bpmn.fromBpmn_toBpmn`); the payload is preserved by
construction.

## Scope of the payload claim

The `attrs` payload is carried *verbatim* across both directions, so the
round-trip is trivially identity on it. To make that more than a vacuous copy,
this module also ties `attrs` to the kind's declared `Kind.schema` via an
explicit `IrElement.WellFormed` invariant (every required attribute present with
its declared type; every present attribute typed by a schema declaration) and
proves the round-trip *preserves* it (`roundtrip_preserves_wellformed`) — so the
theorem is not silently sound for a timer kind with missing or mistyped
attributes: such an element is simply not `WellFormed`, and validity is what the
round-trip is shown to preserve.

What this module deliberately does **not** model is the attribute *encoder*
correspondence — e.g. millisecond ↔ ISO-8601 duration rendering, or message /
error *values* ↔ definition-reference resolution. Those transformations are the
concern of `model_ir.rs` / `bpmn_model.rs`' attribute renderers, not the
structural kind projection this slice proves; the claim here is the structural
kind round-trip plus schema-validity preservation, not attribute transcoding.
-/

namespace Roundtrip

/-- A BPMN element: its structural identity plus the verbatim payload. -/
structure BpmnElement where
  /-- The element's structural identity (`localName` + event definition). -/
  shape : BpmnShape
  /-- The element's BPMN id. -/
  id : String
  /-- The element's attribute values, carried verbatim across the mapping. -/
  attrs : List (String × AttrVal)
  deriving DecidableEq, Repr, Inhabited

/-- Convert an IR element to its BPMN element. -/
def irToBpmn (e : IrElement) : BpmnElement :=
  { shape := toBpmn e.kind, id := e.id, attrs := e.attrs }

/-- Convert a BPMN element back to an IR element. `none` when the shape is not a
supported/canonical BPMN element. -/
def bpmnToIr (b : BpmnElement) : Option IrElement :=
  (fromBpmn b.shape).map (fun k => { kind := k, id := b.id, attrs := b.attrs })

/-- A BPMN element is *canonical* when its shape is. -/
def BpmnElement.Canonical (b : BpmnElement) : Prop := b.shape.Canonical

/-- Every produced BPMN element is canonical (it came from a real kind). -/
theorem irToBpmn_canonical (e : IrElement) : (irToBpmn e).Canonical :=
  ⟨e.kind, rfl⟩

/-- **IR → BPMN → IR is the identity.** For every IR element, converting it to
BPMN and back recovers it exactly. -/
theorem roundtrip_ir_bpmn_ir (e : IrElement) : bpmnToIr (irToBpmn e) = some e := by
  simp [bpmnToIr, irToBpmn, fromBpmn_toBpmn]

/-- **BPMN → IR → BPMN is the identity on canonical elements.** For every
canonical BPMN element (one in the image of `irToBpmn`), converting it to IR and
back recovers it exactly. -/
theorem roundtrip_bpmn_ir_bpmn (b : BpmnElement) (h : b.Canonical) :
    (bpmnToIr b).map irToBpmn = some b := by
  obtain ⟨k, hk⟩ := h
  simp only [bpmnToIr, irToBpmn, hk, fromBpmn_toBpmn, Option.map_some]
  rw [← hk]

/-- `irToBpmn` is injective — a corollary of the round-trip, useful on its own:
distinct IR elements never collide in BPMN. -/
theorem irToBpmn_injective {e₁ e₂ : IrElement} (h : irToBpmn e₁ = irToBpmn e₂) :
    e₁ = e₂ := by
  have h₁ := roundtrip_ir_bpmn_ir e₁
  have h₂ := roundtrip_ir_bpmn_ir e₂
  rw [h, h₂] at h₁
  exact (Option.some.inj h₁).symm

/-- Does an attribute value inhabit a declared attribute type? Mirror of the
`AttrType` ↔ `AttrVal` correspondence in `model_ir.rs`. -/
def AttrVal.hasType : AttrVal → AttrTy → Bool
  | .str _, .str => true
  | .id _, .id => true
  | .duration _, .duration => true
  | .bool _, .bool => true
  | _, _ => false

/-- **An IR element is schema-well-formed** when its `attrs` conform to its
kind's declared `Kind.schema`: every *required* declaration is present with a
value of its declared type, and every present attribute is typed by some schema
declaration. This is the invariant that makes the payload more than an
unconstrained blob — the round-trip theorems below are stated to *preserve* it,
so a kind carrying missing or mistyped attributes is simply not `WellFormed`
rather than a silently "round-tripping" element. -/
def IrElement.WellFormed (e : IrElement) : Prop :=
  (∀ d ∈ e.kind.schema, d.required → ∃ v, (d.key, v) ∈ e.attrs ∧ v.hasType d.ty) ∧
  (∀ kv ∈ e.attrs, ∃ d ∈ e.kind.schema, d.key = kv.1 ∧ kv.2.hasType d.ty)

/-- **The round-trip preserves schema well-formedness.** IR → BPMN → IR recovers
not just the element but its `WellFormed` status: the recovered element is
`WellFormed` exactly when the original was. Because `attrs` (and the kind) are
carried verbatim, validity cannot be manufactured or destroyed by the mapping —
the round-trip is validity-preserving, not merely identity-on-bytes. -/
theorem roundtrip_preserves_wellformed (e : IrElement) (h : e.WellFormed) :
    ∀ e', bpmnToIr (irToBpmn e) = some e' → e'.WellFormed := by
  intro e' he'
  rw [roundtrip_ir_bpmn_ir] at he'
  obtain rfl := Option.some.inj he'
  exact h

end Roundtrip
