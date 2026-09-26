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

end Roundtrip
