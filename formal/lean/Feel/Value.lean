/-!
# FEEL reference values (issue #1230)

The value domain the reference evaluator produces, plus the **canonical
rendering** that both this Lean reference and the Rust `engine-core::feel`
evaluator serialise to. The differential fuzz (`formal/lean/feel-diff.sh`)
compares these canonical strings, so a divergence is any string mismatch.

The numeric domain is the exact integers: FEEL numbers are IEEE doubles in the
Rust engine, but the fuzz generator (`Feel.Gen`) keeps every generated value and
intermediate result an exact integer within the `f64`-exact range, where `Int`
arithmetic and `f64` arithmetic coincide bit-for-bit. That lets the reference be
exact (no float-formatting drift) while still exercising arithmetic, comparison,
three-valued logic, strings, `if`/`then`/`else` and division (incl. the
divide-by-zero → `null` path).
-/

namespace Feel

/-- A FEEL value in the reference's differential-fuzz domain. -/
inductive Val where
  | null
  | bool (b : Bool)
  | num (n : Int)
  | str (s : String)
  deriving Repr, DecidableEq, Inhabited

/-- The result of evaluating a FEEL expression: a value, or a type error. Only
the *distinction* error-vs-value (and the value) is observable across the two
engines, so error messages are deliberately collapsed to a single `err`. -/
inductive Outcome where
  | err
  | ok (v : Val)
  deriving Repr, DecidableEq, Inhabited

/-- Canonical rendering of a value. Mirrors the Rust checker's `canon_value`:
`null`, `bool:true`/`bool:false`, `num:<decimal>`, `str:<raw>`. -/
def Val.canon : Val → String
  | .null => "null"
  | .bool b => s!"bool:{if b then "true" else "false"}"
  | .num n => s!"num:{n}"
  | .str s => s!"str:{s}"

/-- Canonical rendering of an outcome. Mirrors the Rust checker's `canon`. -/
def Outcome.canon : Outcome → String
  | .err => "err"
  | .ok v => s!"ok:{v.canon}"

end Feel
