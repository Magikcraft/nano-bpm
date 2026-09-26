import Feel.Value
import Feel.Syntax
import Feel.Semantics

/-!
# Deterministic FEEL fuzz generator (issue #1230)

A seeded, dependency-free generator of `(context, expression)` cases for the
differential fuzz. It is the single source of truth for the corpus: it renders
each expression to concrete FEEL syntax for the Rust engine, encodes the
context, and computes the **reference outcome** via `Feel.eval`. The Rust
checker (`engine-core/examples/feel_diff.rs`) re-parses and re-evaluates each row
and fails on any mismatch. Being deterministic (fixed LCG seed) keeps the corpus
reproducible — no retries, a divergence is always the same divergence.
-/

namespace Feel

/-- A tiny linear-congruential PRNG (wrapping `UInt64` arithmetic). Deterministic
and dependency-free; good enough to spread the generated corpus. -/
structure Rng where
  state : UInt64
  deriving Inhabited

/-- Advances the generator, returning a fresh 64-bit word. -/
def Rng.next (r : Rng) : UInt64 × Rng :=
  let s := r.state * 6364136223846793005 + 1442695040888963407
  (s, ⟨s⟩)

/-- A pseudo-random natural in `[0, bound)` (with `bound ≥ 1`). -/
def Rng.upto (r : Rng) (bound : Nat) : Nat × Rng :=
  let (v, r') := r.next
  (v.toNat % (max 1 bound), r')

/-- A pseudo-random integer in `[-20, 20]`. -/
def Rng.int (r : Rng) : Int × Rng :=
  let (n, r') := r.upto 41
  (Int.ofNat n - 20, r')

private def strPool : List String := ["", "a", "ab", "b", "hi", "z"]
private def varNames : List String := ["v0", "v1", "v2", "v3"]

/-- Picks a member of a non-empty list by index. -/
private def pick {α} (xs : List α) (dflt : α) (i : Nat) : α :=
  (xs[i % max 1 xs.length]?).getD dflt

/-- Generates a random value for a context slot, or `none` to leave the variable
unbound (which the reference and engine both read as `null`). -/
def genSlot (r : Rng) : Option Val × Rng :=
  let (k, r) := r.upto 5
  match k with
  | 0 => (none, r)                                   -- unbound → null
  | 1 => (some .null, r)
  | 2 => let (b, r) := r.upto 2; (some (.bool (b == 1)), r)
  | 3 => let (i, r) := r.int; (some (.num i), r)
  | _ => let (j, r) := r.upto strPool.length; (some (.str (pick strPool "" j)), r)

/-- Builds a random context: the reference `Ctx` and its wire encoding
(`name=int:3;name=bool:true;name=str:hi;name=null`, unbound names omitted). -/
def genCtx (r : Rng) : Ctx × String × Rng :=
  varNames.foldl
    (fun (acc : Ctx × String × Rng) name =>
      let (ctx, enc, r) := acc
      let (slot, r) := genSlot r
      match slot with
      | none => (ctx, enc, r)
      | some v =>
        let piece :=
          match v with
          | .null => s!"{name}=null"
          | .bool b => s!"{name}=bool:{if b then "true" else "false"}"
          | .num i => s!"{name}=int:{i}"
          | .str s => s!"{name}=str:{s}"
        let enc := if enc.isEmpty then piece else enc ++ ";" ++ piece
        (ctx ++ [(name, v)], enc, r))
    ([], "", r)

/-- A leaf expression: literal or variable (including an occasionally-unbound
name to exercise the `null` default). -/
def genLeaf (r : Rng) : Expr × Rng :=
  let (k, r) := r.upto 6
  match k with
  | 0 => let (n, r) := r.upto 13; (.numLit n, r)
  | 1 => let (b, r) := r.upto 2; (.boolLit (b == 1), r)
  | 2 => let (j, r) := r.upto strPool.length; (.strLit (pick strPool "" j), r)
  | 3 => (.nullLit, r)
  | 4 => let (j, r) := r.upto varNames.length; (.var (pick varNames "v0" j), r)
  | _ => (.var "u0", r)   -- deliberately unbound

/-- A number literal `q*d / d` etc. divisor, kept ≥ 1. -/
private def genExactDiv (r : Rng) : Expr × Rng :=
  let (q, r) := r.upto 7
  let (d0, r) := r.upto 6
  let d := d0 + 1
  (.bin .div (.numLit (q * d)) (.numLit d), r)

/- Generators are **type-directed**: `genNum`/`genStr`/`genBool` build
expressions guaranteed to evaluate to that type, so arithmetic, string and
comparison operators actually receive well-typed operands (an untyped generator
almost never combines two numbers, leaving `+ - *` unexercised). `genAny` mixes
the typed generators with deliberately ill-typed and `null`/unbound operands to
exercise the type-error and three-valued paths too. -/
mutual
  /-- An expression that evaluates to a number (never `null`/error, except the
  exact-or-zero division whose zero case is `null`). -/
  partial def genNum (r : Rng) : Nat → Expr × Rng
    | 0 => let (n, r) := r.upto 13; (.numLit n, r)
    | depth + 1 =>
      let (k, r) := r.upto 6
      match k with
      | 0 => let (n, r) := r.upto 13; (.numLit n, r)
      | 1 => let (e, r) := genNum r depth; (.neg e, r)
      | 2 => let (l, r) := genNum r depth; let (rr, r) := genNum r depth; (.bin .add l rr, r)
      | 3 => let (l, r) := genNum r depth; let (rr, r) := genNum r depth; (.bin .sub l rr, r)
      | 4 => let (l, r) := genNum r depth; let (rr, r) := genNum r depth; (.bin .mul l rr, r)
      | _ => genExactDiv r

  /-- An expression that evaluates to a string. -/
  partial def genStr (r : Rng) : Nat → Expr × Rng
    | 0 => let (j, r) := r.upto strPool.length; (.strLit (pick strPool "" j), r)
    | depth + 1 =>
      let (k, r) := r.upto 3
      match k with
      | 0 | 1 => let (j, r) := r.upto strPool.length; (.strLit (pick strPool "" j), r)
      | _ => let (l, r) := genStr r depth; let (rr, r) := genStr r depth; (.bin .add l rr, r)

  /-- An expression that (usually) evaluates to a boolean, and sometimes to
  `null` via a non-boolean operand of `and`/`or`/`not` — exercising three-valued
  logic. -/
  partial def genBool (r : Rng) : Nat → Expr × Rng
    | 0 => let (b, r) := r.upto 2; (.boolLit (b == 1), r)
    | depth + 1 =>
      let (k, r) := r.upto 9
      match k with
      | 0 => let (b, r) := r.upto 2; (.boolLit (b == 1), r)
      | 1 => let (l, r) := genNum r depth; let (rr, r) := genNum r depth
             let (o, r) := r.upto 6; (.bin (pick [.lt, .le, .gt, .ge, .eq, .ne] .lt o) l rr, r)
      | 2 => let (l, r) := genStr r depth; let (rr, r) := genStr r depth
             let (o, r) := r.upto 6; (.bin (pick [.lt, .le, .gt, .ge, .eq, .ne] .lt o) l rr, r)
      | 3 => let (e, r) := genBool r depth; (.lnot e, r)
      | 4 => let (l, r) := genBool r depth; let (rr, r) := genBool r depth; (.bin .and l rr, r)
      | 5 => let (l, r) := genBool r depth; let (rr, r) := genBool r depth; (.bin .or l rr, r)
      | 6 => let (l, r) := genBool r depth; (.bin .and l (.nullLit), r)   -- three-valued
      | 7 => let (l, r) := genBool r depth; (.bin .or l (.nullLit), r)    -- three-valued
      | _ => (.lnot .nullLit, r)                                          -- not null → null

  /-- A value of any type, freely mixing typed sub-generators with `null`,
  variables and ill-typed operator applications (type-error paths). -/
  partial def genAny (r : Rng) : Nat → Expr × Rng
    | 0 => genLeaf r
    | depth + 1 =>
      let (k, r) := r.upto 12
      match k with
      | 0 => genNum r depth
      | 1 => genNum r depth
      | 2 => genStr r depth
      | 3 => genBool r depth
      | 4 => genBool r depth
      | 5 => (.nullLit, r)
      | 6 => genLeaf r
      | 7 => let (e, r) := genAny r depth; (.neg e, r)          -- often a type error
      | 8 => let (e, r) := genAny r depth; (.lnot e, r)
      | 9 =>   -- if: boolean or (sometimes) non-boolean condition → null
        let (c, r) := (if depth % 2 == 0 then genBool r depth else genAny r depth)
        let (t, r) := genAny r depth
        let (e, r) := genAny r depth
        (.cond c t e, r)
      | 10 =>  -- freely-typed binop: exercises type errors and equality mixing
        let (o, r) := r.upto 9
        let op := pick [.add, .sub, .mul, .lt, .le, .gt, .ge, .eq, .ne] .eq o
        let (l, r) := genAny r depth
        let (rr, r) := genAny r depth
        (.bin op l rr, r)
      | _ => genExactDiv r
end

/-- One corpus row: the printed FEEL expression, the context encoding, and the
reference outcome, tab-separated. -/
def genRow (r : Rng) : String × Rng :=
  let (ctx, enc, r) := genCtx r
  let (e, r) := genAny r 4
  let outcome := eval ctx e
  (s!"{e.pretty}\t{enc}\t{outcome.canon}", r)

/-- Generates `n` corpus rows from a fixed seed. -/
def corpus (n : Nat) (seed : UInt64) : List String :=
  let rec go (r : Rng) : Nat → List String → List String
    | 0, acc => acc.reverse
    | k + 1, acc =>
      let (row, r) := genRow r
      go r k (row :: acc)
  go ⟨seed⟩ n []

end Feel
