/-!
# FEEL expression syntax + pretty-printer (issue #1230)

A small `Expr` AST for the fuzzed FEEL subset and a pretty-printer that renders
it to **concrete FEEL syntax the Rust parser accepts**. The printer is fully
parenthesised so the Rust parser's precedence can never reinterpret a generated
expression: the reference evaluates the `Expr` directly (`Feel.Semantics`) while
the Rust engine re-parses the printed string, and the differential fuzz asserts
they agree.
-/

namespace Feel

/-- Binary operators in the fuzzed subset. -/
inductive BinOp where
  | add | sub | mul | div
  | lt | le | gt | ge | eq | ne
  | and | or
  deriving Repr, DecidableEq, Inhabited

/-- A FEEL expression in the fuzzed subset. Negative numbers and boolean
negation are represented structurally (`neg` / `lnot`) so number literals are
always the non-negative digit sequences the lexer expects. -/
inductive Expr where
  | nullLit
  | boolLit (b : Bool)
  | numLit (n : Nat)
  | strLit (s : String)
  | var (name : String)
  | neg (e : Expr)
  | lnot (e : Expr)
  | bin (op : BinOp) (l r : Expr)
  | cond (c t e : Expr)
  deriving Repr, Inhabited

/-- The concrete FEEL surface form of a binary operator. -/
def BinOp.surface : BinOp → String
  | .add => "+"
  | .sub => "-"
  | .mul => "*"
  | .div => "/"
  | .lt => "<"
  | .le => "<="
  | .gt => ">"
  | .ge => ">="
  | .eq => "="
  | .ne => "!="
  | .and => "and"
  | .or => "or"

/-- Renders an expression to fully-parenthesised concrete FEEL syntax. -/
def Expr.pretty : Expr → String
  | .nullLit => "null"
  | .boolLit b => if b then "true" else "false"
  | .numLit n => toString n
  | .strLit s => "\"" ++ s ++ "\""
  | .var name => name
  | .neg e => "(-" ++ e.pretty ++ ")"
  | .lnot e => "(not (" ++ e.pretty ++ "))"
  | .bin op l r => "(" ++ l.pretty ++ " " ++ op.surface ++ " " ++ r.pretty ++ ")"
  | .cond c t e => "(if " ++ c.pretty ++ " then " ++ t.pretty ++ " else " ++ e.pretty ++ ")"

end Feel
