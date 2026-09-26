import Feel.Gen

/-!
# FEEL differential-fuzz corpus generator entry point (issue #1230)

`lake exe feelfuzz [N] [SEED]` prints `N` tab-separated corpus rows
(`expression \t context \t reference-outcome`) computed by the Lean reference
semantics. `formal/lean/feel-diff.sh` pipes them to the Rust checker, which fails
on any divergence.
-/

def main (args : List String) : IO Unit := do
  let n := (args[0]?.bind (·.toNat?)).getD 500
  let seed : UInt64 := (args[1]?.bind (·.toNat?)).map (·.toUInt64) |>.getD 0x9E3779B97F4A7C15
  for row in Feel.corpus n seed do
    IO.println row
