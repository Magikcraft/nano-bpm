import Lake
open Lake DSL

/-!
# Nano formal — Lean package

One Lake package, one library **target per Lean slice**, each rooted at its own
subdirectory so sibling slices never touch this file or a shared barrel:

* `Feel/`      — FEEL reference semantics + differential fuzz (issue #1230, this slice)
* `Replay/`    — replay-determinism proof            (issue #1231)
* `Roundtrip/` — processos IR ⇄ BPMN round-trip proof (issue #1232)

Each library globs `.submodules` of its own directory, so a slice is extended by
**adding a `.lean` file inside its own subdirectory** — no edit to this lakefile
and no shared barrel module to merge on. All three targets are declared up front
so `lake build` builds every slice that has landed and every empty slice is a
no-op until its owner adds files.
-/

package «nano-formal» where
  -- Keep warnings loud: an unused variable or `sorry` must not slip through CI.
  leanOptions := #[
    ⟨`warningAsError, true⟩,
    ⟨`linter.unusedVariables, true⟩
  ]

/-- FEEL reference semantics (issue #1230). -/
@[default_target]
lean_lib «Feel» where
  globs := #[.submodules `Feel]

/-- Replay-determinism proof (issue #1231). Empty until its owner adds files. -/
@[default_target]
lean_lib «Replay» where
  globs := #[.submodules `Replay]

/-- processos IR ⇄ BPMN round-trip proof (issue #1232). Empty until its owner adds files. -/
@[default_target]
lean_lib «Roundtrip» where
  globs := #[.submodules `Roundtrip]

/-- The FEEL differential-fuzz corpus generator (issue #1230).

Emits a deterministic corpus of `(expression, context, reference-outcome)` rows
that `formal/lean/feel-diff.sh` replays against the Rust `engine-core::feel`
evaluator; a divergence fails the harness. -/
@[default_target]
lean_exe «feelfuzz» where
  root := `Feel.Fuzz
