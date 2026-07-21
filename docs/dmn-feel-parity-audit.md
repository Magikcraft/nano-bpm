# DMN / FEEL parity audit (Nano vs Camunda 8)

Status: audit only — a snapshot of what Nano's native FEEL evaluator supports
today versus Camunda 8 / Zeebe (the `camunda/feel-scala` engine), to guide any
future parity work. No behavioural change is implied by this document.

## Summary

Nano ships a **real recursive-descent FEEL engine**, not a string/regex
approximation. It lives in `engine-core/src/feel/` (`lexer.rs`, `parser.rs`,
`ast.rs`, `eval.rs`, `builtins.rs`, `temporal.rs`, `regex.rs`, `value.rs`) and is
driven by the DMN decision-table evaluator in `engine-core/src/dmn/eval.rs`.

Coverage is **near-complete for the constructs real DMN tables and literal
expressions use.** The earlier "FEEL-ish" characterisation understated it: the
builtin library alone registers ~110 functions
(`engine-core/src/feel/builtins.rs:15-129`), including the full Allen's-interval
algebra. The remaining gaps are small and mostly cosmetic/robustness, not
missing core semantics.

## What is supported

### Decision-table unary tests (`dmn/eval.rs`)

| Construct | Status | Citation |
|---|---|---|
| empty / `-` (always true) | ✅ | `eval.rs:437-447` |
| literal tests (string/number/boolean/null) | ✅ (via FEEL `values_equal`) | `eval.rs:505-507` |
| comparison prefixes `< <= > >=` | ✅ | `eval.rs:489-503` |
| `= != ` list items | ✅ (FEEL binary ops) | `eval.rs:505-507` |
| ranges/intervals `[a..b] (a..b] ]a..b[` | ✅ | `eval.rs:497-503`, `529-553` |
| disjunction (comma lists) | ✅ (OR of subtests) | `eval.rs:459-471` |
| negation `not(...)` | ✅ | `eval.rs:450-456`, `523-527` |
| variable / qualified-name refs | ✅ | `eval.rs:505-507` |
| `?` input placeholder | ✅ (`__dmn_input__`) | `eval.rs:483-487`, `555-583` |
| date/time/duration, function calls, `in` | ✅ *via* the general FEEL fallback | `eval.rs:505-507` |

### FEEL expressions (output entries & literal expressions)

| Area | Status | Citation |
|---|---|---|
| arithmetic `+ - * / **`, string `+` concat | ✅ | `feel/parser.rs:97-157`, `feel/eval.rs:480-663` |
| `if/then/else`, `for`, `some/every` | ✅ | `feel/eval.rs:112-116,178-217` |
| list / context literals, path access | ✅ | `feel/eval.rs:71-111,361-375` |
| ranges, `between…and…`, `in`, `instance of` | ✅ | `feel/eval.rs:123-176,425-476` |
| function definitions & invocation | ✅ | `feel/eval.rs:143-157,286-359` |
| temporal literals `@"..."` | ✅ | `feel/eval.rs:61-63` |
| three-valued boolean logic (`and/or/not` w/ null) | ✅ | `feel/eval.rs:481-498,724-737` |
| ~110 builtins (string/list/number/temporal/interval/JSON/base64/uuid/regex) | ✅ | `feel/builtins.rs:15-129` |

Builtins include the full Allen interval algebra (`before`, `after`, `meets`,
`met by`, `overlaps`, `overlaps before/after`, `finishes`, `finished by`,
`includes`, `during`, `starts`, `started by`, `coincides`), list aggregates
(`all`, `any`, `count`, `sum`, `min`, `max`, `mean`, `median`, `stddev`,
`mode`, `distinct values`, `flatten`, `sort`, `partition`, …), context ops
(`context put`, `context merge`, `put`, `put all`, `get or else`, `is defined`),
and conversions (`from json`, `to json`, `to/from base64`, `uuid`).

## How failures manifest

The DMN layer surfaces most problems as an explicit `EvaluationFailure`, not a
silently-wrong result:

- input / output-entry FEEL error → `EvaluationFailure` (`dmn/eval.rs:172-177,
  193-195, 212-217`);
- unsupported decision-logic kind → `EvaluationFailure`
  (`dmn/eval.rs:144-150`);
- hit-policy violations (UNIQUE overlap, ANY differing outputs) → explicit error
  (`dmn/eval.rs:297-303, 318-327`).

Within the FEEL core, type mismatches return a `FeelError`
(`feel/eval.rs:717-746`); unknown variables resolve to `null`
(`feel/eval.rs:64-70`) — this matches FEEL's own semantics but can mask a typo.

## Gaps and follow-ups (prioritised)

The gaps are minor; none block typical DMN usage.

1. **`outputValues` priority parsing swallows errors (low, robustness).**
   `output_priorities()` maps any un-parseable `<outputValues>` entry to
   `Value::Null` via `feel::eval(t, &empty).unwrap_or(Value::Null)`
   (`dmn/eval.rs:410-418`). For PRIORITY / OUTPUT ORDER hit policies a malformed
   metadata entry would silently mis-rank rather than error. Real tables use
   simple literals here, so impact is small; hardening this to reject malformed
   `outputValues` at deploy time would close it.

2. **Builtin long-tail parity (low).** The registry is broad; a handful of
   rarely-used feel-scala builtins and some edge argument signatures may differ.
   Worth a targeted diff against `feel-scala` only if a specific function is
   requested.

3. **Unary-test diagnostics are thin for exotic syntax (low).** Unsupported
   unary-test syntax falls through to the general FEEL parser, so the error text
   is FEEL-level rather than DMN-specific (`dmn/eval.rs:505-507`). Cosmetic.

## Conclusion

Nano's FEEL/DMN evaluation is at **strong functional parity** with Camunda 8 for
the constructs decision tables and literal expressions actually use. No parity
gap here is severe enough to warrant a code change in the current work; the
`outputValues` robustness nit (item 1) is the only item with any correctness
flavour and is low-impact in practice.
