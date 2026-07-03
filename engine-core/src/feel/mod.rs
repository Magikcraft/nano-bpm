//! A FEEL expression evaluator targeting parity with Zeebe's feel-scala engine.
//!
//! FEEL (Friendly Enough Expression Language) is an OMG DMN standard, so this
//! module keeps the standard name rather than renaming it. It is signed for
//! **Philipp Ossler** (`saig0`), creator and architect of Camunda's `feel-scala`
//! engine — the reference this implementation targets parity with. Nano's
//! subsystems are named for the engineers whose work inspired them; artists sign
//! their work.
//!
//! Camunda/Zeebe express sequence-flow conditions, service-task job types and
//! message correlation keys as FEEL expressions (marked by a leading `=`). This
//! module evaluates them against a variable context, producing an engine
//! [`Value`]. It is deliberately dependency-free (no regex crate, no `chrono`,
//! no external crates) so `engine-core` keeps compiling for every target,
//! including `wasm32-unknown-unknown`.
//!
//! ## Supported surface
//! * Literals: `null`, booleans, numbers, strings, lists `[…]`, contexts
//!   `{a: 1}`, ranges/intervals `[1..10]`, and temporal `@"…"` literals.
//! * Operators: arithmetic (`+ - * / **`, including temporal arithmetic),
//!   comparison (`= != < <= > >=` across numbers, strings and temporals),
//!   `and`/`or`/`not` (three-valued), `between … and …`, `… in …`,
//!   `… instance of …`.
//! * Expressions: `if … then … else …`, `for … in … return …`,
//!   `some/every … in … satisfies …`, path access `a.b`, list filters/indices
//!   `list[…]`, function definitions `function(x) …` and invocation
//!   (positional or named arguments).
//! * The feel-scala builtin library (string/list/numeric/boolean/context/
//!   temporal/range functions) plus the Camunda extensions, in [`builtins`].
//!
//! Temporal, range and function values render to their canonical FEEL string at
//! the API boundary (see [`value::FeelVal::into_value`]).

mod ast;
mod builtins;
mod error;
mod eval;
mod lexer;
mod parser;
mod regex;
mod temporal;
mod value;

use std::collections::HashMap;

pub use error::FeelError;

use crate::model::Value;

/// Evaluates a FEEL expression against `ctx`, returning the resulting [`Value`].
///
/// A single leading `=` (the Zeebe FEEL marker) is stripped so callers can pass
/// the raw model attribute (`=amount > 10`, `=jobType`) directly.
pub fn eval(expr: &str, ctx: &HashMap<String, Value>) -> Result<Value, FeelError> {
    Ok(evaluate(expr, ctx)?.into_value())
}

/// Evaluates a FEEL expression expecting a string result (job type, correlation
/// key). Strings pass through; numbers/booleans/temporals use their natural FEEL
/// rendering; `null` and structured values are an error.
pub fn eval_string(expr: &str, ctx: &HashMap<String, Value>) -> Result<String, FeelError> {
    let v = evaluate(expr, ctx)?;
    if matches!(v, value::FeelVal::Null) {
        return Err(FeelError(
            "expected a string-like result, got null".to_string(),
        ));
    }
    v.to_feel_string().ok_or_else(|| {
        FeelError(format!(
            "expected a string-like result, got {}",
            v.type_name()
        ))
    })
}

/// Evaluates a FEEL expression expecting a boolean result (a sequence-flow
/// condition). A non-boolean result is an error.
pub fn eval_bool(expr: &str, ctx: &HashMap<String, Value>) -> Result<bool, FeelError> {
    match evaluate(expr, ctx)? {
        value::FeelVal::Bool(b) => Ok(b),
        other => Err(FeelError(format!(
            "expected a boolean result, got {}",
            other.type_name()
        ))),
    }
}

fn evaluate(expr: &str, ctx: &HashMap<String, Value>) -> Result<value::FeelVal, FeelError> {
    let src = strip_marker(expr);
    let tokens = lexer::tokenize(src)?;
    let node = parser::parse(tokens)?;
    eval::evaluate(&node, ctx)
}

fn strip_marker(expr: &str) -> &str {
    let trimmed = expr.trim();
    // A single leading `=` marks a FEEL expression; `==` is the equality
    // operator and must be preserved.
    if trimmed.starts_with("==") {
        trimmed
    } else {
        trimmed.strip_prefix('=').unwrap_or(trimmed)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn ctx(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    // --- ported baseline coverage ------------------------------------------

    #[test]
    fn evaluates_literals_and_arithmetic() {
        let c = ctx(&[]);
        assert_eq!(eval("1 + 2 * 3", &c), Ok(Value::Int(7)));
        assert_eq!(eval("(1 + 2) * 3", &c), Ok(Value::Int(9)));
        assert_eq!(eval("7 / 2", &c), Ok(Value::Double(3.5)));
        assert_eq!(eval("10 / 0", &c), Ok(Value::Null));
        assert_eq!(eval("-5 + 8", &c), Ok(Value::Int(3)));
        assert_eq!(eval("2 ** 10", &c), Ok(Value::Int(1024)));
    }

    #[test]
    fn resolves_variables_and_strips_marker() {
        let c = ctx(&[("amount", Value::Int(42))]);
        assert_eq!(eval("=amount", &c), Ok(Value::Int(42)));
        assert_eq!(eval("amount + 8", &c), Ok(Value::Int(50)));
        assert_eq!(eval("missing", &c), Ok(Value::Null));
    }

    #[test]
    fn evaluates_comparisons_and_equality() {
        let c = ctx(&[
            ("amount", Value::Int(42)),
            ("name", Value::Str("ann".into())),
        ]);
        assert_eq!(eval("amount > 10", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("amount >= 42", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("amount = 42", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("amount != 7", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("amount = 42.0", &c), Ok(Value::Bool(true)));
        assert_eq!(eval(r#"name = "ann""#, &c), Ok(Value::Bool(true)));
        assert_eq!(eval(r#"name < "bob""#, &c), Ok(Value::Bool(true)));
    }

    #[test]
    fn evaluates_boolean_logic() {
        let c = ctx(&[("a", Value::Bool(true)), ("b", Value::Bool(false))]);
        assert_eq!(eval("a and b", &c), Ok(Value::Bool(false)));
        assert_eq!(eval("a or b", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("not(b)", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("not b", &c), Ok(Value::Bool(true)));
    }

    #[test]
    fn member_access_reads_context_entries() {
        let mut order = BTreeMap::new();
        order.insert("total".to_string(), Value::Int(99));
        let c = ctx(&[("order", Value::Map(order))]);
        assert_eq!(eval("order.total", &c), Ok(Value::Int(99)));
        assert_eq!(eval("order.total > 50", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("order.missing", &c), Ok(Value::Null));
    }

    #[test]
    fn eval_bool_and_string_helpers() {
        let c = ctx(&[
            ("jobType", Value::Str("payment".into())),
            ("n", Value::Int(3)),
        ]);
        assert_eq!(eval_string("=jobType", &c), Ok("payment".to_string()));
        assert_eq!(eval_string("=n", &c), Ok("3".to_string()));
        assert_eq!(eval_bool("n > 1", &c), Ok(true));
        assert!(eval_bool("n", &c).is_err());
    }

    #[test]
    fn type_errors_are_reported() {
        let c = ctx(&[("name", Value::Str("ann".into()))]);
        assert!(eval("name + 1", &c).is_err());
        assert!(eval("1 +", &c).is_err());
        assert!(eval("(1 + 2", &c).is_err());
    }

    #[test]
    fn list_literals_and_equality() {
        let c = ctx(&[]);
        assert_eq!(
            eval("[1, 2, 3]", &c),
            Ok(Value::List(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]))
        );
    }

    // --- new grammar -------------------------------------------------------

    #[test]
    fn quantifiers() {
        let docs = Value::List(vec![
            ctx_value(&[("status", Value::Str("OK".into()))]),
            ctx_value(&[("status", Value::Str("REJECTED".into()))]),
        ]);
        let c = ctx(&[("documents", docs)]);
        assert_eq!(
            eval_bool(r#"some d in documents satisfies d.status = "REJECTED""#, &c),
            Ok(true)
        );
        assert_eq!(
            eval_bool(r#"every d in documents satisfies d.status = "OK""#, &c),
            Ok(false)
        );
    }

    #[test]
    fn in_operator_with_list_and_range() {
        let c = ctx(&[
            ("t", Value::Str("CDD_REMINDER_7DAY".into())),
            ("n", Value::Int(5)),
        ]);
        assert_eq!(
            eval_bool(r#"t in ["CDD_REFRESH_REQUEST", "CDD_REMINDER_7DAY"]"#, &c),
            Ok(true)
        );
        assert_eq!(eval_bool("n in [1..10]", &c), Ok(true));
        assert_eq!(eval_bool("n in (5..10]", &c), Ok(false));
        assert_eq!(eval_bool("n in < 10", &c), Ok(true));
    }

    #[test]
    fn if_then_else_and_between() {
        let c = ctx(&[("score", Value::Int(680))]);
        assert_eq!(
            eval_string(r#"if score < 700 then "deny" else "approve""#, &c),
            Ok("deny".to_string())
        );
        assert_eq!(eval_bool("score between 600 and 700", &c), Ok(true));
    }

    #[test]
    fn for_comprehension_and_filter() {
        let c = ctx(&[]);
        assert_eq!(
            eval("for x in [1, 2, 3] return x * x", &c),
            Ok(Value::List(vec![
                Value::Int(1),
                Value::Int(4),
                Value::Int(9)
            ]))
        );
        assert_eq!(
            eval("[1, 2, 3, 4][item > 2]", &c),
            Ok(Value::List(vec![Value::Int(3), Value::Int(4)]))
        );
        assert_eq!(eval("[10, 20, 30][1]", &c), Ok(Value::Int(10)));
        assert_eq!(eval("[10, 20, 30][-1]", &c), Ok(Value::Int(30)));
    }

    #[test]
    fn context_literal_and_projection() {
        let c = ctx(&[]);
        assert_eq!(eval("{a: 1, b: a + 1}.b", &c), Ok(Value::Int(2)));
        assert_eq!(
            eval("[{x: 1}, {x: 2}].x", &c),
            Ok(Value::List(vec![Value::Int(1), Value::Int(2)]))
        );
    }

    #[test]
    fn builtin_functions() {
        let c = ctx(&[]);
        assert_eq!(eval("count([1, 2, 3])", &c), Ok(Value::Int(3)));
        assert_eq!(eval("sum([1, 2, 3])", &c), Ok(Value::Int(6)));
        assert_eq!(eval("max(3, 7, 2)", &c), Ok(Value::Int(7)));
        assert_eq!(eval(r#"upper case("hi")"#, &c), Ok(Value::Str("HI".into())));
        assert_eq!(eval(r#"string length("hello")"#, &c), Ok(Value::Int(5)));
        assert_eq!(
            eval(r#"contains("banana", "nan")"#, &c),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            eval(r#"list contains([1, 2], 2)"#, &c),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            eval(r#"substring("hello", 2, 3)"#, &c),
            Ok(Value::Str("ell".into()))
        );
        assert_eq!(
            eval(r#"matches("abc123", "[a-z]+\\d+")"#, &c),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            eval(r#"replace("banana", "a", "o")"#, &c),
            Ok(Value::Str("bonono".into()))
        );
        assert_eq!(
            eval("sort([3, 1, 2])", &c),
            Ok(Value::List(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]))
        );
        assert_eq!(eval("floor(3.7)", &c), Ok(Value::Int(3)));
        assert_eq!(eval("abs(-4)", &c), Ok(Value::Int(4)));
        assert_eq!(eval("odd(3)", &c), Ok(Value::Bool(true)));
    }

    #[test]
    fn named_arguments_and_lambdas() {
        let c = ctx(&[]);
        assert_eq!(
            eval(r#"substring(string: "hello", start position: 2)"#, &c),
            Ok(Value::Str("ello".into()))
        );
        assert_eq!(
            eval("sort([3, 1, 2], function(a, b) a < b)", &c),
            Ok(Value::List(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]))
        );
    }

    #[test]
    fn instance_of_checks() {
        let c = ctx(&[("n", Value::Int(3)), ("s", Value::Str("x".into()))]);
        assert_eq!(eval_bool("n instance of number", &c), Ok(true));
        assert_eq!(eval_bool("s instance of number", &c), Ok(false));
        assert_eq!(eval_bool("s instance of string", &c), Ok(true));
    }

    // --- temporal ----------------------------------------------------------

    #[test]
    fn temporal_literals_and_arithmetic() {
        let c = ctx(&[]);
        assert_eq!(
            eval(r#"date("2024-01-15")"#, &c),
            Ok(Value::Str("2024-01-15".into()))
        );
        assert_eq!(
            eval(r#"date("2024-01-31") + duration("P1M")"#, &c),
            Ok(Value::Str("2024-02-29".into()))
        );
        assert_eq!(
            eval(r#"date("2024-12-31") - date("2024-01-01")"#, &c),
            Ok(Value::Str("P365D".into()))
        );
        assert_eq!(
            eval(r#"@"2024-01-01" < @"2024-06-01""#, &c),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            eval(
                r#"date and time("2024-01-01T10:00:00") + duration("PT2H")"#,
                &c
            ),
            Ok(Value::Str("2024-01-01T12:00:00".into()))
        );
        assert_eq!(eval(r#"date("2024-03-15").month"#, &c), Ok(Value::Int(3)));
        assert_eq!(
            eval(r#"day of week(date("2024-01-01"))"#, &c),
            Ok(Value::Str("Monday".into()))
        );
    }

    #[test]
    fn duration_arithmetic() {
        let c = ctx(&[]);
        assert_eq!(
            eval(r#"duration("P1D") + duration("PT12H")"#, &c),
            Ok(Value::Str("P1DT12H".into()))
        );
        assert_eq!(
            eval(r#"duration("P2Y") = duration("P24M")"#, &c),
            Ok(Value::Bool(true))
        );
    }

    #[test]
    fn motivating_cdd_condition() {
        // The standard-FEEL form of the CDD phase-04 gateway guard.
        let docs = Value::List(vec![
            ctx_value(&[("status", Value::Str("APPROVED".into()))]),
            ctx_value(&[("status", Value::Str("REJECTED".into()))]),
        ]);
        let c = ctx(&[("documents", docs)]);
        assert_eq!(
            eval_bool(r#"some d in documents satisfies d.status = "REJECTED""#, &c),
            Ok(true)
        );
    }

    fn ctx_value(pairs: &[(&str, Value)]) -> Value {
        Value::Map(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }
}
