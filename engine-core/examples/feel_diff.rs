//! FEEL differential-fuzz checker (issue #1230).
//!
//! Reads a corpus produced by the Lean reference semantics (`lake exe feelfuzz`,
//! see `formal/lean/`) — tab-separated `expression \t context \t
//! reference-outcome` rows — re-evaluates each expression with this crate's
//! `feel::eval`, and asserts the canonical outcome matches the Lean reference.
//! Any divergence exits non-zero with the offending row: a mismatch is a real
//! FEEL semantics drift between the Rust engine and the formal reference, never
//! tolerated and never retried.
//!
//! Usage: `feel_diff <corpus-file>` (or `-` / stdin).

use std::collections::HashMap;
use std::io::{self, Read};

use nanobpmn_engine_core::feel;
use nanobpmn_engine_core::Value;

/// Renders a resulting engine [`Value`] to the canonical form the Lean reference
/// also emits. Kept byte-identical to `Feel.Val.canon` / `Feel.Outcome.canon`.
fn canon_value(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => format!("bool:{b}"),
        Value::Int(i) => format!("num:{i}"),
        Value::Double(d) => {
            // Mirror engine-core's `format_double`: an integral finite double
            // renders as its integer, matching a Lean `num:<decimal>`.
            if d.is_finite() && d.fract() == 0.0 && d.abs() < i64::MAX as f64 {
                format!("num:{}", *d as i64)
            } else {
                format!("num:{d}")
            }
        }
        Value::Str(s) => format!("str:{s}"),
        // The fuzz domain never produces these; surface them so a stray one is a
        // visible mismatch rather than a silent pass.
        Value::List(_) => "other:list".to_string(),
        Value::Map(_) => "other:context".to_string(),
    }
}

/// Canonical outcome of evaluating `expr` in `ctx` with the Rust engine.
fn canon_outcome(expr: &str, ctx: &HashMap<String, Value>) -> String {
    match feel::eval(expr, ctx) {
        Ok(v) => format!("ok:{}", canon_value(&v)),
        Err(_) => "err".to_string(),
    }
}

/// Parses a `name=type:value` (or `name=null`) context encoding into engine
/// variables.
fn parse_ctx(enc: &str) -> Result<HashMap<String, Value>, String> {
    let mut ctx = HashMap::new();
    if enc.is_empty() {
        return Ok(ctx);
    }
    for piece in enc.split(';') {
        let (name, rhs) = piece
            .split_once('=')
            .ok_or_else(|| format!("bad context piece {piece:?}"))?;
        let value = if rhs == "null" {
            Value::Null
        } else if let Some(b) = rhs.strip_prefix("bool:") {
            Value::Bool(b == "true")
        } else if let Some(i) = rhs.strip_prefix("int:") {
            Value::Int(i.parse().map_err(|_| format!("bad int {i:?}"))?)
        } else if let Some(s) = rhs.strip_prefix("str:") {
            Value::Str(s.to_string())
        } else {
            return Err(format!("bad context value {rhs:?}"));
        };
        ctx.insert(name.to_string(), value);
    }
    Ok(ctx)
}

fn main() {
    let path = std::env::args().nth(1).unwrap_or_else(|| "-".to_string());
    let mut input = String::new();
    if path == "-" {
        io::stdin()
            .read_to_string(&mut input)
            .expect("read stdin");
    } else {
        input = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    }

    let mut checked = 0usize;
    let mut failures = 0usize;
    for (lineno, line) in input.lines().enumerate() {
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split('\t');
        let expr = fields.next().unwrap_or("");
        let enc = fields.next().unwrap_or("");
        let expected = fields.next().unwrap_or("");
        if fields.next().is_some() {
            eprintln!("line {}: too many fields", lineno + 1);
            failures += 1;
            continue;
        }
        let ctx = match parse_ctx(enc) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("line {}: {e}", lineno + 1);
                failures += 1;
                continue;
            }
        };
        let got = canon_outcome(expr, &ctx);
        checked += 1;
        if got != expected {
            failures += 1;
            eprintln!(
                "DIVERGENCE line {}:\n  expr:     {expr}\n  context:  {enc}\n  expected (Lean): {expected}\n  got (Rust):      {got}",
                lineno + 1
            );
        }
    }

    if failures > 0 {
        eprintln!("FAIL: {failures} divergence(s) over {checked} case(s)");
        std::process::exit(1);
    }
    println!("ok: {checked} FEEL cases agree with the Lean reference");
}
