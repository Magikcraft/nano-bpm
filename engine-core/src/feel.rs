//! A small, in-house FEEL expression evaluator.
//!
//! Camunda/Zeebe express sequence-flow conditions, service-task job types and
//! message correlation keys as FEEL expressions (marked by a leading `=`). This
//! module evaluates a pragmatic subset of FEEL against a variable context,
//! producing an engine [`Value`]. It is deliberately dependency-free (no regex,
//! no external crates) so `engine-core` keeps compiling for every target,
//! including `wasm32-unknown-unknown`.
//!
//! Supported grammar (lowest to highest precedence):
//! * `or`
//! * `and`
//! * comparison: `=`/`==`, `!=`, `<`, `<=`, `>`, `>=`
//! * additive: `+`, `-`
//! * multiplicative: `*`, `/`
//! * unary: `-x`, `not x` / `not(x)`
//! * member access: `a.b`
//! * primary: `null`, `true`/`false`, numbers, `"…"`/`'…'` strings, variable
//!   references, `[…]` list literals and parenthesized expressions.
//!
//! Semantics follow FEEL where it is cheap to do so: numbers compare numerically
//! across integer/decimal, `and`/`or` use three-valued logic, division by zero
//! and other non-finite arithmetic yield `null`, and an unresolved variable
//! resolves to `null`. Anything outside the grammar — or a type error such as
//! adding a string to a number — is a [`FeelError`]; callers decide whether that
//! degrades gracefully or raises an incident.

use std::collections::HashMap;

use crate::model::{format_double, Value};

/// A FEEL parse or evaluation error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeelError(pub String);

impl std::fmt::Display for FeelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FEEL error: {}", self.0)
    }
}

impl std::error::Error for FeelError {}

/// Evaluates a FEEL expression against `ctx`, returning the resulting [`Value`].
///
/// A single leading `=` (the Zeebe FEEL marker) is stripped so callers can pass
/// the raw model attribute (`=amount > 10`, `=jobType`) directly.
pub fn eval(expr: &str, ctx: &HashMap<String, Value>) -> Result<Value, FeelError> {
    let src = strip_marker(expr);
    let tokens = tokenize(src)?;
    let mut parser = Parser { tokens, pos: 0 };
    let node = parser.parse_bp(0)?;
    parser.expect_end()?;
    eval_node(&node, ctx)
}

/// Evaluates a FEEL expression expecting a string result (job type, correlation
/// key). Strings pass through; numbers/booleans use their natural rendering;
/// `null` and structured values are an error.
pub fn eval_string(expr: &str, ctx: &HashMap<String, Value>) -> Result<String, FeelError> {
    match eval(expr, ctx)? {
        Value::Str(s) => Ok(s),
        Value::Int(i) => Ok(i.to_string()),
        Value::Double(d) => Ok(format_double(d)),
        Value::Bool(b) => Ok(b.to_string()),
        other => Err(FeelError(format!(
            "expected a string-like result, got {}",
            type_name(&other)
        ))),
    }
}

/// Evaluates a FEEL expression expecting a boolean result (a sequence-flow
/// condition). A non-boolean result is an error.
pub fn eval_bool(expr: &str, ctx: &HashMap<String, Value>) -> Result<bool, FeelError> {
    match eval(expr, ctx)? {
        Value::Bool(b) => Ok(b),
        other => Err(FeelError(format!(
            "expected a boolean result, got {}",
            type_name(&other)
        ))),
    }
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

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Int(_) | Value::Double(_) => "number",
        Value::Str(_) => "string",
        Value::List(_) => "list",
        Value::Map(_) => "context",
    }
}

// --- Tokens -----------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Num(f64, bool), // value, is_integer
    Str(String),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Dot,
    Comma,
}

fn tokenize(src: &str) -> Result<Vec<Tok>, FeelError> {
    let chars: Vec<char> = src.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '+' => {
                tokens.push(Tok::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Tok::Minus);
                i += 1;
            }
            '*' => {
                tokens.push(Tok::Star);
                i += 1;
            }
            '/' => {
                tokens.push(Tok::Slash);
                i += 1;
            }
            '(' => {
                tokens.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Tok::RParen);
                i += 1;
            }
            '[' => {
                tokens.push(Tok::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Tok::RBracket);
                i += 1;
            }
            ',' => {
                tokens.push(Tok::Comma);
                i += 1;
            }
            '.' => {
                tokens.push(Tok::Dot);
                i += 1;
            }
            '=' => {
                // Accept both `=` and `==` as equality.
                if chars.get(i + 1) == Some(&'=') {
                    i += 2;
                } else {
                    i += 1;
                }
                tokens.push(Tok::Eq);
            }
            '!' => {
                if chars.get(i + 1) == Some(&'=') {
                    tokens.push(Tok::Ne);
                    i += 2;
                } else {
                    return Err(FeelError("unexpected '!'".to_string()));
                }
            }
            '<' => {
                if chars.get(i + 1) == Some(&'=') {
                    tokens.push(Tok::Le);
                    i += 2;
                } else {
                    tokens.push(Tok::Lt);
                    i += 1;
                }
            }
            '>' => {
                if chars.get(i + 1) == Some(&'=') {
                    tokens.push(Tok::Ge);
                    i += 2;
                } else {
                    tokens.push(Tok::Gt);
                    i += 1;
                }
            }
            '"' | '\'' => {
                let (s, next) = lex_string(&chars, i, c)?;
                tokens.push(Tok::Str(s));
                i = next;
            }
            c if c.is_ascii_digit() => {
                let (tok, next) = lex_number(&chars, i)?;
                tokens.push(tok);
                i = next;
            }
            c if is_ident_start(c) => {
                let start = i;
                i += 1;
                while i < chars.len() && is_ident_part(chars[i]) {
                    i += 1;
                }
                tokens.push(Tok::Ident(chars[start..i].iter().collect()));
            }
            other => return Err(FeelError(format!("unexpected character '{other}'"))),
        }
    }
    Ok(tokens)
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_ident_part(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn lex_string(chars: &[char], start: usize, quote: char) -> Result<(String, usize), FeelError> {
    let mut s = String::new();
    let mut i = start + 1;
    while i < chars.len() {
        let c = chars[i];
        if c == quote {
            return Ok((s, i + 1));
        }
        if c == '\\' {
            i += 1;
            match chars.get(i) {
                Some('"') => s.push('"'),
                Some('\'') => s.push('\''),
                Some('\\') => s.push('\\'),
                Some('n') => s.push('\n'),
                Some('r') => s.push('\r'),
                Some('t') => s.push('\t'),
                Some(other) => s.push(*other),
                None => return Err(FeelError("unterminated string escape".to_string())),
            }
            i += 1;
        } else {
            s.push(c);
            i += 1;
        }
    }
    Err(FeelError("unterminated string".to_string()))
}

fn lex_number(chars: &[char], start: usize) -> Result<(Tok, usize), FeelError> {
    let mut i = start;
    while i < chars.len() && chars[i].is_ascii_digit() {
        i += 1;
    }
    let mut is_int = true;
    if i < chars.len() && chars[i] == '.' && chars.get(i + 1).is_some_and(|c| c.is_ascii_digit()) {
        is_int = false;
        i += 1;
        while i < chars.len() && chars[i].is_ascii_digit() {
            i += 1;
        }
    }
    let text: String = chars[start..i].iter().collect();
    let value: f64 = text
        .parse()
        .map_err(|_| FeelError(format!("invalid number '{text}'")))?;
    Ok((Tok::Num(value, is_int), i))
}

// --- AST --------------------------------------------------------------------

#[derive(Debug)]
enum Node {
    Lit(Value),
    Num(f64, bool),
    Var(String),
    Member(Box<Node>, String),
    Neg(Box<Node>),
    Not(Box<Node>),
    Bin(BinOp, Box<Node>, Box<Node>),
    List(Vec<Node>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

// --- Parser (Pratt) ---------------------------------------------------------

struct Parser {
    tokens: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let tok = self.tokens.get(self.pos).cloned();
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn expect_end(&self) -> Result<(), FeelError> {
        if self.pos == self.tokens.len() {
            Ok(())
        } else {
            Err(FeelError("trailing tokens after expression".to_string()))
        }
    }

    /// Parses an expression with binding power >= `min_bp`.
    fn parse_bp(&mut self, min_bp: u8) -> Result<Node, FeelError> {
        let mut lhs = self.parse_prefix()?;

        while let Some((op, left_bp, right_bp)) = self.peek_infix() {
            if left_bp < min_bp {
                break;
            }
            self.advance_infix();
            let rhs = self.parse_bp(right_bp)?;
            lhs = Node::Bin(op, Box::new(lhs), Box::new(rhs));
        }

        Ok(lhs)
    }

    fn parse_prefix(&mut self) -> Result<Node, FeelError> {
        let tok = self
            .next()
            .ok_or_else(|| FeelError("unexpected end of expression".to_string()))?;
        let node = match tok {
            Tok::Num(v, is_int) => Node::Num(v, is_int),
            Tok::Str(s) => Node::Lit(Value::Str(s)),
            Tok::Minus => Node::Neg(Box::new(self.parse_bp(BP_UNARY)?)),
            Tok::LParen => {
                let inner = self.parse_bp(0)?;
                self.expect(Tok::RParen)?;
                inner
            }
            Tok::LBracket => {
                let mut items = Vec::new();
                if self.peek() != Some(&Tok::RBracket) {
                    loop {
                        items.push(self.parse_bp(0)?);
                        if self.peek() == Some(&Tok::Comma) {
                            self.pos += 1;
                            continue;
                        }
                        break;
                    }
                }
                self.expect(Tok::RBracket)?;
                Node::List(items)
            }
            Tok::Ident(name) => match name.as_str() {
                "true" => Node::Lit(Value::Bool(true)),
                "false" => Node::Lit(Value::Bool(false)),
                "null" => Node::Lit(Value::Null),
                "not" => Node::Not(Box::new(self.parse_bp(BP_UNARY)?)),
                _ => Node::Var(name),
            },
            other => return Err(FeelError(format!("unexpected token {other:?}"))),
        };
        self.parse_postfix(node)
    }

    fn parse_postfix(&mut self, mut node: Node) -> Result<Node, FeelError> {
        while self.peek() == Some(&Tok::Dot) {
            self.pos += 1;
            match self.next() {
                Some(Tok::Ident(name)) => node = Node::Member(Box::new(node), name),
                _ => return Err(FeelError("expected a name after '.'".to_string())),
            }
        }
        Ok(node)
    }

    fn expect(&mut self, want: Tok) -> Result<(), FeelError> {
        match self.next() {
            Some(ref got) if *got == want => Ok(()),
            other => Err(FeelError(format!("expected {want:?}, got {other:?}"))),
        }
    }

    /// Returns the infix operator at the cursor with its (left, right) binding
    /// powers, without consuming it.
    fn peek_infix(&self) -> Option<(BinOp, u8, u8)> {
        let op = match self.peek()? {
            Tok::Plus => BinOp::Add,
            Tok::Minus => BinOp::Sub,
            Tok::Star => BinOp::Mul,
            Tok::Slash => BinOp::Div,
            Tok::Eq => BinOp::Eq,
            Tok::Ne => BinOp::Ne,
            Tok::Lt => BinOp::Lt,
            Tok::Le => BinOp::Le,
            Tok::Gt => BinOp::Gt,
            Tok::Ge => BinOp::Ge,
            Tok::Ident(name) if name == "and" => BinOp::And,
            Tok::Ident(name) if name == "or" => BinOp::Or,
            _ => return None,
        };
        let (lbp, rbp) = binding_power(op);
        Some((op, lbp, rbp))
    }

    fn advance_infix(&mut self) {
        self.pos += 1;
    }
}

const BP_UNARY: u8 = 11;

fn binding_power(op: BinOp) -> (u8, u8) {
    match op {
        BinOp::Or => (1, 2),
        BinOp::And => (3, 4),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => (5, 6),
        BinOp::Add | BinOp::Sub => (7, 8),
        BinOp::Mul | BinOp::Div => (9, 10),
    }
}

// --- Evaluation -------------------------------------------------------------

fn eval_node(node: &Node, ctx: &HashMap<String, Value>) -> Result<Value, FeelError> {
    match node {
        Node::Lit(v) => Ok(v.clone()),
        Node::Num(v, is_int) => Ok(if *is_int {
            Value::Int(*v as i64)
        } else {
            Value::number(*v)
        }),
        Node::Var(name) => Ok(ctx.get(name).cloned().unwrap_or(Value::Null)),
        Node::Member(obj, name) => match eval_node(obj, ctx)? {
            Value::Map(entries) => Ok(entries.get(name).cloned().unwrap_or(Value::Null)),
            _ => Ok(Value::Null),
        },
        Node::List(items) => {
            let values = items
                .iter()
                .map(|n| eval_node(n, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::List(values))
        }
        Node::Neg(inner) => {
            let v = eval_node(inner, ctx)?;
            match v.as_f64() {
                Some(n) => Ok(Value::number(-n)),
                None => Err(FeelError(format!(
                    "cannot negate {}",
                    type_name(&v)
                ))),
            }
        }
        Node::Not(inner) => match eval_node(inner, ctx)? {
            Value::Bool(b) => Ok(Value::Bool(!b)),
            Value::Null => Ok(Value::Null),
            other => Err(FeelError(format!("cannot apply not to {}", type_name(&other)))),
        },
        Node::Bin(op, lhs, rhs) => eval_binary(*op, lhs, rhs, ctx),
    }
}

fn eval_binary(
    op: BinOp,
    lhs: &Node,
    rhs: &Node,
    ctx: &HashMap<String, Value>,
) -> Result<Value, FeelError> {
    // `and`/`or` use three-valued logic and short-circuit where possible.
    match op {
        BinOp::And => {
            let l = eval_node(lhs, ctx)?;
            if l == Value::Bool(false) {
                return Ok(Value::Bool(false));
            }
            let r = eval_node(rhs, ctx)?;
            return Ok(ternary_and(&l, &r));
        }
        BinOp::Or => {
            let l = eval_node(lhs, ctx)?;
            if l == Value::Bool(true) {
                return Ok(Value::Bool(true));
            }
            let r = eval_node(rhs, ctx)?;
            return Ok(ternary_or(&l, &r));
        }
        _ => {}
    }

    let l = eval_node(lhs, ctx)?;
    let r = eval_node(rhs, ctx)?;
    match op {
        BinOp::Add => arith(&l, &r, |a, b| a + b),
        BinOp::Sub => arith(&l, &r, |a, b| a - b),
        BinOp::Mul => arith(&l, &r, |a, b| a * b),
        BinOp::Div => match (l.as_f64(), r.as_f64()) {
            (Some(_), Some(0.0)) => Ok(Value::Null),
            (Some(a), Some(b)) => Ok(Value::number(a / b)),
            _ => Err(type_err("/", &l, &r)),
        },
        BinOp::Eq => Ok(Value::Bool(feel_eq(&l, &r))),
        BinOp::Ne => Ok(Value::Bool(!feel_eq(&l, &r))),
        BinOp::Lt => compare(&l, &r, |o| o.is_lt()),
        BinOp::Le => compare(&l, &r, |o| o.is_le()),
        BinOp::Gt => compare(&l, &r, |o| o.is_gt()),
        BinOp::Ge => compare(&l, &r, |o| o.is_ge()),
        BinOp::And | BinOp::Or => unreachable!("handled above"),
    }
}

fn arith(l: &Value, r: &Value, f: impl Fn(f64, f64) -> f64) -> Result<Value, FeelError> {
    match (l.as_f64(), r.as_f64()) {
        (Some(a), Some(b)) => Ok(Value::number(f(a, b))),
        _ => Err(type_err("arithmetic", l, r)),
    }
}

fn compare(l: &Value, r: &Value, f: impl Fn(std::cmp::Ordering) -> bool) -> Result<Value, FeelError> {
    let ordering = match (l, r) {
        (Value::Str(a), Value::Str(b)) => a.cmp(b),
        _ => match (l.as_f64(), r.as_f64()) {
            (Some(a), Some(b)) => a
                .partial_cmp(&b)
                .ok_or_else(|| FeelError("incomparable numbers".to_string()))?,
            _ => return Err(type_err("comparison", l, r)),
        },
    };
    Ok(Value::Bool(f(ordering)))
}

fn feel_eq(l: &Value, r: &Value) -> bool {
    match (l.as_f64(), r.as_f64()) {
        (Some(a), Some(b)) => a == b,
        _ => l == r,
    }
}

fn ternary_and(l: &Value, r: &Value) -> Value {
    match (as_bool(l), as_bool(r)) {
        (Some(false), _) | (_, Some(false)) => Value::Bool(false),
        (Some(true), Some(true)) => Value::Bool(true),
        _ => Value::Null,
    }
}

fn ternary_or(l: &Value, r: &Value) -> Value {
    match (as_bool(l), as_bool(r)) {
        (Some(true), _) | (_, Some(true)) => Value::Bool(true),
        (Some(false), Some(false)) => Value::Bool(false),
        _ => Value::Null,
    }
}

fn as_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        _ => None,
    }
}

fn type_err(op: &str, l: &Value, r: &Value) -> FeelError {
    FeelError(format!(
        "{op} not defined for {} and {}",
        type_name(l),
        type_name(r)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn evaluates_literals_and_arithmetic() {
        let c = ctx(&[]);
        assert_eq!(eval("1 + 2 * 3", &c), Ok(Value::Int(7)));
        assert_eq!(eval("(1 + 2) * 3", &c), Ok(Value::Int(9)));
        assert_eq!(eval("7 / 2", &c), Ok(Value::Double(3.5)));
        assert_eq!(eval("10 / 0", &c), Ok(Value::Null));
        assert_eq!(eval("-5 + 8", &c), Ok(Value::Int(3)));
    }

    #[test]
    fn resolves_variables_and_strips_marker() {
        let c = ctx(&[("amount", Value::Int(42))]);
        assert_eq!(eval("=amount", &c), Ok(Value::Int(42)));
        assert_eq!(eval("amount + 8", &c), Ok(Value::Int(50)));
        // Unknown variable resolves to null.
        assert_eq!(eval("missing", &c), Ok(Value::Null));
    }

    #[test]
    fn evaluates_comparisons_and_equality() {
        let c = ctx(&[("amount", Value::Int(42)), ("name", Value::Str("ann".into()))]);
        assert_eq!(eval("amount > 10", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("amount >= 42", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("amount = 42", &c), Ok(Value::Bool(true)));
        assert_eq!(eval("amount != 7", &c), Ok(Value::Bool(true)));
        // Int compares numerically with Double.
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
        assert_eq!(eval("a and (amount > 10)", &ctx(&[("a", Value::Bool(true)), ("amount", Value::Int(20))])), Ok(Value::Bool(true)));
    }

    #[test]
    fn member_access_reads_context_entries() {
        let mut order = std::collections::BTreeMap::new();
        order.insert("total".to_string(), Value::Int(99));
        let c = ctx(&[("order", Value::Map(order))]);
        assert_eq!(eval("order.total", &c), Ok(Value::Int(99)));
        assert_eq!(eval("order.total > 50", &c), Ok(Value::Bool(true)));
        // Missing member is null.
        assert_eq!(eval("order.missing", &c), Ok(Value::Null));
    }

    #[test]
    fn eval_bool_and_string_helpers() {
        let c = ctx(&[("jobType", Value::Str("payment".into())), ("n", Value::Int(3))]);
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
            Ok(Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]))
        );
    }
}
