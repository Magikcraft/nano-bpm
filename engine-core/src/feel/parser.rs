//! The FEEL parser: a Pratt parser producing an [`ast::Node`] tree.

use super::ast::{BinOp, CallArgs, Node, Quantifier, RangeNode};
use super::error::FeelError;
use super::lexer::Tok;

pub fn parse(tokens: Vec<Tok>) -> Result<Node, FeelError> {
    let mut parser = Parser { tokens, pos: 0 };
    let node = parser.parse_bp(0)?;
    parser.expect_end()?;
    Ok(node)
}

struct Parser {
    tokens: Vec<Tok>,
    pos: usize,
}

const BP_UNARY: u8 = 11;
const CMP_LBP: u8 = 5;
const RANGE_LBP: u8 = 4;
const RANGE_RBP: u8 = 7;

fn is_keyword(s: &str) -> bool {
    matches!(
        s,
        "and"
            | "or"
            | "in"
            | "satisfies"
            | "then"
            | "else"
            | "return"
            | "between"
            | "instance"
            | "of"
            | "true"
            | "false"
            | "null"
            | "not"
            | "function"
            | "some"
            | "every"
            | "for"
            | "if"
    )
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
            Err(FeelError(format!(
                "trailing tokens after expression: {:?}",
                &self.tokens[self.pos..]
            )))
        }
    }

    fn expect(&mut self, want: Tok) -> Result<(), FeelError> {
        match self.next() {
            Some(ref got) if *got == want => Ok(()),
            other => Err(FeelError(format!("expected {want:?}, got {other:?}"))),
        }
    }

    fn eat_keyword(&mut self, kw: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Ident(s)) if s == kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<(), FeelError> {
        if self.eat_keyword(kw) {
            Ok(())
        } else {
            Err(FeelError(format!("expected '{kw}', got {:?}", self.peek())))
        }
    }

    /// Parses an expression with binding power >= `min_bp`.
    fn parse_bp(&mut self, min_bp: u8) -> Result<Node, FeelError> {
        let mut lhs = self.parse_prefix()?;

        loop {
            // Range operator `a..b` (low precedence, inclusive both ends).
            if min_bp <= RANGE_LBP && self.peek() == Some(&Tok::DotDot) {
                self.pos += 1;
                let end = self.parse_bp(RANGE_RBP)?;
                lhs = Node::Range(RangeNode {
                    start_inclusive: true,
                    start: Some(Box::new(lhs)),
                    end: Some(Box::new(end)),
                    end_inclusive: true,
                });
                continue;
            }

            // Textual comparison operators: between / in / instance of.
            if min_bp <= CMP_LBP {
                if let Some(Tok::Ident(k)) = self.peek() {
                    match k.as_str() {
                        "between" => {
                            self.pos += 1;
                            let low = self.parse_bp(RANGE_RBP)?;
                            self.expect_keyword("and")?;
                            let high = self.parse_bp(RANGE_RBP)?;
                            lhs = Node::Between(
                                Box::new(lhs),
                                Box::new(low),
                                Box::new(high),
                            );
                            continue;
                        }
                        "in" => {
                            self.pos += 1;
                            let rhs = self.parse_in_rhs()?;
                            lhs = Node::In(Box::new(lhs), Box::new(rhs));
                            continue;
                        }
                        "instance" => {
                            self.pos += 1;
                            self.expect_keyword("of")?;
                            let ty = self.read_type_name()?;
                            lhs = Node::InstanceOf(Box::new(lhs), ty);
                            continue;
                        }
                        _ => {}
                    }
                }
            }

            if let Some((op, lbp, rbp)) = self.peek_infix() {
                if lbp < min_bp {
                    break;
                }
                self.pos += 1;
                let rhs = self.parse_bp(rbp)?;
                lhs = Node::Bin(op, Box::new(lhs), Box::new(rhs));
                continue;
            }
            break;
        }

        Ok(lhs)
    }

    fn parse_prefix(&mut self) -> Result<Node, FeelError> {
        let tok = self
            .next()
            .ok_or_else(|| FeelError("unexpected end of expression".to_string()))?;
        let node = match tok {
            Tok::Num(v, is_int) => Node::NumLit(v, is_int),
            Tok::Str(s) => Node::StrLit(s),
            Tok::At(s) => Node::AtLit(s),
            Tok::Minus => Node::Neg(Box::new(self.parse_bp(BP_UNARY)?)),
            Tok::LParen => {
                let inner = self.parse_bp(0)?;
                // `(a..b]` etc. — the `..` operator already built a Range; the
                // surrounding parenthesis makes the start exclusive.
                if let Node::Range(mut r) = inner {
                    r.start_inclusive = false;
                    r.end_inclusive = self.read_interval_close()?;
                    Node::Range(r)
                } else {
                    self.expect(Tok::RParen)?;
                    inner
                }
            }
            Tok::LBracket => self.parse_bracket()?,
            Tok::LBrace => self.parse_context()?,
            Tok::Ident(name) => self.parse_ident(name)?,
            other => return Err(FeelError(format!("unexpected token {other:?}"))),
        };
        self.parse_postfix(node)
    }

    /// `[` already consumed: either a list literal or a closed interval `[a..b]`.
    fn parse_bracket(&mut self) -> Result<Node, FeelError> {
        if self.peek() == Some(&Tok::RBracket) {
            self.pos += 1;
            return Ok(Node::List(Vec::new()));
        }
        let first = self.parse_bp(0)?;
        // `[a..b]` etc. — the `..` operator already built a Range; the bracket
        // makes the start inclusive, and the closing token sets the end.
        if matches!(first, Node::Range(_)) && self.peek() != Some(&Tok::Comma) {
            if let Node::Range(mut r) = first {
                r.start_inclusive = true;
                r.end_inclusive = self.read_interval_close()?;
                return Ok(Node::Range(r));
            }
        }
        let mut items = vec![first];
        while self.peek() == Some(&Tok::Comma) {
            self.pos += 1;
            items.push(self.parse_bp(0)?);
        }
        self.expect(Tok::RBracket)?;
        Ok(Node::List(items))
    }

    /// Reads the closing bracket of an interval, returning whether the end is
    /// inclusive. `]` is inclusive; `)` and `[` are exclusive.
    fn read_interval_close(&mut self) -> Result<bool, FeelError> {
        match self.next() {
            Some(Tok::RBracket) => Ok(true),
            Some(Tok::RParen) | Some(Tok::LBracket) => Ok(false),
            other => Err(FeelError(format!("expected interval close, got {other:?}"))),
        }
    }

    /// `{` already consumed: a context literal `{ key: expr, … }`.
    fn parse_context(&mut self) -> Result<Node, FeelError> {
        let mut entries = Vec::new();
        if self.peek() != Some(&Tok::RBrace) {
            loop {
                let key = match self.next() {
                    Some(Tok::Ident(s)) => {
                        // Allow multi-word keys (joined) until the colon.
                        let mut k = s;
                        while let Some(Tok::Ident(n)) = self.peek() {
                            if is_keyword(n) {
                                break;
                            }
                            k.push(' ');
                            k.push_str(n);
                            self.pos += 1;
                        }
                        k
                    }
                    Some(Tok::Str(s)) => s,
                    other => {
                        return Err(FeelError(format!("expected a context key, got {other:?}")))
                    }
                };
                self.expect(Tok::Colon)?;
                let value = self.parse_bp(0)?;
                entries.push((key, value));
                if self.peek() == Some(&Tok::Comma) {
                    self.pos += 1;
                    continue;
                }
                break;
            }
        }
        self.expect(Tok::RBrace)?;
        Ok(Node::Context(entries))
    }

    fn parse_ident(&mut self, name: String) -> Result<Node, FeelError> {
        match name.as_str() {
            "true" => Ok(Node::BoolLit(true)),
            "false" => Ok(Node::BoolLit(false)),
            "null" => Ok(Node::Null),
            "not" => Ok(Node::Not(Box::new(self.parse_bp(BP_UNARY)?))),
            "if" => {
                let cond = self.parse_bp(0)?;
                self.expect_keyword("then")?;
                let then = self.parse_bp(0)?;
                self.expect_keyword("else")?;
                let otherwise = self.parse_bp(0)?;
                Ok(Node::If(
                    Box::new(cond),
                    Box::new(then),
                    Box::new(otherwise),
                ))
            }
            "for" => {
                let clauses = self.parse_iter_clauses("return")?;
                self.expect_keyword("return")?;
                let body = self.parse_bp(0)?;
                Ok(Node::For(clauses, Box::new(body)))
            }
            "some" => {
                let clauses = self.parse_iter_clauses("satisfies")?;
                self.expect_keyword("satisfies")?;
                let body = self.parse_bp(0)?;
                Ok(Node::Quant(Quantifier::Some, clauses, Box::new(body)))
            }
            "every" => {
                let clauses = self.parse_iter_clauses("satisfies")?;
                self.expect_keyword("satisfies")?;
                let body = self.parse_bp(0)?;
                Ok(Node::Quant(Quantifier::Every, clauses, Box::new(body)))
            }
            "function" => {
                self.expect(Tok::LParen)?;
                let mut params = Vec::new();
                if self.peek() != Some(&Tok::RParen) {
                    loop {
                        match self.next() {
                            Some(Tok::Ident(p)) => {
                                params.push(p);
                                // Optional `: type` annotation — consume and ignore.
                                if self.peek() == Some(&Tok::Colon) {
                                    self.pos += 1;
                                    self.read_type_name()?;
                                }
                            }
                            other => {
                                return Err(FeelError(format!(
                                    "expected a parameter name, got {other:?}"
                                )))
                            }
                        }
                        if self.peek() == Some(&Tok::Comma) {
                            self.pos += 1;
                            continue;
                        }
                        break;
                    }
                }
                self.expect(Tok::RParen)?;
                let body = self.parse_bp(0)?;
                Ok(Node::FuncDef(params, Box::new(body)))
            }
            _ => {
                // A (possibly multi-word) variable or function name.
                let mut full = name;
                loop {
                    match self.peek() {
                        Some(Tok::Ident(n)) if !is_keyword(n) => {
                            full.push(' ');
                            full.push_str(n);
                            self.pos += 1;
                        }
                        // Allow keywords (`and`, `of`, …) inside a multi-word
                        // builtin name (e.g. `date and time`, `day of week`)
                        // when they lead toward a known builtin.
                        Some(Tok::Ident(n))
                            if super::builtins::name_prefix(&format!("{full} {n}")) =>
                        {
                            full.push(' ');
                            full.push_str(n);
                            self.pos += 1;
                        }
                        _ => break,
                    }
                }
                Ok(Node::Var(full))
            }
        }
    }

    /// Parses `name in expr (, name in expr)*` up to `terminator`.
    fn parse_iter_clauses(&mut self, _terminator: &str) -> Result<Vec<(String, Node)>, FeelError> {
        let mut clauses = Vec::new();
        loop {
            let name = match self.next() {
                Some(Tok::Ident(n)) if !is_keyword(&n) => n,
                other => {
                    return Err(FeelError(format!(
                        "expected an iteration variable, got {other:?}"
                    )))
                }
            };
            self.expect_keyword("in")?;
            let source = self.parse_bp(0)?;
            clauses.push((name, source));
            if self.peek() == Some(&Tok::Comma) {
                self.pos += 1;
                continue;
            }
            break;
        }
        Ok(clauses)
    }

    /// Parses the right-hand side of `in`: a unary comparison (`< 5`), or a
    /// general expression (list, range, or single value).
    fn parse_in_rhs(&mut self) -> Result<Node, FeelError> {
        let make_range = |start, si, end, ei| {
            Node::Range(RangeNode {
                start_inclusive: si,
                start,
                end,
                end_inclusive: ei,
            })
        };
        match self.peek() {
            Some(Tok::Lt) => {
                self.pos += 1;
                let v = self.parse_bp(CMP_LBP)?;
                Ok(make_range(None, false, Some(Box::new(v)), false))
            }
            Some(Tok::Le) => {
                self.pos += 1;
                let v = self.parse_bp(CMP_LBP)?;
                Ok(make_range(None, false, Some(Box::new(v)), true))
            }
            Some(Tok::Gt) => {
                self.pos += 1;
                let v = self.parse_bp(CMP_LBP)?;
                Ok(make_range(Some(Box::new(v)), false, None, false))
            }
            Some(Tok::Ge) => {
                self.pos += 1;
                let v = self.parse_bp(CMP_LBP)?;
                Ok(make_range(Some(Box::new(v)), true, None, false))
            }
            _ => self.parse_bp(CMP_LBP),
        }
    }

    /// Reads a (possibly multi-word) FEEL type name after `instance of` / `:`.
    fn read_type_name(&mut self) -> Result<String, FeelError> {
        let first = match self.next() {
            Some(Tok::Ident(s)) => s,
            other => return Err(FeelError(format!("expected a type name, got {other:?}"))),
        };
        let extend = |p: &mut Parser, words: &[&str]| -> bool {
            if p.tokens.len() < p.pos + words.len() {
                return false;
            }
            for (i, w) in words.iter().enumerate() {
                match p.tokens.get(p.pos + i) {
                    Some(Tok::Ident(s)) if s == w => {}
                    _ => return false,
                }
            }
            p.pos += words.len();
            true
        };
        let name = match first.as_str() {
            "date" if extend(self, &["and", "time"]) => "date and time".to_string(),
            "days" if extend(self, &["and", "time", "duration"]) => {
                "days and time duration".to_string()
            }
            "years" if extend(self, &["and", "months", "duration"]) => {
                "years and months duration".to_string()
            }
            _ => first,
        };
        Ok(name)
    }

    fn parse_postfix(&mut self, mut node: Node) -> Result<Node, FeelError> {
        loop {
            match self.peek() {
                Some(Tok::Dot) => {
                    self.pos += 1;
                    match self.next() {
                        Some(Tok::Ident(name)) => {
                            node = Node::Member(Box::new(node), name);
                        }
                        _ => return Err(FeelError("expected a name after '.'".to_string())),
                    }
                }
                Some(Tok::LBracket) => {
                    self.pos += 1;
                    let inner = self.parse_bp(0)?;
                    self.expect(Tok::RBracket)?;
                    node = Node::Index(Box::new(node), Box::new(inner));
                }
                Some(Tok::LParen) => {
                    self.pos += 1;
                    let args = self.parse_call_args()?;
                    node = Node::Call(Box::new(node), args);
                }
                _ => break,
            }
        }
        Ok(node)
    }

    fn parse_call_args(&mut self) -> Result<CallArgs, FeelError> {
        if self.peek() == Some(&Tok::RParen) {
            self.pos += 1;
            return Ok(CallArgs::Positional(Vec::new()));
        }
        // Named args when the first argument looks like `name: …` (the name may
        // be multi-word, e.g. `start position: 2`).
        let named = {
            let mut k = self.pos;
            let mut saw_ident = false;
            while matches!(self.tokens.get(k), Some(Tok::Ident(s)) if !is_keyword(s)) {
                saw_ident = true;
                k += 1;
            }
            saw_ident && self.tokens.get(k) == Some(&Tok::Colon)
        };
        if named {
            let mut args = Vec::new();
            loop {
                let mut name = match self.next() {
                    Some(Tok::Ident(s)) => s,
                    other => {
                        return Err(FeelError(format!("expected an argument name, got {other:?}")))
                    }
                };
                while let Some(Tok::Ident(n)) = self.peek() {
                    if is_keyword(n) {
                        break;
                    }
                    name.push(' ');
                    name.push_str(n);
                    self.pos += 1;
                }
                self.expect(Tok::Colon)?;
                let value = self.parse_bp(0)?;
                args.push((name, value));
                if self.peek() == Some(&Tok::Comma) {
                    self.pos += 1;
                    continue;
                }
                break;
            }
            self.expect(Tok::RParen)?;
            Ok(CallArgs::Named(args))
        } else {
            let mut args = Vec::new();
            loop {
                args.push(self.parse_bp(0)?);
                if self.peek() == Some(&Tok::Comma) {
                    self.pos += 1;
                    continue;
                }
                break;
            }
            self.expect(Tok::RParen)?;
            Ok(CallArgs::Positional(args))
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
            Tok::Power => BinOp::Exp,
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
        Some((op, binding_power(op).0, binding_power(op).1))
    }
}

fn binding_power(op: BinOp) -> (u8, u8) {
    match op {
        BinOp::Or => (1, 2),
        BinOp::And => (3, 4),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => (5, 6),
        BinOp::Add | BinOp::Sub => (7, 8),
        BinOp::Mul | BinOp::Div => (9, 10),
        // Right-associative, binds tighter than unary minus.
        BinOp::Exp => (13, 12),
    }
}
