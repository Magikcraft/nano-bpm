//! The FEEL abstract syntax tree.
//!
//! Literals are stored as plain primitives (not [`crate::model::Value`] or
//! `FeelVal`) so this module has no dependency on the value layer — that keeps
//! the module graph one-directional (`value` -> `ast`, never the reverse), which
//! matters because a lambda value embeds an `Rc<Node>` body.

#[derive(Clone, Debug, PartialEq)]
pub enum Node {
    Null,
    BoolLit(bool),
    NumLit(f64, bool), // value, is_integer
    StrLit(String),
    /// An `@"…"` temporal literal (date / time / date-time / duration inferred).
    AtLit(String),
    Var(String),
    Member(Box<Node>, String),
    Index(Box<Node>, Box<Node>),
    Neg(Box<Node>),
    Not(Box<Node>),
    Bin(BinOp, Box<Node>, Box<Node>),
    List(Vec<Node>),
    Context(Vec<(String, Node)>),
    If(Box<Node>, Box<Node>, Box<Node>),
    For(Vec<(String, Node)>, Box<Node>),
    Quant(Quantifier, Vec<(String, Node)>, Box<Node>),
    Between(Box<Node>, Box<Node>, Box<Node>),
    In(Box<Node>, Box<Node>),
    InstanceOf(Box<Node>, String),
    FuncDef(Vec<String>, Box<Node>),
    Call(Box<Node>, CallArgs),
    Range(RangeNode),
}

#[derive(Clone, Debug, PartialEq)]
pub enum CallArgs {
    Positional(Vec<Node>),
    Named(Vec<(String, Node)>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct RangeNode {
    pub start_inclusive: bool,
    pub start: Option<Box<Node>>,
    pub end: Option<Box<Node>>,
    pub end_inclusive: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Quantifier {
    Some,
    Every,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Exp,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}
