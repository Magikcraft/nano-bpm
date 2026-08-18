//! Rule #853 — unsupported flow elements / event definitions (STUB).
//!
//! Sibling slice #853 implements this rule by editing **only** this file. The
//! streaming parser already records every unmodelled flow-element tag / event
//! definition it does not model as `{ tag, element_id }` in
//! `capture.unmodelled` (an explicit ignore-list keeps out non-flow noise).
//! #853 must reject each recorded entry that is genuinely unsupported —
//! deriving the *supported* set from the canonical registry (`processos`
//! `ELEMENT_KIND_SPECS` / engine-core `ElementKind`) so it cannot drift — with
//! [`ParseError::UnsupportedElement`](crate::bpmn::ParseError::UnsupportedElement).
//!
//! Do **not** edit `crate::bpmn`'s streaming parser / catch-all (the tags are
//! already recorded), `validate/mod.rs` (this validator is already registered),
//! or the `ParseError` enum. No-op until #853 lands.

use super::ValidationInput;
use crate::bpmn::ParseError;

pub(crate) fn validate(_input: &ValidationInput<'_>) -> Result<(), ParseError> {
    Ok(())
}
