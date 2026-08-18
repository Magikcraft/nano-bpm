//! Rule #854 — gateway condition-or-default (STUB).
//!
//! Sibling slice #854 implements this rule by editing **only** this file. For a
//! gateway (exclusive/inclusive) with more than one outgoing flow, reject any
//! outgoing flow that has no condition and is not the gateway's `default`,
//! returning
//! [`ParseError::InvalidGateway`](crate::bpmn::ParseError::InvalidGateway).
//! Gateway outgoing flows, their conditions, and the `default` marker are
//! already available on the built [`ProcessDefinition`](crate::model::ProcessDefinition)
//! (`input.def`); the raw `default` flow id is also in `capture.references`
//! (`kind == "default"`).
//!
//! Do **not** edit `crate::bpmn`'s streaming parser, `validate/mod.rs` (this
//! validator is already registered), or the `ParseError` enum. No-op until #854
//! lands.

use super::ValidationInput;
use crate::bpmn::ParseError;

pub(crate) fn validate(_input: &ValidationInput<'_>) -> Result<(), ParseError> {
    Ok(())
}
