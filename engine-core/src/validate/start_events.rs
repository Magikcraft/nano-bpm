//! Rule #855 — start-event count parity (STUB).
//!
//! Sibling slice #855 implements this rule. Zeebe allows multiple *typed* start
//! events (none + message + timer + signal) but forbids more than one *none*
//! start, and requires at least one start event. Unlike its siblings, #855 also
//! needs a targeted change to `crate::model::ProcessDefinition`'s builder
//! (`build()`), whose existing `MultipleStartEvents` logic is over-strict — keep
//! that change confined to the start-event logic there plus this file. Reject
//! with
//! [`ParseError::InvalidStartEvents`](crate::bpmn::ParseError::InvalidStartEvents).
//!
//! Do **not** edit `crate::bpmn`'s streaming parser (start-event types are
//! already parsed), `validate/mod.rs` (this validator is already registered),
//! or the `ParseError` enum. No-op until #855 lands.

use super::ValidationInput;
use crate::bpmn::ParseError;

pub(crate) fn validate(_input: &ValidationInput<'_>) -> Result<(), ParseError> {
    Ok(())
}
