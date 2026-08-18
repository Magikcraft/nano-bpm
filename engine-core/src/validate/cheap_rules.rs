//! Rule #856 — remaining cheap validators (STUB).
//!
//! Sibling slice #856 implements three independent rules by editing **only**
//! this file:
//!   1. an end event must have no outgoing `<sequenceFlow>`
//!      ([`ParseError::InvalidEndEvent`](crate::bpmn::ParseError::InvalidEndEvent));
//!   2. no two start events may correlate on the same message or signal
//!      ([`ParseError::DuplicateStartEvent`](crate::bpmn::ParseError::DuplicateStartEvent));
//!   3. a required `zeebe:taskDefinition` attribute (`type`, and `retries` when
//!      present) must be non-empty
//!      ([`ParseError::InvalidTaskDefinition`](crate::bpmn::ParseError::InvalidTaskDefinition)).
//!
//! End-event outgoing flows and start-event definitions are on the built
//! [`ProcessDefinition`](crate::model::ProcessDefinition) (`input.def`); the raw
//! `zeebe:taskDefinition` attribute strings are in `capture.task_definitions`.
//!
//! Do **not** edit `crate::bpmn`'s streaming parser, `validate/mod.rs` (this
//! validator is already registered), or the `ParseError` enum. No-op until #856
//! lands.

use super::ValidationInput;
use crate::bpmn::ParseError;

pub(crate) fn validate(_input: &ValidationInput<'_>) -> Result<(), ParseError> {
    Ok(())
}
