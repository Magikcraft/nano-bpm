//! Rule #851 — generic reference integrity (STUB).
//!
//! Sibling slice #851 implements this rule by editing **only** this file. It
//! must reject every remaining dangling QName/id reference the parser captured
//! on [`ProcessCapture`](super::ProcessCapture) — `messageRef`, `errorRef`,
//! `signalRef`, `escalationRef`, gateway/activity `default`, boundary
//! `attachedToRef` (all in `capture.references`), and link throw↔catch pairing
//! (`capture.link_throws` / `capture.link_catches`) — returning
//! [`ParseError::UnresolvedReference`](crate::bpmn::ParseError::UnresolvedReference)
//! (the single shared unresolved-reference variant; do not add bespoke ones).
//! Declared definition ids are in `capture.declared_messages` / `_errors` /
//! `_signals` / `_escalations`.
//!
//! Do **not** edit `crate::bpmn`'s streaming parser (the references are already
//! captured), `validate/mod.rs` (this validator is already registered), or the
//! `ParseError` enum. No-op until #851 lands.

use super::ValidationInput;
use crate::bpmn::ParseError;

pub(crate) fn validate(_input: &ValidationInput<'_>) -> Result<(), ParseError> {
    Ok(())
}
