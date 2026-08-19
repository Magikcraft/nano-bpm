//! Rule #855 — start-event count parity.
//!
//! Zeebe allows a process to declare more than one start event — a *none* start
//! alongside any number of *typed* (message / timer / signal) starts — but
//! forbids **more than one none start** ("Multiple none start events are not
//! allowed"), and requires **at least one** start event ("Must have at least one
//! start event"). Nano historically treated the process-level start as unique
//! (`crate::model::ProcessBuilder::build` returned a "more than one start event"
//! error for any second start), which both wrongly *rejected* valid
//! multi-typed-start models and under-specified the none-start rule.
//!
//! The fix has three parts:
//! * `crate::model::ProcessBuilder::build` now permits multiple start events and
//!   deterministically designates the process-entry start (preferring the none
//!   start). It keeps returning `NoStartEvent` when a process declares none, so
//!   the "at least one start event" rule is enforced there (a zero-start process
//!   never builds and thus never reaches this validator).
//! * The streaming parser keeps *every* none, message and timer process-level
//!   start (message and timer starts are each wired to their own deploy-time
//!   trigger by `crate::engine`), and demotes only surplus *signal* starts to
//!   inert throw events — nano has no dedicated signal-start element kind, so a
//!   surviving signal start would be indistinguishable from a none start here. A
//!   model with two none starts therefore reaches this validator with both
//!   present.
//! * This validator rejects a definition that still carries more than one none
//!   start with
//!   [`ParseError::InvalidStartEvents`](crate::bpmn::ParseError::InvalidStartEvents).
//!
//! A none start is an untyped process-level `ElementKind::StartEvent`; message
//! and timer starts are their own element kinds, and a surplus signal start is
//! demoted (a lone signal start survives as the sole entry `StartEvent` when
//! there is no none start). So a definition whose process-level `StartEvent`-kind
//! count exceeds one genuinely declares multiple none starts.

use super::ValidationInput;
use crate::bpmn::ParseError;
use crate::model::ElementKind;

pub(crate) fn validate(input: &ValidationInput<'_>) -> Result<(), ParseError> {
    let def = input.def;
    let none_starts = def
        .elements
        .values()
        .filter(|e| e.parent.is_none() && matches!(e.kind, ElementKind::StartEvent))
        .count();
    if none_starts > 1 {
        return Err(ParseError::InvalidStartEvents {
            process_id: def.id.clone(),
            reason: "Multiple none start events are not allowed".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::bpmn::{parse_bpmn, ParseError};

    /// Wraps process-body `inner` in a `<definitions>` declaring a reusable
    /// `<message>` and `<signal>` so typed start events resolve.
    fn wrap(inner: &str) -> String {
        format!(
            r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
                 <bpmn:message id="Msg" name="msgName"/>
                 <bpmn:signal id="Sig" name="sigName"/>
                 <bpmn:process id="p" isExecutable="true">{inner}</bpmn:process>
               </bpmn:definitions>"#
        )
    }

    /// A process with two start events `s1` and `s2`, each flowing to its own end
    /// event. `def2` is the (optional) event definition nested in `s2`.
    fn two_starts(def2: &str) -> String {
        wrap(&format!(
            r#"<bpmn:startEvent id="s1"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:startEvent id="s2">{def2}<bpmn:outgoing>f2</bpmn:outgoing></bpmn:startEvent>
               <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
               <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s1" targetRef="e1"/>
               <bpmn:sequenceFlow id="f2" sourceRef="s2" targetRef="e2"/>"#
        ))
    }

    const MESSAGE_DEF: &str = r#"<bpmn:messageEventDefinition messageRef="Msg"/>"#;
    const TIMER_DEF: &str = r#"<bpmn:timerEventDefinition><bpmn:timeCycle>R/PT1H</bpmn:timeCycle></bpmn:timerEventDefinition>"#;
    const SIGNAL_DEF: &str = r#"<bpmn:signalEventDefinition signalRef="Sig"/>"#;

    fn is_multiple_none(err: &ParseError) -> bool {
        matches!(
            err,
            ParseError::InvalidStartEvents { reason, .. }
                if reason.contains("Multiple none start events")
        )
    }

    #[test]
    fn multiple_none_starts_are_rejected() {
        // RED on `main` (which silently demoted the second none start and
        // accepted): two none starts must now be rejected.
        let err = parse_bpmn(&two_starts("")).expect_err("two none starts rejected");
        assert!(
            is_multiple_none(&err),
            "expected multiple-none rejection, got {err:?}"
        );
    }

    #[test]
    fn one_none_plus_each_typed_start_is_accepted_with_the_none_as_entry() {
        // The whole positive class: a none start alongside any single typed start
        // is accepted, and the engine's process-entry start is the none start.
        for def2 in [MESSAGE_DEF, TIMER_DEF, SIGNAL_DEF] {
            let defs = parse_bpmn(&two_starts(def2))
                .unwrap_or_else(|e| panic!("none + typed start accepted (def2={def2}): {e:?}"));
            assert_eq!(defs.len(), 1);
            assert_eq!(
                defs[0].start_event, "s1",
                "none start is the process entry (def2={def2})"
            );
        }
    }

    #[test]
    fn a_single_none_start_is_accepted() {
        let inner = r#"<bpmn:startEvent id="s1"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                       <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
                       <bpmn:sequenceFlow id="f1" sourceRef="s1" targetRef="e1"/>"#;
        let defs = parse_bpmn(&wrap(inner)).expect("single none start accepted");
        assert_eq!(defs[0].start_event, "s1");
    }

    #[test]
    fn zero_start_events_is_rejected_as_no_start_event() {
        // The "at least one start event" rule is enforced by the builder, which
        // never produces a definition for a start-less process.
        let err = parse_bpmn(&wrap(r#"<bpmn:endEvent id="e1"/>"#))
            .expect_err("zero start events rejected");
        assert!(
            matches!(&err, ParseError::InvalidProcess { reason, .. } if reason.contains("no start event")),
            "expected NoStartEvent, got {err:?}"
        );
    }

    #[test]
    fn multiple_typed_starts_without_a_none_start_are_accepted() {
        // No none start, several typed starts: permitted (at most one none).
        let inner = format!(
            r#"<bpmn:startEvent id="s1">{MESSAGE_DEF}<bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:startEvent id="s2">{SIGNAL_DEF}<bpmn:outgoing>f2</bpmn:outgoing></bpmn:startEvent>
               <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
               <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s1" targetRef="e1"/>
               <bpmn:sequenceFlow id="f2" sourceRef="s2" targetRef="e2"/>"#
        );
        parse_bpmn(&wrap(&inner)).expect("multiple typed starts (no none) accepted");
    }

    #[test]
    fn two_none_starts_plus_a_typed_start_is_still_rejected() {
        // Asserts the class, not just the 2-start case: the none count is what
        // matters, regardless of any accompanying typed starts.
        let inner = r#"<bpmn:startEvent id="s1"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                       <bpmn:startEvent id="s2"><bpmn:outgoing>f2</bpmn:outgoing></bpmn:startEvent>
                       <bpmn:startEvent id="s3"><bpmn:messageEventDefinition messageRef="Msg"/><bpmn:outgoing>f3</bpmn:outgoing></bpmn:startEvent>
                       <bpmn:endEvent id="e1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
                       <bpmn:endEvent id="e2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
                       <bpmn:endEvent id="e3"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
                       <bpmn:sequenceFlow id="f1" sourceRef="s1" targetRef="e1"/>
                       <bpmn:sequenceFlow id="f2" sourceRef="s2" targetRef="e2"/>
                       <bpmn:sequenceFlow id="f3" sourceRef="s3" targetRef="e3"/>"#;
        let err = parse_bpmn(&wrap(inner)).expect_err("two none + typed rejected");
        assert!(
            is_multiple_none(&err),
            "expected multiple-none rejection, got {err:?}"
        );
    }
}
