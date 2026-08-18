//! Rule #849 — dangling `<incoming>`/`<outgoing>` sequence-flow references.
//!
//! Nano builds the process graph solely from `<sequenceFlow>` `sourceRef`/
//! `targetRef` and otherwise ignores a flow node's `<incoming>`/`<outgoing>`
//! child references. A node that declares an `<incoming>`/`<outgoing>` reference
//! to a `<sequenceFlow>` id that is not declared is therefore silently accepted
//! at deploy, whereas Zeebe rejects it: `<incoming>`/`<outgoing>` are QName
//! element references to `SequenceFlow` (`FlowNodeImpl` incoming/outgoing
//! collections in camunda-xml-model), and an unresolved QName reference fails
//! deploy with `INVALID_ARGUMENT`. This validator restores that parity.
//!
//! It reads the `<incoming>`/`<outgoing>` references the streaming parser
//! captured (`capture.flow_refs`) and asserts each resolves to a declared
//! `<sequenceFlow>` id (`capture.flow_ids`) — checked against the raw parsed
//! flows (before ad-hoc pruning), mirroring what Zeebe sees.

use super::ValidationInput;
use crate::bpmn::ParseError;

pub(crate) fn validate(input: &ValidationInput<'_>) -> Result<(), ParseError> {
    let capture = input.capture;
    for reference in &capture.flow_refs {
        if !capture.flow_ids.contains(&reference.flow_id) {
            return Err(ParseError::UnresolvedReference {
                kind: reference.direction.to_string(),
                id: reference.flow_id.clone(),
                process_id: capture.process_id.clone(),
                from_node: reference.node_id.clone(),
            });
        }
    }
    Ok(())
}
