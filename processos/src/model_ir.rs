//! Phase 1 of the reversible semantic IR (ADR 0001): the **canonical pretty-printer**
//! `ProcessDefinition (+ names) → IR text`.
//!
//! The IR is the surface syntax of the engine's own executable model, so it cannot drift from
//! execution semantics. This module only *emits* the IR; the validating parser (IR → model) and
//! the engine-derived grammar/GBNF are later phases. The output is a **normal form**: elements are
//! rendered in a deterministic order (sorted by id) and sequence flows as a separate,
//! globally-sorted block, so `definition_to_ir` is a pure function of the model and two runs always
//! agree (a prerequisite for the future `emit(parse(ir)) == ir` idempotence guarantee).
//!
//! ## Concrete syntax
//!
//! ```text
//! process "<id>" {
//!   start <startEventId>
//!
//!   <node statements, sorted by id>
//!
//!   <flow statements, sorted by (from, to)>
//! }
//! ```
//!
//! A **node statement** is `<kindKeyword> <id> ["<name>"] [{ <attrs> }]` where the keyword is the
//! BPMN element kind (`serviceTask`, `exclusiveGateway`, …), the optional quoted name is the
//! preserved human label, and the optional brace block carries the load-bearing attributes for that
//! kind plus any element-level annotations (io mappings, retries, timer expression, multi-instance,
//! sub-process parent).
//!
//! A **flow statement** is `<from> -> <to> [when "<expr>"] [default]`: `when` carries a guard
//! condition, `default` marks an exclusive gateway's fallback flow — the exact construct the
//! imperative `edit_model` API could not express.
//!
//! The kind dispatch (`render_kind_attrs`) is an **exhaustive match** over `ElementKind`, so adding
//! a new engine element kind fails to compile until the IR learns to render it — the
//! coverage guarantee ADR 0001 relies on.

// Phase 1 is the pretty-printer only. Its public API (`definition_to_ir`/`xml_to_ir`) is exercised
// by this module's tests now and wired to a `read_model_ir` LLM tool in ADR 0001 Phase 6; until
// then it has no non-test caller in the binary.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};

use nanobpmn_engine_core::bpmn::parse_bpmn;
use nanobpmn_engine_core::{
    Element, ElementKind, ProcessDefinition, SequenceFlow, TimerDef, TimerDefKind,
};

/// Render a process definition to canonical IR text. `names` maps element id → human label (as
/// recovered from the source BPMN, which the engine model itself drops); ids absent from the map
/// render without a quoted name.
pub fn definition_to_ir(def: &ProcessDefinition, names: &HashMap<String, String>) -> String {
    let mut out = String::new();
    out.push_str(&format!("process {} {{\n", quote(&def.id)));
    out.push_str(&format!("  start {}\n", def.start_event));

    // Nodes in a deterministic order (sorted by id) so the emission is a normal form.
    let ordered: BTreeMap<&str, &Element> =
        def.elements.iter().map(|(k, v)| (k.as_str(), v)).collect();

    if !ordered.is_empty() {
        out.push('\n');
    }
    for (id, el) in &ordered {
        render_node(id, el, names.get(*id).map(String::as_str), &mut out);
    }

    // Sequence flows as one globally-sorted block, keyed by (from, to), decoupled from node order.
    let mut flows: Vec<(&str, &SequenceFlow)> = Vec::new();
    for (id, el) in &ordered {
        for f in &el.outgoing {
            flows.push((id, f));
        }
    }
    flows.sort_by(|a, b| (a.0, a.1.to.as_str()).cmp(&(b.0, b.1.to.as_str())));
    if !flows.is_empty() {
        out.push('\n');
    }
    for (from, f) in &flows {
        render_flow(from, f, &mut out);
    }

    out.push_str("}\n");
    out
}

/// Parse BPMN and render it to IR in one step, recovering element names from the XML. A convenience
/// for callers (and the future `read_model_ir` tool) that hold raw BPMN rather than a parsed model.
pub fn xml_to_ir(xml: &str) -> Result<String, String> {
    let defs = parse_bpmn(xml).map_err(|e| format!("model failed to parse: {e:?}"))?;
    let def = defs
        .into_iter()
        .next()
        .ok_or_else(|| "no process definition in the document".to_string())?;
    let names = crate::bpmn_model::parse_element_names(xml);
    Ok(definition_to_ir(&def, &names))
}

fn render_node(id: &str, el: &Element, name: Option<&str>, out: &mut String) {
    out.push_str("  ");
    out.push_str(kind_keyword(&el.kind));
    out.push(' ');
    out.push_str(id);
    if let Some(n) = name {
        if !n.trim().is_empty() {
            out.push(' ');
            out.push_str(&quote(n));
        }
    }

    let mut attrs: Vec<String> = Vec::new();
    render_kind_attrs(&el.kind, &mut attrs);
    render_element_attrs(el, &mut attrs);

    if attrs.is_empty() {
        out.push('\n');
    } else {
        out.push_str(" {\n");
        for a in &attrs {
            out.push_str("    ");
            out.push_str(a);
            out.push('\n');
        }
        out.push_str("  }\n");
    }
}

/// The IR keyword for an element kind — the surface-syntax half of the notation table. Mirrors the
/// BPMN element names so a modeler recognises them.
fn kind_keyword(kind: &ElementKind) -> &'static str {
    match kind {
        ElementKind::StartEvent => "startEvent",
        ElementKind::EndEvent => "endEvent",
        ElementKind::ServiceTask { .. } => "serviceTask",
        ElementKind::UserTask(_) => "userTask",
        ElementKind::ExclusiveGateway => "exclusiveGateway",
        ElementKind::ParallelGateway => "parallelGateway",
        ElementKind::ErrorBoundaryEvent { .. } => "errorBoundaryEvent",
        ElementKind::TimerIntermediateCatchEvent { .. } => "timerIntermediateCatchEvent",
        ElementKind::TimerBoundaryEvent { .. } => "timerBoundaryEvent",
        ElementKind::MessageIntermediateCatchEvent { .. } => "messageIntermediateCatchEvent",
        ElementKind::MessageBoundaryEvent { .. } => "messageBoundaryEvent",
        ElementKind::MessageStartEvent { .. } => "messageStartEvent",
        ElementKind::TimerStartEvent { .. } => "timerStartEvent",
        ElementKind::SubProcess { .. } => "subProcess",
        ElementKind::IntermediateThrowEvent => "intermediateThrowEvent",
        ElementKind::ScriptTask { .. } => "scriptTask",
        ElementKind::CallActivity { .. } => "callActivity",
        ElementKind::SignalIntermediateCatchEvent { .. } => "signalIntermediateCatchEvent",
        ElementKind::SignalBoundaryEvent { .. } => "signalBoundaryEvent",
        ElementKind::ConditionalIntermediateCatchEvent { .. } => {
            "conditionalIntermediateCatchEvent"
        }
        ElementKind::ConditionalBoundaryEvent { .. } => "conditionalBoundaryEvent",
    }
}

/// Emit the kind-specific attribute lines. Exhaustive over `ElementKind`: a new engine element kind
/// will not compile until its IR notation is added here (ADR 0001 coverage guarantee).
fn render_kind_attrs(kind: &ElementKind, attrs: &mut Vec<String>) {
    match kind {
        ElementKind::StartEvent
        | ElementKind::EndEvent
        | ElementKind::ExclusiveGateway
        | ElementKind::ParallelGateway
        | ElementKind::IntermediateThrowEvent => {}
        ElementKind::ServiceTask { job_type, priority } => {
            attrs.push(format!("jobType {}", quote(job_type)));
            if let Some(p) = priority {
                attrs.push(format!("priority {}", quote(p)));
            }
        }
        ElementKind::UserTask(props) => {
            if let Some(v) = &props.assignee {
                attrs.push(format!("assignee {}", quote(v)));
            }
            if let Some(v) = &props.candidate_groups {
                attrs.push(format!("candidateGroups {}", quote(v)));
            }
            if let Some(v) = &props.candidate_users {
                attrs.push(format!("candidateUsers {}", quote(v)));
            }
            if let Some(v) = &props.due_date {
                attrs.push(format!("dueDate {}", quote(v)));
            }
            if let Some(v) = &props.follow_up_date {
                attrs.push(format!("followUpDate {}", quote(v)));
            }
            if let Some(v) = &props.priority {
                attrs.push(format!("priority {}", quote(v)));
            }
        }
        ElementKind::ErrorBoundaryEvent {
            attached_to,
            error_code,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            attrs.push(format!("errorCode {}", quote(error_code)));
        }
        ElementKind::TimerIntermediateCatchEvent { duration_millis } => {
            attrs.push(format!("duration {duration_millis}ms"));
        }
        ElementKind::TimerBoundaryEvent {
            attached_to,
            duration_millis,
            interrupting,
            repeating,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            attrs.push(format!("duration {duration_millis}ms"));
            attrs.push(format!("interrupting {interrupting}"));
            attrs.push(format!("repeating {repeating}"));
        }
        ElementKind::MessageIntermediateCatchEvent {
            message_name,
            correlation_key,
        } => {
            attrs.push(format!("message {}", quote(message_name)));
            attrs.push(format!("correlationKey {}", quote(correlation_key)));
        }
        ElementKind::MessageBoundaryEvent {
            attached_to,
            message_name,
            correlation_key,
            interrupting,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            attrs.push(format!("message {}", quote(message_name)));
            attrs.push(format!("correlationKey {}", quote(correlation_key)));
            attrs.push(format!("interrupting {interrupting}"));
        }
        ElementKind::MessageStartEvent { message_name } => {
            attrs.push(format!("message {}", quote(message_name)));
        }
        ElementKind::TimerStartEvent {
            interval_millis,
            repeating,
        } => {
            attrs.push(format!("interval {interval_millis}ms"));
            attrs.push(format!("repeating {repeating}"));
        }
        ElementKind::SubProcess { start_event } => {
            attrs.push(format!("startEvent {start_event}"));
        }
        ElementKind::ScriptTask {
            expression,
            result_variable,
        } => {
            attrs.push(format!("expression {}", quote(expression)));
            attrs.push(format!("resultVariable {}", quote(result_variable)));
        }
        ElementKind::CallActivity { called_process_id } => {
            attrs.push(format!("calledElement {}", quote(called_process_id)));
        }
        ElementKind::SignalIntermediateCatchEvent { signal_name } => {
            attrs.push(format!("signal {}", quote(signal_name)));
        }
        ElementKind::SignalBoundaryEvent {
            attached_to,
            signal_name,
            interrupting,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            attrs.push(format!("signal {}", quote(signal_name)));
            attrs.push(format!("interrupting {interrupting}"));
        }
        ElementKind::ConditionalIntermediateCatchEvent { condition } => {
            attrs.push(format!("condition {}", quote(condition)));
        }
        ElementKind::ConditionalBoundaryEvent {
            attached_to,
            condition,
            interrupting,
        } => {
            attrs.push(format!("attachedTo {attached_to}"));
            attrs.push(format!("condition {}", quote(condition)));
            attrs.push(format!("interrupting {interrupting}"));
        }
    }
}

/// Emit element-level annotations shared across kinds (sub-process parent, job retries, timer FEEL
/// expression, io mappings, multi-instance). These are the engine-ignored-but-load-bearing extras
/// the IR preserves.
fn render_element_attrs(el: &Element, attrs: &mut Vec<String>) {
    if let Some(parent) = &el.parent {
        attrs.push(format!("parent {parent}"));
    }
    if let Some(retries) = &el.retries {
        attrs.push(format!("retries {}", quote(retries)));
    }
    if let Some(timer) = &el.timer {
        attrs.push(format!("timer {}", render_timer(timer)));
    }
    for m in &el.io.inputs {
        attrs.push(format!("input {} <- {}", m.target, quote(&m.source)));
    }
    for m in &el.io.outputs {
        attrs.push(format!("output {} <- {}", m.target, quote(&m.source)));
    }
    if let Some(mi) = &el.multi_instance {
        let mut parts: Vec<String> = vec![format!("collection {}", quote(&mi.input_collection))];
        if let Some(v) = &mi.input_element {
            parts.push(format!("inputElement {}", quote(v)));
        }
        if let Some(v) = &mi.output_collection {
            parts.push(format!("outputCollection {}", quote(v)));
        }
        if let Some(v) = &mi.output_element {
            parts.push(format!("outputElement {}", quote(v)));
        }
        if let Some(v) = &mi.completion_condition {
            parts.push(format!("completionCondition {}", quote(v)));
        }
        parts.push(format!("sequential {}", mi.sequential));
        attrs.push(format!("multiInstance {{ {} }}", parts.join(", ")));
    }
}

fn render_timer(timer: &TimerDef) -> String {
    let kind = match timer.kind {
        TimerDefKind::Duration => "duration",
        TimerDefKind::Cycle => "cycle",
        TimerDefKind::Date => "date",
    };
    format!("{kind} {}", quote(&timer.expr))
}

fn render_flow(from: &str, f: &SequenceFlow, out: &mut String) {
    out.push_str(&format!("  {from} -> {}", f.to));
    if let Some(c) = &f.condition {
        out.push_str(&format!(" when {}", quote(&c.expression)));
    }
    if f.is_default {
        out.push_str(" default");
    }
    out.push('\n');
}

/// Quote a free-text value (name, FEEL expression, job type, …) as a double-quoted string with `\`
/// and `"` escaped, so values containing spaces or IR punctuation round-trip unambiguously.
fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::ProcessBuilder;

    use super::*;

    const LOAN_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="loan-approval" isExecutable="true">
    <bpmn:startEvent id="Start" name="Application received">
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:serviceTask id="CheckCredit" name="Check credit">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="check-credit" />
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:exclusiveGateway id="Decision" name="Route by amount" default="f4">
      <bpmn:incoming>f2</bpmn:incoming>
      <bpmn:outgoing>f3</bpmn:outgoing>
      <bpmn:outgoing>f4</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:endEvent id="Approve" name="Approved">
      <bpmn:incoming>f3</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:endEvent id="Manual" name="Manual review">
      <bpmn:incoming>f4</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="CheckCredit" />
    <bpmn:sequenceFlow id="f2" sourceRef="CheckCredit" targetRef="Decision" />
    <bpmn:sequenceFlow id="f3" sourceRef="Decision" targetRef="Approve">
      <bpmn:conditionExpression>= amount &gt;= 1000</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="f4" sourceRef="Decision" targetRef="Manual" />
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn renders_the_loan_model_with_names_default_and_condition() {
        let ir = xml_to_ir(LOAN_BPMN).expect("render loan model");
        // Header + start.
        assert!(ir.contains("process \"loan-approval\" {"), "\n{ir}");
        assert!(ir.contains("  start Start\n"), "\n{ir}");
        // Node statements: kind keyword + id + preserved name + attr block.
        assert!(
            ir.contains("startEvent Start \"Application received\"\n"),
            "\n{ir}"
        );
        assert!(
            ir.contains(
                "serviceTask CheckCredit \"Check credit\" {\n    jobType \"check-credit\"\n  }\n"
            ),
            "\n{ir}"
        );
        assert!(
            ir.contains("exclusiveGateway Decision \"Route by amount\"\n"),
            "\n{ir}"
        );
        // The two constructs the imperative API could not express, now first-class in the IR.
        assert!(
            ir.contains("Decision -> Approve when \"= amount >= 1000\"\n"),
            "\n{ir}"
        );
        assert!(ir.contains("Decision -> Manual default\n"), "\n{ir}");
    }

    #[test]
    fn is_deterministic_across_runs() {
        let a = xml_to_ir(LOAN_BPMN).expect("render");
        let b = xml_to_ir(LOAN_BPMN).expect("render");
        assert_eq!(a, b, "the pretty-printer must be a normal form");
    }

    #[test]
    fn nodes_sorted_by_id_flows_after_nodes() {
        let ir = xml_to_ir(LOAN_BPMN).expect("render");
        // Node ids appear in sorted order.
        let pos = |needle: &str| ir.find(needle).unwrap_or(usize::MAX);
        assert!(pos("\n  serviceTask CheckCredit") < pos("\n  exclusiveGateway Decision"));
        assert!(pos("\n  exclusiveGateway Decision") < pos("\n  startEvent Start"));
        // All flow statements come after all node statements.
        assert!(pos("startEvent Start") < pos("  Decision -> Approve"));
    }

    #[test]
    fn renders_io_mappings_and_retries() {
        // A service task carrying io mappings + a retries expression exercises the element-level
        // annotation channel.
        let def = ProcessBuilder::new("Mapped")
            .start_event("Start")
            .service_task("Work", "do-work")
            .end_event("Done")
            .connect("Start", "Work")
            .connect("Work", "Done")
            .build()
            .unwrap();
        let mut def = def;
        let work = def.elements.get_mut("Work").unwrap();
        work.io.inputs.push(nanobpmn_engine_core::Mapping {
            source: "=order.amount".into(),
            target: "amount".into(),
        });
        work.io.outputs.push(nanobpmn_engine_core::Mapping {
            source: "=result.ok".into(),
            target: "approved".into(),
        });
        work.retries = Some("5".into());
        let ir = definition_to_ir(&def, &HashMap::new());
        assert!(ir.contains("input amount <- \"=order.amount\""), "\n{ir}");
        assert!(ir.contains("output approved <- \"=result.ok\""), "\n{ir}");
        assert!(ir.contains("retries \"5\""), "\n{ir}");
    }

    #[test]
    fn renders_an_error_boundary_attached_to_a_task() {
        let def = ProcessBuilder::new("Boundaried")
            .start_event("Start")
            .service_task("Charge", "charge-card")
            .error_boundary_event("OnDecline", "Charge", "CARD_DECLINED")
            .end_event("Done")
            .end_event("Declined")
            .connect("Start", "Charge")
            .connect("Charge", "Done")
            .connect("OnDecline", "Declined")
            .build()
            .unwrap();
        let ir = definition_to_ir(&def, &HashMap::new());
        assert!(
            ir.contains("errorBoundaryEvent OnDecline {\n    attachedTo Charge\n    errorCode \"CARD_DECLINED\"\n  }\n"),
            "\n{ir}"
        );
        // A boundary event has no incoming flow but does have an outgoing one.
        assert!(ir.contains("OnDecline -> Declined\n"), "\n{ir}");
    }

    #[test]
    fn escapes_quotes_in_names() {
        let def = ProcessBuilder::new("Q")
            .start_event("Start")
            .end_event("Done")
            .connect("Start", "Done")
            .build()
            .unwrap();
        let mut names = HashMap::new();
        names.insert("Start".to_string(), "the \"real\" start".to_string());
        let ir = definition_to_ir(&def, &names);
        assert!(
            ir.contains("startEvent Start \"the \\\"real\\\" start\"\n"),
            "\n{ir}"
        );
    }
}
