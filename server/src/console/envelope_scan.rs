//! Model scan for data-envelope worker & message I/O (ADR 0040 slices 1–2 /
//! ADR 0033 §6 increment 12).
//!
//! The BPMN model is the authoritative source of the data envelope
//! (`io.nanobpm.dataEnvelope.in`/`.out`, carried as a `zeebe:property`, written by
//! the modeler's *Data envelope* panel): on a **service task** it types the
//! worker's job I/O, and on a shared **`bpmn:message`** it types the message's
//! payload. The typed-worker codegen historically read the worker I/O map from the
//! manifest's hand-maintained `workers[]` projection, so the two could drift; the
//! message payload was not consumed by codegen at all.
//!
//! This scan closes both gaps: it parses the project's process models and derives
//! `taskType -> {in,out}` (slice 1) and `messageName -> {in,out}` (slice 2)
//! **from the model**, so the reifier rebuilds the worker-IO map and the typed
//! `publishMessage` registry from the source of truth. It is the model-scan half
//! of the Fused Domain Model pipeline (ADR 0040 §7/§8): the models contribute
//! their motion-shape bindings, and the derived registries are recomputed from
//! them.
//!
//! The scan is intentionally Urban-specific and lives in the console, not in
//! `engine-core`: the envelope is an Urban concept and the engine stays
//! Zeebe-pure. It reuses only the engine's generic XML tokenizer
//! ([`nanobpmn_engine_core::xml`]).

use std::collections::BTreeMap;
use std::path::Path;

use nanobpmn_engine_core::xml::{Token, attr, local_name, tokenize};
use serde::Serialize;

/// Reserved `zeebe:property` name carrying the input data-envelope type ref.
/// Must match `console/src/lib/dataEnvelope.ts` `ENVELOPE_KEY.inputType`.
const ENVELOPE_IN: &str = "io.nanobpm.dataEnvelope.in";
/// Reserved `zeebe:property` name carrying the output data-envelope type ref.
/// Must match `console/src/lib/dataEnvelope.ts` `ENVELOPE_KEY.outputType`.
const ENVELOPE_OUT: &str = "io.nanobpm.dataEnvelope.out";

/// The service-ish task local names whose `zeebe:taskDefinition:type` keys a
/// `workers[]` entry. Mirrors `SERVICE_TASK_TYPES` in `dataEnvelope.ts`.
const SERVICE_TASK_LOCALS: [&str; 4] =
    ["serviceTask", "businessRuleTask", "scriptTask", "sendTask"];

/// A model-derived worker I/O binding: a job type and the envelope type refs it
/// carries. Serializes to the shape the reifier's `WorkerDecl` reads
/// (`{ taskType, inputType?, outputType? }`), so it can be injected straight into
/// the `domaintypes` op request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct WorkerIo {
    #[serde(rename = "taskType")]
    pub task_type: String,
    #[serde(rename = "inputType", skip_serializing_if = "Option::is_none")]
    pub input_type: Option<String>,
    #[serde(rename = "outputType", skip_serializing_if = "Option::is_none")]
    pub output_type: Option<String>,
}

/// A model-derived message payload binding: a message name and the envelope type
/// refs it carries. Serializes to `{ messageName, inputType?, outputType? }` — the
/// shape the reifier's message-IO codegen reads — so it can be injected straight
/// into the `domaintypes` op request (ADR 0040 slice 2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MessageIo {
    #[serde(rename = "messageName")]
    pub message_name: String,
    #[serde(rename = "inputType", skip_serializing_if = "Option::is_none")]
    pub input_type: Option<String>,
    #[serde(rename = "outputType", skip_serializing_if = "Option::is_none")]
    pub output_type: Option<String>,
}

/// Everything one model scan lifts from a BPMN document: the service-task worker
/// I/O map (slice 1) and the message payload map (slice 2).
#[derive(Default)]
pub struct BpmnScan {
    pub workers: Vec<WorkerIo>,
    pub messages: Vec<MessageIo>,
}

/// Which motion-shape carrier the scanner is accumulating inside an open element.
enum Kind {
    /// A service-ish task; `key` is its `zeebe:taskDefinition` type.
    Worker,
    /// A `bpmn:message`; `key` is its `name`.
    Message,
}

struct Accum {
    kind: Kind,
    /// The map key: a worker's `taskDefinition` type (set while open), or a
    /// message's `name` (set at open).
    key: Option<String>,
    input_type: Option<String>,
    output_type: Option<String>,
}

impl Accum {
    fn new(kind: Kind, key: Option<String>) -> Self {
        Self {
            kind,
            key,
            input_type: None,
            output_type: None,
        }
    }
}

/// A literal (non-FEEL, non-empty) job type keys a worker; a FEEL `=expr` type or
/// an empty type does not. Mirrors the modeler's `envelopeContext` guard.
fn literal_task_type(t: &str) -> Option<&str> {
    let t = t.trim();
    if t.is_empty() || t.starts_with('=') {
        None
    } else {
        Some(t)
    }
}

/// Normalise an envelope type ref read from the model: trim surrounding
/// whitespace and treat an empty (or all-whitespace) value as "no envelope", so a
/// hand-edited `.bpmn` with a blank or padded value doesn't yield a ref that
/// silently fails to resolve against any declared type id.
fn envelope_ref(value: Option<&str>) -> Option<String> {
    let v = value?.trim();
    if v.is_empty() {
        None
    } else {
        Some(v.to_string())
    }
}

/// A message name keys a message-IO entry when non-empty. Unlike a worker's job
/// type, a message name is a correlation identity (matched verbatim by the
/// engine), so it is *not* trimmed — only an absent/empty attribute is dropped.
fn message_name(value: Option<&str>) -> Option<String> {
    value.filter(|v| !v.is_empty()).map(str::to_string)
}

/// Scan a single BPMN document into its worker- and message-IO maps. Every
/// service-ish task carrying a literal job type contributes a worker entry (even
/// with no envelope, so a stale manifest projection is *cleared* on rebuild), and
/// every named `bpmn:message` contributes a message entry (envelope optional, so
/// the derived `MessageName` union is complete). A malformed document yields an
/// empty scan.
pub fn scan_bpmn(xml: &str) -> BpmnScan {
    let tokens = match tokenize(xml) {
        Ok(t) => t,
        Err(_) => return BpmnScan::default(),
    };

    // Entries keyed by job type / message name so a rebuild is deterministic and
    // duplicates fold to one entry (the first non-empty envelope on each side
    // wins; the fuse-conflict diagnostic of ADR 0040 §6 is a later slice).
    let mut workers: BTreeMap<String, WorkerIo> = BTreeMap::new();
    let mut messages: BTreeMap<String, MessageIo> = BTreeMap::new();

    // Depth of the currently-open carrier's start tag; `None` when not inside one.
    // Sub-elements (`extensionElements`, `zeebe:taskDefinition`, `zeebe:properties`,
    // `zeebe:property`) are matched only while a carrier is open.
    let mut depth: usize = 0;
    let mut cur: Option<(usize, Accum)> = None;

    for tok in &tokens {
        match tok {
            Token::Start {
                name,
                attrs,
                self_closing,
            } => {
                let ln = local_name(name);
                if cur.is_none() {
                    if !self_closing && SERVICE_TASK_LOCALS.contains(&ln) {
                        cur = Some((depth, Accum::new(Kind::Worker, None)));
                    } else if !self_closing && ln == "message" {
                        // A `bpmn:message` keys by its own `name` (read now).
                        cur = Some((
                            depth,
                            Accum::new(Kind::Message, message_name(attr(attrs, "name"))),
                        ));
                    }
                } else if let Some((_, acc)) = cur.as_mut() {
                    match ln {
                        "taskDefinition" if matches!(acc.kind, Kind::Worker) => {
                            if let Some(t) = attr(attrs, "type") {
                                acc.key = Some(t.to_string());
                            }
                        }
                        "property" => match attr(attrs, "name") {
                            Some(ENVELOPE_IN) => {
                                acc.input_type = envelope_ref(attr(attrs, "value"));
                            }
                            Some(ENVELOPE_OUT) => {
                                acc.output_type = envelope_ref(attr(attrs, "value"));
                            }
                            _ => {}
                        },
                        _ => {}
                    }
                }
                if !self_closing {
                    depth += 1;
                }
            }
            Token::End { .. } => {
                depth = depth.saturating_sub(1);
                if let Some((open_depth, acc)) = cur.take() {
                    if depth == open_depth {
                        flush(&mut workers, &mut messages, acc);
                    } else {
                        cur = Some((open_depth, acc));
                    }
                }
            }
            Token::Text(_) => {}
        }
    }

    BpmnScan {
        workers: workers.into_values().collect(),
        messages: messages.into_values().collect(),
    }
}

/// Fold one closed carrier's accumulator into the right map. Workers are keyed by
/// their literal job type (non-literal or missing types are dropped); messages by
/// their name. On a duplicate key, fill any still-empty envelope side so a binding
/// is not lost.
fn flush(
    workers: &mut BTreeMap<String, WorkerIo>,
    messages: &mut BTreeMap<String, MessageIo>,
    acc: Accum,
) {
    let Some(raw) = acc.key.as_deref() else {
        return;
    };
    match acc.kind {
        Kind::Worker => {
            let Some(task_type) = literal_task_type(raw) else {
                return;
            };
            let entry = workers
                .entry(task_type.to_string())
                .or_insert_with(|| WorkerIo {
                    task_type: task_type.to_string(),
                    input_type: None,
                    output_type: None,
                });
            fill(&mut entry.input_type, acc.input_type);
            fill(&mut entry.output_type, acc.output_type);
        }
        Kind::Message => {
            let entry = messages
                .entry(raw.to_string())
                .or_insert_with(|| MessageIo {
                    message_name: raw.to_string(),
                    input_type: None,
                    output_type: None,
                });
            fill(&mut entry.input_type, acc.input_type);
            fill(&mut entry.output_type, acc.output_type);
        }
    }
}

/// Set `slot` from `incoming` only when `slot` is still empty (first-wins merge).
fn fill(slot: &mut Option<String>, incoming: Option<String>) {
    if slot.is_none() {
        *slot = incoming;
    }
}

/// Scan every `resources/processes/*.bpmn` under `project_dir` into the merged
/// worker- and message-IO maps. Unreadable or malformed files are skipped; a
/// project with no processes yields an empty scan.
pub fn scan_project(project_dir: &Path) -> BpmnScan {
    let processes = project_dir.join("resources").join("processes");
    let entries = match std::fs::read_dir(&processes) {
        Ok(e) => e,
        Err(_) => return BpmnScan::default(),
    };
    let mut workers: BTreeMap<String, WorkerIo> = BTreeMap::new();
    let mut messages: BTreeMap<String, MessageIo> = BTreeMap::new();
    let mut files: Vec<std::path::PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "bpmn"))
        .collect();
    files.sort();
    for path in files {
        let Ok(xml) = std::fs::read_to_string(&path) else {
            continue;
        };
        let scan = scan_bpmn(&xml);
        for io in scan.workers {
            let entry = workers
                .entry(io.task_type.clone())
                .or_insert_with(|| WorkerIo {
                    task_type: io.task_type.clone(),
                    input_type: None,
                    output_type: None,
                });
            fill(&mut entry.input_type, io.input_type);
            fill(&mut entry.output_type, io.output_type);
        }
        for io in scan.messages {
            let entry = messages
                .entry(io.message_name.clone())
                .or_insert_with(|| MessageIo {
                    message_name: io.message_name.clone(),
                    input_type: None,
                    output_type: None,
                });
            fill(&mut entry.input_type, io.input_type);
            fill(&mut entry.output_type, io.output_type);
        }
    }
    BpmnScan {
        workers: workers.into_values().collect(),
        messages: messages.into_values().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper: the worker-IO map derived from a single document.
    fn workers(xml: &str) -> Vec<WorkerIo> {
        scan_bpmn(xml).workers
    }

    /// Test helper: the message-IO map derived from a single document.
    fn messages(xml: &str) -> Vec<MessageIo> {
        scan_bpmn(xml).messages
    }

    fn task(id: &str, kind: &str, job: &str, env: &str) -> String {
        format!(
            r#"<bpmn:{kind} id="{id}" name="{id}">
              <bpmn:extensionElements>
                <zeebe:taskDefinition type="{job}" />
                <zeebe:ioMapping />
                <zeebe:properties>{env}</zeebe:properties>
              </bpmn:extensionElements>
            </bpmn:{kind}>"#
        )
    }

    fn doc(body: &str) -> String {
        format!(
            r#"<?xml version="1.0"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
              <bpmn:process id="p" isExecutable="true">{body}</bpmn:process>
            </bpmn:definitions>"#
        )
    }

    const IN_ORDER: &str = r#"<zeebe:property name="io.nanobpm.dataEnvelope.in" value="Order" />"#;
    const OUT_RECEIPT: &str =
        r#"<zeebe:property name="io.nanobpm.dataEnvelope.out" value="Receipt" />"#;

    #[test]
    fn derives_in_and_out_from_a_service_task() {
        let xml = doc(&task(
            "t1",
            "serviceTask",
            "charge",
            &format!("{IN_ORDER}{OUT_RECEIPT}"),
        ));
        assert_eq!(
            workers(&xml),
            vec![WorkerIo {
                task_type: "charge".into(),
                input_type: Some("Order".into()),
                output_type: Some("Receipt".into()),
            }]
        );
    }

    #[test]
    fn emits_a_typed_task_with_no_envelope_so_stale_io_is_cleared() {
        let xml = doc(&task("t1", "serviceTask", "noop", ""));
        assert_eq!(
            workers(&xml),
            vec![WorkerIo {
                task_type: "noop".into(),
                input_type: None,
                output_type: None
            }]
        );
    }

    #[test]
    fn covers_all_service_ish_task_kinds() {
        let body = format!(
            "{}{}{}{}",
            task("a", "serviceTask", "svc", IN_ORDER),
            task("b", "businessRuleTask", "rule", IN_ORDER),
            task("c", "scriptTask", "script", IN_ORDER),
            task("d", "sendTask", "send", IN_ORDER),
        );
        let got: Vec<String> = workers(&doc(&body))
            .into_iter()
            .map(|w| w.task_type)
            .collect();
        assert_eq!(got, vec!["rule", "script", "send", "svc"]); // BTree-sorted
    }

    #[test]
    fn skips_feel_and_empty_job_types() {
        let body = format!(
            "{}{}",
            task("a", "serviceTask", "=worker + 1", IN_ORDER),
            task("b", "serviceTask", "", IN_ORDER),
        );
        assert_eq!(workers(&doc(&body)), vec![]);
    }

    #[test]
    fn ignores_user_tasks_and_non_envelope_properties() {
        let body = format!(
            r#"<bpmn:userTask id="u" name="u">
                 <bpmn:extensionElements>
                   <zeebe:properties>
                     <zeebe:property name="something.else" value="X" />
                   </zeebe:properties>
                 </bpmn:extensionElements>
               </bpmn:userTask>
               {}"#,
            task(
                "s",
                "serviceTask",
                "svc",
                &format!(r#"<zeebe:property name="unrelated" value="Y" />{IN_ORDER}"#)
            ),
        );
        assert_eq!(
            workers(&doc(&body)),
            vec![WorkerIo {
                task_type: "svc".into(),
                input_type: Some("Order".into()),
                output_type: None,
            }]
        );
    }

    #[test]
    fn malformed_xml_yields_no_bindings() {
        assert_eq!(workers("<bpmn:definitions <<>"), vec![]);
    }

    #[test]
    fn trims_whitespace_and_drops_blank_envelope_refs() {
        let env = concat!(
            r#"<zeebe:property name="io.nanobpm.dataEnvelope.in" value="  Order  " />"#,
            r#"<zeebe:property name="io.nanobpm.dataEnvelope.out" value="   " />"#,
        );
        let xml = doc(&task("t", "serviceTask", "charge", env));
        assert_eq!(
            workers(&xml),
            vec![WorkerIo {
                task_type: "charge".into(),
                input_type: Some("Order".into()),
                output_type: None,
            }]
        );
    }

    #[test]
    fn serializes_to_worker_decl_shape() {
        let io = WorkerIo {
            task_type: "charge".into(),
            input_type: Some("Order".into()),
            output_type: None,
        };
        let v = serde_json::to_value(&io).unwrap();
        assert_eq!(v["taskType"], "charge");
        assert_eq!(v["inputType"], "Order");
        assert!(v.get("outputType").is_none());
    }

    // --- message-carried envelopes (ADR 0040 slice 2) ----------------------

    fn message(name: &str, env: &str) -> String {
        format!(
            r#"<bpmn:message id="Msg_{name}" name="{name}">
              <bpmn:extensionElements>
                <zeebe:properties>{env}</zeebe:properties>
              </bpmn:extensionElements>
            </bpmn:message>"#
        )
    }

    /// A `bpmn:message` is a top-level definitions child, so wrap without a process.
    fn defs(body: &str) -> String {
        format!(
            r#"<?xml version="1.0"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                              xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">{body}</bpmn:definitions>"#
        )
    }

    #[test]
    fn derives_message_payload_from_the_envelope() {
        let xml = defs(&message("orderPlaced", IN_ORDER));
        assert_eq!(
            messages(&xml),
            vec![MessageIo {
                message_name: "orderPlaced".into(),
                input_type: Some("Order".into()),
                output_type: None,
            }]
        );
    }

    #[test]
    fn emits_named_message_with_no_envelope_so_the_name_union_is_complete() {
        let xml = defs(&message("ping", ""));
        assert_eq!(
            messages(&xml),
            vec![MessageIo {
                message_name: "ping".into(),
                input_type: None,
                output_type: None,
            }]
        );
    }

    #[test]
    fn skips_unnamed_messages_but_keeps_message_names_verbatim() {
        // An anonymous message (no `name`) is dropped; a named one is kept exactly
        // (message names are correlation identities, not trimmed).
        let body = format!(
            "{}{}",
            r#"<bpmn:message id="anon"><bpmn:extensionElements><zeebe:properties>"#.to_string()
                + IN_ORDER
                + r#"</zeebe:properties></bpmn:extensionElements></bpmn:message>"#,
            message(" spaced ", IN_ORDER),
        );
        let got: Vec<String> = messages(&defs(&body))
            .into_iter()
            .map(|m| m.message_name)
            .collect();
        assert_eq!(got, vec![" spaced "]);
    }

    #[test]
    fn scans_workers_and_messages_from_one_document() {
        let body = format!(
            "{}{}",
            message("orderPlaced", IN_ORDER),
            "<bpmn:process id=\"p\">".to_string()
                + &task("t", "serviceTask", "charge", OUT_RECEIPT)
                + "</bpmn:process>",
        );
        let scan = scan_bpmn(&defs(&body));
        assert_eq!(
            scan.workers,
            vec![WorkerIo {
                task_type: "charge".into(),
                input_type: None,
                output_type: Some("Receipt".into()),
            }]
        );
        assert_eq!(
            scan.messages,
            vec![MessageIo {
                message_name: "orderPlaced".into(),
                input_type: Some("Order".into()),
                output_type: None,
            }]
        );
    }

    #[test]
    fn serializes_to_message_decl_shape() {
        let io = MessageIo {
            message_name: "orderPlaced".into(),
            input_type: Some("Order".into()),
            output_type: None,
        };
        let v = serde_json::to_value(&io).unwrap();
        assert_eq!(v["messageName"], "orderPlaced");
        assert_eq!(v["inputType"], "Order");
        assert!(v.get("outputType").is_none());
    }
}
