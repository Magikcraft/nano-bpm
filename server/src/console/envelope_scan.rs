//! Model scan for data-envelope worker I/O (ADR 0040 slice 1 / ADR 0033 §6
//! increment 12).
//!
//! The BPMN model is the authoritative source of the service-task data envelope
//! (`io.nanobpm.dataEnvelope.in`/`.out`, carried as `zeebe:property` on the task,
//! written by the modeler's *Data envelope* panel). The typed-worker codegen,
//! however, historically read the I/O map from the manifest's hand-maintained
//! `workers[]` projection, so the two could drift.
//!
//! This scan closes that gap: it parses the project's process models and derives
//! `taskType -> {in,out}` **from the model**, so the reifier can rebuild the
//! worker-IO map from the source of truth rather than the projection. It is the
//! first slice of the Fused Domain Model's model-scan pipeline (ADR 0040 §7/§8):
//! the models contribute their motion-shape bindings, and the derived registry
//! (here, the worker-IO cache) is recomputed from them.
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

#[derive(Default)]
struct Accum {
    task_type: Option<String>,
    input_type: Option<String>,
    output_type: Option<String>,
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

/// Derive the `taskType -> {in,out}` worker-IO map from a single BPMN document.
/// Every service-ish task carrying a literal job type contributes an entry (even
/// with no envelope, so a stale manifest projection is *cleared* on rebuild, not
/// merely left untouched). A malformed document yields an empty map.
pub fn scan_bpmn_worker_io(xml: &str) -> Vec<WorkerIo> {
    let tokens = match tokenize(xml) {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };

    // Entries keyed by job type so a rebuild is deterministic and duplicate job
    // types across tasks fold to one entry (last non-empty envelope wins — the
    // fuse-conflict diagnostic of ADR 0040 §6 is a later slice).
    let mut out: BTreeMap<String, WorkerIo> = BTreeMap::new();

    // Depth of the currently-open service task's start tag; `None` when not
    // inside one. Sub-elements (`extensionElements`, `zeebe:taskDefinition`,
    // `zeebe:properties`, `zeebe:property`) are matched only while open.
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
                        cur = Some((depth, Accum::default()));
                    }
                } else if let Some((_, acc)) = cur.as_mut() {
                    match ln {
                        "taskDefinition" => {
                            if let Some(t) = attr(attrs, "type") {
                                acc.task_type = Some(t.to_string());
                            }
                        }
                        "property" => match attr(attrs, "name") {
                            Some(ENVELOPE_IN) => {
                                acc.input_type = attr(attrs, "value").map(str::to_string);
                            }
                            Some(ENVELOPE_OUT) => {
                                acc.output_type = attr(attrs, "value").map(str::to_string);
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
                        flush(&mut out, acc);
                    } else {
                        cur = Some((open_depth, acc));
                    }
                }
            }
            Token::Text(_) => {}
        }
    }

    out.into_values().collect()
}

/// Fold one closed task's accumulator into the output map (keyed by literal job
/// type; non-literal or missing types are dropped). On a duplicate job type,
/// fill any still-empty envelope side so a binding is not lost.
fn flush(out: &mut BTreeMap<String, WorkerIo>, acc: Accum) {
    let Some(raw) = acc.task_type.as_deref() else {
        return;
    };
    let Some(task_type) = literal_task_type(raw) else {
        return;
    };
    let entry = out
        .entry(task_type.to_string())
        .or_insert_with(|| WorkerIo {
            task_type: task_type.to_string(),
            input_type: None,
            output_type: None,
        });
    if entry.input_type.is_none() {
        entry.input_type = acc.input_type;
    }
    if entry.output_type.is_none() {
        entry.output_type = acc.output_type;
    }
}

/// Scan every `resources/processes/*.bpmn` under `project_dir` and derive the
/// merged `taskType -> {in,out}` worker-IO map from the models. Unreadable or
/// malformed files are skipped. Returns `[]` when the project declares no
/// processes.
pub fn scan_project_worker_io(project_dir: &Path) -> Vec<WorkerIo> {
    let processes = project_dir.join("resources").join("processes");
    let entries = match std::fs::read_dir(&processes) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    let mut merged: BTreeMap<String, WorkerIo> = BTreeMap::new();
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
        for io in scan_bpmn_worker_io(&xml) {
            let entry = merged
                .entry(io.task_type.clone())
                .or_insert_with(|| WorkerIo {
                    task_type: io.task_type.clone(),
                    input_type: None,
                    output_type: None,
                });
            if entry.input_type.is_none() {
                entry.input_type = io.input_type;
            }
            if entry.output_type.is_none() {
                entry.output_type = io.output_type;
            }
        }
    }
    merged.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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
            scan_bpmn_worker_io(&xml),
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
            scan_bpmn_worker_io(&xml),
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
        let got: Vec<String> = scan_bpmn_worker_io(&doc(&body))
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
        assert_eq!(scan_bpmn_worker_io(&doc(&body)), vec![]);
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
            scan_bpmn_worker_io(&doc(&body)),
            vec![WorkerIo {
                task_type: "svc".into(),
                input_type: Some("Order".into()),
                output_type: None,
            }]
        );
    }

    #[test]
    fn malformed_xml_yields_no_bindings() {
        assert_eq!(scan_bpmn_worker_io("<bpmn:definitions <<>"), vec![]);
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
}
