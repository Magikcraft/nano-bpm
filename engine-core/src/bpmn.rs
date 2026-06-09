//! A minimal BPMN 2.0 XML parser.
//!
//! This turns the slice of BPMN that the engine understands into
//! [`ProcessDefinition`]s, so processes can be deployed from `.bpmn` files
//! (e.g. exported by the Camunda Modeler) instead of only being built
//! programmatically with [`crate::ProcessBuilder`].
//!
//! It is deliberately tiny and dependency-free: a hand-rolled, namespace-prefix
//! agnostic XML scanner plus a single pass that recognises the supported flow
//! nodes. It is **not** a conformant BPMN/XML implementation — it understands
//! exactly the constructs the engine can execute and ignores the rest
//! (diagram interchange, documentation, lanes, …).
//!
//! ## Supported subset
//!
//! * `process` (one or more per file) with its `id`.
//! * Flow nodes: `startEvent`, `endEvent`, `serviceTask`, `exclusiveGateway`,
//!   `parallelGateway`.
//! * `boundaryEvent` with `attachedToRef` and a nested `errorEventDefinition`
//!   `errorRef`, resolved against definitions-level `error` elements
//!   (`<error id="…" errorCode="…">`) into an error boundary event.
//! * A service task's job type is taken from a nested
//!   `zeebe:taskDefinition type="…"`; if absent it defaults to the task id.
//! * `sequenceFlow` with `sourceRef`/`targetRef`, and an optional
//!   `conditionExpression` whose FEEL body is parsed as a simple equality
//!   (`= var = "literal"`); anything else becomes an unconditional flow.

use std::collections::HashMap;

use crate::model::{Condition, ProcessBuilder, ProcessDefinition, Value};

/// An error encountered while parsing BPMN XML.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// The XML was malformed (unterminated tag, bad quoting, …).
    MalformedXml(String),
    /// A `process` element had no `id` attribute.
    ProcessWithoutId,
    /// A `sequenceFlow` was missing `sourceRef`/`targetRef`.
    IncompleteSequenceFlow { process_id: String },
    /// The XML contained no `process` elements.
    NoProcess,
    /// A parsed process failed validation (e.g. no/many start events, dangling
    /// flow). Carries the underlying [`crate::BuildError`] message.
    InvalidProcess { process_id: String, reason: String },
    /// A `boundaryEvent` was missing `attachedToRef`, or its
    /// `errorEventDefinition` referenced an `error` that was not declared.
    InvalidBoundaryEvent { process_id: String, reason: String },
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::MalformedXml(detail) => write!(f, "malformed BPMN XML: {detail}"),
            ParseError::ProcessWithoutId => write!(f, "<process> element has no id"),
            ParseError::IncompleteSequenceFlow { process_id } => write!(
                f,
                "process {process_id} has a sequenceFlow without sourceRef/targetRef"
            ),
            ParseError::NoProcess => write!(f, "no <process> element found"),
            ParseError::InvalidProcess { process_id, reason } => {
                write!(f, "invalid process {process_id}: {reason}")
            }
            ParseError::InvalidBoundaryEvent { process_id, reason } => {
                write!(
                    f,
                    "invalid boundary event in process {process_id}: {reason}"
                )
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// Parses BPMN 2.0 XML into the executable [`ProcessDefinition`]s it contains.
///
/// Returns one definition per `<process>` element, in document order.
///
/// ```
/// use nanobpmn_engine_core::bpmn::parse_bpmn;
/// let xml = r#"
///   <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
///     <bpmn:process id="p">
///       <bpmn:startEvent id="s" />
///       <bpmn:endEvent id="e" />
///       <bpmn:sequenceFlow id="f" sourceRef="s" targetRef="e" />
///     </bpmn:process>
///   </bpmn:definitions>"#;
/// let defs = parse_bpmn(xml).unwrap();
/// assert_eq!(defs.len(), 1);
/// assert_eq!(defs[0].id, "p");
/// assert_eq!(defs[0].start_event, "s");
/// ```
pub fn parse_bpmn(xml: &str) -> Result<Vec<ProcessDefinition>, ParseError> {
    let tokens = tokenize(xml)?;

    let mut processes: Vec<ProcessAcc> = Vec::new();
    let mut current: Option<ProcessAcc> = None;
    // Index of the service task currently being read (to attach its job type).
    let mut cur_service_task: Option<usize> = None;
    // Index of the sequence flow currently being read (to attach a condition).
    let mut cur_flow: Option<usize> = None;
    let mut condition_text: Option<String> = None;
    // The boundary event currently being read (to attach its errorEventDefinition).
    let mut cur_boundary: Option<PendingBoundary> = None;
    // Index of the timer intermediate catch event currently being read, and a
    // buffer for its nested `timeDuration` text while inside that element.
    let mut cur_timer_catch: Option<usize> = None;
    let mut duration_text: Option<String> = None;
    // Definitions-level `<error id=… errorCode=…>` declarations: id -> code.
    let mut errors: HashMap<String, String> = HashMap::new();

    for token in &tokens {
        match token {
            Token::Start {
                name,
                attrs,
                self_closing,
            } => {
                match local_name(name) {
                    "process" => {
                        let id = attr(attrs, "id").ok_or(ParseError::ProcessWithoutId)?;
                        current = Some(ProcessAcc::new(id.to_string()));
                    }
                    // Definitions-level error declarations live outside <process>.
                    "error" => {
                        if let (Some(id), Some(code)) =
                            (attr(attrs, "id"), attr(attrs, "errorCode"))
                        {
                            errors.insert(id.to_string(), code.to_string());
                        }
                    }
                    tag if current.is_some() => {
                        let acc = current.as_mut().expect("current process set");
                        match tag {
                            "startEvent" => {
                                acc.add_node(attrs, NodeKind::Start);
                            }
                            "endEvent" => {
                                acc.add_node(attrs, NodeKind::End);
                            }
                            "exclusiveGateway" => {
                                acc.add_node(attrs, NodeKind::Exclusive);
                            }
                            "parallelGateway" => {
                                acc.add_node(attrs, NodeKind::Parallel);
                            }
                            "serviceTask" => {
                                let idx = acc.add_node(attrs, NodeKind::Service);
                                if !self_closing {
                                    cur_service_task = idx;
                                }
                            }
                            "boundaryEvent" => {
                                // Buffered until end: kept only if it carries an
                                // errorEventDefinition (error boundary) or a
                                // timerEventDefinition (interrupting timer
                                // boundary); other boundaries are ignored.
                                if let Some(id) = attr(attrs, "id") {
                                    cur_boundary = Some(PendingBoundary {
                                        id: id.to_string(),
                                        attached_to: attr(attrs, "attachedToRef")
                                            .map(str::to_string),
                                        error_ref: None,
                                        timer_duration_millis: None,
                                    });
                                }
                            }
                            "errorEventDefinition" => {
                                if let Some(boundary) = cur_boundary.as_mut() {
                                    boundary.error_ref =
                                        Some(attr(attrs, "errorRef").unwrap_or("").to_string());
                                }
                            }
                            "taskDefinition" => {
                                // zeebe:taskDefinition type="…" inside a service task.
                                if let (Some(idx), Some(t)) =
                                    (cur_service_task, attr(attrs, "type"))
                                {
                                    acc.nodes[idx].job_type = Some(t.to_string());
                                }
                            }
                            "sequenceFlow" => {
                                let idx = acc.add_flow(attrs);
                                if !self_closing {
                                    cur_flow = idx;
                                }
                            }
                            "intermediateCatchEvent" => {
                                let idx = acc.add_node(attrs, NodeKind::TimerCatch);
                                if !self_closing {
                                    cur_timer_catch = idx;
                                }
                            }
                            "timeDuration"
                                if cur_timer_catch.is_some() || cur_boundary.is_some() =>
                            {
                                duration_text = Some(String::new());
                            }
                            "conditionExpression" if cur_flow.is_some() => {
                                condition_text = Some(String::new());
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            Token::Text(text) => {
                if let Some(buf) = condition_text.as_mut() {
                    buf.push_str(text);
                }
                if let Some(buf) = duration_text.as_mut() {
                    buf.push_str(text);
                }
            }
            Token::End { name } => match local_name(name) {
                "process" => {
                    if let Some(acc) = current.take() {
                        processes.push(acc);
                    }
                    cur_service_task = None;
                    cur_flow = None;
                    cur_boundary = None;
                    cur_timer_catch = None;
                    duration_text = None;
                }
                "serviceTask" => cur_service_task = None,
                "boundaryEvent" => {
                    // Keep error boundaries (errorEventDefinition) and timer
                    // boundaries (timerEventDefinition); ignore the rest.
                    if let (Some(acc), Some(boundary)) = (current.as_mut(), cur_boundary.take()) {
                        if boundary.error_ref.is_some() || boundary.timer_duration_millis.is_some()
                        {
                            acc.boundaries.push(boundary);
                        }
                    }
                }
                "conditionExpression" => {
                    if let (Some(acc), Some(idx), Some(text)) =
                        (current.as_mut(), cur_flow, condition_text.take())
                    {
                        acc.flows[idx].condition = parse_condition(&text);
                    }
                }
                "intermediateCatchEvent" => cur_timer_catch = None,
                "timeDuration" => {
                    if let Some(text) = duration_text.take() {
                        let millis = parse_iso8601_duration(&text);
                        if let (Some(acc), Some(idx)) = (current.as_mut(), cur_timer_catch) {
                            // An intermediate catch event's duration.
                            acc.nodes[idx].duration_millis = millis;
                        } else if let Some(boundary) = cur_boundary.as_mut() {
                            // A timer boundary event's duration.
                            boundary.timer_duration_millis = millis;
                        }
                    }
                }
                "sequenceFlow" => cur_flow = None,
                _ => {}
            },
        }
    }

    if processes.is_empty() {
        return Err(ParseError::NoProcess);
    }

    processes
        .into_iter()
        .map(|acc| acc.build(&errors))
        .collect()
}

/// A flow node collected while scanning, before it becomes an [`crate::Element`].
struct NodeAcc {
    id: String,
    kind: NodeKind,
    /// For service tasks: the resolved job type (defaults to the id at build).
    job_type: Option<String>,
    /// For timer intermediate catch events: the parsed timer duration in
    /// milliseconds (from a nested `timerEventDefinition`/`timeDuration`).
    duration_millis: Option<u64>,
}

#[derive(Clone, Copy)]
enum NodeKind {
    Start,
    End,
    Service,
    Exclusive,
    Parallel,
    TimerCatch,
}

/// A sequence flow collected while scanning.
struct FlowAcc {
    source: Option<String>,
    target: Option<String>,
    condition: Option<Condition>,
}

/// A boundary event collected while scanning. An `error_ref` (resolved to an
/// error code at build time) makes it an error boundary; a `timer_duration_millis`
/// makes it an interrupting timer boundary. A boundary with neither is ignored.
#[derive(Clone)]
struct PendingBoundary {
    id: String,
    attached_to: Option<String>,
    error_ref: Option<String>,
    timer_duration_millis: Option<u64>,
}

/// Accumulates the nodes and flows of one `<process>` as it is scanned.
struct ProcessAcc {
    id: String,
    nodes: Vec<NodeAcc>,
    flows: Vec<FlowAcc>,
    boundaries: Vec<PendingBoundary>,
}

impl ProcessAcc {
    fn new(id: String) -> Self {
        Self {
            id,
            nodes: Vec::new(),
            flows: Vec::new(),
            boundaries: Vec::new(),
        }
    }

    /// Adds a flow node; returns its index, or `None` if it had no `id`.
    fn add_node(&mut self, attrs: &[(String, String)], kind: NodeKind) -> Option<usize> {
        let id = attr(attrs, "id")?;
        self.nodes.push(NodeAcc {
            id: id.to_string(),
            kind,
            job_type: None,
            duration_millis: None,
        });
        Some(self.nodes.len() - 1)
    }

    /// Adds a sequence flow; returns its index.
    fn add_flow(&mut self, attrs: &[(String, String)]) -> Option<usize> {
        self.flows.push(FlowAcc {
            source: attr(attrs, "sourceRef").map(str::to_string),
            target: attr(attrs, "targetRef").map(str::to_string),
            condition: None,
        });
        Some(self.flows.len() - 1)
    }

    /// Assembles the [`ProcessDefinition`] via [`ProcessBuilder`].
    ///
    /// `errors` maps definitions-level `<error>` ids to their codes, used to
    /// resolve each boundary event's `errorRef`.
    fn build(self, errors: &HashMap<String, String>) -> Result<ProcessDefinition, ParseError> {
        let mut builder = ProcessBuilder::new(self.id.clone());
        for node in self.nodes {
            builder = match node.kind {
                NodeKind::Start => builder.start_event(node.id),
                NodeKind::End => builder.end_event(node.id),
                NodeKind::Exclusive => builder.exclusive_gateway(node.id),
                NodeKind::Parallel => builder.parallel_gateway(node.id),
                NodeKind::Service => {
                    let job_type = node.job_type.unwrap_or_else(|| node.id.clone());
                    builder.service_task(node.id, job_type)
                }
                NodeKind::TimerCatch => {
                    let duration_millis = node.duration_millis.unwrap_or(0);
                    builder.timer_intermediate_catch_event(node.id, duration_millis)
                }
            };
        }
        for boundary in self.boundaries {
            let attached_to =
                boundary
                    .attached_to
                    .ok_or_else(|| ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!("boundary event {} has no attachedToRef", boundary.id),
                    })?;
            // A timer boundary carries a duration; otherwise it is an error
            // boundary whose errorRef must resolve to a declared error.
            if let Some(duration_millis) = boundary.timer_duration_millis {
                builder = builder.timer_boundary_event(boundary.id, attached_to, duration_millis);
            } else {
                let error_ref = boundary.error_ref.unwrap_or_default();
                let error_code = errors.get(&error_ref).cloned().ok_or_else(|| {
                    ParseError::InvalidBoundaryEvent {
                        process_id: self.id.clone(),
                        reason: format!(
                            "boundary event {} references unknown error '{error_ref}'",
                            boundary.id
                        ),
                    }
                })?;
                builder = builder.error_boundary_event(boundary.id, attached_to, error_code);
            }
        }
        for flow in self.flows {
            let (source, target) = match (flow.source, flow.target) {
                (Some(s), Some(t)) => (s, t),
                _ => {
                    return Err(ParseError::IncompleteSequenceFlow {
                        process_id: self.id,
                    })
                }
            };
            builder = match flow.condition {
                Some(condition) => builder.connect_when(source, target, condition),
                None => builder.connect(source, target),
            };
        }
        builder.build().map_err(|e| ParseError::InvalidProcess {
            process_id: self.id,
            reason: e.to_string(),
        })
    }
}

/// Parses an ISO-8601 duration (e.g. `PT5S`, `PT1M30S`, `PT2H`, `P1DT6H`,
/// `P1W`) into milliseconds. Supports weeks, days, hours, minutes and seconds
/// (the date-portion years/months are ambiguous in length and not supported).
/// Returns `None` if the string is not a recognisable duration.
fn parse_iso8601_duration(raw: &str) -> Option<u64> {
    let s = raw.trim();
    let s = s.strip_prefix('P')?;
    if s.is_empty() {
        return None;
    }

    let mut total_millis: u64 = 0;
    let mut in_time = false;
    let mut num = String::new();
    let mut saw_unit = false;

    for c in s.chars() {
        match c {
            'T' => in_time = true,
            '0'..='9' => num.push(c),
            _ => {
                if num.is_empty() {
                    return None;
                }
                let value: u64 = num.parse().ok()?;
                num.clear();
                let millis = match (in_time, c) {
                    (false, 'W') => value.checked_mul(7 * 24 * 60 * 60 * 1000),
                    (false, 'D') => value.checked_mul(24 * 60 * 60 * 1000),
                    (true, 'H') => value.checked_mul(60 * 60 * 1000),
                    (true, 'M') => value.checked_mul(60 * 1000),
                    (true, 'S') => value.checked_mul(1000),
                    // 'M' before 'T' is months (unsupported) and 'Y' is years.
                    _ => return None,
                }?;
                total_millis = total_millis.checked_add(millis)?;
                saw_unit = true;
            }
        }
    }

    // Trailing digits without a unit, or no units at all, are invalid.
    if !num.is_empty() || !saw_unit {
        return None;
    }
    Some(total_millis)
}

/// Parses a FEEL-ish `conditionExpression` body into a [`Condition`].
///
/// Recognises a single equality such as `= shipping = "express"`,
/// `=amount == 10` or `=approved = true`. The leading `=` FEEL marker is
/// optional and `=`/`==` are both accepted. Anything it cannot parse yields
/// `None`, i.e. an unconditional flow.
fn parse_condition(raw: &str) -> Option<Condition> {
    let expr = raw.trim();
    // Strip the leading FEEL `=` marker, taking care not to eat a `==` operator.
    let expr = if expr.starts_with("==") {
        expr
    } else {
        expr.strip_prefix('=').unwrap_or(expr)
    }
    .trim();

    // Split on the first `==` or `=` operator.
    let (lhs, rhs) = if let Some(pos) = expr.find("==") {
        (&expr[..pos], &expr[pos + 2..])
    } else {
        let pos = expr.find('=')?;
        (&expr[..pos], &expr[pos + 1..])
    };

    let variable = lhs.trim();
    let literal = rhs.trim();
    if variable.is_empty() || literal.is_empty() {
        return None;
    }

    Some(Condition::Equals {
        variable: variable.to_string(),
        value: parse_literal(literal),
    })
}

/// Parses a FEEL literal into a [`Value`]: quoted → string, `true`/`false` →
/// bool, integer → int, otherwise an unquoted string.
fn parse_literal(literal: &str) -> Value {
    if (literal.starts_with('"') && literal.ends_with('"') && literal.len() >= 2)
        || (literal.starts_with('\'') && literal.ends_with('\'') && literal.len() >= 2)
    {
        return Value::Str(literal[1..literal.len() - 1].to_string());
    }
    match literal {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }
    if let Ok(n) = literal.parse::<i64>() {
        return Value::Int(n);
    }
    Value::Str(literal.to_string())
}

/// The local part of a possibly-namespaced XML name (`bpmn:process` -> `process`).
fn local_name(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}

/// Looks up an attribute by local name.
fn attr<'a>(attrs: &'a [(String, String)], local: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(k, _)| local_name(k) == local)
        .map(|(_, v)| v.as_str())
}

/// A scanned XML token.
enum Token {
    Start {
        name: String,
        attrs: Vec<(String, String)>,
        self_closing: bool,
    },
    End {
        name: String,
    },
    Text(String),
}

/// A tiny, allocation-light XML scanner. Handles elements, attributes (single or
/// double quoted), self-closing tags, comments, processing instructions,
/// `DOCTYPE`, and `CDATA`. It does not validate the document.
fn tokenize(xml: &str) -> Result<Vec<Token>, ParseError> {
    let bytes = xml.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] != b'<' {
            // Text run up to the next '<'.
            let start = i;
            while i < bytes.len() && bytes[i] != b'<' {
                i += 1;
            }
            let text = &xml[start..i];
            if !text.trim().is_empty() {
                tokens.push(Token::Text(unescape(text)));
            }
            continue;
        }

        // We are at '<'.
        if xml[i..].starts_with("<!--") {
            let end = find(xml, i + 4, "-->")?;
            i = end + 3;
        } else if xml[i..].starts_with("<![CDATA[") {
            let end = find(xml, i + 9, "]]>")?;
            tokens.push(Token::Text(xml[i + 9..end].to_string()));
            i = end + 3;
        } else if xml[i..].starts_with("<!") || xml[i..].starts_with("<?") {
            // DOCTYPE / processing instruction / XML declaration: skip to '>'.
            let end = find(xml, i + 2, ">")?;
            i = end + 1;
        } else if xml[i..].starts_with("</") {
            let end = find(xml, i + 2, ">")?;
            let name = xml[i + 2..end].trim().to_string();
            tokens.push(Token::End { name });
            i = end + 1;
        } else {
            // Start (or self-closing) tag. Find the closing '>' that is not
            // inside a quoted attribute value.
            let end = find_tag_end(xml, i + 1)?;
            let inner = xml[i + 1..end].trim();
            let (inner, self_closing) = match inner.strip_suffix('/') {
                Some(stripped) => (stripped.trim_end(), true),
                None => (inner, false),
            };
            let (name, attrs) = parse_tag(inner)?;
            tokens.push(Token::Start {
                name,
                attrs,
                self_closing,
            });
            i = end + 1;
        }
    }

    Ok(tokens)
}

/// Finds the byte index of the next `needle` at or after `from`.
fn find(haystack: &str, from: usize, needle: &str) -> Result<usize, ParseError> {
    haystack[from..]
        .find(needle)
        .map(|p| from + p)
        .ok_or_else(|| ParseError::MalformedXml(format!("expected `{needle}`")))
}

/// Finds the `>` ending a start tag, skipping any inside quoted attribute values.
fn find_tag_end(xml: &str, from: usize) -> Result<usize, ParseError> {
    let bytes = xml.as_bytes();
    let mut i = from;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if c == b'"' || c == b'\'' => quote = Some(c),
            None if c == b'>' => return Ok(i),
            None => {}
        }
        i += 1;
    }
    Err(ParseError::MalformedXml("unterminated tag".to_string()))
}

/// Splits a tag's inner text into its name and attributes.
fn parse_tag(inner: &str) -> Result<(String, Vec<(String, String)>), ParseError> {
    let inner = inner.trim();
    let name_end = inner
        .find(|c: char| c.is_whitespace())
        .unwrap_or(inner.len());
    let name = inner[..name_end].to_string();
    if name.is_empty() {
        return Err(ParseError::MalformedXml("empty tag name".to_string()));
    }

    let mut attrs = Vec::new();
    let rest = inner[name_end..].trim_start();
    let bytes = rest.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Attribute name up to '='.
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let key = rest[key_start..i].trim();
        // Skip whitespace and the '='.
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            // Valueless attribute; ignore.
            if key.is_empty() {
                i += 1;
            }
            continue;
        }
        i += 1; // consume '='
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || (bytes[i] != b'"' && bytes[i] != b'\'') {
            return Err(ParseError::MalformedXml(format!(
                "attribute {key} has no quoted value"
            )));
        }
        let q = bytes[i];
        i += 1;
        let val_start = i;
        while i < bytes.len() && bytes[i] != q {
            i += 1;
        }
        if i >= bytes.len() {
            return Err(ParseError::MalformedXml(
                "unterminated attribute".to_string(),
            ));
        }
        let value = unescape(&rest[val_start..i]);
        i += 1; // consume closing quote
        if !key.is_empty() {
            attrs.push((key.to_string(), value));
        }
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
    }

    Ok((name, attrs))
}

/// Expands the five predefined XML entities.
fn unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ElementKind;

    const ORDER_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="order" isExecutable="true">
    <bpmn:startEvent id="start" />
    <bpmn:serviceTask id="charge">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="payment" />
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="done" />
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="charge" />
    <bpmn:sequenceFlow id="f2" sourceRef="charge" targetRef="done" />
  </bpmn:process>
</bpmn:definitions>"#;

    #[test]
    fn should_parse_a_timer_intermediate_catch_event() {
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="delayed">
              <bpmn:startEvent id="s" />
              <bpmn:intermediateCatchEvent id="wait">
                <bpmn:timerEventDefinition>
                  <bpmn:timeDuration>PT1M30S</bpmn:timeDuration>
                </bpmn:timerEventDefinition>
              </bpmn:intermediateCatchEvent>
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="wait" />
              <bpmn:sequenceFlow id="b" sourceRef="wait" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        let def = &parse_bpmn(xml).unwrap()[0];

        assert_eq!(
            def.element("wait").unwrap().kind,
            ElementKind::TimerIntermediateCatchEvent {
                duration_millis: 90_000
            }
        );
        assert_eq!(def.element("wait").unwrap().outgoing[0].to, "e");
    }

    #[test]
    fn should_parse_iso8601_durations() {
        assert_eq!(parse_iso8601_duration("PT5S"), Some(5_000));
        assert_eq!(parse_iso8601_duration("PT1M"), Some(60_000));
        assert_eq!(parse_iso8601_duration("PT2H"), Some(7_200_000));
        assert_eq!(parse_iso8601_duration("P1D"), Some(86_400_000));
        assert_eq!(parse_iso8601_duration("P1W"), Some(604_800_000));
        assert_eq!(parse_iso8601_duration("P1DT6H30M"), Some(109_800_000));
        assert_eq!(parse_iso8601_duration(" PT10S "), Some(10_000));
        // invalid / unsupported
        assert_eq!(parse_iso8601_duration("5S"), None);
        assert_eq!(parse_iso8601_duration("P"), None);
        assert_eq!(parse_iso8601_duration("PT"), None);
        assert_eq!(parse_iso8601_duration("P1Y"), None);
        assert_eq!(parse_iso8601_duration("PT5"), None);
    }

    #[test]
    fn should_parse_a_linear_process_with_a_service_task() {
        // given / when
        let defs = parse_bpmn(ORDER_BPMN).unwrap();

        // then
        assert_eq!(defs.len(), 1);
        let def = &defs[0];
        assert_eq!(def.id, "order");
        assert_eq!(def.start_event, "start");
        assert_eq!(
            def.element("charge").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "payment".to_string()
            }
        );
        assert_eq!(def.element("start").unwrap().outgoing[0].to, "charge");
    }

    #[test]
    fn should_default_service_task_job_type_to_its_id() {
        // given
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="work" />
              <bpmn:endEvent id="e" />
              <bpmn:sequenceFlow id="a" sourceRef="s" targetRef="work" />
              <bpmn:sequenceFlow id="b" sourceRef="work" targetRef="e" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("work").unwrap().kind,
            ElementKind::ServiceTask {
                job_type: "work".to_string()
            }
        );
    }

    #[test]
    fn should_parse_exclusive_gateway_with_conditions() {
        // given
        let xml = r#"
          <definitions>
            <process id="route">
              <startEvent id="s" />
              <exclusiveGateway id="gw" />
              <endEvent id="yes" />
              <endEvent id="no" />
              <sequenceFlow id="f0" sourceRef="s" targetRef="gw" />
              <sequenceFlow id="f1" sourceRef="gw" targetRef="yes">
                <conditionExpression xsi:type="tFormalExpression">= decision = "yes"</conditionExpression>
              </sequenceFlow>
              <sequenceFlow id="f2" sourceRef="gw" targetRef="no" />
            </process>
          </definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        let gw = def.element("gw").unwrap();
        let to_yes = gw.outgoing.iter().find(|f| f.to == "yes").unwrap();
        assert_eq!(
            to_yes.condition,
            Some(Condition::Equals {
                variable: "decision".to_string(),
                value: Value::Str("yes".to_string()),
            })
        );
        let to_no = gw.outgoing.iter().find(|f| f.to == "no").unwrap();
        assert_eq!(to_no.condition, None);
    }

    #[test]
    fn should_parse_multiple_processes_in_one_file() {
        // given
        let xml = r#"
          <definitions>
            <process id="a"><startEvent id="s" /><endEvent id="e" />
              <sequenceFlow id="f" sourceRef="s" targetRef="e" /></process>
            <process id="b"><startEvent id="s" /><endEvent id="e" />
              <sequenceFlow id="f" sourceRef="s" targetRef="e" /></process>
          </definitions>"#;

        // when
        let defs = parse_bpmn(xml).unwrap();

        // then
        assert_eq!(
            defs.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[test]
    fn should_reject_a_file_without_a_process() {
        // given
        let xml = r#"<definitions xmlns="x"></definitions>"#;

        // when / then
        assert_eq!(parse_bpmn(xml), Err(ParseError::NoProcess));
    }

    #[test]
    fn should_reject_a_process_without_a_start_event() {
        // given
        let xml = r#"<definitions><process id="p"><endEvent id="e" /></process></definitions>"#;

        // when
        let err = parse_bpmn(xml).unwrap_err();

        // then
        assert!(matches!(err, ParseError::InvalidProcess { .. }));
    }

    #[test]
    fn should_parse_an_error_boundary_event() {
        // given
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="charge-card" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="declined" attachedToRef="charge">
                <bpmn:errorEventDefinition errorRef="Error_1" />
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="refunded" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="declined" targetRef="refunded" />
            </bpmn:process>
            <bpmn:error id="Error_1" name="Declined" errorCode="CARD_DECLINED" />
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("declined").unwrap().kind,
            ElementKind::ErrorBoundaryEvent {
                attached_to: "charge".to_string(),
                error_code: "CARD_DECLINED".to_string(),
            }
        );
        let boundary = def.element("declined").unwrap();
        assert!(boundary.outgoing.iter().any(|f| f.to == "refunded"));
    }

    #[test]
    fn should_parse_a_timer_boundary_event() {
        // given
        let xml = r#"
          <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
            <bpmn:process id="p">
              <bpmn:startEvent id="s" />
              <bpmn:serviceTask id="charge">
                <bpmn:extensionElements>
                  <zeebe:taskDefinition type="charge-card" />
                </bpmn:extensionElements>
              </bpmn:serviceTask>
              <bpmn:endEvent id="done" />
              <bpmn:boundaryEvent id="timeout" attachedToRef="charge">
                <bpmn:timerEventDefinition>
                  <bpmn:timeDuration>PT5S</bpmn:timeDuration>
                </bpmn:timerEventDefinition>
              </bpmn:boundaryEvent>
              <bpmn:endEvent id="escalated" />
              <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="charge" />
              <bpmn:sequenceFlow id="f1" sourceRef="charge" targetRef="done" />
              <bpmn:sequenceFlow id="f2" sourceRef="timeout" targetRef="escalated" />
            </bpmn:process>
          </bpmn:definitions>"#;

        // when
        let def = &parse_bpmn(xml).unwrap()[0];

        // then
        assert_eq!(
            def.element("timeout").unwrap().kind,
            ElementKind::TimerBoundaryEvent {
                attached_to: "charge".to_string(),
                duration_millis: 5_000,
            }
        );
        let boundary = def.element("timeout").unwrap();
        assert!(boundary.outgoing.iter().any(|f| f.to == "escalated"));
    }

    #[test]
    fn should_reject_a_boundary_event_referencing_an_unknown_error() {
        // given
        let xml = r#"
          <definitions>
            <process id="p">
              <startEvent id="s" />
              <serviceTask id="t" />
              <endEvent id="e" />
              <boundaryEvent id="b" attachedToRef="t">
                <errorEventDefinition errorRef="missing" />
              </boundaryEvent>
              <endEvent id="caught" />
              <sequenceFlow id="f0" sourceRef="s" targetRef="t" />
              <sequenceFlow id="f1" sourceRef="t" targetRef="e" />
              <sequenceFlow id="f2" sourceRef="b" targetRef="caught" />
            </process>
          </definitions>"#;

        // when
        let err = parse_bpmn(xml).unwrap_err();

        // then
        assert!(matches!(err, ParseError::InvalidBoundaryEvent { .. }));
    }

    #[test]
    fn should_parse_integer_and_boolean_literals() {
        // given / when / then
        assert_eq!(
            parse_condition("= n == 7"),
            Some(Condition::Equals {
                variable: "n".to_string(),
                value: Value::Int(7),
            })
        );
        assert_eq!(
            parse_condition("=flag = true"),
            Some(Condition::Equals {
                variable: "flag".to_string(),
                value: Value::Bool(true),
            })
        );
    }
}
