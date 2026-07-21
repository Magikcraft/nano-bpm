//! Parses DMN 1.3 XML into a [`DecisionRequirementsGraph`].
//!
//! Like the BPMN parser it is namespace-prefix agnostic and understands exactly
//! the DMN Nano evaluates natively: `<decision>`s containing either a
//! `<decisionTable>` (inputs/outputs/rules, hit policy, aggregation) or a
//! `<literalExpression>`, wired by `<informationRequirement>`. Other decision
//! types are recorded as [`DecisionLogic::Unsupported`] so the graph still parses
//! and unrelated decisions remain evaluable.

use super::model::{
    Aggregation, Decision, DecisionLogic, DecisionRequirementsGraph, DecisionRule, DecisionTable,
    HitPolicy, InputClause, OutputClause,
};
use crate::xml::{parse_tree, Element};

/// An error encountered while parsing DMN XML.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DmnParseError {
    /// The XML was malformed.
    MalformedXml(String),
    /// The root element was not `<definitions>`.
    NotDmn,
    /// A `<decision>` had no `id`.
    DecisionWithoutId,
    /// The resource declared no decisions.
    NoDecisions,
}

impl std::fmt::Display for DmnParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DmnParseError::MalformedXml(m) => write!(f, "malformed DMN XML: {m}"),
            DmnParseError::NotDmn => write!(f, "root element is not <definitions>"),
            DmnParseError::DecisionWithoutId => write!(f, "a <decision> is missing its id"),
            DmnParseError::NoDecisions => write!(f, "the DMN resource declares no decisions"),
        }
    }
}

impl std::error::Error for DmnParseError {}

/// Parses a DMN resource into a [`DecisionRequirementsGraph`].
pub fn parse_dmn(xml: &str) -> Result<DecisionRequirementsGraph, DmnParseError> {
    let root = parse_tree(xml).map_err(|e| DmnParseError::MalformedXml(e.0))?;
    if root.local_name() != "definitions" {
        return Err(DmnParseError::NotDmn);
    }

    let mut decisions = Vec::new();
    for decision_el in root.children_named("decision") {
        decisions.push(parse_decision(decision_el)?);
    }
    if decisions.is_empty() {
        return Err(DmnParseError::NoDecisions);
    }

    Ok(DecisionRequirementsGraph {
        id: root.attr("id").unwrap_or("").to_string(),
        name: root.attr("name").unwrap_or("").to_string(),
        namespace: root.attr("namespace").unwrap_or("").to_string(),
        decisions,
        xml: xml.to_string(),
    })
}

fn parse_decision(el: &Element) -> Result<Decision, DmnParseError> {
    let id = el
        .attr("id")
        .filter(|s| !s.is_empty())
        .ok_or(DmnParseError::DecisionWithoutId)?
        .to_string();
    let name = el.attr("name").unwrap_or(&id).to_string();

    let variable_name = el
        .child("variable")
        .and_then(|v| v.attr("name"))
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let mut required_decisions = Vec::new();
    for req in el.children_named("informationRequirement") {
        if let Some(rd) = req.child("requiredDecision") {
            if let Some(href) = rd.attr("href") {
                required_decisions.push(href.trim_start_matches('#').to_string());
            }
        }
    }

    let logic = if let Some(table) = el.child("decisionTable") {
        DecisionLogic::DecisionTable(parse_decision_table(table))
    } else if let Some(lit) = el.child("literalExpression") {
        DecisionLogic::LiteralExpression(lit.child_text("text").unwrap_or_default())
    } else {
        // Record the first recognised decision-logic child (context, invocation,
        // list, relation) so the decision type is reported faithfully.
        let kind = el
            .children
            .iter()
            .map(|c| c.local_name())
            .find(|n| matches!(*n, "context" | "invocation" | "list" | "relation"))
            .unwrap_or("unknown")
            .to_string();
        DecisionLogic::Unsupported(kind)
    };

    Ok(Decision {
        id,
        name,
        variable_name,
        required_decisions,
        logic,
    })
}

fn parse_decision_table(el: &Element) -> DecisionTable {
    let hit_policy = HitPolicy::parse(el.attr("hitPolicy").unwrap_or("UNIQUE"));
    let aggregation = el.attr("aggregation").and_then(Aggregation::parse);

    let inputs = el
        .children_named("input")
        .map(|inp| InputClause {
            id: inp.attr("id").unwrap_or("").to_string(),
            label: inp.attr("label").map(str::to_string),
            expression: inp
                .child("inputExpression")
                .and_then(|ie| ie.child_text("text"))
                .unwrap_or_default(),
            type_ref: inp
                .child("inputExpression")
                .and_then(|ie| ie.attr("typeRef"))
                .map(str::to_string),
        })
        .collect();

    let outputs: Vec<OutputClause> = el
        .children_named("output")
        .map(|out| OutputClause {
            id: out.attr("id").unwrap_or("").to_string(),
            label: out.attr("label").map(str::to_string),
            name: out.attr("name").map(str::to_string),
            type_ref: out.attr("typeRef").map(str::to_string),
            output_values: out
                .child("outputValues")
                .and_then(|ov| ov.child_text("text"))
                .map(|t| split_top_level_commas(&t))
                .unwrap_or_default(),
        })
        .collect();

    let rules = el
        .children_named("rule")
        .map(|rule| DecisionRule {
            id: rule.attr("id").unwrap_or("").to_string(),
            input_entries: rule
                .children_named("inputEntry")
                .map(|e| e.child_text("text").unwrap_or_default())
                .collect(),
            output_entries: rule
                .children_named("outputEntry")
                .map(|e| e.child_text("text").unwrap_or_default())
                .collect(),
        })
        .collect();

    DecisionTable {
        hit_policy,
        aggregation,
        inputs,
        outputs,
        rules,
    }
}

/// Splits a comma-separated FEEL list (e.g. an `<outputValues>` text) into its
/// top-level items, respecting quotes, parentheses and brackets.
pub(super) fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut start = 0usize;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if in_str {
            if c == b'"' {
                in_str = false;
            }
        } else {
            match c {
                b'"' => in_str = true,
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => depth -= 1,
                b',' if depth == 0 => {
                    let item = s[start..i].trim();
                    if !item.is_empty() {
                        items.push(item.to_string());
                    }
                    start = i + 1;
                }
                _ => {}
            }
        }
        i += 1;
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        items.push(last.to_string());
    }
    items
}
