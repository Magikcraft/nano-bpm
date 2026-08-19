//! Rule #854 — gateway condition-or-default.
//!
//! A diverging gateway that routes by evaluating flow conditions (an **exclusive**
//! or **inclusive** gateway with more than one outgoing flow) must gate every
//! non-`default` outgoing branch with a `conditionExpression`; the single
//! `default` branch is the only one allowed to be unconditional. Zeebe rejects a
//! model that violates this at deploy — `SequenceFlowValidator` reports
//! *"Must have a condition or be default flow"* for an exclusive gateway and
//! *"Must have a condition"* for an inclusive one. Nano historically accepted such
//! a model and then selected the unconditional branch by document order, silently
//! diverging from Zeebe. This validator restores parity.
//!
//! It reads the built [`ProcessDefinition`](crate::model::ProcessDefinition)
//! (`input.def`): each gateway element's outgoing [`SequenceFlow`]s already carry
//! their `condition` and the `is_default` marker (the parser resolves the
//! gateway's `default="…"` attribute onto the flow), so the rule is a pure read.
//! A gateway with a single outgoing flow is exempt (there is nothing to gate),
//! matching Zeebe.
//!
//! Only **exclusive** gateways are reachable here today: Nano does not model the
//! **inclusive** gateway, so an `<inclusiveGateway>` is rejected earlier — by the
//! `unsupported_elements` validator (#853) as an `UnsupportedElement`, or, before
//! that lands, by the builder as a dangling flow target — and never reaches this
//! pass. The rule is written to cover every condition-routed gateway kind Nano
//! models; when inclusive-gateway support is added to
//! [`ElementKind`](crate::model::ElementKind), extend [`is_condition_routed`] to
//! include it and this rule guards it with no further change.
//!
//! [`SequenceFlow`]: crate::model::SequenceFlow

use super::ValidationInput;
use crate::bpmn::ParseError;
use crate::model::ElementKind;

/// Whether `kind` is a diverging gateway that selects outgoing flows by
/// evaluating their conditions — the gateway kinds Zeebe's condition-or-default
/// rule applies to. Parallel gateways (take every outgoing flow) and
/// event-based gateways (a deferred choice over downstream catch events, not
/// conditions) are deliberately excluded.
fn is_condition_routed(kind: &ElementKind) -> bool {
    matches!(kind, ElementKind::ExclusiveGateway)
}

pub(crate) fn validate(input: &ValidationInput<'_>) -> Result<(), ParseError> {
    let def = input.def;

    // Deterministic order so the reported gateway is stable when several are
    // invalid (`elements` is a HashMap).
    let mut gateways: Vec<&crate::model::Element> = def
        .elements
        .values()
        .filter(|el| is_condition_routed(&el.kind))
        .collect();
    gateways.sort_by(|a, b| a.id.cmp(&b.id));

    for gateway in gateways {
        // A gateway with one (or zero) outgoing flow has nothing to gate.
        if gateway.outgoing.len() < 2 {
            continue;
        }
        for flow in &gateway.outgoing {
            if flow.condition.is_none() && !flow.is_default {
                return Err(ParseError::InvalidGateway {
                    process_id: def.id.clone(),
                    gateway_id: gateway.id.clone(),
                    reason: format!(
                        "the outgoing branch to '{}' has no condition and is not the default flow; \
                         every non-default outgoing branch of a diverging gateway must have a \
                         condition or be the default flow",
                        flow.to
                    ),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::bpmn::{parse_bpmn, ParseError};

    /// Wraps process bodies in a `<definitions>` envelope and parses the first.
    fn parse_one(process_body: &str) -> Result<(), ParseError> {
        let xml = format!(
            r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
                 <bpmn:process id="p" isExecutable="true">{process_body}</bpmn:process>
               </bpmn:definitions>"#
        );
        parse_bpmn(&xml).map(|_| ())
    }

    /// An exclusive gateway with two outgoing flows, each branch's condition
    /// expressed via `cond` (`Some(expr)` → conditional, `None` → unconditional),
    /// and an optional `default` flow id on the gateway.
    fn exclusive_two_out(
        default_attr: &str,
        f1_cond: Option<&str>,
        f2_cond: Option<&str>,
    ) -> String {
        let cond1 = f1_cond
            .map(|c| format!("<bpmn:conditionExpression>{c}</bpmn:conditionExpression>"))
            .unwrap_or_default();
        let cond2 = f2_cond
            .map(|c| format!("<bpmn:conditionExpression>{c}</bpmn:conditionExpression>"))
            .unwrap_or_default();
        format!(
            r#"<bpmn:startEvent id="s"/>
               <bpmn:exclusiveGateway id="gw" {default_attr}/>
               <bpmn:endEvent id="e1"/>
               <bpmn:endEvent id="e2"/>
               <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="gw"/>
               <bpmn:sequenceFlow id="f1" sourceRef="gw" targetRef="e1">{cond1}</bpmn:sequenceFlow>
               <bpmn:sequenceFlow id="f2" sourceRef="gw" targetRef="e2">{cond2}</bpmn:sequenceFlow>"#
        )
    }

    fn is_invalid_gateway(res: &Result<(), ParseError>) -> bool {
        matches!(res, Err(ParseError::InvalidGateway { .. }))
    }

    /// Class-scoped table: for a multi-out exclusive gateway, every non-default
    /// branch must have a condition. The `(f1, f2, default, accepted)` rows cover
    /// {has-condition, is-default, neither} across both branches.
    #[test]
    fn exclusive_gateway_condition_or_default_class() {
        struct Case {
            name: &'static str,
            f1_cond: Option<&'static str>,
            f2_cond: Option<&'static str>,
            default_attr: &'static str,
            accepted: bool,
        }
        let cases = [
            Case {
                name: "one conditionless non-default branch is rejected",
                f1_cond: Some("=x&gt;1"),
                f2_cond: None,
                default_attr: "",
                accepted: false,
            },
            Case {
                name: "two conditionless non-default branches are rejected",
                f1_cond: None,
                f2_cond: None,
                default_attr: "",
                accepted: false,
            },
            Case {
                name: "conditionless branch that is the default is accepted",
                f1_cond: Some("=x&gt;1"),
                f2_cond: None,
                default_attr: r#"default="f2""#,
                accepted: true,
            },
            Case {
                name: "every non-default branch conditioned is accepted",
                f1_cond: Some("=x&gt;1"),
                f2_cond: Some("=x&lt;=1"),
                default_attr: "",
                accepted: true,
            },
            Case {
                name: "conditioned branches plus a conditionless default is accepted",
                f1_cond: Some("=x&gt;1"),
                f2_cond: None,
                default_attr: r#"default="f2""#,
                accepted: true,
            },
        ];
        for case in cases {
            let res = parse_one(&exclusive_two_out(
                case.default_attr,
                case.f1_cond,
                case.f2_cond,
            ));
            if case.accepted {
                assert!(
                    res.is_ok(),
                    "case '{}' should be accepted, got {res:?}",
                    case.name
                );
            } else {
                assert!(
                    is_invalid_gateway(&res),
                    "case '{}' should be rejected with InvalidGateway, got {res:?}",
                    case.name
                );
            }
        }
    }

    /// A single-outgoing exclusive gateway has nothing to gate, so an
    /// unconditional flow is accepted (Zeebe applies the rule only to diverging
    /// gateways with more than one outgoing flow).
    #[test]
    fn single_outgoing_gateway_needs_no_condition() {
        let body = r#"<bpmn:startEvent id="s"/>
            <bpmn:exclusiveGateway id="gw"/>
            <bpmn:endEvent id="e1"/>
            <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="gw"/>
            <bpmn:sequenceFlow id="f1" sourceRef="gw" targetRef="e1"/>"#;
        assert!(parse_one(body).is_ok());
    }

    /// The rule applies only to condition-routed gateways: a parallel gateway
    /// takes every outgoing flow, so conditionless branches are legitimate and
    /// must stay accepted.
    #[test]
    fn parallel_gateway_conditionless_branches_are_accepted() {
        let body = r#"<bpmn:startEvent id="s"/>
            <bpmn:parallelGateway id="gw"/>
            <bpmn:endEvent id="e1"/>
            <bpmn:endEvent id="e2"/>
            <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="gw"/>
            <bpmn:sequenceFlow id="f1" sourceRef="gw" targetRef="e1"/>
            <bpmn:sequenceFlow id="f2" sourceRef="gw" targetRef="e2"/>"#;
        assert!(parse_one(body).is_ok());
    }

    /// Inclusive gateways are equally condition-routed in Zeebe, but Nano does
    /// not model them, so an `<inclusiveGateway>` with a conditionless
    /// non-default branch never deploys — it is rejected upstream (as an
    /// unsupported element / a dangling flow target) before this pass runs. This
    /// guards the parity outcome (an invalid inclusive gateway does not silently
    /// deploy) and will fail loudly if inclusive support is ever added without
    /// extending this rule to cover it.
    #[test]
    fn inclusive_gateway_conditionless_branch_does_not_deploy() {
        let body = r#"<bpmn:startEvent id="s"/>
            <bpmn:inclusiveGateway id="gw"/>
            <bpmn:endEvent id="e1"/>
            <bpmn:endEvent id="e2"/>
            <bpmn:sequenceFlow id="f0" sourceRef="s" targetRef="gw"/>
            <bpmn:sequenceFlow id="f1" sourceRef="gw" targetRef="e1"><bpmn:conditionExpression>=x&gt;1</bpmn:conditionExpression></bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="f2" sourceRef="gw" targetRef="e2"/>"#;
        assert!(parse_one(body).is_err());
    }
}
