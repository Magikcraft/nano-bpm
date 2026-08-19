//! Rule #853 — unsupported flow elements / event definitions (STUB).
//!
//! Sibling slice #853 implements this rule by editing **only** this file. The
//! streaming parser already records every unmodelled flow-element tag / event
//! definition it does not model as `{ tag, element_id }` in
//! `capture.unmodelled` (an explicit ignore-list keeps out non-flow noise).
//! #853 must reject each recorded entry that is genuinely unsupported —
//! deriving the *supported* set from the canonical registry (`processos`
//! `ELEMENT_KIND_SPECS` / engine-core `ElementKind`) so it cannot drift — with
//! [`ParseError::UnsupportedElement`](crate::bpmn::ParseError::UnsupportedElement).
//!
//! Do **not** edit `crate::bpmn`'s streaming parser / catch-all (the tags are
//! already recorded), `validate/mod.rs` (this validator is already registered),
//! or the `ParseError` enum.
//!
//! ## Why this cannot maintain (or drift from) a parallel allowlist
//!
//! The *supported* set is not written here — it is the parser's own modelled
//! `match` arms in [`crate::bpmn`], which correspond one-to-one to the canonical
//! [`crate::model::ElementKind`] registry (mirrored by `processos`'s
//! `ELEMENT_KIND_SPECS`). A tag reaches `capture.unmodelled` **only** when the
//! parser has no arm that models it *and* it is not genuinely-ignorable non-flow
//! noise (`is_ignorable_tag`). So the candidate list handed to this validator is
//! already exactly `all tags − supported − noise`; this rule simply rejects the
//! residual. Because it reads that pre-derived list rather than duplicating a
//! keyword allowlist, the drift surface is closed: teaching the engine a new
//! `ElementKind` (a new parser arm) makes its tag stop appearing in
//! `capture.unmodelled`, so it is accepted here with **no change to this file**.
//!
//! ## The one residual filter: foreign extension-element children
//!
//! The scaffold's parse-side ignore-list is deliberately an *exclusion list of
//! noise* and, as its own doc-comment notes, "over-recording a rare non-flow tag
//! is harmless (the validator filters it)". The streaming gates that consume
//! Zeebe extension-element children (`<zeebe:header>` inside `<zeebe:taskHeaders>`,
//! `<zeebe:linkedResource>` inside `<zeebe:linkedResources>`, …) only match those
//! children *in position*; a **misplaced** extension child therefore falls
//! through to the flow-element catch-all and is over-recorded. Such a child is
//! foreign-namespace extension metadata, not a BPMN flow element or event
//! definition — Zeebe ignores it rather than failing the deploy — so this rule
//! must ignore it too. [`is_extension_noise`] performs exactly that final filter
//! (the parser strips namespace prefixes, so the discriminator is the set of
//! Zeebe extension-element local names). It is *not* a supported-element
//! allowlist: every BPMN flow element / event definition Nano does not model
//! still rejects.

use super::ValidationInput;
use crate::bpmn::ParseError;

/// Local names of the Zeebe extension-element tags that reach the flow-element
/// catch-all as foreign-namespace extension metadata (which Zeebe ignores),
/// rather than as an unmodelled BPMN flow element — so the unsupported-element
/// rule filters them out. There are two ways such a tag reaches the catch-all,
/// and both belong here:
///
/// - **Consumed positionally, but misplaced.** Most entries are children the
///   parser consumes inside their designated container / owner in `crate::bpmn`
///   (e.g. `zeebe:header` inside `zeebe:taskHeaders`, `zeebe:input` inside
///   `zeebe:ioMapping`). Correctly placed they never reach the catch-all; only
///   when *misplaced* does the container guard fail and the tag fall through and
///   get over-recorded on `capture.unmodelled`.
/// - **Not modelled at all.** A few are Zeebe extension containers the parser
///   does not handle anywhere (e.g. the `zeebe:properties` container — Nano does
///   not model element/process properties). These are *never* consumed, so they
///   *always* reach the catch-all, misplaced or not, and would be over-recorded
///   without this filter.
///
/// This is therefore **not** simply "the `zeebe:` local names the parser
/// handles": it is the set that must be suppressed at the catch-all, which is
/// neither a subset nor a superset of the handled set.
///
/// Only local names that would otherwise fall through to the flow-element
/// catch-all (and so be over-recorded on `capture.unmodelled`) belong here. A
/// `zeebe:` extension local name that *collides* with a modelled BPMN
/// flow-element tag — e.g. `zeebe:userTask`, whose local name `userTask` is
/// consumed by the same modelled `"userTask"` parser arm as `<bpmn:userTask>`
/// (and, being id-less, is a no-op there) — is **not** listed: it never reaches
/// the catch-all, so it is not noise this filter needs to suppress. Listing
/// such a BPMN flow-element local name would be dead and, worse, would silently
/// mask a genuinely unsupported `<bpmn:userTask>` capture should the parser's
/// recording logic ever change.
///
/// Likewise, only a name the parser ever encounters as an *element tag* belongs
/// here. A Zeebe attribute name or attribute *value* — e.g. `versionTag`, which
/// the parser only ever reads as the `zeebe:linkedResource` `versionTag`
/// attribute and as the `bindingType="versionTag"` value, never as a tag — can
/// never be recorded on `capture.unmodelled`, so listing it is dead and, worse,
/// would silently mask a genuinely unsupported element named `versionTag`
/// should one ever appear.
///
/// Finally, a name already covered by `is_ignorable_tag` (in `crate::bpmn`) must
/// **not** be duplicated here. That noise filter runs *first* at the catch-all,
/// so such a tag never reaches this filter — e.g. `property`, listed there as
/// structural BPMN, also captures a namespace-stripped `zeebe:property`. Listing
/// it here too would be dead and duplicate a second source of truth for the same
/// tag.
const EXTENSION_NOISE: &[&str] = &[
    "taskDefinition",
    "taskHeaders",
    "header",
    "ioMapping",
    "input",
    "output",
    "subscription",
    "calledDecision",
    "calledElement",
    "formDefinition",
    "assignmentDefinition",
    "taskSchedule",
    "priorityDefinition",
    "properties",
    "script",
    "linkedResources",
    "linkedResource",
    "executionListeners",
    "executionListener",
    "taskListeners",
    "taskListener",
];

/// Whether a recorded unmodelled tag is a foreign extension-element child
/// (surfaced only when misplaced) rather than a BPMN flow element / event
/// definition. See the module docs.
fn is_extension_noise(tag: &str) -> bool {
    EXTENSION_NOISE.contains(&tag)
}

/// Rejects the first recorded unmodelled flow element / event definition.
///
/// Every entry in `capture.unmodelled` is a flow-element tag or event definition
/// the engine does not model — the parser's noise `is_ignorable_tag` filter has
/// already excluded diagram interchange, documentation, structural/data BPMN,
/// and the event definitions the engine *does* model — save for the rare
/// misplaced extension child [`is_extension_noise`] filters out here.
/// Historically such a tag was silently dropped: a flow into it then failed
/// deploy with a misleading "unknown target element" error at the flow, and an
/// element with no inbound flow deployed clean and mis-executed. Zeebe instead
/// only transforms known element types and rejects the rest at deploy; matching
/// that, we reject with an actionable [`ParseError::UnsupportedElement`] naming
/// the offending tag and element id.
pub(crate) fn validate(input: &ValidationInput<'_>) -> Result<(), ParseError> {
    if let Some(unsupported) = input
        .capture
        .unmodelled
        .iter()
        .find(|u| !is_extension_noise(&u.tag))
    {
        return Err(ParseError::UnsupportedElement {
            tag: unsupported.tag.clone(),
            element_id: unsupported.element_id.clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::bpmn::{parse_bpmn, ParseError};

    /// Wraps `body` (the children of a `<process>`) in a minimal, otherwise
    /// deploy-valid definition: a none start event flowing into `body`'s first
    /// declared element is *not* assumed — callers provide whatever they need.
    fn defs_xml(body: &str) -> String {
        format!(
            r#"<bpmn:definitions
                 xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                 xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
                 <bpmn:process id="p" isExecutable="true">
                   {body}
                 </bpmn:process>
               </bpmn:definitions>"#
        )
    }

    fn parse_err(xml: &str) -> Option<ParseError> {
        parse_bpmn(xml).err()
    }

    // ── The defect CLASS: every unmodelled flow element / event definition is
    //    rejected with `UnsupportedElement{tag, element_id}` naming the culprit,
    //    instead of being silently dropped (green here, RED on `main`). ──

    #[test]
    fn unsupported_flow_elements_and_event_definitions_are_rejected() {
        // (fragment, expected offending tag, expected element id). Each fragment
        // is embedded in an otherwise deploy-valid process (a real start + end +
        // flows) so the *only* reason to reject is the unsupported construct —
        // proving the rejection is this rule's, not a start/flow rule's.
        let cases: &[(&str, &str, &str)] = &[
            // Unsupported flow-element tags. These are *detached* (no sequence
            // flow references them) — the exact "deploys clean and silently
            // mis-executes" bug: on `main` the element is dropped and the
            // process deploys; here it must reject. (A flow *into* an unmodelled
            // element is a different symptom the build already rejects, with a
            // misleading message, before this validator runs.)
            (
                r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                   <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
                   <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
                   <bpmn:complexGateway id="cg" />"#,
                "complexGateway",
                "cg",
            ),
            (
                r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                   <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
                   <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
                   <bpmn:transaction id="tx" />"#,
                "transaction",
                "tx",
            ),
            (
                r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                   <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
                   <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
                   <bpmn:sendTask id="snd" />"#,
                "sendTask",
                "snd",
            ),
            // Unsupported event definitions: an event the engine does not model
            // must reject, not silently degrade to a bare none event. The owning
            // event *is* modelled, so the build succeeds and the definition is
            // recorded as unmodelled. It carries no id of its own, so it is
            // attributed to the owning element.
            (
                r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                   <bpmn:endEvent id="ce"><bpmn:incoming>f1</bpmn:incoming>
                     <bpmn:cancelEventDefinition />
                   </bpmn:endEvent>
                   <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="ce" />"#,
                "cancelEventDefinition",
                "ce",
            ),
            (
                r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
                   <bpmn:intermediateThrowEvent id="cmp"><bpmn:incoming>f1</bpmn:incoming>
                     <bpmn:outgoing>f2</bpmn:outgoing>
                     <bpmn:compensateEventDefinition />
                   </bpmn:intermediateThrowEvent>
                   <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
                   <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="cmp" />
                   <bpmn:sequenceFlow id="f2" sourceRef="cmp" targetRef="e" />"#,
                "compensateEventDefinition",
                "cmp",
            ),
        ];

        for (fragment, want_tag, want_id) in cases {
            let xml = defs_xml(fragment);
            match parse_err(&xml) {
                Some(ParseError::UnsupportedElement { tag, element_id }) => {
                    assert_eq!(
                        &tag, want_tag,
                        "wrong tag reported for fragment:\n{fragment}"
                    );
                    assert_eq!(
                        &element_id, want_id,
                        "wrong element id reported for tag `{want_tag}`",
                    );
                }
                other => panic!(
                    "expected UnsupportedElement{{tag: {want_tag:?}, ..}} for fragment:\n{fragment}\ngot {other:?}"
                ),
            }
        }
    }

    // ── Positive: modelled elements (the whole executable surface) and
    //    genuinely-ignorable non-flow noise are accepted. This is also the
    //    DRIFT guard — the validator holds no keyword list, so the supported set
    //    is exactly what the parser (≡ `ElementKind`) models. An element that
    //    looks "exotic" but IS modelled (conditional event, business-rule task,
    //    ad-hoc sub-process) is accepted with no entry in this file. ──

    #[test]
    fn modelled_elements_and_ignorable_noise_are_accepted() {
        let xml = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:serviceTask id="st" name="do">
                 <bpmn:extensionElements>
                   <zeebe:taskDefinition type="work" />
                 </bpmn:extensionElements>
                 <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
               </bpmn:serviceTask>
               <bpmn:exclusiveGateway id="gw" default="f4">
                 <bpmn:incoming>f2</bpmn:incoming>
                 <bpmn:outgoing>f3</bpmn:outgoing><bpmn:outgoing>f4</bpmn:outgoing>
               </bpmn:exclusiveGateway>
               <bpmn:intermediateCatchEvent id="cond">
                 <bpmn:incoming>f3</bpmn:incoming><bpmn:outgoing>f5</bpmn:outgoing>
                 <bpmn:conditionalEventDefinition>
                   <bpmn:condition>=x &gt; 1</bpmn:condition>
                 </bpmn:conditionalEventDefinition>
               </bpmn:intermediateCatchEvent>
               <bpmn:endEvent id="e1"><bpmn:incoming>f4</bpmn:incoming></bpmn:endEvent>
               <bpmn:endEvent id="e2"><bpmn:incoming>f5</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="st" />
               <bpmn:sequenceFlow id="f2" sourceRef="st" targetRef="gw" />
               <bpmn:sequenceFlow id="f3" sourceRef="gw" targetRef="cond">
                 <bpmn:conditionExpression>=x &gt; 0</bpmn:conditionExpression>
               </bpmn:sequenceFlow>
               <bpmn:sequenceFlow id="f4" sourceRef="gw" targetRef="e1" />
               <bpmn:sequenceFlow id="f5" sourceRef="cond" targetRef="e2" />
               <bpmn:textAnnotation id="note"><bpmn:text>hi</bpmn:text></bpmn:textAnnotation>
               <bpmn:association id="a1" sourceRef="st" targetRef="note" />"#,
        );
        assert!(
            parse_bpmn(&xml).is_ok(),
            "a process built only from modelled elements + ignorable noise must deploy: {:?}",
            parse_bpmn(&xml).err()
        );
    }

    /// The drift guard, made explicit: the *same* tag that is rejected as
    /// unsupported when the engine does not model it is accepted the moment the
    /// engine models it — and this file never mentions the tag. `receiveTask`
    /// (a modelled `ElementKind`) stands in for "a synthetic 'known' kind":
    /// swapping the unsupported `sendTask` for the modelled `receiveTask` flips
    /// reject → accept with no edit here, because the decision is derived from
    /// the parser's registry, not a list maintained in this validator.
    #[test]
    fn supported_set_is_derived_from_the_parser_not_a_local_list() {
        let unsupported = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:endEvent id="e"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="e" />
               <bpmn:sendTask id="t" />"#,
        );
        assert!(
            matches!(
                parse_err(&unsupported),
                Some(ParseError::UnsupportedElement { .. })
            ),
            "an unmodelled `sendTask` must reject",
        );

        let supported = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:receiveTask id="t"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:receiveTask>
               <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
               <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />"#,
        );
        assert!(
            parse_bpmn(&supported).is_ok(),
            "a modelled `receiveTask` must deploy without any change to this validator: {:?}",
            parse_bpmn(&supported).err(),
        );
    }

    /// Foreign extension-element children that fall through the streaming gates
    /// when misplaced (a `<zeebe:header>` outside `<zeebe:taskHeaders>`, a
    /// `<zeebe:linkedResource>` outside `<zeebe:linkedResources>`) are extension
    /// metadata Zeebe ignores — they must NOT be reported as unsupported flow
    /// elements. Guards the [`is_extension_noise`] filter against regressing to
    /// rejecting valid deploys.
    #[test]
    fn misplaced_extension_children_are_not_unsupported_elements() {
        let stray_header = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:serviceTask id="t"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
                 <bpmn:extensionElements>
                   <zeebe:taskDefinition type="w" />
                   <zeebe:header key="stray" value="nope" />
                 </bpmn:extensionElements>
               </bpmn:serviceTask>
               <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
               <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />"#,
        );
        assert!(
            parse_bpmn(&stray_header).is_ok(),
            "a misplaced <zeebe:header> is extension noise Zeebe ignores, not an \
             unsupported element: {:?}",
            parse_bpmn(&stray_header).err(),
        );

        // The `zeebe:properties` container is a distinct sub-case: the parser
        // models it *nowhere*, so it always reaches the flow-element catch-all
        // (not only when misplaced) and would be over-recorded without the
        // `EXTENSION_NOISE` entry. Its `zeebe:property` children are absorbed by
        // `is_ignorable_tag` upstream. A service task carrying element
        // properties must still deploy clean.
        let element_properties = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:serviceTask id="t"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
                 <bpmn:extensionElements>
                   <zeebe:taskDefinition type="w" />
                   <zeebe:properties>
                     <zeebe:property name="k" value="v" />
                   </zeebe:properties>
                 </bpmn:extensionElements>
               </bpmn:serviceTask>
               <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
               <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />"#,
        );
        assert!(
            parse_bpmn(&element_properties).is_ok(),
            "a <zeebe:properties> container is extension metadata Zeebe ignores, not \
             an unsupported element: {:?}",
            parse_bpmn(&element_properties).err(),
        );

        // A genuine unsupported element sitting alongside extension noise must
        // still reject (the noise filter never masks a real flow element).
        let noise_plus_unsupported = defs_xml(
            r#"<bpmn:startEvent id="s"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
               <bpmn:serviceTask id="t"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
                 <bpmn:extensionElements>
                   <zeebe:taskDefinition type="w" />
                   <zeebe:header key="stray" value="nope" />
                 </bpmn:extensionElements>
               </bpmn:serviceTask>
               <bpmn:endEvent id="e"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
               <bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />
               <bpmn:sequenceFlow id="f2" sourceRef="t" targetRef="e" />
               <bpmn:transaction id="tx" />"#,
        );
        assert!(
            matches!(
                parse_err(&noise_plus_unsupported),
                Some(ParseError::UnsupportedElement { tag, .. }) if tag == "transaction"
            ),
            "a real unsupported `transaction` must reject even when extension noise \
             was also over-recorded: {:?}",
            parse_err(&noise_plus_unsupported),
        );
    }
}
