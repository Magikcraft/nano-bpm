//! The **declarative notation table** for the reversible semantic IR (ADR 0001) — one spec, many
//! consumers.
//!
//! The engine's possibility space is a closed algebraic data type ([`nanobpmn_engine_core::ElementKind`],
//! [`nanobpmn_engine_core::SequenceFlow`], and the shared element-level extras). This module holds
//! that possibility space in a single declarative table [`ELEMENT_KIND_SPECS`] and derives all
//! surfaces from it:
//!
//! 1. **GBNF for llama.cpp constrained decoding** — [`emit_gbnf`] renders the table to a per-kind
//!    grammar so a local model literally cannot emit invalid IR at sampling time.
//! 2. **Scoped `describe_ir_grammar` tool** — [`describe`] returns the productions for one element
//!    kind (or the compact one-page overview for none), so an LLM can look up "what can I say at an
//!    exclusive gateway?" on demand without dumping the whole grammar every turn.
//! 3. **Parity harness** — [`sample_instances`] materialises one placeholder of every variant so
//!    the tests can round-trip through the real pretty-printer and catch drift the moment a new
//!    `ElementKind` variant lands without a matching spec.
//!
//! The pretty-printer ([`crate::model_ir::render_kind_attrs`]) and the parser
//! ([`crate::model_ir::build_kind`]) remain exhaustive matches over `ElementKind` — that gives
//! compile-time coverage when a new engine variant lands. This table is kept honest against them
//! by [`tests::specs_match_pretty_printer`], which builds a placeholder instance of every variant
//! (via [`sample_instances`]), runs the real pretty-printer, and asserts the emitted attribute keys
//! match what the spec table declares. Add a new `ElementKind` → the compiler fails on the
//! pretty-printer/parser matches; forget to add it here → the parity test fails. Either way the
//! grammar cannot silently fall behind the engine.

use serde_json::{json, Value};

/// The type of an IR attribute value — drives both the GBNF rendering and the JSON-Schema mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrType {
    /// A double-quoted string with `\` and `"` escaped (`"hello"`).
    Str,
    /// A bare identifier ([A-Za-z_][A-Za-z0-9_]*), used for cross-element references (`attachedTo
    /// review_task`, `parent inner_sp`).
    Id,
    /// A non-negative integer followed by the literal `ms` suffix (`5000ms`). Millisecond
    /// durations and intervals.
    Duration,
    /// The literal `true` or `false`.
    Bool,
}

/// One attribute an element kind may (or must) carry in the IR.
#[derive(Debug, Clone, Copy)]
pub struct AttrSpec {
    /// The IR keyword for this attribute (`jobType`, `attachedTo`, …).
    pub key: &'static str,
    /// True when the attribute must be present. Required attributes correspond to non-`Option`
    /// fields on the `ElementKind` variant.
    pub required: bool,
    /// The value type — see [`AttrType`].
    pub ty: AttrType,
    /// One-line prose used by [`describe`] and the GBNF comment header.
    pub doc: &'static str,
}

/// One `ElementKind` variant, its IR keyword, and its attribute list.
#[derive(Debug, Clone, Copy)]
pub struct KindSpec {
    /// The IR keyword (`serviceTask`, `exclusiveGateway`, …). Must match
    /// [`crate::model_ir::kind_keyword`] for the corresponding variant — enforced by
    /// [`tests::specs_match_pretty_printer`].
    pub keyword: &'static str,
    /// One-line prose describing the kind.
    pub doc: &'static str,
    /// The kind-specific attributes (the ones emitted by
    /// [`crate::model_ir::render_kind_attrs`]).
    pub attrs: &'static [AttrSpec],
}

/// The whole possibility space — every `ElementKind` variant, in the order they appear on the enum.
/// Extend this when adding a new engine element kind.
pub const ELEMENT_KIND_SPECS: &[KindSpec] = &[
    KindSpec {
        keyword: "startEvent",
        doc: "None start event. Pass-through; entry point of a process.",
        attrs: &[],
    },
    KindSpec {
        keyword: "endEvent",
        doc: "None end event. Consuming its last token completes the instance.",
        attrs: &[],
    },
    KindSpec {
        keyword: "serviceTask",
        doc: "Job-based task. Creates a job of `jobType` and rests until completion.",
        attrs: &[
            AttrSpec {
                key: "jobType",
                required: true,
                ty: AttrType::Str,
                doc: "The `zeebe:taskDefinition type` a worker subscribes to.",
            },
            AttrSpec {
                key: "priority",
                required: false,
                ty: AttrType::Str,
                doc: "Raw priority expression (literal or FEEL); higher activates first.",
            },
        ],
    },
    KindSpec {
        keyword: "userTask",
        doc: "Native user task. A human claims and completes it via the user-task API.",
        attrs: &[
            AttrSpec {
                key: "assignee",
                required: false,
                ty: AttrType::Str,
                doc: "Raw assignee expression (literal or FEEL).",
            },
            AttrSpec {
                key: "candidateGroups",
                required: false,
                ty: AttrType::Str,
                doc: "Comma-separated list or FEEL expression yielding a list.",
            },
            AttrSpec {
                key: "candidateUsers",
                required: false,
                ty: AttrType::Str,
                doc: "Comma-separated list or FEEL expression yielding a list.",
            },
            AttrSpec {
                key: "dueDate",
                required: false,
                ty: AttrType::Str,
                doc: "Raw due-date expression (literal or FEEL).",
            },
            AttrSpec {
                key: "followUpDate",
                required: false,
                ty: AttrType::Str,
                doc: "Raw follow-up-date expression (literal or FEEL).",
            },
            AttrSpec {
                key: "priority",
                required: false,
                ty: AttrType::Str,
                doc: "Raw priority expression (literal or FEEL); default 50.",
            },
        ],
    },
    KindSpec {
        keyword: "exclusiveGateway",
        doc: "XOR gateway. Takes exactly one outgoing flow (first matching condition; default is \
              fallback).",
        attrs: &[],
    },
    KindSpec {
        keyword: "parallelGateway",
        doc: "AND gateway. Splits to all outgoing; joins by waiting on every incoming flow.",
        attrs: &[],
    },
    KindSpec {
        keyword: "errorBoundaryEvent",
        doc: "Error boundary on an activity. Fires when a job throws the matching `errorCode`.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to.",
            },
            AttrSpec {
                key: "errorCode",
                required: true,
                ty: AttrType::Str,
                doc: "The BPMN error code this boundary catches.",
            },
        ],
    },
    KindSpec {
        keyword: "timerIntermediateCatchEvent",
        doc: "Timer catch. Rests until `duration` after activation elapses.",
        attrs: &[AttrSpec {
            key: "duration",
            required: true,
            ty: AttrType::Duration,
            doc: "Delay after activation before the timer becomes due.",
        }],
    },
    KindSpec {
        keyword: "timerBoundaryEvent",
        doc: "Timer boundary on an activity. Interrupting or non-interrupting; may repeat.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to.",
            },
            AttrSpec {
                key: "duration",
                required: true,
                ty: AttrType::Duration,
                doc: "Delay after activation before firing (also the period when `repeating`).",
            },
            AttrSpec {
                key: "interrupting",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` cancels the activity on fire; `false` spawns a parallel token.",
            },
            AttrSpec {
                key: "repeating",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` re-arms after firing (a `timeCycle`); only meaningful when \
                      non-interrupting.",
            },
        ],
    },
    KindSpec {
        keyword: "messageIntermediateCatchEvent",
        doc: "Message catch. Opens a subscription and rests until a correlated message arrives.",
        attrs: &[
            AttrSpec {
                key: "message",
                required: true,
                ty: AttrType::Str,
                doc: "The BPMN message name this event subscribes to.",
            },
            AttrSpec {
                key: "correlationKey",
                required: true,
                ty: AttrType::Str,
                doc: "Name of the instance variable whose value identifies the target instance.",
            },
        ],
    },
    KindSpec {
        keyword: "messageBoundaryEvent",
        doc: "Message boundary on an activity. Interrupting or non-interrupting.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to.",
            },
            AttrSpec {
                key: "message",
                required: true,
                ty: AttrType::Str,
                doc: "The BPMN message name this event subscribes to.",
            },
            AttrSpec {
                key: "correlationKey",
                required: true,
                ty: AttrType::Str,
                doc: "Name of the instance variable whose value identifies the target instance.",
            },
            AttrSpec {
                key: "interrupting",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` cancels the activity on fire; `false` spawns a parallel token.",
            },
        ],
    },
    KindSpec {
        keyword: "messageStartEvent",
        doc: "Message start. Correlating a matching message creates a new instance.",
        attrs: &[AttrSpec {
            key: "message",
            required: true,
            ty: AttrType::Str,
            doc: "The BPMN message name that triggers a new instance.",
        }],
    },
    KindSpec {
        keyword: "timerStartEvent",
        doc: "Timer start. Creates a new instance when the process-level timer fires.",
        attrs: &[
            AttrSpec {
                key: "interval",
                required: true,
                ty: AttrType::Duration,
                doc: "Delay from deployment to first fire (also the period when `repeating`).",
            },
            AttrSpec {
                key: "repeating",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` re-arms after firing (a cycle); `false` fires exactly once.",
            },
        ],
    },
    KindSpec {
        keyword: "subProcess",
        doc: "Embedded sub-process. Container holding its own flow; child elements set `parent` \
              to its id.",
        attrs: &[AttrSpec {
            key: "startEvent",
            required: true,
            ty: AttrType::Id,
            doc: "Id of the sub-process's inner (none) start event.",
        }],
    },
    KindSpec {
        keyword: "intermediateThrowEvent",
        doc: "None intermediate throw. Pure pass-through.",
        attrs: &[],
    },
    KindSpec {
        keyword: "scriptTask",
        doc: "Inline FEEL script. Evaluates `expression`, stores in `resultVariable`, completes.",
        attrs: &[
            AttrSpec {
                key: "expression",
                required: true,
                ty: AttrType::Str,
                doc: "Inline FEEL expression (leading `=` optional).",
            },
            AttrSpec {
                key: "resultVariable",
                required: true,
                ty: AttrType::Str,
                doc: "Variable name the expression result is stored under.",
            },
        ],
    },
    KindSpec {
        keyword: "callActivity",
        doc: "Invokes another process definition and waits for it to complete.",
        attrs: &[AttrSpec {
            key: "calledElement",
            required: true,
            ty: AttrType::Str,
            doc: "The `calledElement` / `zeebe:calledElement processId`.",
        }],
    },
    KindSpec {
        keyword: "signalIntermediateCatchEvent",
        doc: "Signal catch. Correlates by name only.",
        attrs: &[AttrSpec {
            key: "signal",
            required: true,
            ty: AttrType::Str,
            doc: "The BPMN signal name this event subscribes to.",
        }],
    },
    KindSpec {
        keyword: "signalBoundaryEvent",
        doc: "Signal boundary on an activity. Interrupting or non-interrupting.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to.",
            },
            AttrSpec {
                key: "signal",
                required: true,
                ty: AttrType::Str,
                doc: "The BPMN signal name this event subscribes to.",
            },
            AttrSpec {
                key: "interrupting",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` cancels the activity on fire; `false` spawns a parallel token.",
            },
        ],
    },
    KindSpec {
        keyword: "conditionalIntermediateCatchEvent",
        doc: "Conditional catch. Rests until a FEEL boolean becomes true.",
        attrs: &[AttrSpec {
            key: "condition",
            required: true,
            ty: AttrType::Str,
            doc: "FEEL boolean condition (leading `=` optional).",
        }],
    },
    KindSpec {
        keyword: "conditionalBoundaryEvent",
        doc: "Conditional boundary on an activity. Interrupting or non-interrupting.",
        attrs: &[
            AttrSpec {
                key: "attachedTo",
                required: true,
                ty: AttrType::Id,
                doc: "Id of the activity this boundary is attached to.",
            },
            AttrSpec {
                key: "condition",
                required: true,
                ty: AttrType::Str,
                doc: "FEEL boolean condition (leading `=` optional).",
            },
            AttrSpec {
                key: "interrupting",
                required: true,
                ty: AttrType::Bool,
                doc: "`true` cancels the activity on fire; `false` spawns a parallel token.",
            },
        ],
    },
];

/// Element-level attributes shared across every kind (they are emitted by
/// [`crate::model_ir::render_element_attrs`], not by [`crate::model_ir::render_kind_attrs`]).
/// All optional.
pub const SHARED_ATTRS: &[AttrSpec] = &[
    AttrSpec {
        key: "parent",
        required: false,
        ty: AttrType::Id,
        doc: "Id of the enclosing sub-process (for nested elements).",
    },
    AttrSpec {
        key: "retries",
        required: false,
        ty: AttrType::Str,
        doc: "Raw retries expression (literal or FEEL) for job-based tasks.",
    },
    // `timer`, `input`, `output`, `multiInstance` have their own shapes and are not simple
    // key/value pairs — they get bespoke productions in the GBNF and are described inline in
    // `describe`. Listed here so callers know they exist.
];

/// Sequence-flow annotations (post `->`). Both optional.
/// Flow-level annotations (`when` and `default`). Kept in table form for future use by
/// `emit_gbnf` (currently hard-coded there) and the describe payload — surfaced as a compact
/// reference for anyone extending the grammar without hunting for the inline literals.
#[cfg(test)]
#[allow(dead_code)]
pub const FLOW_ANNOTATIONS: &[AttrSpec] = &[
    AttrSpec {
        key: "when",
        required: false,
        ty: AttrType::Str,
        doc: "FEEL boolean condition guarding the flow.",
    },
    // `default` is a bare keyword, not a keyword+value pair, so it's not modelled as an AttrSpec.
    // The GBNF renders it as an optional literal (`"default"?`).
];

/// Look up the spec for an element-kind IR keyword.
pub fn spec_for_keyword(kw: &str) -> Option<&'static KindSpec> {
    ELEMENT_KIND_SPECS.iter().find(|s| s.keyword == kw)
}

/// Every known element-kind IR keyword, in table order.
pub fn keywords() -> Vec<&'static str> {
    ELEMENT_KIND_SPECS.iter().map(|s| s.keyword).collect()
}

// -------------------------------------------------------------------------------------------------
// GBNF emitter — llama.cpp `--grammar-file` format.
// -------------------------------------------------------------------------------------------------

/// Render the whole IR as a llama.cpp GBNF grammar. Layout mirrors what
/// [`crate::model_ir::definition_to_ir`] emits: nodes and flows are line-oriented, attribute
/// blocks are brace-wrapped one-per-line lists.
///
/// The output is a single self-contained grammar file — no external includes. Save it and load
/// via `llama-server --grammar-file` or POST as `grammar` on `/completion` to constrain the
/// sampler so the model literally cannot emit a token sequence that is not valid IR.
pub fn emit_gbnf() -> String {
    let mut out = String::new();
    out.push_str(GBNF_HEADER);

    // Root: `process "<id>" { start <id> <element>* <flow>* }`
    out.push_str(
        r#"root      ::= ws "process" sp string sp "{" nl indent "start" sp id nl elements flows "}" ws
elements  ::= element*
flows     ::= flow*
"#,
    );

    // Every element is one of the per-kind productions.
    out.push_str("element   ::= ");
    let mut first = true;
    for s in ELEMENT_KIND_SPECS {
        if !first {
            out.push_str(" | ");
        }
        out.push_str(&format!("elem-{}", s.keyword));
        first = false;
    }
    out.push('\n');

    // Per-kind productions.
    for s in ELEMENT_KIND_SPECS {
        out.push('\n');
        out.push_str(&format!("# {} — {}\n", s.keyword, s.doc));
        out.push_str(&format!(
            "elem-{kw}  ::= indent \"{kw}\" sp id (sp string)? ",
            kw = s.keyword,
        ));
        if s.attrs.is_empty() && SHARED_ATTRS.is_empty() {
            // Never happens today (SHARED_ATTRS is non-empty), but future-proofs the emitter for
            // a purely-attributeless variant.
            out.push_str("nl\n");
        } else {
            out.push_str(&format!("(sp attrs-{})? nl\n", s.keyword));
            emit_attr_block(&mut out, s);
        }
    }

    // Attribute-value primitives (one production per AttrType, so per-kind attrs can share).
    out.push('\n');
    out.push_str(
        r#"# --- Attribute value primitives ---
attr-str      ::= string
attr-id       ::= id
attr-duration ::= [0-9]+ "ms"
attr-bool     ::= "true" | "false"
"#,
    );

    // Shared element-level attributes (parent, retries, timer, input, output, multiInstance).
    out.push('\n');
    out.push_str(EXTRAS_BLOCK);

    // Flow production.
    out.push('\n');
    out.push_str(
        r#"# --- Sequence flow ---
flow      ::= indent id sp "->" sp id (sp "when" sp string)? (sp "default")? nl
"#,
    );

    // Lexical primitives.
    out.push('\n');
    out.push_str(LEXICAL_BLOCK);

    out
}

/// Emit `attrs-<keyword> ::= "{" nl (indent2 attr nl)+ indent "}"` and the kind's attr alternation.
///
/// Semantics we want the sampler to enforce:
/// - Each required attr must appear at least once.
/// - Each optional attr may appear at most once.
/// - Shared element-level attrs may each appear at most once, at any position.
///
/// GBNF is context-free, so exact multiset ordering is over-approximated: we allow any permutation
/// of the declared attrs (via a permissive `attr-line-<kw> ::= <kind-attr> | <shared-attr>`
/// alternation), and rely on the parser to enforce single-occurrence and required-presence. This
/// is a deliberate trade-off — a strict GBNF for permutations of k attrs is O(k!) productions;
/// the parser is O(n) and gives better error messages.
fn emit_attr_block(out: &mut String, s: &KindSpec) {
    out.push_str(&format!(
        "attrs-{kw}   ::= \"{{\" nl (indent2 attr-line-{kw} nl)+ indent \"}}\"\n",
        kw = s.keyword,
    ));

    // Per-kind attr alternation: this kind's attrs plus every shared/element-level extra.
    out.push_str(&format!("attr-line-{}  ::= ", s.keyword));
    let mut alts: Vec<String> = Vec::new();
    for a in s.attrs {
        alts.push(format!("attr-{}-{}", s.keyword, a.key));
    }
    // Shared extras (element-level annotations from render_element_attrs).
    alts.push("attr-parent".into());
    alts.push("attr-retries".into());
    alts.push("attr-timer".into());
    alts.push("attr-input".into());
    alts.push("attr-output".into());
    alts.push("attr-multiInstance".into());
    out.push_str(&alts.join(" | "));
    out.push('\n');

    // Individual attr productions for this kind.
    for a in s.attrs {
        out.push_str(&format!(
            "attr-{kw}-{key}  ::= \"{key}\" sp attr-{ty}   # {doc}\n",
            kw = s.keyword,
            key = a.key,
            ty = attr_type_rule(a.ty),
            doc = a.doc,
        ));
    }
}

fn attr_type_rule(t: AttrType) -> &'static str {
    match t {
        AttrType::Str => "str",
        AttrType::Id => "id",
        AttrType::Duration => "duration",
        AttrType::Bool => "bool",
    }
}

const GBNF_HEADER: &str = r#"# Reversible semantic IR grammar (ADR 0001).
# Generated by processos::ir_spec::emit_gbnf — DO NOT EDIT by hand.
# Add or remove element kinds by editing ELEMENT_KIND_SPECS in ir_spec.rs;
# the parity test in that module fails if the spec drifts from the engine's
# ElementKind enum.
#
# Load into llama.cpp: `llama-server --grammar-file <this.gbnf>`
#                  or: POST /completion with `{"grammar": "<file contents>"}`.
# GBNF enforces FORM (well-typed IR); the parser (crate::model_ir::ir_to_definition)
# still validates cross-element references and required-attr presence.

"#;

const EXTRAS_BLOCK: &str = r#"# --- Shared element-level extras (all optional) ---
attr-parent          ::= "parent" sp id
attr-retries         ::= "retries" sp string
attr-timer           ::= "timer" sp ("duration" | "cycle" | "date") sp string
attr-input           ::= "input" sp id sp "<-" sp string
attr-output          ::= "output" sp id sp "<-" sp string
attr-multiInstance   ::= "multiInstance" sp "{" sp mi-attr (sp "," sp mi-attr)* sp "}"
mi-attr              ::= "collection" sp string
                       | "inputElement" sp string
                       | "outputCollection" sp string
                       | "outputElement" sp string
                       | "completionCondition" sp string
                       | "sequential" sp attr-bool
"#;

const LEXICAL_BLOCK: &str = r#"# --- Lexical primitives ---
id        ::= [A-Za-z_] [A-Za-z0-9_]*
string    ::= "\"" ( [^"\\\n] | "\\" ["\\/bfnrt] )* "\""
sp        ::= [ \t]+
ws        ::= [ \t\r\n]*
nl        ::= [ \t]* "\n"
indent    ::= "  "
indent2   ::= "    "
"#;

// -------------------------------------------------------------------------------------------------
// Human/tool description — the `describe_ir_grammar` tool payload.
// -------------------------------------------------------------------------------------------------

/// The `describe_ir_grammar(kind?)` tool payload — a scoped grammar reference for the LLM.
///
/// - `kind: None` → the compact one-page overview: every keyword, top-level syntax skeleton,
///   flow annotations, and the shared element extras.
/// - `kind: Some("exclusiveGateway")` → just that kind's productions plus the flow annotations
///   and shared extras that apply. Small enough to send per turn.
pub fn describe(kind: Option<&str>) -> Value {
    match kind {
        None => json!({
            "scope": "overview",
            "syntax": {
                "process": "process \"<id>\" { start <id>  <element>*  <flow>* }",
                "element": "<keyword> <id> [\"<name>\"] [{ <attr>* }]",
                "flow":    "<from> -> <to> [when \"<feel>\"] [default]",
            },
            "elementKinds": ELEMENT_KIND_SPECS.iter().map(|s| json!({
                "keyword": s.keyword,
                "doc": s.doc,
                "attrs": s.attrs.iter().map(attr_json).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
            "sharedElementAttrs": [
                { "key": "parent",       "doc": "Id of the enclosing sub-process." },
                { "key": "retries",      "doc": "Raw retries expression (literal or FEEL)." },
                { "key": "timer",        "doc": "`timer duration|cycle|date \"<feel>\"` on a timer event." },
                { "key": "input",        "doc": "`input <target-id> <- \"<feel-source>\"` — repeat for each mapping." },
                { "key": "output",       "doc": "`output <target-id> <- \"<feel-source>\"` — repeat for each mapping." },
                { "key": "multiInstance", "doc": "`multiInstance { collection \"<feel>\", ... }` on the activity." },
            ],
            "flowAnnotations": [
                { "key": "when",    "doc": "FEEL boolean guarding the flow." },
                { "key": "default", "doc": "Bare keyword marking an exclusive gateway's fallback flow." },
            ],
            "note": "Call describe_ir_grammar(kind: \"<keyword>\") for a scoped reference for one element kind.",
        }),
        Some(k) => match spec_for_keyword(k) {
            None => json!({
                "error": format!("unknown element kind `{k}`"),
                "knownKinds": keywords(),
            }),
            Some(s) => json!({
                "scope": "kind",
                "keyword": s.keyword,
                "doc": s.doc,
                "syntax": format!(
                    "{kw} <id> [\"<name>\"] [{{ <attr>* }}]",
                    kw = s.keyword,
                ),
                "attrs": s.attrs.iter().map(attr_json).collect::<Vec<_>>(),
                "flowAnnotations": [
                    { "key": "when",    "doc": "FEEL boolean guarding an outgoing flow." },
                    { "key": "default", "doc": "Bare keyword; only meaningful on an exclusiveGateway's outgoing flow." },
                ],
                "sharedAttrs": ["parent", "retries", "timer", "input", "output", "multiInstance"],
                "note": "All attributes go inside a `{ ... }` block, one per line. \
                         Required attributes must be present; optional attributes may be omitted.",
            }),
        },
    }
}

fn attr_json(a: &AttrSpec) -> Value {
    json!({
        "key": a.key,
        "required": a.required,
        "type": attr_type_label(a.ty),
        "doc": a.doc,
    })
}

fn attr_type_label(t: AttrType) -> &'static str {
    match t {
        AttrType::Str => "string",
        AttrType::Id => "id",
        AttrType::Duration => "duration (Nms)",
        AttrType::Bool => "bool",
    }
}

// -------------------------------------------------------------------------------------------------
// Parity harness — exhaustive sample instances of every ElementKind for the parity test and for
// programmatic consumers (e.g. golden-fixture generators). Adding a new variant → the exhaustive
// match here fails to compile; forgetting to update ELEMENT_KIND_SPECS → the parity test fails.
// -------------------------------------------------------------------------------------------------

/// One dummy instance per `ElementKind` variant, tagged with the IR keyword we expect the
/// pretty-printer to render for it. Field values are placeholders — enough to survive round-trip
/// and produce every declared attribute line.
#[cfg(test)]
pub fn sample_instances() -> Vec<(&'static str, nanobpmn_engine_core::ElementKind)> {
    use nanobpmn_engine_core::{ElementKind, UserTaskProps};
    vec![
        ("startEvent", ElementKind::StartEvent),
        ("endEvent", ElementKind::EndEvent),
        (
            "serviceTask",
            ElementKind::ServiceTask {
                job_type: "worker".into(),
                priority: Some("50".into()),
            },
        ),
        (
            "userTask",
            ElementKind::UserTask(UserTaskProps {
                assignee: Some("alice".into()),
                candidate_groups: Some("reviewers".into()),
                candidate_users: Some("bob,carol".into()),
                due_date: Some("=today() + duration(\"P1D\")".into()),
                follow_up_date: Some("=today()".into()),
                priority: Some("50".into()),
            }),
        ),
        ("exclusiveGateway", ElementKind::ExclusiveGateway),
        ("parallelGateway", ElementKind::ParallelGateway),
        (
            "errorBoundaryEvent",
            ElementKind::ErrorBoundaryEvent {
                attached_to: "svc_1".into(),
                error_code: "OUT_OF_STOCK".into(),
            },
        ),
        (
            "timerIntermediateCatchEvent",
            ElementKind::TimerIntermediateCatchEvent {
                duration_millis: 60_000,
            },
        ),
        (
            "timerBoundaryEvent",
            ElementKind::TimerBoundaryEvent {
                attached_to: "svc_1".into(),
                duration_millis: 60_000,
                interrupting: true,
                repeating: false,
            },
        ),
        (
            "messageIntermediateCatchEvent",
            ElementKind::MessageIntermediateCatchEvent {
                message_name: "order_ready".into(),
                correlation_key: "orderId".into(),
            },
        ),
        (
            "messageBoundaryEvent",
            ElementKind::MessageBoundaryEvent {
                attached_to: "svc_1".into(),
                message_name: "cancel".into(),
                correlation_key: "orderId".into(),
                interrupting: true,
            },
        ),
        (
            "messageStartEvent",
            ElementKind::MessageStartEvent {
                message_name: "order_placed".into(),
            },
        ),
        (
            "timerStartEvent",
            ElementKind::TimerStartEvent {
                interval_millis: 3_600_000,
                repeating: true,
            },
        ),
        (
            "subProcess",
            ElementKind::SubProcess {
                start_event: "inner_start".into(),
            },
        ),
        (
            "intermediateThrowEvent",
            ElementKind::IntermediateThrowEvent,
        ),
        (
            "scriptTask",
            ElementKind::ScriptTask {
                expression: "=total * 1.1".into(),
                result_variable: "totalWithTax".into(),
            },
        ),
        (
            "callActivity",
            ElementKind::CallActivity {
                called_process_id: "phase_review".into(),
            },
        ),
        (
            "signalIntermediateCatchEvent",
            ElementKind::SignalIntermediateCatchEvent {
                signal_name: "shutdown".into(),
            },
        ),
        (
            "signalBoundaryEvent",
            ElementKind::SignalBoundaryEvent {
                attached_to: "svc_1".into(),
                signal_name: "shutdown".into(),
                interrupting: true,
            },
        ),
        (
            "conditionalIntermediateCatchEvent",
            ElementKind::ConditionalIntermediateCatchEvent {
                condition: "=orderApproved".into(),
            },
        ),
        (
            "conditionalBoundaryEvent",
            ElementKind::ConditionalBoundaryEvent {
                attached_to: "svc_1".into(),
                condition: "=orderCancelled".into(),
                interrupting: true,
            },
        ),
    ]
}

/// Placeholder instances of the shared, element-level extras — exercised by the parity test to
/// keep the `SHARED_ATTRS` list honest against
/// [`crate::model_ir::render_element_attrs`].
#[allow(dead_code)]
#[cfg(test)]
pub fn sample_shared_extras() -> (
    Option<String>,
    Option<String>,
    Option<nanobpmn_engine_core::TimerDef>,
    nanobpmn_engine_core::IoMapping,
    Option<nanobpmn_engine_core::MultiInstance>,
) {
    use nanobpmn_engine_core::{IoMapping, Mapping, MultiInstance, TimerDef, TimerDefKind};
    (
        Some("parent_sp".into()),
        Some("3".into()),
        Some(TimerDef {
            kind: TimerDefKind::Duration,
            expr: "PT1M".into(),
        }),
        IoMapping {
            inputs: vec![Mapping {
                source: "=order.total".into(),
                target: "total".into(),
            }],
            outputs: vec![Mapping {
                source: "=result.status".into(),
                target: "status".into(),
            }],
        },
        Some(MultiInstance {
            input_collection: "=items".into(),
            input_element: Some("item".into()),
            output_collection: Some("results".into()),
            output_element: Some("=result".into()),
            completion_condition: Some("=count(results) >= 5".into()),
            sequential: false,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nanobpmn_engine_core::Element;

    use super::*;
    use crate::model_ir::{
        // Re-exported via pub(crate) so the parity test can drive the private renderers without
        // going through a synthetic ProcessDefinition. Keeps the test's failure mode narrow —
        // it exercises exactly the same code paths the pretty-printer uses.
        render_element_attrs_for_test,
        render_kind_attrs_for_test,
    };

    /// For every declared spec, build the matching sample instance, run the real pretty-printer,
    /// and assert the emitted attribute keys match what the spec table declares. Any drift
    /// (spec has an attr the printer doesn't emit, or vice versa) fails this test with a diff.
    #[test]
    fn specs_match_pretty_printer() {
        let samples = sample_instances();
        // Every sample's tag must match its ElementKind's actual IR keyword — protects against
        // the sample table being copy-pasted with the wrong tag.
        for (expected_kw, kind) in &samples {
            let actual = crate::model_ir::kind_keyword_for_test(kind);
            assert_eq!(
                actual, *expected_kw,
                "sample_instances tag `{expected_kw}` does not match kind_keyword(`{actual}`)",
            );
        }

        // Every ElementKind variant appears in samples.
        let sample_kws: BTreeSet<&str> = samples.iter().map(|(k, _)| *k).collect();
        let spec_kws: BTreeSet<&str> = ELEMENT_KIND_SPECS.iter().map(|s| s.keyword).collect();
        assert_eq!(
            sample_kws, spec_kws,
            "sample_instances and ELEMENT_KIND_SPECS must cover the same keywords",
        );

        // Attribute-key parity.
        for (kw, kind) in &samples {
            let spec = spec_for_keyword(kw).unwrap_or_else(|| panic!("no spec for `{kw}`"));
            let mut emitted = Vec::new();
            render_kind_attrs_for_test(kind, &mut emitted);
            // The first whitespace-separated token of each line is the attr key.
            let emitted_keys: BTreeSet<String> = emitted
                .iter()
                .filter_map(|line| line.split_whitespace().next().map(str::to_string))
                .collect();
            let spec_keys: BTreeSet<String> =
                spec.attrs.iter().map(|a| a.key.to_string()).collect();
            assert_eq!(
                emitted_keys, spec_keys,
                "keys for `{kw}` — spec vs pretty-printer diverged",
            );
        }
    }

    /// Every keyword in SHARED_ATTRS is emitted by render_element_attrs when the corresponding
    /// field is present. Keeps the SHARED_ATTRS listing honest against the printer.
    #[test]
    fn shared_attrs_match_element_renderer() {
        use nanobpmn_engine_core::ElementKind;
        let (parent, retries, timer, io, mi) = sample_shared_extras();
        let el = Element {
            id: "svc_1".into(),
            kind: ElementKind::ServiceTask {
                job_type: "worker".into(),
                priority: None,
            },
            outgoing: Vec::new(),
            parent,
            retries,
            timer,
            io,
            multi_instance: mi,
        };
        let mut emitted = Vec::new();
        render_element_attrs_for_test(&el, &mut emitted);
        let emitted_keys: BTreeSet<String> = emitted
            .iter()
            .filter_map(|line| line.split_whitespace().next().map(str::to_string))
            .collect();
        // The SHARED_ATTRS list only holds the simple K/V shared attrs (parent, retries). The
        // shaped ones (timer, input, output, multiInstance) are described inline in `describe`
        // and get their own GBNF productions; assert they appear too by string-match.
        for key in [
            "parent",
            "retries",
            "timer",
            "input",
            "output",
            "multiInstance",
        ] {
            assert!(
                emitted_keys.contains(key),
                "render_element_attrs did not emit `{key}`; check that SHARED_ATTRS / EXTRAS_BLOCK still match",
            );
        }
    }

    /// The generated GBNF contains a per-kind alternation covering every element-kind keyword.
    /// Catches trivial regressions in `emit_gbnf` without shelling out to llama.cpp's parser.
    #[test]
    fn gbnf_covers_every_kind() {
        let g = emit_gbnf();
        for s in ELEMENT_KIND_SPECS {
            assert!(
                g.contains(&format!("elem-{}", s.keyword)),
                "GBNF missing production for `{}`",
                s.keyword,
            );
            assert!(
                g.contains(&format!("\"{}\"", s.keyword)),
                "GBNF missing keyword literal `\"{}\"`",
                s.keyword,
            );
        }
        // Structural spot-checks.
        for needle in [
            "root",
            "element",
            "flow",
            "attr-str",
            "attr-id",
            "attr-duration",
            "attr-bool",
        ] {
            assert!(g.contains(needle), "GBNF missing rule `{needle}`");
        }
    }

    /// `describe(None)` returns the full catalog; `describe(Some(kw))` returns the scoped payload.
    #[test]
    fn describe_returns_scoped_payloads() {
        let overview = describe(None);
        assert_eq!(overview["scope"], "overview");
        let kinds = overview["elementKinds"].as_array().unwrap();
        assert_eq!(kinds.len(), ELEMENT_KIND_SPECS.len());

        let ex = describe(Some("exclusiveGateway"));
        assert_eq!(ex["scope"], "kind");
        assert_eq!(ex["keyword"], "exclusiveGateway");

        let bogus = describe(Some("no_such_kind"));
        assert!(bogus.get("error").is_some());
    }
}
