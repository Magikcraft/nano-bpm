// Agent-task prompt binding (issue #950) ------------------------------------
// An *agent task* is a `bpmn:ServiceTask` whose worker is an LLM agent. The
// canonical, structural signal is a `zeebe:linkedResource` carrying
// `linkName="prompt"` (`resourceType="GenericScript"`) — the base-prompt
// side-car every external agent task delivers its prompt through. This is the
// single agentic marker the toolchain emits and consumers detect:
//
//   - Emitter  — `@nanobpm/workflow` `nodes/task.ts` (`taskExtensions`), which
//     `task(name, { prompt })` renders as:
//         <zeebe:linkedResources>
//           <zeebe:linkedResource resourceId="…" bindingType="latest"
//                                 resourceType="GenericScript" linkName="prompt" />
//         </zeebe:linkedResources>
//         <zeebe:ioMapping>            (only when `prompt.append` is present)
//           <zeebe:input source="…" target="appendPrompt" />
//         </zeebe:ioMapping>
//   - Detector — `@nanobpm/agentic` `demand/taskdef.ts` (`agentic` flag): true
//     iff a service task carries a `linkName="prompt"` linked resource.
//
// This module is the console's SINGLE source of truth for that shape — render,
// inspect and edit all derive the emitted `extensionElements` from the constants
// and helpers here, so the modeler can never drift from the toolchain (AGENTS.md
// "Derivation Over Duplication"). The helpers are pure (moddle/modeling are
// injected), so they live here — apart from the browser-only bpmn-js modeler
// component — to stay unit-testable. See agentTask.test.ts.

// --- The emitted shape (mirrors `@nanobpm/workflow` `taskExtensions`) ---------

/** The `linkedResource` `resourceType` that marks a prompt side-car. */
export const PROMPT_RESOURCE_TYPE = "GenericScript";
/** The `linkedResource` `linkName` that marks a prompt side-car — the agentic
 *  signal `@nanobpm/agentic` detects. */
export const PROMPT_LINK_NAME = "prompt";
/** The `bindingType` the toolchain defaults to when one is not supplied. */
export const PROMPT_DEFAULT_BINDING_TYPE = "latest";
/** The `zeebe:ioMapping` input `target` the optional prompt addendum uses. */
export const APPEND_PROMPT_TARGET = "appendPrompt";
/** The BPMN element type an agent task always is (the toolchain emits a plain
 *  service task carrying the prompt link). */
export const AGENT_TASK_ELEMENT_TYPE = "bpmn:ServiceTask";

// --- The external-agent marker (issue #1180) ---------------------------------
// The fleet is converging on ONE canonical agentic-task signal: a
// `<zeebe:agentDefinition agentType="external"/>` marker inside a service task's
// `extensionElements` (the harness `--auto` scan keys on it —
// jwulf/c8ctl-plugin-nano#235; the engine parser recognises it —
// `engine-core/src/bpmn.rs`). It is a bare marker (no children), sibling to the
// prompt link. `zeebe-bpmn-moddle` has no descriptor for it, so the modeler
// registers the augmented descriptor in `moddle/zeebeAgent.ts` — this module owns
// the shape it reads/writes.
/** The moddle `$type` of the external-agent marker element. */
export const AGENT_DEFINITION_TYPE = "zeebe:AgentDefinition";
/** The `agentType` value that marks a fleet external-agent task. */
export const AGENT_TYPE_EXTERNAL = "external";

// --- The `--auto` opt-out property (issue #1180) -----------------------------
// A namespaced `zeebe:property` an author sets to exclude an agent task from the
// harness's `--auto` subscription set (specific `--job-type`/profile targeting
// still serves it — jwulf/c8ctl-plugin-nano#235). Fail-safe: ONLY the exact value
// `"false"` opts out; any other value (or absence) means auto-subscribed.
/** The opt-out property `name`. */
export const AUTO_SUBSCRIBE_PROPERTY = "io.nanobpm.agentTask.autoSubscribe";
/** The one `value` that opts a task out of `--auto` (fail-safe). */
export const AUTO_SUBSCRIBE_OPT_OUT_VALUE = "false";
/** The moddle `$type` of the `zeebe:properties` container. */
export const ZEEBE_PROPERTIES_TYPE = "zeebe:Properties";
/** The moddle `$type` of a single `zeebe:property`. */
export const ZEEBE_PROPERTY_TYPE = "zeebe:Property";

// The Camunda binding types the modeler offers for the prompt resource — how the
// engine resolves the resource version at deploy time. `bindingType` is a free
// string to the engine; these are the standard values a maker picks between.
export const PROMPT_BINDING_TYPES = [
  "latest",
  "deployment",
  "versionTag",
] as const;

// --- Minimal moddle view -----------------------------------------------------
// Structural subset of a moddle business object; the modeler's richer
// `ModdleElement` is a superset and assigns structurally.
export interface AgentModdleElement {
  $type?: string;
  type?: string;
  resourceId?: string;
  resourceType?: string;
  linkName?: string;
  bindingType?: string;
  source?: string;
  target?: string;
  /** `zeebe:agentDefinition`'s marker attribute. */
  agentType?: string;
  /** A `zeebe:property`'s `name`/`value` attributes. */
  name?: string;
  value?: string;
  values?: AgentModdleElement[];
  /** The `zeebe:Properties` container's child list (moddle property `properties`,
   *  distinct from the generic `values`). */
  properties?: AgentModdleElement[];
  inputParameters?: AgentModdleElement[];
  outputParameters?: AgentModdleElement[];
  extensionElements?: AgentModdleElement;
  $parent?: unknown;
}

export interface AgentModdle {
  create(type: string, attrs?: Record<string, unknown>): AgentModdleElement;
}

export interface AgentModeling {
  updateModdleProperties(
    element: unknown,
    moddleElement: unknown,
    props: Record<string, unknown>,
  ): void;
}

/** A prompt binding read off (or written onto) an agent service task. */
export interface PromptBinding {
  /** The bound `GenericScript` resource id (the `linkedResource` `resourceId`). */
  resourceId: string;
  /** How the engine resolves the resource version (`bindingType`), defaulting to
   *  `"latest"`. */
  bindingType: string;
  /** The optional prompt addendum — a FEEL expression (or literal) fed to the
   *  worker through the `appendPrompt` `zeebe:ioMapping` input. Absent → no
   *  addendum. */
  append?: string;
}

// --- Reading -----------------------------------------------------------------

function extValues(bo: AgentModdleElement | undefined): AgentModdleElement[] {
  return bo?.extensionElements?.values ?? [];
}

function findExt(
  bo: AgentModdleElement | undefined,
  type: string,
): AgentModdleElement | undefined {
  return extValues(bo).find((v) => v.$type === type);
}

/** The `zeebe:linkedResource` carrying the prompt side-car, if present. */
export function promptLinkedResource(
  bo: AgentModdleElement | undefined,
): AgentModdleElement | undefined {
  const container = findExt(bo, "zeebe:LinkedResources");
  return (container?.values ?? []).find((v) => v.linkName === PROMPT_LINK_NAME);
}

/** The `zeebe:ioMapping` `appendPrompt` input, if present. */
function appendPromptInput(
  bo: AgentModdleElement | undefined,
): AgentModdleElement | undefined {
  const io = findExt(bo, "zeebe:IoMapping");
  return (io?.inputParameters ?? []).find(
    (p) => p.target === APPEND_PROMPT_TARGET,
  );
}

/** The prompt binding on `bo`, or undefined when it carries no prompt link. */
export function readPromptBinding(
  bo: AgentModdleElement | undefined,
): PromptBinding | undefined {
  const link = promptLinkedResource(bo);
  if (!link) return undefined;
  const binding: PromptBinding = {
    resourceId: typeof link.resourceId === "string" ? link.resourceId : "",
    bindingType:
      typeof link.bindingType === "string" && link.bindingType
        ? link.bindingType
        : PROMPT_DEFAULT_BINDING_TYPE,
  };
  const append = appendPromptInput(bo)?.source;
  if (typeof append === "string" && append) binding.append = append;
  return binding;
}

/** Whether `bo` carries the prompt side-car (the `@nanobpm/agentic` prompt
 *  signal). Kept distinct from {@link isAgentTask} so the prompt-binding UI
 *  reflects only the prompt link, not the newer external marker. */
export function hasPromptBinding(bo: AgentModdleElement | undefined): boolean {
  return promptLinkedResource(bo) !== undefined;
}

/** The `zeebe:agentDefinition` external-agent marker element, if present. */
export function agentDefinition(
  bo: AgentModdleElement | undefined,
): AgentModdleElement | undefined {
  return findExt(bo, AGENT_DEFINITION_TYPE);
}

/** Whether `bo` carries the canonical external-agent marker
 *  (`<zeebe:agentDefinition agentType="external"/>`). */
export function hasExternalAgentMarker(
  bo: AgentModdleElement | undefined,
): boolean {
  return agentDefinition(bo)?.agentType === AGENT_TYPE_EXTERNAL;
}

/** The `zeebe:Properties` container, if present. */
function zeebeProperties(
  bo: AgentModdleElement | undefined,
): AgentModdleElement | undefined {
  return findExt(bo, ZEEBE_PROPERTIES_TYPE);
}

/** The `io.nanobpm.agentTask.autoSubscribe` property element, if present. */
function autoSubscribeProperty(
  bo: AgentModdleElement | undefined,
): AgentModdleElement | undefined {
  return (zeebeProperties(bo)?.properties ?? []).find(
    (p) => p.name === AUTO_SUBSCRIBE_PROPERTY,
  );
}

/** Whether the task opts OUT of the harness `--auto` subscription set —
 *  `io.nanobpm.agentTask.autoSubscribe="false"` (fail-safe: only the exact value
 *  `"false"` opts out; any other value, or absence, is auto-subscribed). */
export function readAutoSubscribeOptOut(
  bo: AgentModdleElement | undefined,
): boolean {
  return autoSubscribeProperty(bo)?.value === AUTO_SUBSCRIBE_OPT_OUT_VALUE;
}

/** Whether `element` is an agent service task — a service task carrying EITHER
 *  the prompt side-car (the `@nanobpm/agentic` prompt signal) OR the canonical
 *  `<zeebe:agentDefinition agentType="external"/>` marker (the fleet's
 *  single-convention agentic signal, issue #1180). */
export function isAgentTask(
  element: { type?: string; businessObject?: AgentModdleElement } | undefined,
): boolean {
  if (element?.type !== AGENT_TASK_ELEMENT_TYPE) return false;
  return (
    hasPromptBinding(element.businessObject) ||
    hasExternalAgentMarker(element.businessObject)
  );
}

// --- Writing -----------------------------------------------------------------
// Each write is a single undoable modeling command per touched container, so the
// binding travels in the `.bpmn` exactly as the toolchain emits it. Containers
// (`bpmn:extensionElements`, `zeebe:LinkedResources`, `zeebe:IoMapping`) are
// created on demand and torn down when they empty out, so a round-trip through
// the modeler leaves no orphan wrappers.

// The canonical order of the extension-element children the toolchain emits, so
// a newly attached child slots into its stable position (e.g. LinkedResources
// before IoMapping) and the serialized XML never drifts on insertion order. The
// external-agent marker leads (a bare marker), and the zeebe:Properties container
// (the `--auto` opt-out lives here) trails.
const EXT_CHILD_ORDER = [
  "zeebe:AgentDefinition",
  "zeebe:LinkedResources",
  "zeebe:IoMapping",
  "zeebe:Properties",
];

function extRank(type: string | undefined): number {
  const i = EXT_CHILD_ORDER.indexOf(type ?? "");
  return i === -1 ? EXT_CHILD_ORDER.length : i;
}

function isKnownExtChild(type: string | undefined): boolean {
  return EXT_CHILD_ORDER.includes(type ?? "");
}

function attachExtChild(
  moddle: AgentModdle,
  modeling: AgentModeling,
  element: unknown,
  bo: AgentModdleElement,
  child: AgentModdleElement,
): void {
  const ext = bo.extensionElements;
  if (ext) {
    const existing = ext.values ?? [];
    const rank = extRank(child.$type);
    // Order only relative to the child types we enumerate (LinkedResources,
    // IoMapping). Unknown children the toolchain also emits — e.g.
    // zeebe:TaskDefinition, which is canonically *before* linkedResources — are
    // skipped when picking the insertion point, so we never reorder them ahead
    // of their canonical position. LinkedResources still lands before any
    // existing IoMapping without our having to enumerate every extension type.
    const at = existing.findIndex(
      (v) => isKnownExtChild(v.$type) && extRank(v.$type) > rank,
    );
    const values =
      at === -1
        ? [...existing, child]
        : [...existing.slice(0, at), child, ...existing.slice(at)];
    child.$parent = ext;
    modeling.updateModdleProperties(element, ext, { values });
    return;
  }
  const newExt = moddle.create("bpmn:ExtensionElements", { values: [child] });
  child.$parent = newExt;
  newExt.$parent = bo;
  modeling.updateModdleProperties(element, bo, { extensionElements: newExt });
}

function removeExtChild(
  modeling: AgentModeling,
  element: unknown,
  bo: AgentModdleElement,
  child: AgentModdleElement,
): void {
  const ext = bo.extensionElements;
  if (!ext) return;
  const values = (ext.values ?? []).filter((v) => v !== child);
  // Tear down the wrapper itself once it empties out, so a round-trip through
  // the modeler leaves no orphan bpmn:extensionElements behind.
  if (values.length === 0) {
    modeling.updateModdleProperties(element, bo, {
      extensionElements: undefined,
    });
    return;
  }
  modeling.updateModdleProperties(element, ext, { values });
}

/** Add or update the prompt `linkedResource` (`resourceType="GenericScript"`,
 *  `linkName="prompt"`) with `resourceId` + `bindingType`. A blank `resourceId`
 *  is written as an *omitted* attribute (never `resourceId=""`, which the engine
 *  rejects on deploy) so an in-progress agent task stays a well-formed, editable
 *  model instead of one carrying an invalid `resourceId=""`. Note the marker-only
 *  shape (no `resourceId`) is deliberately still **not** deploy-valid — the engine
 *  also requires a non-empty `resourceId` — until the user picks a prompt resource;
 *  it is an in-modeller work-in-progress state, not a deployable document. Any
 *  non-prompt linked resources are preserved. */
export function writePromptLink(
  moddle: AgentModdle,
  modeling: AgentModeling,
  element: unknown,
  bo: AgentModdleElement,
  resourceId: string,
  bindingType: string,
): void {
  const container = findExt(bo, "zeebe:LinkedResources");
  const kept = (container?.values ?? []).filter(
    (v) => v.linkName !== PROMPT_LINK_NAME,
  );
  // Only emit `resourceId` when the user has actually chosen a resource. A blank
  // (or whitespace-only) id — e.g. the moment the toggle is switched on, or when
  // the resource field is cleared — must NOT serialize as `resourceId=""`: the
  // engine rejects a linkedResource with a missing/empty resourceId on deploy.
  // Omitting the attribute keeps the `linkName="prompt"` marker (so the task is
  // still a recognizable, in-progress agent task in the modeller). This marker-only
  // shape is deliberately **not** yet deploy-valid — the engine equally rejects a
  // missing resourceId — but it avoids the strictly-worse `resourceId=""` and keeps
  // the model editable until the user selects a prompt resource.
  const attrs: Record<string, unknown> = {
    bindingType: bindingType || PROMPT_DEFAULT_BINDING_TYPE,
    resourceType: PROMPT_RESOURCE_TYPE,
    linkName: PROMPT_LINK_NAME,
  };
  if (resourceId.trim()) attrs.resourceId = resourceId;
  const link = moddle.create("zeebe:LinkedResource", attrs);
  const values = [...kept, link];
  if (container) {
    for (const v of values) v.$parent = container;
    modeling.updateModdleProperties(element, container, { values });
    return;
  }
  const newContainer = moddle.create("zeebe:LinkedResources", { values });
  for (const v of values) v.$parent = newContainer;
  attachExtChild(moddle, modeling, element, bo, newContainer);
}

/** Remove the prompt `linkedResource`, dropping the `zeebe:LinkedResources`
 *  container when no other linked resources remain. */
export function removePromptLink(
  modeling: AgentModeling,
  element: unknown,
  bo: AgentModdleElement,
): void {
  const container = findExt(bo, "zeebe:LinkedResources");
  if (!container) return;
  const kept = (container.values ?? []).filter(
    (v) => v.linkName !== PROMPT_LINK_NAME,
  );
  if (kept.length) {
    for (const v of kept) v.$parent = container;
    modeling.updateModdleProperties(element, container, { values: kept });
    return;
  }
  removeExtChild(modeling, element, bo, container);
}

/** Set (or clear, on "") the `appendPrompt` `zeebe:ioMapping` input to `value`.
 *  The append input trails any explicit inputs (matching the toolchain), and the
 *  `zeebe:IoMapping` container is created on demand and dropped when it holds no
 *  further inputs or outputs. */
export function writeAppendPrompt(
  moddle: AgentModdle,
  modeling: AgentModeling,
  element: unknown,
  bo: AgentModdleElement,
  value: string,
): void {
  const io = findExt(bo, "zeebe:IoMapping");
  const keptInputs = (io?.inputParameters ?? []).filter(
    (p) => p.target !== APPEND_PROMPT_TARGET,
  );
  const inputs = value
    ? [
        ...keptInputs,
        moddle.create("zeebe:Input", {
          source: value,
          target: APPEND_PROMPT_TARGET,
        }),
      ]
    : keptInputs;
  if (io) {
    const outputs = io.outputParameters ?? [];
    if (inputs.length === 0 && outputs.length === 0) {
      removeExtChild(modeling, element, bo, io);
      return;
    }
    for (const p of inputs) p.$parent = io;
    modeling.updateModdleProperties(element, io, { inputParameters: inputs });
    return;
  }
  // No ioMapping yet: clearing is a no-op; setting creates the container.
  if (!value) return;
  const newIo = moddle.create("zeebe:IoMapping", { inputParameters: inputs });
  for (const p of inputs) p.$parent = newIo;
  attachExtChild(moddle, modeling, element, bo, newIo);
}

/** Remove the whole prompt binding from an agent task — the prompt
 *  `linkedResource` and its `appendPrompt` addendum — turning it back into a
 *  plain service task. */
export function removePromptBinding(
  moddle: AgentModdle,
  modeling: AgentModeling,
  element: unknown,
  bo: AgentModdleElement,
): void {
  removePromptLink(modeling, element, bo);
  writeAppendPrompt(moddle, modeling, element, bo, "");
}

// --- The external-agent marker (issue #1180) ---------------------------------

/** Add the canonical `<zeebe:agentDefinition agentType="external"/>` marker to a
 *  service task (idempotent — a stale `agentType` is corrected in place, and a
 *  marker already set to `external` is left untouched). This is the single
 *  convention that makes a task agentic (aligns with the harness `--auto` scan). */
export function writeExternalAgentMarker(
  moddle: AgentModdle,
  modeling: AgentModeling,
  element: unknown,
  bo: AgentModdleElement,
): void {
  const existing = agentDefinition(bo);
  if (existing) {
    if (existing.agentType !== AGENT_TYPE_EXTERNAL)
      modeling.updateModdleProperties(element, existing, {
        agentType: AGENT_TYPE_EXTERNAL,
      });
    return;
  }
  const def = moddle.create(AGENT_DEFINITION_TYPE, {
    agentType: AGENT_TYPE_EXTERNAL,
  });
  attachExtChild(moddle, modeling, element, bo, def);
}

/** Remove the external-agent marker, tearing down the `bpmn:extensionElements`
 *  wrapper when it was the last child. */
export function removeExternalAgentMarker(
  modeling: AgentModeling,
  element: unknown,
  bo: AgentModdleElement,
): void {
  const def = agentDefinition(bo);
  if (def) removeExtChild(modeling, element, bo, def);
  // The `--auto` opt-out is meaningless without the marker (the modeler hides
  // its toggle once the marker is gone, see BpmnModeler's provider condition),
  // so tear any opt-out down with the marker rather than stranding an orphaned
  // `autoSubscribe="false"` in the saved BPMN with no visible control to clear
  // it. Clearing is a no-op when no opt-out is present.
  clearAutoSubscribeOptOut(modeling, element, bo);
}

// --- The `--auto` opt-out property (issue #1180) -----------------------------

/** Set (`true`) or clear (`false`) the `io.nanobpm.agentTask.autoSubscribe="false"`
 *  opt-out property. Setting creates the `zeebe:Properties` container on demand;
 *  clearing removes the property and tears the container (and empty wrapper) down
 *  when nothing else remains. Any non-opt-out `zeebe:property` siblings are
 *  preserved. */
export function writeAutoSubscribeOptOut(
  moddle: AgentModdle,
  modeling: AgentModeling,
  element: unknown,
  bo: AgentModdleElement,
  optOut: boolean,
): void {
  const container = zeebeProperties(bo);
  const kept = (container?.properties ?? []).filter(
    (p) => p.name !== AUTO_SUBSCRIBE_PROPERTY,
  );
  if (optOut) {
    const prop = moddle.create(ZEEBE_PROPERTY_TYPE, {
      name: AUTO_SUBSCRIBE_PROPERTY,
      value: AUTO_SUBSCRIBE_OPT_OUT_VALUE,
    });
    const properties = [...kept, prop];
    if (container) {
      for (const p of properties) p.$parent = container;
      modeling.updateModdleProperties(element, container, { properties });
      return;
    }
    const newContainer = moddle.create(ZEEBE_PROPERTIES_TYPE, { properties });
    for (const p of properties) p.$parent = newContainer;
    attachExtChild(moddle, modeling, element, bo, newContainer);
    return;
  }
  // Clearing: drop the opt-out property; keep the container only if other
  // properties remain, else tear it (and any orphan wrapper) down.
  clearAutoSubscribeOptOut(modeling, element, bo);
}

/** Clear the `io.nanobpm.agentTask.autoSubscribe="false"` opt-out: drop the
 *  property, keep the `zeebe:Properties` container only if other properties
 *  remain, else tear the container (and any orphan `bpmn:extensionElements`
 *  wrapper) down. A no-op when no opt-out is present. Needs no `moddle` (it only
 *  removes), so callers tearing an element down (e.g. `removeExternalAgentMarker`)
 *  can reuse it without threading a `moddle` in. */
export function clearAutoSubscribeOptOut(
  modeling: AgentModeling,
  element: unknown,
  bo: AgentModdleElement,
): void {
  const container = zeebeProperties(bo);
  if (!container) return;
  const kept = (container.properties ?? []).filter(
    (p) => p.name !== AUTO_SUBSCRIBE_PROPERTY,
  );
  if (kept.length) {
    for (const p of kept) p.$parent = container;
    modeling.updateModdleProperties(element, container, { properties: kept });
    return;
  }
  removeExtChild(modeling, element, bo, container);
}
