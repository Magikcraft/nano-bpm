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
  resourceId?: string;
  resourceType?: string;
  linkName?: string;
  bindingType?: string;
  source?: string;
  target?: string;
  values?: AgentModdleElement[];
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

/** Whether `element` is an agent service task — a service task carrying the
 *  `linkName="prompt"` side-car (the `@nanobpm/agentic` `agentic` signal). */
export function isAgentTask(
  element: { type?: string; businessObject?: AgentModdleElement } | undefined,
): boolean {
  if (element?.type !== AGENT_TASK_ELEMENT_TYPE) return false;
  return promptLinkedResource(element.businessObject) !== undefined;
}

// --- Writing -----------------------------------------------------------------
// Each write is a single undoable modeling command per touched container, so the
// binding travels in the `.bpmn` exactly as the toolchain emits it. Containers
// (`bpmn:extensionElements`, `zeebe:LinkedResources`, `zeebe:IoMapping`) are
// created on demand and torn down when they empty out, so a round-trip through
// the modeler leaves no orphan wrappers.

function attachExtChild(
  moddle: AgentModdle,
  modeling: AgentModeling,
  element: unknown,
  bo: AgentModdleElement,
  child: AgentModdleElement,
): void {
  const ext = bo.extensionElements;
  if (ext) {
    child.$parent = ext;
    modeling.updateModdleProperties(element, ext, {
      values: [...(ext.values ?? []), child],
    });
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
  modeling.updateModdleProperties(element, ext, {
    values: (ext.values ?? []).filter((v) => v !== child),
  });
}

/** Add or update the prompt `linkedResource` (`resourceType="GenericScript"`,
 *  `linkName="prompt"`) with `resourceId` + `bindingType`. Any non-prompt linked
 *  resources are preserved. */
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
  const link = moddle.create("zeebe:LinkedResource", {
    resourceId,
    bindingType: bindingType || PROMPT_DEFAULT_BINDING_TYPE,
    resourceType: PROMPT_RESOURCE_TYPE,
    linkName: PROMPT_LINK_NAME,
  });
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
