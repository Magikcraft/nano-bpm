// The connector palette gate (ADR 0050, amending ADR 0033 §2): pure helpers,
// deliberately free of the generated API client so they can be unit-tested with
// the node-native runner (`node --experimental-strip-types --test`).

import type { ElementTemplate } from "./urbanComponents";

/**
 * The `zeebe:taskDefinition:type` an element template binds, if any — the value
 * a service task created from the component enqueues as its job `type` (the
 * design→runtime seam a connector worker subscribes to, ADR 0050 §2). Mirrors
 * the backend `connectors::component_task_type`. Returns `undefined` for a
 * design-only component (no task-definition binding).
 */
export function componentTaskType(tpl: ElementTemplate): string | undefined {
  const props = (tpl as { properties?: unknown }).properties;
  if (!Array.isArray(props)) return undefined;
  for (const p of props) {
    if (typeof p !== "object" || p === null) continue;
    const binding = (p as { binding?: { type?: unknown } }).binding;
    if (binding?.type !== "zeebe:taskDefinition:type") continue;
    const value = (p as { value?: unknown }).value;
    if (typeof value === "string" && value !== "") return value;
  }
  return undefined;
}

/**
 * Gates pack-contributed connector components on project enablement (ADR 0050,
 * amending ADR 0033 §2): a pack component that binds a `zeebe:taskDefinition:
 * type` only belongs in the palette once that connector is enabled in the
 * project (its worker is in `nano.app.json`'s `workers[]`). Design-only pack
 * components (no task-definition binding) always show — they carry no runtime
 * seam to gate. Project-local components are never filtered here.
 */
export function filterEnabledPackComponents(
  pack: ElementTemplate[],
  enabledTaskTypes: Iterable<string>,
): ElementTemplate[] {
  const enabled = new Set(enabledTaskTypes);
  return pack.filter((tpl) => {
    const taskType = componentTaskType(tpl);
    return taskType === undefined || enabled.has(taskType);
  });
}
