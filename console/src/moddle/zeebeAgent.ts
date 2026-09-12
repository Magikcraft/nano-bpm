// Zeebe moddle descriptor, augmented with the external-agent marker (#1180) ----
//
// `zeebe-bpmn-moddle` (v1.x) ships NO descriptor for `zeebe:agentDefinition` —
// the fleet's canonical agentic-task marker (`<zeebe:agentDefinition
// agentType="external"/>`, see `engine-core/src/bpmn.rs` and the harness `--auto`
// scan jwulf/c8ctl-plugin-nano#235). bpmn-js DROPS any extension element it has
// no moddle type for on save, so without this the modeler would silently strip
// the marker on every round-trip. This module registers the stock zeebe
// descriptor PLUS the one missing `AgentDefinition` type, and is what the modeler
// hands to `moddleExtensions.zeebe` in place of the raw import.
//
// The stock descriptor already carries `zeebe:Properties`/`zeebe:Property` (the
// `--auto` opt-out property lives there), so only the marker needs adding. Its
// `xml.tagAlias: "lowerCase"` lowercases the first letter, so `AgentDefinition`
// serialises as `zeebe:agentDefinition` and `agentType` stays as authored.
import ZeebeModdle from "zeebe-bpmn-moddle/resources/zeebe.json" with { type: "json" };

/** The moddle type descriptor for `<zeebe:agentDefinition agentType="…"/>`. */
export const AGENT_DEFINITION_MODDLE_TYPE = {
  name: "AgentDefinition",
  superClass: ["Element"],
  meta: { allowedIn: ["bpmn:ServiceTask"] },
  properties: [{ name: "agentType", type: "String", isAttr: true }],
};

interface ModdleDescriptor {
  types: { name?: unknown }[];
  [key: string]: unknown;
}

/** Return the stock zeebe descriptor extended with the external-agent marker
 *  type — a FRESH object (never a mutation of the shared import) so nothing else
 *  sees the augmented `types`.
 *
 *  The append is CONDITIONAL: `console/package.json` allows `zeebe-bpmn-moddle
 *  ^1.15.0`, and upstream 1.18.0 ships its own `AgentDefinition` descriptor. Once
 *  the lock refreshes to such a release, appending unconditionally would register
 *  a SECOND `AgentDefinition` type in the same package — moddle rejects a
 *  duplicate type name (or silently lets ours shadow upstream's). So we only add
 *  the shim when the stock `types` does not already carry an `AgentDefinition`,
 *  deferring to upstream's descriptor whenever it exists. */
export function buildZeebeModdleWithAgent(
  stock: ModdleDescriptor,
): ModdleDescriptor {
  const stockHasAgentDefinition = stock.types.some(
    (t) => t?.name === AGENT_DEFINITION_MODDLE_TYPE.name,
  );
  return {
    ...stock,
    types: stockHasAgentDefinition
      ? [...stock.types]
      : [...stock.types, AGENT_DEFINITION_MODDLE_TYPE],
  };
}

/** The stock zeebe descriptor augmented with the external-agent marker, handed
 *  to `moddleExtensions.zeebe` in place of the raw import. */
export const zeebeModdleWithAgent: ModdleDescriptor = buildZeebeModdleWithAgent(
  ZeebeModdle as ModdleDescriptor,
);
