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
import ZeebeModdle from "zeebe-bpmn-moddle/resources/zeebe.json";

/** The moddle type descriptor for `<zeebe:agentDefinition agentType="…"/>`. */
export const AGENT_DEFINITION_MODDLE_TYPE = {
  name: "AgentDefinition",
  superClass: ["Element"],
  meta: { allowedIn: ["bpmn:ServiceTask"] },
  properties: [{ name: "agentType", type: "String", isAttr: true }],
};

interface ModdleDescriptor {
  types: unknown[];
  [key: string]: unknown;
}

/** The stock zeebe descriptor extended with the external-agent marker type. A
 *  fresh object (never a mutation of the shared import) so nothing else sees the
 *  augmented `types`. */
export const zeebeModdleWithAgent: ModdleDescriptor = {
  ...(ZeebeModdle as ModdleDescriptor),
  types: [
    ...(ZeebeModdle as ModdleDescriptor).types,
    AGENT_DEFINITION_MODDLE_TYPE,
  ],
};
