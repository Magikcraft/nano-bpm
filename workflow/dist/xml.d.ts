import type { Workflow } from "./types.js";
export declare function escapeXml(s: string): string;
/** Derived job type for a declarative `run` step / imperative orchestrator. */
export declare const jobType: (workflowId: string, step: string) => string;
/** Derived message name for a declarative `signal` step. */
export declare const messageName: (workflowId: string, step: string) => string;
/** The single orchestrator job type of an imperative workflow. */
export declare const orchestrateType: (workflowId: string) => string;
export declare function assertIdent(kind: string, value: string): void;
export declare function assertWorkflowIds(wf: Workflow): void;
