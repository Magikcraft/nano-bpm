export interface BpmnRuntimeViewProps {
    /** The diagram XML to render. */
    xml: string;
    /** Element ids to highlight as active (token) — marker class `nano-active`. */
    activeIds: string[];
    /** Element ids to highlight as incidents — marker class `nano-incident`. */
    incidentIds: string[];
    /** Optional class for the container element (it always fills its parent). */
    className?: string;
}
/**
 * Read-only diagram that imports the XML once and updates token/incident markers
 * in place (no re-import, so the zoom/scroll position is preserved while
 * stepping through the simulation). This is the token-movement half of the
 * Bojtos visual contract (ADR 0043 §4): drive `activeIds` / `incidentIds` from a
 * session snapshot's `activeElementIds` / `incidentElementIds`.
 *
 * The consumer must load bpmn-js's diagram CSS (`bpmn-js/dist/assets/
 * diagram-js.css` and `.../bpmn-font/css/bpmn-embedded.css`) once in the app,
 * and provide the `.nano-active` / `.nano-incident` marker styles plus a
 * `.nano-token` style for the token badge overlaid on each active element.
 */
export declare function BpmnRuntimeView({ xml, activeIds, incidentIds, className, }: BpmnRuntimeViewProps): import("react").JSX.Element;
