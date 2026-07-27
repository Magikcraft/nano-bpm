import { jsx as _jsx } from "react/jsx-runtime";
import { useEffect, useRef } from "react";
import NavigatedViewer from "bpmn-js/lib/NavigatedViewer";
/**
 * Read-only diagram that imports the XML once and updates token/incident markers
 * in place (no re-import, so the zoom/scroll position is preserved while
 * stepping through the simulation). This is the token-movement half of the
 * Bojtos visual contract (ADR 0043 §4): drive `activeIds` / `incidentIds` from a
 * session snapshot's `activeElementIds` / `incidentElementIds`.
 *
 * The consumer must load bpmn-js's diagram CSS (`bpmn-js/dist/assets/
 * diagram-js.css` and `.../bpmn-font/css/bpmn-embedded.css`) once in the app,
 * and provide the `.nano-active` / `.nano-incident` marker styles.
 */
export function BpmnRuntimeView({ xml, activeIds, incidentIds, className, }) {
    const containerRef = useRef(null);
    const viewerRef = useRef(null);
    const importedRef = useRef(false);
    const markedRef = useRef([]);
    useEffect(() => {
        if (!containerRef.current)
            return;
        const viewer = new NavigatedViewer({ container: containerRef.current });
        viewerRef.current = viewer;
        importedRef.current = false;
        viewer
            .importXML(xml)
            .then(() => {
            viewer.get("canvas").zoom("fit-viewport");
            importedRef.current = true;
            applyMarkers();
        })
            .catch(() => {
            /* malformed XML — leave blank */
        });
        return () => {
            viewer.destroy();
            viewerRef.current = null;
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [xml]);
    function applyMarkers() {
        const viewer = viewerRef.current;
        if (!viewer || !importedRef.current)
            return;
        const canvas = viewer.get("canvas");
        for (const { id, cls } of markedRef.current) {
            try {
                canvas.removeMarker(id, cls);
            }
            catch {
                /* ignore */
            }
        }
        const next = [];
        for (const id of activeIds)
            next.push({ id, cls: "nano-active" });
        for (const id of incidentIds)
            next.push({ id, cls: "nano-incident" });
        for (const { id, cls } of next) {
            try {
                canvas.addMarker(id, cls);
            }
            catch {
                /* element not in this diagram */
            }
        }
        markedRef.current = next;
    }
    useEffect(() => {
        applyMarkers();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [activeIds, incidentIds]);
    return (_jsx("div", { ref: containerRef, className: className, style: { width: "100%", height: "100%" } }));
}
