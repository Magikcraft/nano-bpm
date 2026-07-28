import { useEffect, useRef } from "react";
import NavigatedViewer from "bpmn-js/lib/NavigatedViewer";

interface Canvas {
  zoom(mode: string): void;
  addMarker(elementId: string, marker: string): void;
  removeMarker(elementId: string, marker: string): void;
}

interface Overlays {
  add(
    elementId: string,
    overlay: {
      position: Record<string, number>;
      html: string | HTMLElement;
    },
  ): string;
  remove(id: string): void;
}

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
export function BpmnRuntimeView({
  xml,
  activeIds,
  incidentIds,
  className,
}: BpmnRuntimeViewProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewerRef = useRef<NavigatedViewer | null>(null);
  const importedRef = useRef(false);
  const markedRef = useRef<{ id: string; cls: string }[]>([]);
  const tokenOverlaysRef = useRef<string[]>([]);
  // Track the latest ids in a ref so the post-import `applyMarkers()` (fired from
  // the `[xml]` effect's async `.then`) uses current values, not the ids that
  // were current when the import started — otherwise ids changing mid-import
  // would leave the diagram unmarked until the next change.
  const idsRef = useRef({ activeIds, incidentIds });
  idsRef.current = { activeIds, incidentIds };

  useEffect(() => {
    if (!containerRef.current) return;
    const viewer = new NavigatedViewer({ container: containerRef.current });
    viewerRef.current = viewer;
    importedRef.current = false;
    viewer
      .importXML(xml)
      .then(() => {
        viewer.get<Canvas>("canvas").zoom("fit-viewport");
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
    if (!viewer || !importedRef.current) return;
    const canvas = viewer.get<Canvas>("canvas");
    for (const { id, cls } of markedRef.current) {
      try {
        canvas.removeMarker(id, cls);
      } catch {
        /* ignore */
      }
    }
    const next: { id: string; cls: string }[] = [];
    for (const id of idsRef.current.activeIds) next.push({ id, cls: "nano-active" });
    for (const id of idsRef.current.incidentIds)
      next.push({ id, cls: "nano-incident" });
    for (const { id, cls } of next) {
      try {
        canvas.addMarker(id, cls);
      } catch {
        /* element not in this diagram */
      }
    }
    markedRef.current = next;

    // A visible token badge on each active element: an explicit "token is here"
    // marker so movement reads clearly even when a class-only highlight is too
    // subtle. Overlays are removed/re-added each update so the token hops with
    // the frontier.
    const overlays = viewer.get<Overlays>("overlays");
    for (const id of tokenOverlaysRef.current) {
      try {
        overlays.remove(id);
      } catch {
        /* ignore */
      }
    }
    const nextOverlays: string[] = [];
    for (const id of idsRef.current.activeIds) {
      try {
        nextOverlays.push(
          overlays.add(id, {
            position: { top: -12, left: -12 },
            html: '<div class="nano-token" aria-hidden="true"></div>',
          }),
        );
      } catch {
        /* element not in this diagram */
      }
    }
    tokenOverlaysRef.current = nextOverlays;
  }

  useEffect(() => {
    applyMarkers();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeIds, incidentIds]);

  return (
    <div
      ref={containerRef}
      className={className}
      style={{ width: "100%", height: "100%" }}
    />
  );
}
