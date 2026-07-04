import { useEffect, useRef } from "react";
import NavigatedViewer from "bpmn-js/lib/NavigatedViewer";
import "bpmn-js/dist/assets/diagram-js.css";
import "bpmn-js/dist/assets/bpmn-font/css/bpmn-embedded.css";

interface Canvas {
  zoom(mode: string): void;
  addMarker(elementId: string, marker: string): void;
}

interface BpmnViewerProps {
  xml: string | null;
  /// Element ids to highlight as carrying an active token (e.g. pending jobs).
  activeElementIds?: string[];
  /// Element ids to highlight as having an incident.
  incidentElementIds?: string[];
}

/// Renders a deployed BPMN definition with diagram-js (read-only), overlaying
/// markers on the elements that currently hold work or an incident. The XML is
/// fetched from the gateway's getProcessDefinitionXML endpoint by the caller.
export default function BpmnViewer({
  xml,
  activeElementIds = [],
  incidentElementIds = [],
}: BpmnViewerProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewerRef = useRef<NavigatedViewer | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const viewer = new NavigatedViewer({ container: containerRef.current });
    viewerRef.current = viewer;
    return () => {
      viewer.destroy();
      viewerRef.current = null;
    };
  }, []);

  useEffect(() => {
    const viewer = viewerRef.current;
    if (!viewer || !xml) return;
    let cancelled = false;
    viewer
      .importXML(xml)
      .then(() => {
        if (cancelled) return;
        const canvas = viewer.get<Canvas>("canvas");
        canvas.zoom("fit-viewport");
        for (const id of activeElementIds) {
          try {
            canvas.addMarker(id, "nano-active");
          } catch {
            /* element may not exist in this version's diagram */
          }
        }
        for (const id of incidentElementIds) {
          try {
            canvas.addMarker(id, "nano-incident");
          } catch {
            /* ignore unknown element */
          }
        }
      })
      .catch(() => {
        /* malformed/unsupported XML — leave the canvas blank */
      });
    return () => {
      cancelled = true;
    };
  }, [xml, activeElementIds, incidentElementIds]);

  // Render a single, stable structure so the container ref points at the same
  // DOM node for the component's whole life (the viewer is created against it
  // on mount, before the async XML query resolves). The empty-state message is
  // an overlay shown until the XML arrives — never an alternate tree that would
  // leave the ref unmounted and prevent the viewer from being created.
  return (
    <div className="relative h-full w-full">
      <div ref={containerRef} className="h-full w-full" />
      {!xml && (
        <div className="absolute inset-0 flex items-center justify-center text-sm text-fg-faint">
          No diagram available for this definition.
        </div>
      )}
    </div>
  );
}
