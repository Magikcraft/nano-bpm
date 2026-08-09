import { useEffect, useRef } from "react";
import NavigatedViewer from "bpmn-js/lib/NavigatedViewer";
import "bpmn-js/dist/assets/diagram-js.css";
import "bpmn-js/dist/assets/bpmn-font/css/bpmn-embedded.css";

interface Canvas {
  zoom(mode: string): void;
  addMarker(elementId: string, marker: string): void;
  removeMarker(elementId: string, marker: string): void;
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
  // Set once the viewer has been destroyed, so async work already in flight (an
  // importXML, a marker pass) doesn't touch a dead instance.
  const disposedRef = useRef(false);
  // Serializes every viewer op on a single chain. Overlapping importXML calls
  // corrupt diagram-js's internal state — it then throws from deep inside a
  // render ("Cannot read properties of undefined (reading 'root-0')"). This is
  // the same race the modeler hit and was fixed the same way (`runLoad`, commit
  // 999ad23): InstanceDetail re-renders on every live SSE tick with freshly
  // derived activeElementIds/incidentElementIds arrays, so the effect below
  // re-fires constantly and, unserialized, those imports race each other.
  const opChainRef = useRef<Promise<unknown>>(Promise.resolve());
  // The XML currently imported into the live viewer, so a marker-only update
  // (the common live-tick case) skips a full, flashing re-import. Reset whenever
  // the viewer is (re)created.
  const importedXmlRef = useRef<string | null>(null);
  // Markers currently applied, so a marker pass can clear the previous set
  // before adding the new one (a re-import wipes them, a marker-only pass does
  // not).
  const markedActiveRef = useRef<string[]>([]);
  const markedIncidentRef = useRef<string[]>([]);

  useEffect(() => {
    if (!containerRef.current) return;
    disposedRef.current = false;
    const viewer = new NavigatedViewer({ container: containerRef.current });
    viewerRef.current = viewer;
    importedXmlRef.current = null;
    markedActiveRef.current = [];
    markedIncidentRef.current = [];
    return () => {
      disposedRef.current = true;
      viewer.destroy();
      viewerRef.current = null;
    };
  }, []);

  // Stable dep keys: the parent hands new array identities every render, so
  // depend on their content, not their reference, to avoid needless re-runs.
  const activeKey = activeElementIds.join(",");
  const incidentKey = incidentElementIds.join(",");

  useEffect(() => {
    const viewer = viewerRef.current;
    if (!viewer || !xml) return;
    const op = opChainRef.current.then(async () => {
      if (disposedRef.current || viewerRef.current !== viewer) return;
      // Only (re)import when the document itself changed; a live tick that only
      // moves the active/incident markers reuses the imported diagram.
      if (importedXmlRef.current !== xml) {
        try {
          await viewer.importXML(xml);
        } catch {
          // malformed/unsupported XML — leave the canvas blank
          return;
        }
        if (disposedRef.current || viewerRef.current !== viewer) return;
        importedXmlRef.current = xml;
        // A fresh import clears every marker, so drop our bookkeeping too.
        markedActiveRef.current = [];
        markedIncidentRef.current = [];
        viewer.get<Canvas>("canvas").zoom("fit-viewport");
      }
      const canvas = viewer.get<Canvas>("canvas");
      for (const id of markedActiveRef.current) {
        try {
          canvas.removeMarker(id, "nano-active");
        } catch {
          /* element may not exist in this version's diagram */
        }
      }
      for (const id of markedIncidentRef.current) {
        try {
          canvas.removeMarker(id, "nano-incident");
        } catch {
          /* ignore unknown element */
        }
      }
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
      markedActiveRef.current = activeElementIds;
      markedIncidentRef.current = incidentElementIds;
    });
    // Keep the chain alive even when this op fails, so one bad import doesn't
    // wedge every later load.
    opChainRef.current = op.catch(() => {});
  }, [xml, activeKey, incidentKey]);

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
