import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";
import Modeler from "bpmn-js/lib/Modeler";
import {
  BpmnPropertiesPanelModule,
  BpmnPropertiesProviderModule,
  ZeebePropertiesProviderModule,
} from "bpmn-js-properties-panel";
import ZeebeModdle from "zeebe-bpmn-moddle/resources/zeebe.json";
import "bpmn-js/dist/assets/diagram-js.css";
import "bpmn-js/dist/assets/bpmn-font/css/bpmn-embedded.css";
import "bpmn-js/dist/assets/bpmn-js.css";
import "@bpmn-io/properties-panel/dist/assets/properties-panel.css";

interface RootElement {
  id: string;
  businessObject: { id: string; name?: string };
}
interface Canvas {
  zoom(mode: string): void;
  getRootElement(): RootElement;
}
interface Modeling {
  updateProperties(element: unknown, props: Record<string, unknown>): void;
}

/// Imperative handle the Modeler view drives. Keeps the live bpmn document inside
/// this component and exposes just the operations the toolbar needs.
export interface BpmnModelerHandle {
  /// Serializes the current document to formatted BPMN XML.
  getXml(): Promise<string>;
  /// Replaces the document with `xml` (used to open a model or pull a deployed one).
  importXml(xml: string): Promise<void>;
  /// Loads a blank diagram (New).
  createBlank(): Promise<void>;
  /// The id of the root process element, or null when none is loaded.
  getProcessId(): string | null;
  /// Renames the root process element's id (the BPMN deploy identity).
  setProcessId(id: string): void;
}

interface BpmnModelerProps {
  /// Called whenever the document changes (after the first import/createDiagram),
  /// so the parent can drive dirty state. The initial load does not mark dirty.
  onChange?: () => void;
  /// Called once after the initial blank diagram has loaded, so the parent can
  /// read the starting process id.
  onReady?: () => void;
}

const BpmnModeler = forwardRef<BpmnModelerHandle, BpmnModelerProps>(
  function BpmnModeler({ onChange, onReady }, ref) {
    const containerRef = useRef<HTMLDivElement>(null);
    const panelRef = useRef<HTMLDivElement>(null);
    const modelerRef = useRef<Modeler | null>(null);
    // Suppress the change callback for programmatic loads (import/createDiagram),
    // so opening a model doesn't immediately look dirty.
    const suppressChange = useRef(false);
    const onChangeRef = useRef(onChange);
    onChangeRef.current = onChange;
    const onReadyRef = useRef(onReady);
    onReadyRef.current = onReady;

    useEffect(() => {
      if (!containerRef.current || !panelRef.current) return;
      const modeler = new Modeler({
        container: containerRef.current,
        propertiesPanel: { parent: panelRef.current },
        additionalModules: [
          BpmnPropertiesPanelModule,
          BpmnPropertiesProviderModule,
          ZeebePropertiesProviderModule,
        ],
        moddleExtensions: { zeebe: ZeebeModdle },
      });
      modelerRef.current = modeler;
      const handleChanged = () => {
        if (suppressChange.current) return;
        onChangeRef.current?.();
      };
      modeler.on("commandStack.changed", handleChanged);
      // Start on a blank diagram so the canvas is never empty.
      suppressChange.current = true;
      modeler
        .createDiagram()
        .then(() => onReadyRef.current?.())
        .finally(() => {
          suppressChange.current = false;
        });
      return () => {
        modeler.off("commandStack.changed", handleChanged);
        modeler.destroy();
        modelerRef.current = null;
      };
    }, []);

    const load = async (loader: (m: Modeler) => Promise<unknown>) => {
      const modeler = modelerRef.current;
      if (!modeler) return;
      suppressChange.current = true;
      try {
        await loader(modeler);
        modeler.get<Canvas>("canvas").zoom("fit-viewport");
      } finally {
        suppressChange.current = false;
      }
    };

    useImperativeHandle(ref, () => ({
      async getXml() {
        const modeler = modelerRef.current;
        if (!modeler) return "";
        const { xml } = await modeler.saveXML({ format: true });
        return xml;
      },
      importXml: (xml: string) => load((m) => m.importXML(xml)),
      createBlank: () => load((m) => m.createDiagram()),
      getProcessId() {
        const modeler = modelerRef.current;
        if (!modeler) return null;
        try {
          return modeler.get<Canvas>("canvas").getRootElement().businessObject.id;
        } catch {
          return null;
        }
      },
      setProcessId(id: string) {
        const modeler = modelerRef.current;
        if (!modeler) return;
        const canvas = modeler.get<Canvas>("canvas");
        const modeling = modeler.get<Modeling>("modeling");
        modeling.updateProperties(canvas.getRootElement(), { id });
      },
    }));

    return (
      <div className="flex h-full w-full">
        <div ref={containerRef} className="h-full min-w-0 flex-1 bg-white" />
        <div
          ref={panelRef}
          className="bpmn-properties h-full w-80 shrink-0 overflow-auto border-l border-zinc-300 bg-white"
        />
      </div>
    );
  },
);

export default BpmnModeler;
