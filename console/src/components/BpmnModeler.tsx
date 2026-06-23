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
    // Set once the modeler has been destroyed, so async work that was already
    // in flight (an import, the initial createDiagram) doesn't touch a dead
    // instance — diagram-js throws "reading 'root-0'" when operated on after
    // destroy or while a load is mid-flight.
    const disposedRef = useRef(false);
    // Serializes document loads. importXML/createDiagram must never overlap;
    // every load is chained after the previous one so they run one at a time.
    const opChainRef = useRef<Promise<unknown>>(Promise.resolve());
    // Suppress the change callback for programmatic loads (import/createDiagram),
    // so opening a model doesn't immediately look dirty.
    const suppressChange = useRef(false);
    const onChangeRef = useRef(onChange);
    onChangeRef.current = onChange;
    const onReadyRef = useRef(onReady);
    onReadyRef.current = onReady;

    // Queues a load on a single chain so imports can't race each other or the
    // initial blank diagram. Each op captures the modeler instance it was
    // enqueued for and bails if that instance was destroyed or swapped out
    // (e.g. StrictMode's mount→unmount→mount) before it ran. Returns a promise
    // that rejects on a genuine load failure (e.g. invalid XML) so callers can
    // surface it, while the internal chain keeps going regardless.
    const runLoad = (
      loader: (m: Modeler) => Promise<unknown>,
      after?: () => void,
    ): Promise<void> => {
      const modeler = modelerRef.current;
      if (!modeler) return Promise.resolve();
      const run = opChainRef.current.then(async () => {
        if (disposedRef.current || modelerRef.current !== modeler) return;
        suppressChange.current = true;
        try {
          await loader(modeler);
          if (disposedRef.current || modelerRef.current !== modeler) return;
          modeler.get<Canvas>("canvas").zoom("fit-viewport");
          after?.();
        } finally {
          suppressChange.current = false;
        }
      });
      // Keep the chain alive even when this op fails, so one bad import doesn't
      // wedge every later load.
      opChainRef.current = run.catch(() => {});
      return run;
    };

    useEffect(() => {
      if (!containerRef.current || !panelRef.current) return;
      disposedRef.current = false;
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
      // Start on a blank diagram so the canvas is never empty. Queued like any
      // other load so a quick open() serializes after it instead of racing it.
      void runLoad(
        (m) => m.createDiagram(),
        () => onReadyRef.current?.(),
      );
      return () => {
        disposedRef.current = true;
        modeler.off("commandStack.changed", handleChanged);
        modeler.destroy();
        modelerRef.current = null;
      };
    }, []);

    useImperativeHandle(ref, () => ({
      async getXml() {
        const modeler = modelerRef.current;
        if (!modeler || disposedRef.current) return "";
        const { xml } = await modeler.saveXML({ format: true });
        return xml;
      },
      importXml: (xml: string) => runLoad((m) => m.importXML(xml)),
      createBlank: () => runLoad((m) => m.createDiagram()),
      getProcessId() {
        const modeler = modelerRef.current;
        if (!modeler || disposedRef.current) return null;
        try {
          return modeler.get<Canvas>("canvas").getRootElement().businessObject.id;
        } catch {
          return null;
        }
      },
      setProcessId(id: string) {
        const modeler = modelerRef.current;
        if (!modeler || disposedRef.current) return;
        try {
          const canvas = modeler.get<Canvas>("canvas");
          const modeling = modeler.get<Modeling>("modeling");
          modeling.updateProperties(canvas.getRootElement(), { id });
        } catch {
          // Root not ready (e.g. a load is still settling) — ignore.
        }
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
