import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";
import Modeler from "dmn-js/lib/Modeler";
import "dmn-js/dist/assets/diagram-js.css";
import "dmn-js/dist/assets/dmn-js-shared.css";
import "dmn-js/dist/assets/dmn-js-drd.css";
import "dmn-js/dist/assets/dmn-js-decision-table.css";
import "dmn-js/dist/assets/dmn-js-decision-table-controls.css";
import "dmn-js/dist/assets/dmn-js-literal-expression.css";
import "dmn-js/dist/assets/dmn-font/css/dmn.css";

interface EventBus {
  on(event: string, callback: (...args: unknown[]) => void): void;
  off(event: string, callback: (...args: unknown[]) => void): void;
}

interface Viewer {
  get<T = unknown>(service: string): T;
}

/// Imperative handle the DMN editor view drives. Keeps the live DMN document
/// inside this component and exposes just the operations the toolbar needs.
export interface DmnModelerHandle {
  /// Serializes the current document to formatted DMN XML.
  getXml(): Promise<string>;
  /// Replaces the document with `xml`.
  importXml(xml: string): Promise<void>;
  /// Loads a blank DMN model.
  createBlank(): Promise<void>;
}

interface DmnModelerProps {
  /// Called whenever the document changes (after the first import). The initial
  /// blank model does not mark dirty.
  onChange?: () => void;
}

const EMPTY_DMN = `<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" xmlns:dmndi="https://www.omg.org/spec/DMN/20191111/DMNDI/" xmlns:dc="http://www.omg.org/spec/DMN/20180521/DC/" id="Definitions_1" name="Definitions" namespace="http://nanobpmn/dmn">
  <decision id="Decision_1" name="Decision">
    <decisionTable id="DecisionTable_1">
      <input id="Input_1">
        <inputExpression id="InputExpression_1" typeRef="string">
          <text></text>
        </inputExpression>
      </input>
      <output id="Output_1" typeRef="string" />
    </decisionTable>
  </decision>
  <dmndi:DMNDI>
    <dmndi:DMNDiagram id="DMNDiagram_1">
      <dmndi:DMNShape id="DMNShape_Decision_1" dmnElementRef="Decision_1">
        <dc:Bounds x="160" y="100" width="180" height="80" />
      </dmndi:DMNShape>
    </dmndi:DMNDiagram>
  </dmndi:DMNDI>
</definitions>`;

const DmnModeler = forwardRef<DmnModelerHandle, DmnModelerProps>(
  function DmnModeler({ onChange }, ref) {
    const containerRef = useRef<HTMLDivElement>(null);
    const modelerRef = useRef<Modeler | null>(null);
    // Set once the modeler has been destroyed, so async work already in flight
    // doesn't touch a dead instance.
    const disposedRef = useRef(false);
    // Serializes document loads. importXML must never overlap.
    const opChainRef = useRef<Promise<unknown>>(Promise.resolve());
    // Suppress the change callback for programmatic loads (import/createBlank).
    const suppressChange = useRef(false);
    const suppressTimerRef = useRef<number | null>(null);
    const onChangeRef = useRef(onChange);
    onChangeRef.current = onChange;
    const activeEventBusRef = useRef<EventBus | null>(null);
    const activeHandlerRef = useRef<((...args: unknown[]) => void) | null>(null);

    const clearSuppressTimer = () => {
      if (suppressTimerRef.current === null) return;
      window.clearTimeout(suppressTimerRef.current);
      suppressTimerRef.current = null;
    };

    const runLoad = (loader: (m: Modeler) => Promise<unknown>): Promise<void> => {
      const modeler = modelerRef.current;
      if (!modeler) return Promise.resolve();
      const run = opChainRef.current.then(async () => {
        if (disposedRef.current || modelerRef.current !== modeler) return;
        clearSuppressTimer();
        suppressChange.current = true;
        try {
          await loader(modeler);
        } finally {
          suppressTimerRef.current = window.setTimeout(() => {
            if (!disposedRef.current && modelerRef.current === modeler) {
              suppressChange.current = false;
            }
            suppressTimerRef.current = null;
          }, 0);
        }
      });
      // Keep the chain alive even when this op fails, so one bad import doesn't
      // wedge every later load.
      opChainRef.current = run.catch(() => {});
      return run;
    };

    useEffect(() => {
      if (!containerRef.current) return;
      disposedRef.current = false;
      const modeler = new Modeler({ container: containerRef.current });
      modelerRef.current = modeler;

      const handleChanged = () => {
        if (suppressChange.current) return;
        onChangeRef.current?.();
      };

      const attachActiveViewerChangeListener = () => {
        if (activeEventBusRef.current && activeHandlerRef.current) {
          activeEventBusRef.current.off("commandStack.changed", activeHandlerRef.current);
          activeEventBusRef.current = null;
          activeHandlerRef.current = null;
        }
        if (disposedRef.current || modelerRef.current !== modeler) return;
        try {
          const activeViewer = modeler.getActiveViewer() as Viewer | null;
          const eventBus = activeViewer?.get<EventBus>("eventBus");
          if (!eventBus) return;
          eventBus.on("commandStack.changed", handleChanged);
          activeEventBusRef.current = eventBus;
          activeHandlerRef.current = handleChanged;
        } catch {
          // Active view not ready yet — views.changed will fire again once it is.
        }
      };

      const handleViewsChanged = () => {
        attachActiveViewerChangeListener();
        handleChanged();
      };

      modeler.on("views.changed", handleViewsChanged);
      void runLoad((m) => m.importXML(EMPTY_DMN));

      return () => {
        disposedRef.current = true;
        clearSuppressTimer();
        if (activeEventBusRef.current && activeHandlerRef.current) {
          activeEventBusRef.current.off("commandStack.changed", activeHandlerRef.current);
        }
        activeEventBusRef.current = null;
        activeHandlerRef.current = null;
        modeler.off("views.changed", handleViewsChanged);
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
      createBlank: () => runLoad((m) => m.importXML(EMPTY_DMN)),
    }));

    return <div ref={containerRef} className="h-full w-full bg-white" />;
  },
);

export default DmnModeler;
