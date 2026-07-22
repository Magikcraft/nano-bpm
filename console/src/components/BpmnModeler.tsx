import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";
import Modeler from "bpmn-js/lib/Modeler";
import {
  BpmnPropertiesPanelModule,
  BpmnPropertiesProviderModule,
  ZeebePropertiesProviderModule,
} from "bpmn-js-properties-panel";
import ZeebeModdle from "zeebe-bpmn-moddle/resources/zeebe.json";
import {
  CloudElementTemplatesCoreModule,
  CloudElementTemplatesPropertiesProviderModule,
  type ElementTemplatesService,
} from "bpmn-js-element-templates";
import CloudBehaviorsModule from "camunda-bpmn-js-behaviors/lib/camunda-cloud";
import { getVariablesForElement as extractZeebeVariables } from "@bpmn-io/extract-process-variables/zeebe";
import type { ElementTemplate } from "../lib/urbanComponents";
import type { FeelVariable } from "../lib/feelVariables";
import type { ComponentOutput } from "../lib/bpmnDomainVariables";
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

// --- Component output extraction (ADR 0033 §3) -------------------------------
// Walks the diagram's service tasks to pair each output-mapped process variable
// with the `taskType` of the component that writes it. The schema package joins
// these `{ taskType, target }` pairs against `workers[].outputType` to type the
// variables, so a task placed after a component autocompletes on its result's
// fields — component output → typed process variable → next component input.
interface ModdleElement {
  $type?: string;
  type?: string;
  target?: string;
  values?: ModdleElement[];
  outputParameters?: ModdleElement[];
  extensionElements?: { values?: ModdleElement[] };
}
interface RegistryElement {
  type?: string;
  businessObject?: ModdleElement;
}
interface ElementRegistry {
  getAll(): RegistryElement[];
}

const SERVICE_TASK_TYPES = new Set([
  "bpmn:ServiceTask",
  "bpmn:BusinessRuleTask",
  "bpmn:ScriptTask",
  "bpmn:SendTask",
]);

function collectComponentOutputs(registry: ElementRegistry): ComponentOutput[] {
  const outputs: ComponentOutput[] = [];
  for (const el of registry.getAll()) {
    if (!el.type || !SERVICE_TASK_TYPES.has(el.type)) continue;
    const ext = el.businessObject?.extensionElements?.values ?? [];
    const taskDef = ext.find((v) => v.$type === "zeebe:TaskDefinition");
    const taskType = typeof taskDef?.type === "string" ? taskDef.type : undefined;
    if (!taskType) continue;
    const io = ext.find((v) => v.$type === "zeebe:IoMapping");
    for (const p of io?.outputParameters ?? []) {
      if (typeof p.target === "string" && p.target) outputs.push({ taskType, target: p.target });
    }
  }
  return outputs;
}

// --- Urban components palette (ADR 0033) -------------------------------------
// A diagram-js palette provider that adds an entry per installed Urban component
// (an element template). Dragging one onto the canvas stamps a service task
// pre-bound to the template's task type + input/output mappings — the "drag a
// Delphi component off the palette" loop. Runtime behaviour is the matching
// `workers[].taskType` (ADR 0022); the properties panel is the Object Inspector.
// The component set comes from the open project (ADR 0033 increment 2,
// `loadProjectComponents`), read live through a ref so a newly-added component
// file surfaces without recreating the modeler; the provider class is defined
// inside the mount effect so it can close over that ref.
interface PaletteService {
  registerProvider(provider: unknown): void;
  /** diagram-js internal palette re-render; re-invokes every provider's
   *  `getPaletteEntries`. Used to refresh entries when the component set loads
   *  after mount. Optional/guarded — absence just means no live refresh. */
  _update?(): void;
}
interface CreateService {
  start(event: Event, shape: unknown): void;
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
  /// Supplies the domain variables in scope for the open diagram's process,
  /// given the component outputs the modeler extracted. Combines the process's
  /// bound-type `body` scope (ADR 0030) with the component-output scope (ADR
  /// 0033 §3: each output-mapped variable typed by its worker's `outputType`),
  /// so component-input / gateway FEEL autocomplete offers both. Empty/absent →
  /// only bpmn-js's extracted process variables are offered. Read live, so
  /// binding/output edits reflect without a remount.
  getVariables?: (ctx: { taskOutputs: ComponentOutput[] }) => FeelVariable[];
  /// The component element templates installed for the open project (ADR 0033
  /// increment 2, loaded by `loadProjectComponents`). Feeds both the palette
  /// (one draggable entry per component) and the template chooser. Read live, so
  /// adding a component file refreshes the palette without a remount. Empty/absent
  /// → an empty component palette (nothing installed).
  components?: ElementTemplate[];
}

const BpmnModeler = forwardRef<BpmnModelerHandle, BpmnModelerProps>(
  function BpmnModeler({ onChange, onReady, getVariables, components }, ref) {
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
    const getVariablesRef = useRef(getVariables);
    getVariablesRef.current = getVariables;
    // The installed component set, read live by the palette provider and the
    // template-sync effect so a newly-loaded/edited component surfaces without
    // recreating the modeler.
    const componentsRef = useRef<ElementTemplate[]>(components ?? []);
    componentsRef.current = components ?? [];

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
      // A `variableResolver` service the bpmn-js FEEL fields consult
      // (`useServiceIfAvailable('variableResolver', …)` in properties-panel and
      // element-templates). We keep bpmn-js's own extracted process variables
      // (output mappings written upstream) and append the domain scope: the type
      // bound to this diagram's process (ADR 0030, `body`) plus each component
      // output typed by its worker's `outputType` (ADR 0033 §3). Typed domain
      // variables override the plain extracted ones of the same name so their
      // fields autocomplete. Read live via the ref so binding/output edits
      // reflect without recreating the modeler.
      class DomainVariableResolver {
        static $inject = ["elementRegistry"];
        private readonly elementRegistry: ElementRegistry;
        constructor(elementRegistry: ElementRegistry) {
          this.elementRegistry = elementRegistry;
        }
        getVariablesForElement(bo: unknown): FeelVariable[] {
          let base: FeelVariable[] = [];
          try {
            base = (extractZeebeVariables(bo) as FeelVariable[]) ?? [];
          } catch {
            base = [];
          }
          let taskOutputs: ComponentOutput[] = [];
          try {
            taskOutputs = collectComponentOutputs(this.elementRegistry);
          } catch {
            taskOutputs = [];
          }
          const domain = getVariablesRef.current?.({ taskOutputs }) ?? [];
          // Dedupe by name; a typed domain variable supersedes its plain
          // extracted counterpart so nested-field completion wins.
          const byName = new Map<string, FeelVariable>();
          for (const v of base) if (v?.name) byName.set(v.name, v);
          for (const v of domain) if (v?.name) byName.set(v.name, v);
          return [...byName.values()];
        }
      }
      const domainVariableResolverModule = {
        variableResolver: ["type", DomainVariableResolver],
      };
      // Palette provider: one draggable entry per installed component. Defined
      // here so it can read the live `componentsRef` — the set loads from the
      // project after mount and can change as component files are added.
      class UrbanComponentsPaletteProvider {
        // Explicit annotation so didi injection survives Vite minification.
        static $inject = ["palette", "create", "elementTemplates"];
        private readonly create: CreateService;
        private readonly elementTemplates: ElementTemplatesService;
        constructor(palette: PaletteService, create: CreateService, elementTemplates: ElementTemplatesService) {
          this.create = create;
          this.elementTemplates = elementTemplates;
          palette.registerProvider(this);
        }
        getPaletteEntries(): Record<string, unknown> {
          const entries: Record<string, unknown> = {};
          for (const tpl of componentsRef.current) {
            const start = (event: Event) => {
              const shape = this.elementTemplates.createElement(tpl);
              this.create.start(event, shape);
            };
            entries[`create.urban-${tpl.id}`] = {
              group: "urban-components",
              className: "bpmn-icon-service-task",
              title: tpl.name,
              action: { dragstart: start, click: start },
            };
          }
          return entries;
        }
      }
      const urbanComponentsPaletteModule = {
        __init__: ["urbanComponentsPaletteProvider"],
        urbanComponentsPaletteProvider: ["type", UrbanComponentsPaletteProvider],
      };
      const modeler = new Modeler({
        container: containerRef.current,
        propertiesPanel: { parent: panelRef.current },
        additionalModules: [
          BpmnPropertiesPanelModule,
          BpmnPropertiesProviderModule,
          ZeebePropertiesProviderModule,
          // Element templates (ADR 0033): the component model. Core registers the
          // `elementTemplates` service + create-append behaviour; the provider
          // renders the "Template" group (the component's Object Inspector); the
          // cloud behaviours keep Zeebe extension elements consistent on edits.
          CloudElementTemplatesCoreModule,
          CloudElementTemplatesPropertiesProviderModule,
          CloudBehaviorsModule,
          urbanComponentsPaletteModule,
          domainVariableResolverModule,
        ],
        moddleExtensions: { zeebe: ZeebeModdle },
      });
      modelerRef.current = modeler;
      // Install the project's components so the palette + template chooser
      // surface them (ADR 0033 increment 2). The set may still be loading at
      // mount; the `[components]` effect below re-installs + refreshes the
      // palette once it arrives or changes.
      try {
        modeler.get<ElementTemplatesService>("elementTemplates").set(componentsRef.current);
      } catch {
        // Non-fatal: without the core module the palette simply shows nothing.
      }
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

    // Re-install the component set + refresh the palette when it changes. The
    // project loads components asynchronously (and can add more), so the mount
    // effect's `set()` may have run against an empty/stale set. Reads live via
    // `componentsRef`; the palette re-render re-invokes `getPaletteEntries`.
    useEffect(() => {
      const modeler = modelerRef.current;
      if (!modeler || disposedRef.current) return;
      try {
        modeler.get<ElementTemplatesService>("elementTemplates").set(componentsRef.current);
        modeler.get<PaletteService>("palette")._update?.();
      } catch {
        // Non-fatal: core/palette absent, or modeler torn down mid-flight.
      }
    }, [components]);

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
      // The bpmn-js canvas + properties panel are light-themed third-party
      // widgets, so both surfaces stay physically white (with a matching light
      // divider) regardless of the console theme to keep diagrams legible.
      <div className="flex h-full w-full">
        <div ref={containerRef} className="h-full min-w-0 flex-1 bg-white" />
        <div
          ref={panelRef}
          className="bpmn-properties h-full w-80 shrink-0 overflow-auto border-l border-[#d4d4d8] bg-white"
        />
      </div>
    );
  },
);

export default BpmnModeler;
