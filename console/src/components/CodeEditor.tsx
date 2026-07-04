import { useEffect, useRef } from "react";
import * as monaco from "monaco-editor";
import { Editor, loader } from "@monaco-editor/react";
import { useTheme } from "../theme/ThemeProvider";

// Bundle Monaco's web workers through Vite (the `?worker` suffix emits each as a
// separate chunk) instead of fetching them from a CDN. This keeps the console
// fully self-contained: everything is embedded in the gateway binary and works
// offline, with no network round-trips when the editor mounts.
import editorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker";
import jsonWorker from "monaco-editor/esm/vs/language/json/json.worker?worker";
import tsWorker from "monaco-editor/esm/vs/language/typescript/ts.worker?worker";

(self as unknown as { MonacoEnvironment: monaco.Environment }).MonacoEnvironment = {
  getWorker(_workerId, label) {
    if (label === "json") return new jsonWorker();
    if (label === "typescript" || label === "javascript") return new tsWorker();
    return new editorWorker();
  },
};

// Point @monaco-editor/react at our locally bundled monaco instance rather than
// letting it lazy-load the AMD build from jsdelivr (the default).
loader.config({ monaco });

// Worker code targets Deno: ESM modules, npm:/jsr:/https: import specifiers, and
// top-level await. Configure the TS language service to match and silence the
// "cannot find module" diagnostics that those non-resolvable specifiers produce.
monaco.languages.typescript.typescriptDefaults.setCompilerOptions({
  target: monaco.languages.typescript.ScriptTarget.ESNext,
  module: monaco.languages.typescript.ModuleKind.ESNext,
  moduleResolution: monaco.languages.typescript.ModuleResolutionKind.NodeJs,
  allowNonTsExtensions: true,
  allowJs: true,
  // Match Deno/modern bundler semantics so default imports of CommonJS-typed
  // npm packages (e.g. `import _ from "lodash"`) resolve to their members.
  esModuleInterop: true,
  allowSyntheticDefaultImports: true,
  noEmit: true,
  lib: ["esnext", "dom"],
  // Mirror the worker `deno.json` import map's `@lib/` alias so that
  // `import { fmt } from "@lib/money.ts"` resolves to the shared-library models
  // registered at `file:///lib/*` (see `registerModels`).
  baseUrl: "file:///",
  paths: { "@lib/*": ["lib/*"] },
});
monaco.languages.typescript.typescriptDefaults.setDiagnosticsOptions({
  diagnosticCodesToIgnore: [
    2307, // Cannot find module '...' (Deno URL / npm: specifiers)
    2792, // Cannot find module — consider moduleResolution
    1375, // Top-level await "needs imports/exports": Deno runs every file as an
    //       ES module, so top-level await is always valid here.
  ],
});

// --- IntelliSense: ambient Deno types --------------------------------------
// Worker and main.ts code runs on Deno, so `Deno.env`, `Deno.readDir`,
// `Deno.serve`, etc. must resolve. Fetch the embedded Deno namespace types once
// and register them as a Monaco extra-lib. Fully offline (served by the gateway
// from its embedded copy of `deno types`).
let denoLibRegistered = false;
async function ensureDenoLib(): Promise<void> {
  if (denoLibRegistered) return;
  denoLibRegistered = true;
  try {
    const res = await fetch("/console/api/deno-types");
    if (!res.ok) {
      denoLibRegistered = false;
      return;
    }
    const src = await res.text();
    monaco.languages.typescript.typescriptDefaults.addExtraLib(
      src,
      "file:///node_modules/@types/deno/index.d.ts",
    );
  } catch {
    denoLibRegistered = false; // let a later mount retry
  }
}

// --- IntelliSense: the @nanobpm/worker SDK ---------------------------------
// Fetch the embedded SDK source once and register it under a node_modules path
// so a bare `import { defineWorker } from "@nanobpm/worker"` resolves with full
// types, signatures, and JSDoc. Fully offline (served by the gateway).
let sdkLibRegistered = false;
async function ensureSdkLib(): Promise<void> {
  if (sdkLibRegistered) return;
  sdkLibRegistered = true;
  try {
    const res = await fetch("/console/api/worker-sdk");
    if (!res.ok) {
      sdkLibRegistered = false;
      return;
    }
    const src = await res.text();
    monaco.languages.typescript.typescriptDefaults.addExtraLib(
      src,
      "file:///node_modules/@nanobpm/worker/index.ts",
    );
  } catch {
    sdkLibRegistered = false; // let a later mount retry
  }
}

// --- IntelliSense: Automatic Type Acquisition (npm packages) ---------------
// Scan the edited source for imported packages and fetch their .d.ts from the
// jsdelivr CDN, registering each as a Monaco extra-lib — so importing e.g.
// `@camunda8/orchestration-cluster-api` (or any npm package) lights up with
// full IntelliSense. This is a *progressive enhancement*: it needs network
// access from the browser. The editor and the SDK types above stay offline.
type AtaRun = (code: string) => void;
let ataPromise: Promise<AtaRun | null> | null = null;
function ensureAta(): Promise<AtaRun | null> {
  if (!ataPromise) {
    ataPromise = (async () => {
      try {
        // typescript is multi-MB; load it (and ATA) in their own async chunk,
        // only the first time a TS/JS file is edited.
        const [ata, tsmod] = await Promise.all([
          import("@typescript/ata"),
          import("typescript"),
        ]);
        const ts = (tsmod as unknown as { default?: unknown }).default ?? tsmod;
        return ata.setupTypeAcquisition({
          projectName: "nano-workers",
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          typescript: ts as any,
          delegate: {
            receivedFile: (code: string, path: string) => {
              monaco.languages.typescript.typescriptDefaults.addExtraLib(
                code,
                `file://${path}`,
              );
            },
          },
        });
      } catch {
        return null; // offline or load failure — editor still works
      }
    })();
  }
  return ataPromise;
}

function acquireTypes(code: string): void {
  void ensureAta().then((run) => run?.(code));
}

export function languageForFile(file: string): string {
  if (file.endsWith(".json")) return "json";
  if (file.endsWith(".js") || file.endsWith(".mjs") || file.endsWith(".cjs")) return "javascript";
  if (file.endsWith(".rs")) return "rust";
  if (file.endsWith(".toml") || file.endsWith(".lock")) return "ini";
  if (file.endsWith(".md")) return "markdown";
  return "typescript";
}

// --- Cross-file IntelliSense: sibling + shared-library models --------------
// A worker is a folder of files, and shared logic lives under `@lib/…`. For the
// TS service to resolve `import "./helper.ts"` or `import "@lib/money.ts"`, the
// *imported* files must exist as Monaco models — not just the one in the active
// tab. We register each as a background model at its `file:///…` URI (creating
// it, or updating its text if it already exists). The active editing model is
// owned by <Editor> via its `path`; we never touch or dispose that one.
export type ExtraModel = { path: string; content: string };

function registerModels(models: ExtraModel[], activePath?: string): void {
  for (const m of models) {
    if (m.path === activePath) continue;
    const uri = monaco.Uri.parse(m.path);
    const existing = monaco.editor.getModel(uri);
    if (existing) {
      if (existing.getValue() !== m.content) existing.setValue(m.content);
    } else {
      const file = m.path.split("/").pop() ?? m.path;
      monaco.editor.createModel(m.content, languageForFile(file), uri);
    }
  }
}

export default function CodeEditor({
  value,
  language,
  path,
  readOnly,
  extraModels,
  onChange,
  onSave,
}: {
  value: string;
  language: string;
  /** Virtual file path (a `file:///…` URI) so module resolution + the SDK and
   * acquired npm types resolve from the editing model. */
  path?: string;
  readOnly?: boolean;
  /** Other files visible to module resolution (sibling worker files + shared
   * `@lib/` modules), registered as background Monaco models so cross-file and
   * `@lib/…` imports type-check and complete. */
  extraModels?: ExtraModel[];
  onChange: (value: string) => void;
  onSave?: () => void;
}) {
  // Monaco is theme-managed per instance; follow the console's resolved
  // appearance (light/dark/system/theme packs all reduce to one of the two).
  const { appearance } = useTheme();

  // Keep the latest onSave in a ref so the Cmd/Ctrl+S keybinding registered on
  // mount always calls the current handler (avoids a stale closure).
  const saveRef = useRef(onSave);
  saveRef.current = onSave;

  // Debounce type acquisition so we don't refetch on every keystroke.
  const ataTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const isCode = language === "typescript" || language === "javascript";
  const scheduleAta = (code: string) => {
    if (!isCode) return;
    clearTimeout(ataTimer.current);
    ataTimer.current = setTimeout(() => acquireTypes(code), 600);
  };

  // Keep sibling/library models in sync so cross-file resolution sees current
  // text. Serialized so the effect re-runs whenever any imported file changes.
  const extraKey = JSON.stringify(extraModels ?? []);
  useEffect(() => {
    if (extraModels?.length) registerModels(extraModels, path);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [extraKey, path]);

  useEffect(() => {
    void ensureSdkLib();
    void ensureDenoLib();
    if (isCode) acquireTypes(value);
    return () => clearTimeout(ataTimer.current);
    // Re-run when switching to a different file/value.
  }, [path, isCode, value]);

  return (
    <Editor
      value={value}
      language={language}
      path={path}
      theme={appearance === "light" ? "light" : "vs-dark"}
      onChange={(v) => {
        const next = v ?? "";
        onChange(next);
        scheduleAta(next);
      }}
      onMount={(editor) => {
        editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => saveRef.current?.());
      }}
      loading={<div className="p-4 text-sm text-fg-faint">Loading editor…</div>}
      options={{
        readOnly,
        fontSize: 13,
        tabSize: 2,
        insertSpaces: true,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        automaticLayout: true,
        smoothScrolling: true,
        renderWhitespace: "none",
        fixedOverflowWidgets: true,
      }}
    />
  );
}
