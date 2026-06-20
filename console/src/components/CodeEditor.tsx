import { useRef } from "react";
import * as monaco from "monaco-editor";
import { Editor, loader } from "@monaco-editor/react";

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
  noEmit: true,
  lib: ["esnext", "dom"],
});
monaco.languages.typescript.typescriptDefaults.setDiagnosticsOptions({
  diagnosticCodesToIgnore: [
    2307, // Cannot find module '...' (Deno URL / npm: specifiers)
    2792, // Cannot find module — consider moduleResolution
  ],
});

export function languageForFile(file: string): string {
  if (file.endsWith(".json") || file.endsWith(".lock")) return "json";
  if (file.endsWith(".js") || file.endsWith(".mjs") || file.endsWith(".cjs")) return "javascript";
  if (file.endsWith(".md")) return "markdown";
  return "typescript";
}

export default function CodeEditor({
  value,
  language,
  readOnly,
  onChange,
  onSave,
}: {
  value: string;
  language: string;
  readOnly?: boolean;
  onChange: (value: string) => void;
  onSave?: () => void;
}) {
  // Keep the latest onSave in a ref so the Cmd/Ctrl+S keybinding registered on
  // mount always calls the current handler (avoids a stale closure).
  const saveRef = useRef(onSave);
  saveRef.current = onSave;

  return (
    <Editor
      value={value}
      language={language}
      theme="vs-dark"
      onChange={(v) => onChange(v ?? "")}
      onMount={(editor) => {
        editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => saveRef.current?.());
      }}
      loading={<div className="p-4 text-sm text-zinc-500">Loading editor…</div>}
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
