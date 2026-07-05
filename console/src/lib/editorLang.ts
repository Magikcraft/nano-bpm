// Map a worker file name to a Monaco language id. Kept in its own module (free
// of any `monaco-editor` import) so views can call it without dragging the
// multi-MB Monaco bundle out of its lazy-loaded chunk.
//
// Two sources feed the mapping:
// - A static table below for the runtimes we've always shipped (Deno/TS, Rust)
//   and the common companions we don't want to be silent about (Java, XML,
//   YAML, Python, Go, C/C++, Shell, Dockerfile). Monaco's default bundle
//   already ships tokenisers for all of these via basic-languages; the map
//   just wires the file extension to the language id.
// - Installed lang packs' `fileTypes[]`, registered at runtime by
//   `registerFileTypes()`. Views that fetch `/console/api/extensions` should
//   call this once so `.java` etc. light up as soon as the pack is loaded —
//   without it, editing a `.java` file falls back to "typescript" and Monaco
//   paints nonsense highlighting on Java source.

const STATIC_EXT_TO_LANG: Record<string, string> = {
  ".json": "json",
  ".lock": "json",
  ".js": "javascript",
  ".mjs": "javascript",
  ".cjs": "javascript",
  ".ts": "typescript",
  ".tsx": "typescript",
  ".rs": "rust",
  ".toml": "ini",
  ".md": "markdown",
  ".java": "java",
  ".xml": "xml",
  ".yaml": "yaml",
  ".yml": "yaml",
  ".py": "python",
  ".go": "go",
  ".c": "c",
  ".h": "c",
  ".cpp": "cpp",
  ".hpp": "cpp",
  ".sh": "shell",
  ".bash": "shell",
  ".html": "html",
  ".css": "css",
  ".scss": "scss",
};

const dynamicExtToLang = new Map<string, string>();

/// Register file-type -> Monaco language mappings from installed extension
/// packs. Later calls override earlier ones for the same extension so a pack
/// can shadow a stale static default. `ext` is a leading-dot suffix (".java").
export function registerFileTypes(
  fileTypes: ReadonlyArray<{ ext: string; monacoLang: string }>,
): void {
  for (const ft of fileTypes) {
    if (!ft?.ext || !ft?.monacoLang) continue;
    const key = ft.ext.startsWith(".") ? ft.ext : `.${ft.ext}`;
    dynamicExtToLang.set(key.toLowerCase(), ft.monacoLang);
  }
}

export function languageForFile(file: string): string {
  const dot = file.lastIndexOf(".");
  if (dot >= 0) {
    const ext = file.slice(dot).toLowerCase();
    const dyn = dynamicExtToLang.get(ext);
    if (dyn) return dyn;
    const stat = STATIC_EXT_TO_LANG[ext];
    if (stat) return stat;
  }
  // Files without any known extension fall through to TypeScript because the
  // console's original worker surface was Deno-only and unmarked files were
  // always TS. Kept for back-compat; anything with a real extension is
  // routed via the tables above.
  return "typescript";
}
