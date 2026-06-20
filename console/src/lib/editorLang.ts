// Map a worker file name to a Monaco language id. Kept in its own module (free
// of any `monaco-editor` import) so views can call it without dragging the
// multi-MB Monaco bundle out of its lazy-loaded chunk.
export function languageForFile(file: string): string {
  if (file.endsWith(".json") || file.endsWith(".lock")) return "json";
  if (file.endsWith(".js") || file.endsWith(".mjs") || file.endsWith(".cjs")) return "javascript";
  if (file.endsWith(".md")) return "markdown";
  return "typescript";
}
