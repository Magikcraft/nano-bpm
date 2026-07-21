// Monaco IntelliSense providers fed by pack-contributed data (see
// lib/langIntellisense.ts). This module imports `monaco-editor`, so it must only
// be pulled in from the lazily-loaded editor chunk (CodeEditor.tsx) — never from
// App-level code — to keep Monaco out of the initial bundle.
//
// TS/JS get Monaco's real language service; every other language relies on the
// curated, SDK-derived data a lang pack ships. We register one
// completion/hover/signature provider per contributed language, each reading the
// shared store live so a pack installed mid-session lights up without a reload.

import * as monaco from "monaco-editor";
import {
  getIntellisense,
  intellisenseLangs,
  subscribeIntellisense,
} from "./langIntellisense";

export function completionKind(
  kind: string | undefined,
): monaco.languages.CompletionItemKind {
  const K = monaco.languages.CompletionItemKind;
  switch (kind) {
    case "keyword":
      return K.Keyword;
    case "snippet":
      return K.Snippet;
    case "function":
      return K.Function;
    case "method":
      return K.Method;
    case "class":
      return K.Class;
    case "struct":
      return K.Struct;
    case "interface":
      return K.Interface;
    case "enum":
      return K.Enum;
    case "module":
      return K.Module;
    case "property":
      return K.Property;
    case "field":
      return K.Field;
    case "variable":
      return K.Variable;
    case "constant":
      return K.Constant;
    default:
      return K.Value;
  }
}

// Find the innermost enclosing call at the end of `text`: the identifier before
// the nearest still-open "(" and the number of top-level argument separators
// after it (the active-parameter index). Returns null when not inside a call.
export function enclosingCall(
  text: string,
): { name: string; arg: number } | null {
  let depth = 0;
  let commas = 0;
  let i = text.length - 1;
  for (; i >= 0; i--) {
    const ch = text[i];
    if (ch === ")" || ch === "]" || ch === "}") depth++;
    else if (ch === "[" || ch === "{") depth--;
    else if (ch === "(") {
      if (depth === 0) break;
      depth--;
    } else if (ch === "," && depth === 0) commas++;
  }
  if (i < 0) return null;
  let j = i - 1;
  while (j >= 0 && /\s/.test(text[j])) j--;
  const end = j + 1;
  while (j >= 0 && /[A-Za-z0-9_]/.test(text[j])) j--;
  const name = text.slice(j + 1, end);
  if (!name) return null;
  return { name, arg: commas };
}

const providersByLang = new Set<string>();

function registerProvidersForLang(lang: string): void {
  // TS/JS already have a full language service; never shadow it.
  if (lang === "typescript" || lang === "javascript") return;
  if (providersByLang.has(lang)) return;
  const initial = getIntellisense(lang);
  if (!initial) return;
  providersByLang.add(lang);

  monaco.languages.registerCompletionItemProvider(lang, {
    triggerCharacters: initial.triggerCharacters,
    provideCompletionItems(model, position) {
      const merged = getIntellisense(lang);
      if (!merged) return { suggestions: [] };
      const word = model.getWordUntilPosition(position);
      const range = new monaco.Range(
        position.lineNumber,
        word.startColumn,
        position.lineNumber,
        word.endColumn,
      );
      const suggestions = merged.completions.map((c) => ({
        label: c.label,
        kind: completionKind(c.kind),
        insertText: c.insertText ?? c.label,
        insertTextRules: c.snippet
          ? monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
          : undefined,
        detail: c.detail,
        documentation: c.documentation ? { value: c.documentation } : undefined,
        range,
      }));
      return { suggestions };
    },
  });

  monaco.languages.registerHoverProvider(lang, {
    provideHover(model, position) {
      const merged = getIntellisense(lang);
      const word = model.getWordAtPosition(position);
      if (!merged || !word) return null;
      const contents = merged.hovers.get(word.word);
      if (!contents) return null;
      return { contents: [{ value: contents }] };
    },
  });

  monaco.languages.registerSignatureHelpProvider(lang, {
    signatureHelpTriggerCharacters: ["(", ","],
    signatureHelpRetriggerCharacters: [","],
    provideSignatureHelp(model, position) {
      const merged = getIntellisense(lang);
      if (!merged) return null;
      const textUntil = model.getValueInRange({
        startLineNumber: 1,
        startColumn: 1,
        endLineNumber: position.lineNumber,
        endColumn: position.column,
      });
      const call = enclosingCall(textUntil);
      if (!call) return null;
      const sigs = merged.signatures.get(call.name);
      if (!sigs || !sigs.length) return null;
      return {
        value: {
          signatures: sigs.map((s) => ({
            label: s.label,
            documentation: s.documentation
              ? { value: s.documentation }
              : undefined,
            parameters: (s.parameters ?? []).map((p) => ({
              label: p.label,
              documentation: p.documentation
                ? { value: p.documentation }
                : undefined,
            })),
          })),
          activeSignature: 0,
          activeParameter: call.arg,
        },
        dispose() {},
      };
    },
  });
}

function syncIntellisenseProviders(): void {
  for (const lang of intellisenseLangs()) registerProvidersForLang(lang);
}

let initialised = false;

/// Register pack-fed IntelliSense providers for every contributed language, and
/// keep registering for any languages a mid-session pack install adds. Safe to
/// call more than once (subsequent calls are no-ops beyond a resync).
export function initMonacoIntellisense(): void {
  syncIntellisenseProviders();
  if (initialised) return;
  initialised = true;
  subscribeIntellisense(syncIntellisenseProviders);
}
