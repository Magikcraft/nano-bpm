import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { projectsApi } from "../lib/api";
import {
  applySelection,
  isThemeSpec,
  loadImportedThemes,
  loadSelection,
  saveImportedThemes,
  saveSelection,
  type ThemeSelection,
  type ThemeSpec,
} from "./themes";

interface ThemeContextValue {
  selection: ThemeSelection;
  /** Resolved appearance of whatever is active — drives Monaco etc. */
  appearance: "light" | "dark";
  /** Loadable themes from installed extension packs. */
  packThemes: ThemeSpec[];
  /** Loadable themes the user imported as JSON. */
  importedThemes: ThemeSpec[];
  select: (sel: ThemeSelection) => void;
  /** Parse + store a theme JSON string; returns an error message or null. */
  importTheme: (json: string) => string | null;
  removeImportedTheme: (id: string) => void;
}

const ThemeContext = createContext<ThemeContextValue | null>(null);

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [selection, setSelection] = useState<ThemeSelection>(loadSelection);
  const [packThemes, setPackThemes] = useState<ThemeSpec[]>([]);
  const [importedThemes, setImportedThemes] = useState<ThemeSpec[]>(loadImportedThemes);
  const [appearance, setAppearance] = useState<"light" | "dark">(
    document.documentElement.dataset.appearance === "light" ? "light" : "dark",
  );

  // Themes contributed by installed `kind: "theme"` extension packs.
  useEffect(() => {
    projectsApi
      .extensions()
      .then((ov) =>
        setPackThemes(ov.extensions.flatMap((e) => (e.themes ?? []).filter(isThemeSpec))),
      )
      .catch(() => {}); // offline/dev — built-ins and imports still work
  }, []);

  const resolveTheme = useCallback(
    (id: string) =>
      packThemes.find((t) => t.id === id) ?? importedThemes.find((t) => t.id === id) ?? null,
    [packThemes, importedThemes],
  );

  // (Re-)apply on any change; track OS appearance while in system mode.
  useEffect(() => {
    setAppearance(applySelection(selection, resolveTheme));
    if (selection.mode !== "system") return;
    const mq = window.matchMedia("(prefers-color-scheme: light)");
    const onChange = () => setAppearance(applySelection(selection, resolveTheme));
    mq.addEventListener("change", onChange);
    return () => mq.removeEventListener("change", onChange);
  }, [selection, resolveTheme]);

  const select = useCallback((sel: ThemeSelection) => {
    saveSelection(sel);
    setSelection(sel);
  }, []);

  const importTheme = useCallback(
    (json: string): string | null => {
      let spec: unknown;
      try {
        spec = JSON.parse(json);
      } catch (e) {
        return `Not valid JSON: ${e instanceof Error ? e.message : e}`;
      }
      if (!isThemeSpec(spec)) {
        return 'Not a theme: expected { id, label, appearance: "light"|"dark", tokens: {…} }.';
      }
      const next = [...importedThemes.filter((t) => t.id !== spec.id), spec];
      saveImportedThemes(next);
      setImportedThemes(next);
      select({ mode: "theme", id: spec.id });
      return null;
    },
    [importedThemes, select],
  );

  const removeImportedTheme = useCallback(
    (id: string) => {
      const next = importedThemes.filter((t) => t.id !== id);
      saveImportedThemes(next);
      setImportedThemes(next);
      if (selection.mode === "theme" && selection.id === id) select({ mode: "system" });
    },
    [importedThemes, selection, select],
  );

  const value = useMemo(
    () => ({
      selection,
      appearance,
      packThemes,
      importedThemes,
      select,
      importTheme,
      removeImportedTheme,
    }),
    [selection, appearance, packThemes, importedThemes, select, importTheme, removeImportedTheme],
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme(): ThemeContextValue {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error("useTheme must be used within ThemeProvider");
  return ctx;
}
