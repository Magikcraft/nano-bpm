// Determinism guard for the `assertThat*` DSL (issue Magikcraft/nano-bpm#912).
//
// Ported from urban-testkit's `src/assert/determinism-guard.test.ts` when the
// Tier-A matcher source was lifted here (#894 / #895 / nanobpm/nano-ide#402) but
// its guards were left behind — retargeted at engine-testkit's own `src/**`.
//
// The whole assertion DSL MUST be a pure function of `snapshot()` / the user-task
// read model — never of the wall-clock or an entropy source. This guard scans
// every IMPLEMENTATION file under `src/**` (the `*.ts` files that are not
// themselves tests) and fails if any of the forbidden non-deterministic APIs
// appear in real code:
//
//   • `Date.now`            — wall-clock read
//   • `setTimeout` / `setInterval` — real-time scheduling / polling
//   • `Math.random`         — entropy
//   • `performance.now`     — high-resolution wall-clock read
//
// Comments (which legitimately mention these tokens when documenting the
// determinism contract — every matcher carries such a header) are stripped before
// scanning, so only genuine code usage turns the guard red. Adding a
// wall-clock/random dependency to the DSL later — in any matcher or shared helper
// — flips this test to failing, which is the whole point: determinism is
// enforced, not merely documented.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

/** The directory holding the DSL implementation + this guard. Derived from the
 *  module URL via `fileURLToPath`/`dirname` (widely-supported ESM primitives)
 *  rather than the Node-specific `import.meta.dirname`, keeping the derivation on
 *  standard ESM URL semantics rather than a runtime-specific extension. */
const SRC_DIR = dirname(fileURLToPath(import.meta.url));

/** The forbidden non-deterministic APIs, as they appear in code. */
const FORBIDDEN: readonly { readonly token: string; readonly why: string }[] = [
  { token: "Date.now", why: "wall-clock read" },
  { token: "setTimeout", why: "real-time scheduling / polling" },
  { token: "setInterval", why: "real-time scheduling / polling" },
  { token: "Math.random", why: "entropy source" },
  { token: "performance.now", why: "high-resolution wall-clock read" },
];

/** Build a whitespace-tolerant matcher for a (possibly dotted) forbidden token.
 *  `stripComments()` replaces a comment with a single space and preserves
 *  newlines, so a bypass like `Date/* x *​/.now()` collapses to `Date .now()` and
 *  `Date// x\n.now()` to `Date \n.now()`. A literal `source.includes("Date.now")`
 *  would miss both, undermining the guard — so we match each dotted segment with
 *  `\s*\.\s*` between the parts, catching any whitespace (or stripped comment)
 *  wedged between the identifier, the dot, and the property. Single-identifier
 *  tokens (`setTimeout`, `setInterval`) carry no dot and stay literal. */
function forbiddenPattern(token: string): RegExp {
  const escape = (part: string) => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(token.split(".").map(escape).join("\\s*\\.\\s*"));
}

/** The forbidden tokens paired with their whitespace-tolerant scan patterns. */
const FORBIDDEN_SCAN: readonly {
  readonly token: string;
  readonly why: string;
  readonly pattern: RegExp;
}[] = FORBIDDEN.map(({ token, why }) => ({ token, why, pattern: forbiddenPattern(token) }));

/** Remove line comments and block comments so a token mentioned only in prose
 *  (e.g. the determinism-contract header every matcher carries) does not trip the
 *  guard. String literals are copied verbatim: a `//` or `/*` that lives INSIDE a
 *  string is string CONTENT, never a comment start (stripping it would let a
 *  forbidden call after it slip through the scan). Template literals copy their
 *  text spans verbatim for the same reason, but a `${…}` interpolation is real
 *  CODE, so we recurse into it — stripping comments and handling nested
 *  strings/templates there too. A comment wedged inside an interpolation (e.g.
 *  `` `${Date/* x *​/.now()}` ``) is therefore removed, so the whitespace-tolerant
 *  scan pattern still catches the forbidden call rather than being defeated by a
 *  `/*…*​/` the scan can't span. */
function stripComments(source: string): string {
  const n = source.length;

  // Process code from `start`. Comments are stripped; string literals copied
  // verbatim; template literals recurse into their `${…}` interpolations. When
  // `inInterpolation` is true we are scanning an interpolation body and stop at
  // (without consuming) the matching `}` that closes it, tracking nested braces.
  function processCode(start: number, inInterpolation: boolean): { out: string; end: number } {
    let out = "";
    let i = start;
    let depth = 0;
    while (i < n) {
      const c = source[i];
      const next = source[i + 1];
      if (c === "/" && next === "/") {
        i += 2;
        while (i < n && source[i] !== "\n") i++;
        out += " ";
        continue;
      }
      if (c === "/" && next === "*") {
        i += 2;
        while (i < n && !(source[i] === "*" && source[i + 1] === "/")) i++;
        i += 2;
        out += " ";
        continue;
      }
      if (c === '"' || c === "'") {
        const quote = c;
        out += c;
        i++;
        while (i < n) {
          const d = source[i];
          out += d;
          if (d === "\\") {
            if (i + 1 < n) out += source[i + 1];
            i += 2;
            continue;
          }
          i++;
          if (d === quote) break;
        }
        continue;
      }
      if (c === "`") {
        out += c;
        i++;
        const t = processTemplate(i);
        out += t.out;
        i = t.end;
        continue;
      }
      if (inInterpolation) {
        if (c === "{") {
          depth++;
        } else if (c === "}") {
          if (depth === 0) return { out, end: i };
          depth--;
        }
      }
      out += c;
      i++;
    }
    return { out, end: i };
  }

  // Process a template-literal body (after the opening backtick). Text spans are
  // copied verbatim; each `${…}` interpolation recurses through `processCode`.
  function processTemplate(start: number): { out: string; end: number } {
    let out = "";
    let i = start;
    while (i < n) {
      const c = source[i];
      if (c === "\\") {
        out += c;
        if (i + 1 < n) out += source[i + 1];
        i += 2;
        continue;
      }
      if (c === "`") {
        out += c;
        i++;
        return { out, end: i };
      }
      if (c === "$" && source[i + 1] === "{") {
        out += "${";
        i += 2;
        const body = processCode(i, true);
        out += body.out;
        i = body.end;
        if (i < n && source[i] === "}") {
          out += "}";
          i++;
        }
        continue;
      }
      out += c;
      i++;
    }
    return { out, end: i };
  }

  return processCode(0, false).out;
}

/** Source files EXCLUDED from the published build (`tsconfig.build.json`'s
 *  `exclude`) are not shipped implementation — e.g. the test-only `fixtures.ts`
 *  fake engine — so the determinism guard must not scan them: a test helper must
 *  not gate the shipped DSL. Derived from `tsconfig.build.json` so the exclusion
 *  tracks the build config's single source of truth rather than duplicating a
 *  hard-coded file list. Returns paths relative to `SRC_DIR` (matching
 *  `implementationFiles`). Glob entries (e.g. `**\/*.test.ts`) are left to the
 *  `.test.ts` filter in `implementationFiles`; only concrete `src/<file>.ts`
 *  entries are collected here. */
async function nonShippedSources(): Promise<Set<string>> {
  const cfg = JSON.parse(
    await readFile(join(SRC_DIR, "..", "tsconfig.build.json"), "utf8"),
  ) as { exclude?: string[] };
  const excluded = new Set<string>();
  for (const pattern of cfg.exclude ?? []) {
    if (pattern.startsWith("src/") && pattern.endsWith(".ts") && !pattern.includes("*")) {
      excluded.add(pattern.slice("src/".length));
    }
  }
  return excluded;
}

/** The implementation files under `src/**`: every `.ts` that is not a test and
 *  not a `.d.ts`. Walks subdirectories recursively so nested helpers (e.g.
 *  `src/utils/*.ts`) added later stay covered by the guard. Paths are returned
 *  relative to `SRC_DIR` (top-level files keep their bare name). */
async function implementationFiles(dir: string = SRC_DIR, prefix = ""): Promise<string[]> {
  const entries = await readdir(dir, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const rel = prefix ? join(prefix, entry.name) : entry.name;
    if (entry.isDirectory()) {
      files.push(...(await implementationFiles(join(dir, entry.name), rel)));
    } else if (
      entry.isFile() &&
      entry.name.endsWith(".ts") &&
      !entry.name.endsWith(".test.ts") &&
      !entry.name.endsWith(".d.ts")
    ) {
      files.push(rel);
    }
  }
  return files.sort();
}

test("the assertion DSL implementation scans clean of wall-clock / entropy APIs", async () => {
  const excluded = await nonShippedSources();
  const files = (await implementationFiles()).filter((name) => !excluded.has(name));
  // Sanity: the scan must actually cover the shipped DSL, or a clean result is
  // meaningless. The lifted Tier-A matcher/implementation sources.
  assert.ok(files.length >= 6, `expected to scan the DSL implementation files, found ${files.join(", ")}`);
  for (const expected of ["instance.ts", "user-task.ts", "selectors.ts", "format.ts", "port.ts", "state.ts"]) {
    assert.ok(files.includes(expected), `determinism guard must scan ${expected}`);
  }

  const offenders: string[] = [];
  for (const name of files) {
    const source = stripComments(await readFile(join(SRC_DIR, name), "utf8"));
    for (const { token, why, pattern } of FORBIDDEN_SCAN) {
      if (pattern.test(source)) {
        offenders.push(`${name}: uses \`${token}\` (${why})`);
      }
    }
  }

  assert.deepEqual(
    offenders,
    [],
    `The assertThat* DSL must stay deterministic — no wall-clock or entropy APIs — but found:\n${offenders.join("\n")}`,
  );
});

test("the comment stripper does not mask a real forbidden call", () => {
  // Guards the guard: a token used in CODE (not a comment) must still be caught,
  // even when the same file documents the token in prose.
  const disguised = [
    "// This matcher never calls Date.now or Math.random.",
    "/* setInterval is forbidden here. */",
    "const t = setTimeout(fn, 1000); // schedule",
  ].join("\n");
  const stripped = stripComments(disguised);
  assert.ok(!stripped.includes("Math.random"), "prose mention of Math.random should be stripped");
  assert.ok(stripped.includes("setTimeout"), "a real setTimeout call must survive comment stripping");
});

test("the comment stripper does not treat `//` inside a string literal as a comment", () => {
  // Regression guard for the defect class: a `//` (or `/*`) that lives inside a
  // string or template literal is string CONTENT, not a comment start. Treating
  // it as a comment would strip the rest of the line and hide any forbidden call
  // after it, silently bypassing the determinism guard.
  const inString = 'const s = "foo//bar"; const t = Date.now();';
  assert.ok(
    stripComments(inString).includes("Date.now"),
    "a forbidden call after an in-string `//` must survive stripping",
  );

  const inTemplate = "const u = `x/*y`; const r = Math.random();";
  assert.ok(
    stripComments(inTemplate).includes("Math.random"),
    "a forbidden call after an in-template `/*` must survive stripping",
  );

  const interpolated = "const v = `${Date.now()}`;";
  assert.ok(
    stripComments(interpolated).includes("Date.now"),
    "a forbidden call inside a template `${…}` interpolation must stay visible",
  );

  // A comment INSIDE an interpolation is real code being commented, not template
  // text — it must be stripped, or `Date/* x */.now()` would defeat the
  // whitespace-tolerant scan (which cannot span a `/*…*/`).
  const interpComment = "const w = `${Date/* sneaky */.now()}`;";
  const strippedInterp = stripComments(interpComment);
  assert.ok(
    !strippedInterp.includes("/* sneaky */"),
    "a comment inside a template `${…}` interpolation must be stripped",
  );
  assert.ok(
    FORBIDDEN_SCAN.some(({ pattern }) => pattern.test(strippedInterp)),
    "a forbidden call comment-obscured inside a `${…}` interpolation must be detected",
  );
});

test("a forbidden call obscured by whitespace/comments across the dot is still caught", () => {
  // Defect class: `stripComments()` replaces a comment with a space and preserves
  // newlines, so `Date/*x*​/.now()` collapses to `Date .now()` and `Date// x\n.now()`
  // to `Date \n.now()`. A literal `includes("Date.now")` would miss both, silently
  // bypassing the determinism guard — the whitespace-tolerant scan pattern must
  // catch them.
  const bypasses = [
    "Date/* sneaky */.now()",
    "Date .now()",
    "Date\n.now()",
    "performance/**/.now()",
    "Math . random()",
    "`${Date/* sneaky */.now()}`",
  ];
  for (const raw of bypasses) {
    const stripped = stripComments(raw);
    assert.ok(
      FORBIDDEN_SCAN.some(({ pattern }) => pattern.test(stripped)),
      `whitespace/comment-separated forbidden call must be detected: ${JSON.stringify(raw)}`,
    );
  }
});
