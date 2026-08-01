// nanobpmn Node ESM loader (ADR 0036).
//
// Makes Node honor the project's `deno.json` import map so worker/App code
// authored for Deno runs unchanged under Node — the fallback worker runtime on
// hosts with no Deno build (e.g. 32-bit ARM). Node's built-in type stripping
// (`--experimental-strip-types`) handles the `.ts` sources; this loader only
// rewrites *specifiers*. Registered off-thread by `node-register.mjs`.
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { resolve as resolvePath } from "node:path";

// The run's working directory is the directory that holds the `deno.json` whose
// import map applies — the project root for a RAD project run (`main.ts`), or the
// worker directory for a standalone worker (`worker.ts`). Both the map file and
// its relative values are resolved against it.
const baseDir = process.cwd();

function readImportMap() {
  for (const name of ["deno.json", "deno.jsonc"]) {
    try {
      const raw = readFileSync(resolvePath(baseDir, name), "utf8");
      return JSON.parse(raw).imports ?? {};
    } catch {
      // not present / unreadable — try the next candidate
    }
  }
  return {};
}
const MAP = readImportMap();

// Deno import maps support exact keys and "prefix" keys ending in `/`.
function mapSpecifier(spec) {
  if (Object.prototype.hasOwnProperty.call(MAP, spec)) return MAP[spec];
  for (const [key, val] of Object.entries(MAP)) {
    if (key.endsWith("/") && spec.startsWith(key)) return val + spec.slice(key.length);
  }
  return null;
}

export async function resolve(specifier, context, next) {
  const mapped = mapSpecifier(specifier);
  if (mapped == null) return next(specifier, context);

  if (mapped.startsWith("npm:")) {
    // Resolve a Deno `npm:[/]<name>[@<range>][/<subpath>]` specifier to the module
    // specifier Node resolves from `node_modules`: strip the scheme + optional
    // leading slash, DROP the version range, and KEEP the package name and any
    // subpath (`npm:lodash@^4/fp` -> `lodash/fp`). `npm install` provides the
    // package — its `package.json` dependency is derived from this SAME grammar by
    // `npm_dep_from_import` in projects.rs (keep the two in lockstep).
    const bare = npmBareSpecifier(mapped);
    // A degenerate value (`npm:`, `npm:/`) reduces to an empty specifier, which
    // Rust's `npm_dep_from_import` also rejects (returns None). Fail early with a
    // clear message instead of handing `next()` an empty string.
    if (!bare) {
      throw new Error(
        `Node worker runtime cannot resolve '${specifier}' -> '${mapped}': ` +
          `malformed npm: specifier (no package name).`,
      );
    }
    return next(bare, context);
  }
  if (mapped.startsWith("jsr:") || mapped.startsWith("http:") || mapped.startsWith("https:")) {
    throw new Error(
      `Node worker runtime cannot resolve '${specifier}' -> '${mapped}': ` +
        `jsr:/https: imports require Deno. Install Deno, or vendor the dependency via npm.`,
    );
  }
  // A relative path from the import map, resolved against the base dir.
  return next(pathToFileURL(resolvePath(baseDir, mapped)).href, context);
}

// Reduce an `npm:` import-map value to the bare `<name>[/<subpath>]` Node resolves
// from `node_modules`, dropping only the `@range`. Mirrors `npm_dep_from_import`
// (projects.rs) on name/range boundaries — that derives the installable package
// (name only); this derives the import specifier (name + subpath).
export function npmBareSpecifier(mapped) {
  let spec = mapped.slice(4); // drop "npm:"
  if (spec.startsWith("/")) spec = spec.slice(1); // optional leading slash
  // Where the package name ends: after `@scope/pkg` or `pkg`, at the `@` that
  // starts the range or the `/` that starts a subpath.
  let nameEnd;
  if (spec.startsWith("@")) {
    // Scoped `@scope/pkg`. Reject every degenerate form Rust's
    // `npm_dep_from_import` also rejects (→ None), so the loader throws a clear
    // error instead of handing `next()` an unresolvable specifier:
    //   `@scope` / `@`     — no scope/package separator;
    //   `@/pkg`            — empty scope;
    //   `@scope/`, `@scope/@1` — empty package segment.
    const scopeSlash = spec.indexOf("/", 1);
    if (scopeSlash <= 1) return ""; // no `/`, or empty scope (`@/pkg`)
    const after = spec.slice(scopeSlash + 1);
    const rel = after.search(/[/@]/);
    if (after === "" || rel === 0) return ""; // empty package (`@scope/`, `@scope/@1`)
    nameEnd = rel < 0 ? spec.length : scopeSlash + 1 + rel;
  } else {
    const rel = spec.search(/[/@]/);
    nameEnd = rel < 0 ? spec.length : rel;
  }
  const name = spec.slice(0, nameEnd);
  const rest = spec.slice(nameEnd); // "" | "@range" | "/subpath" | "@range/subpath"
  let subpath = "";
  if (rest.startsWith("@")) {
    const slash = rest.indexOf("/"); // range ends at the subpath boundary
    subpath = slash < 0 ? "" : rest.slice(slash);
  } else {
    subpath = rest; // "/subpath" or ""
  }
  return name + subpath;
}
