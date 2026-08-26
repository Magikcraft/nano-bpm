// Drift guard for the console theme palette (issue #1005) — build-time check that
// the console and `@nanobpm/nano-app-schema` cannot disagree on the token
// vocabulary or the palette. Run with `node --experimental-strip-types --test`.
//
// The `--nano-*` palette used to be byte-duplicated: inline in
// `theme/tokens.css` here AND restated in the Urban runtime. The `nano-theme`
// postMessage bridge hid any divergence at runtime; nothing caught it at build
// time. Now the palette lives once in the schema package (imported as CSS here,
// inlined by Urban from `./tokens`). This test is the guard that closes the loop:
// it fails the console build if the console's theming contract ever drifts from
// the schema's, or if palette hex creeps back into the console CSS.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join, relative } from "node:path";
import {
  TOKEN_KEYS as SCHEMA_TOKEN_KEYS,
  NANO_PALETTE,
  tokenCssVar,
} from "@nanobpm/nano-app-schema/tokens";
import { TOKEN_KEYS as CONSOLE_TOKEN_KEYS, cssVar } from "./themes.ts";

const themeDir = dirname(fileURLToPath(import.meta.url));

test("console TOKEN_KEYS match the schema's token vocabulary (order included)", () => {
  assert.deepEqual(
    [...CONSOLE_TOKEN_KEYS],
    [...SCHEMA_TOKEN_KEYS],
    "themes.ts TOKEN_KEYS drifted from @nanobpm/nano-app-schema — reconcile them",
  );
});

test("console cssVar() names the same custom properties as the schema", () => {
  for (const key of CONSOLE_TOKEN_KEYS) {
    assert.equal(
      cssVar(key),
      tokenCssVar(key),
      `cssVar(${key}) drifted from the schema`,
    );
  }
});

test("schema palette defines every console token for both appearances", () => {
  for (const appearance of ["dark", "light"] as const) {
    for (const key of CONSOLE_TOKEN_KEYS) {
      assert.match(
        NANO_PALETTE[appearance][key],
        /^#[0-9a-f]{6}$/,
        `schema ${appearance} palette is missing a value for ${key}`,
      );
    }
  }
});

test("console CSS inlines no --nano-* palette hex (single source of truth)", () => {
  // Every .css under console/src must only MAP tokens (var() references) or
  // consume/import them. A `--nano-*: #rrggbb` assignment in ANY of them means the
  // palette forked from the schema again. Scanning the whole tree (rather than a
  // hard-coded file list) keeps the guard correct as new CSS files are added.
  const inlineHex = /--nano-[a-z0-9-]+\s*:\s*#[0-9a-fA-F]{3,8}/;
  const srcDir = join(themeDir, "..");
  const cssFiles = readdirSync(srcDir, { recursive: true, encoding: "utf8" })
    .filter((entry) => entry.endsWith(".css"));
  assert.ok(cssFiles.length > 0, "no .css files found under console/src");
  for (const file of cssFiles) {
    const css = readFileSync(join(srcDir, file), "utf8");
    assert.ok(
      !inlineHex.test(css),
      `${relative(themeDir, join(srcDir, file))} inlines --nano-* hex — import @nanobpm/nano-app-schema/tokens.css instead`,
    );
  }
});
