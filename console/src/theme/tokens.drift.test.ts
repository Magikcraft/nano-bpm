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
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
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
  // theme/tokens.css must only MAP tokens (var() references); index.css must only
  // consume/import them. A `--nano-*: #rrggbb` assignment anywhere here means the
  // palette forked from the schema again.
  const inlineHex = /--nano-[a-z0-9-]+\s*:\s*#[0-9a-fA-F]{3,8}/;
  for (const file of ["tokens.css", join("..", "index.css")]) {
    const css = readFileSync(join(themeDir, file), "utf8");
    assert.ok(
      !inlineHex.test(css),
      `${file} inlines --nano-* hex — import @nanobpm/nano-app-schema/tokens.css instead`,
    );
  }
});
