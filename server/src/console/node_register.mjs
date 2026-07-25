// nanobpmn Node loader bootstrap (ADR 0036).
//
// Registers the import-map loader (node-loader.mjs) on the module resolution
// thread. Passed to Node via `--import ./nano-generated/node-register.mjs` when a
// project runs under the Node fallback runtime (no Deno build available).
import { register } from "node:module";

register("./node-loader.mjs", import.meta.url);
