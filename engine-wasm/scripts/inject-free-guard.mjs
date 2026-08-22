// Post-build DX guard for the wasm-pack generated TestEngine glue.
//
// wasm-bindgen's runtime refuses a call on a freed handle by throwing the
// opaque "null pointer passed to rust" (its `assert_not_null`, compiled into the
// `_bg.wasm`). That guard is correct — it prevents a use-after-free from
// corrupting memory — but the message names neither the engine, the method, nor
// the cause. This script injects a single canonical prototype guard into the
// generated glue so a post-free call instead throws a self-describing
//
//     TestEngine used after free(): '<method>' called on a released engine handle
//
// It is idempotent (keyed on MARKER) and is re-run by `make console-wasm` after
// each wasm-pack regeneration, so the friendlier message survives regeneration.
//
// Usage: node scripts/inject-free-guard.mjs <glue.js> [<glue.js> ...]
import { readFileSync, writeFileSync } from "node:fs";

const MARKER = "// --- nanobpm: use-after-free DX guard (scripts/inject-free-guard.mjs) ---";

const GUARD = `
${MARKER}
// wasm-bindgen throws the opaque "null pointer passed to rust" when a method is
// called on a freed handle (__wbg_ptr === 0). Re-describe that as a clear
// use-after-free so a host lifecycle bug is diagnosable at the call site. The
// call is still refused — this only changes the message, not the behaviour.
for (const __name of Object.getOwnPropertyNames(TestEngine.prototype)) {
    if (__name === "constructor" || __name === "free" || __name === "__destroy_into_raw") continue;
    const __desc = Object.getOwnPropertyDescriptor(TestEngine.prototype, __name);
    if (!__desc || typeof __desc.value !== "function") continue;
    const __orig = __desc.value;
    const __guarded = {
        [__name](...__args) {
            if (this.__wbg_ptr === 0) {
                throw new Error("TestEngine used after free(): '" + __name + "' called on a released engine handle");
            }
            return __orig.apply(this, __args);
        },
    }[__name];
    Object.defineProperty(TestEngine.prototype, __name, { ...__desc, value: __guarded });
}
// --- end nanobpm use-after-free DX guard ---
`;

let failed = false;
for (const file of process.argv.slice(2)) {
    let src;
    try {
        src = readFileSync(file, "utf8");
    } catch (err) {
        console.error(`inject-free-guard: cannot read ${file}: ${err.message}`);
        failed = true;
        continue;
    }
    if (src.includes(MARKER)) {
        console.log(`inject-free-guard: ${file} already guarded (skipped)`);
        continue;
    }
    if (!/export class TestEngine\b/.test(src)) {
        console.error(`inject-free-guard: ${file} has no exported TestEngine class — refusing to inject`);
        failed = true;
        continue;
    }
    writeFileSync(file, src.replace(/\s*$/, "\n") + GUARD);
    console.log(`inject-free-guard: guarded ${file}`);
}

if (failed) process.exit(1);
