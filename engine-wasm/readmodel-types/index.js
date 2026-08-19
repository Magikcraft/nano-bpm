// Runtime-empty: `@nanobpm/engine-wasm/readmodel-types` ships types only (the
// read-model DTOs are compile-time contracts; the actual JSON is minted by the
// wasm engine's `search*` / `get*ByKey` methods). This stub exists so the
// package subpath resolves under Node's `exports` map; all real declarations
// live in the sibling `index.d.ts` / `types.gen.d.ts`.
export {};
