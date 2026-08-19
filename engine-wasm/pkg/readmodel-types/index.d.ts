// Public surface of `@nanobpm/engine-wasm/readmodel-types`: the derived DTO
// types for the read-model engine's query results, so browser consumers
// (@nanobpm/bojtos-kit → the web-demo-framework) can type
// `searchUserTasks` / `searchProcessInstances` / `searchVariables` /
// `getFormByKey` / `getResourceByKey` without hand-mirroring the shapes.
//
// These are re-exported from the generated `types.gen.d.ts`, which is derived
// from the single source of truth — the Camunda-parity REST OpenAPI in `spec/`
// — via `engine-wasm/readmodel-types` (`npm run gen`). Do not hand-edit; a
// stale artifact fails CI (`npm run check` = regen + `git diff --exit-code`).
export type {
  // `searchUserTasks(filter): UserTaskSearchQueryResult`
  UserTaskSearchQueryResult,
  UserTaskResult,
  // `searchProcessInstances(filter): ProcessInstanceSearchQueryResult`
  ProcessInstanceSearchQueryResult,
  ProcessInstanceResult,
  // `searchVariables(filter): VariableSearchQueryResult`
  VariableSearchQueryResult,
  VariableResult,
  // `getFormByKey(key): FormResult | null`
  FormResult,
  // `getResourceByKey(key): ResourceResult | null`
  ResourceResult,
  // Shared search envelope (`{ items, page }`) the *SearchQueryResult types extend.
  SearchQueryResponse,
  SearchQueryPageResponse,
} from "./types.gen.js";
