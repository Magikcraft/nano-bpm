import type { CreateClientConfig } from "../gen/client.gen";

// Runtime configuration for the generated console API fetch client. The console
// SPA is served same-origin by the gateway, and the OpenAPI `servers` entry is
// `/console/api`, so we only need to pin that base path. Everything else uses
// the fetch defaults (same-origin cookies, etc.).
export const createClientConfig: CreateClientConfig = (config) => ({
  ...config,
  baseUrl: "/console/api",
  // Match the previous hand-written client's ergonomics: reject on any non-2xx
  // response so callers can use try/catch. The generated fetch client parses the
  // body by declared content-type (and treats 204 / empty 201 bodies as no-data),
  // which is the contract fix that motivated this codegen.
  throwOnError: true,
});
