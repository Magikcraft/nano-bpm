// Playwright config for the guided-journey e2e guards (ADR 0049, #417).
//
// Scoped deliberately narrowly: this exists to catch the class of breakage no
// other gate can see — a journey step pointing at a `data-tour` anchor that has
// been renamed or deleted. Nothing in a normal build or unit test fails when that
// happens; the step just silently spotlights nothing.
//
// The suite never talks to a real gateway. Every test stubs the console API, so a
// run is deterministic and needs no engine, no Deno, and no scaffolded project —
// which is what keeps it honest under this repo's no-flaky-tests, no-retries rule.

import { defineConfig, devices } from "@playwright/test";

/** Kept off 5173 so a developer's own `npm run dev` can stay up during a run. */
const PORT = Number(process.env.E2E_PORT ?? 5177);

export default defineConfig({
  testDir: "./e2e",
  // The gateway serves the SPA under /console and the router shares that
  // basename, so tests navigate relative paths like "projects".
  use: {
    baseURL: `http://localhost:${PORT}/console/`,
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  // No retries, anywhere. A journey guard that only passes on a second attempt is
  // reporting a real defect (in the app or in the test), and this repo does not
  // paper over either.
  // One worker: the suite shares a single vite dev server, and running specs in
  // parallel against it made first-hit lazy-chunk compilation the slowest thing in
  // the run — which showed up as different tests failing on different runs. A
  // deterministic 20s serial run beats a flaky 15s parallel one, and this repo
  // does not tolerate flaky tests.
  workers: 1,
  retries: 0,
  forbidOnly: !!process.env.CI,
  reporter: process.env.CI ? "list" : "line",
  webServer: {
    // Bind explicitly: vite's default `localhost` can resolve to ::1, which left
    // Playwright polling 127.0.0.1 until it timed out.
    command: `npm run dev -- --port ${PORT} --strictPort --host 127.0.0.1`,
    url: `http://localhost:${PORT}/console/`,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
