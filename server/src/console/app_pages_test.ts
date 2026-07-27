// Unit tests for the Urban App page runtime (ADR 0042 §3).
// Run: `deno test --allow-read server/src/console/app_pages_test.ts`
import { assert, assertEquals } from "jsr:@std/assert";
import { createPagesHandler, type PagesContext } from "./app_pages.ts";

function ctx(over: Partial<PagesContext> = {}): PagesContext {
  return {
    db: {
      schema: () => Promise.resolve([{ name: "pull_requests" }]),
      query: (_sql: string) => Promise.resolve([{ pr_key: "o/r#1", status: "converging" }]),
    },
    nano: {
      createProcessInstance: (_i) => Promise.resolve({ processInstanceKey: 42 }),
    },
    readPage: (path: string) =>
      path.endsWith("home.page.json")
        ? Promise.resolve(JSON.stringify({ schemaVersion: "1.0", title: "T", nodes: [] }))
        : Promise.reject(new Error("nope")),
    ...over,
  };
}

Deno.test("GET / serves the renderer shell with the home page id", async () => {
  const res = await createPagesHandler(ctx({ homePage: "home" }))(new Request("http://x/"));
  assertEquals(res.status, 200);
  const html = await res.text();
  assert(html.includes('data-home="home"'));
  assert(html.includes("/app/runtime.js"));
});

Deno.test("GET /app/pages/home returns the page.json", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/app/pages/home"));
  assertEquals(res.status, 200);
  const doc = await res.json();
  assertEquals(doc.title, "T");
});

Deno.test("GET /app/pages/missing is 404", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/app/pages/missing"));
  assertEquals(res.status, 404);
});

Deno.test("GET /app/data returns rows for a known table", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/app/data/app/pull_requests"));
  assertEquals(res.status, 200);
  const body = await res.json();
  assertEquals(body.rows.length, 1);
  assertEquals(body.rows[0].pr_key, "o/r#1");
});

Deno.test("GET /app/data rejects an unknown table (no SQL injection surface)", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/app/data/app/secrets"));
  assertEquals(res.status, 404);
});

Deno.test("GET /app/data rejects a source other than the injected default", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/app/data/other/pull_requests"));
  assertEquals(res.status, 404);
  assertEquals((await res.json()).error, 'unknown datasource "other"');
});

Deno.test("POST /app/actions/start starts a process with the posted variables", async () => {
  let seen: unknown = null;
  const c = ctx({
    nano: {
      createProcessInstance: (i) => {
        seen = i;
        return Promise.resolve({ processInstanceKey: 7 });
      },
    },
  });
  const res = await createPagesHandler(c)(
    new Request("http://x/app/actions/start/convergence-loop", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ variables: { pr: "o/r#1" } }),
    }),
  );
  assertEquals(res.status, 200);
  assertEquals((await res.json()).processInstanceKey, 7);
  assertEquals(seen, { processDefinitionId: "convergence-loop", variables: { pr: "o/r#1" } });
});

Deno.test("POST /app/actions/start defaults a non-object `variables` to {}", async () => {
  let seen: unknown = null;
  const c = ctx({
    nano: {
      createProcessInstance: (i) => {
        seen = i;
        return Promise.resolve({ processInstanceKey: 9 });
      },
    },
  });
  const res = await createPagesHandler(c)(
    new Request("http://x/app/actions/start/p", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ variables: ["not", "an", "object"] }),
    }),
  );
  assertEquals(res.status, 200);
  assertEquals(seen, { processDefinitionId: "p", variables: {} });
});

Deno.test("POST /app/actions/start surfaces an engine error as 502", async () => {
  const c = ctx({
    nano: { createProcessInstance: () => Promise.reject(new Error("engine down")) },
  });
  const res = await createPagesHandler(c)(
    new Request("http://x/app/actions/start/p", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
    }),
  );
  assertEquals(res.status, 502);
  assert((await res.json()).error.includes("engine down"));
});

Deno.test("unknown route is 404", async () => {
  const res = await createPagesHandler(ctx())(new Request("http://x/nope"));
  assertEquals(res.status, 404);
});
