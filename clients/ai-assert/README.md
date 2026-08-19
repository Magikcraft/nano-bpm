# @nanobpm/ai-assert

Deterministic **AI-judge and semantic-similarity test assertions** — an `assertThatText`
surface over pluggable chat/embedding model seams, with opt-in real OpenAI/Transformers.js
backends and on-disk record/replay.

This package is **framework- and engine-agnostic**: it has no dependency on the Nano engine
or the Urban runtime — just Node and two *optional* peers. It was lifted out of
`@nanobpm/urban-testkit`'s `/ai` subpath (issue [#297]) so the AI matchers are reusable in
any app or package ([#894], S3). A follow-up will re-export this package from
`@nanobpm/urban-testkit` to keep its `/ai` subpath stable.

```bash
npm i -D @nanobpm/ai-assert
```

Requires Node `>=22.6` (the test suite runs on `node --test --experimental-strip-types`).

## The matchers

The default backends are **deterministic fakes** (zero network): the same input always
yields the same vector/verdict, so tests are reproducible in CI. A **record/replay** adapter
wraps either seam against an on-disk JSON cassette — a missing or edited cassette **fails
loudly** — and its capture source is pluggable so live backends can be recorded without
editing the adapter. `seamInventory()` is the derived source of truth over the two seams
(which backends exist per seam) for the completeness guard.

```ts
import { assertThatText, seamInventory } from "@nanobpm/ai-assert";

// The derived seam inventory (the completeness guard's source of truth).
seamInventory();

// Semantic-similarity and LLM-judge matchers — available and deterministic by default
// (backed by the deterministic fakes, zero network):
await assertThatText(output).matchesSemantically("a warm greeting", { threshold: 0.8 });
await assertThatText(output).satisfiesJudge("is a polite apology");
```

This surface ships the full stack — seams, deterministic fakes, record/replay, the fluent
matcher-registration seam, the derived seam inventory, the `matchesSemantically` and
`satisfiesJudge` matchers, and the opt-in real adapters described below.

## Real AI adapters

Behind the two seams live **real** backends that are OFF by default. Each seam has both a
**hosted-provider** adapter (`HostedEmbeddingAdapter` / `HostedChatModelAdapter`, over an
OpenAI-compatible service — the chat adapter also serves the optional image part for vision
judging) and a **local / on-device** adapter (`LocalEmbeddingAdapter` /
`LocalChatModelAdapter`, over Transformers.js). Their heavy SDKs are declared as **optional
peer dependencies** (`peerDependencies` + `peerDependenciesMeta.optional`), so a plain
install never pulls them in, and they are never imported at module load — the barrel stays
import-safe even when they are not installed.

`seamInventory()` reports `hasReal: true` with a `docRef` for both seams **unconditionally**
(a static existence fact registered at import — no opt-in, no network). This is decoupled
from **live activation**: constructing a real adapter loads its optional dependency and
performs network/model I/O, and is gated behind an explicit opt-in — set
**`URBAN_TESTKIT_AI_REAL=1`**. Without it, every construction factory throws
`real AI adapter requires explicit opt-in` before touching a dependency or the network, so
the default CI path can never reach a live model.

> The opt-in environment variable is `URBAN_TESTKIT_AI_REAL` for now, preserved from the
> `@nanobpm/urban-testkit` origin so existing `/ai` consumers keep working when urban-testkit
> re-exports this package. A package-neutral alias is a follow-up.

```ts
import {
  Cassette,
  createRealAdapters,
  createRecordingChatModelAdapter,
} from "@nanobpm/ai-assert";

// Throws unless URBAN_TESTKIT_AI_REAL is set (default CI is network-free):
const { embedding, chat } = await createRealAdapters({ provider: "hosted" });

// Regenerate a cassette from a live backend, injected as the record/replay capture source.
// Start a fresh cassette (or `await Cassette.load(path)` to append to an existing one):
const cassette = new Cassette("src/__cassettes__/judge.json");
const recorder = await createRecordingChatModelAdapter({ cassette, real: { provider: "local" } });
```

## Optional peer dependencies

| Peer                     | Enables                                             |
| ------------------------ | --------------------------------------------------- |
| `openai`                 | the **hosted** OpenAI-compatible chat/embedding adapters |
| `@xenova/transformers`   | the **local** on-device Transformers.js adapters    |

Neither is required for the default deterministic-fake and record/replay paths.

## Scripts

- `npm run build` — emit `dist/` (`tsc -p tsconfig.build.json`, type declarations included).
- `npm run typecheck` — `tsc --noEmit`.
- `npm test` — `node --test --experimental-strip-types` over `src/**/*.test.ts`, including
  the `__guards__/` guard tests (no-network, cassette-integrity, completeness, matchers,
  threshold).

[#297]: https://github.com/nanobpm/nano-ide/issues/297
[#894]: https://github.com/Magikcraft/nano-bpm/issues/894
