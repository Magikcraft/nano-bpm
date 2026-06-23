# nanobpmn console

The embedded web console (React + Vite + Tailwind). In a release build it is
compiled to `console/dist` and baked into the gateway binary via RustEmbed
(`server/src/console/mod.rs`), served under `/console/`.

## Local development (HMR)

For fast iteration you don't want to rebuild `console/dist` and recompile the
gateway on every change. Instead run the Vite dev server with hot-module reload
against a separately-running gateway:

1. Start a backing gateway on `:8080` (e.g. with `c8ctl`).
2. In this directory, run the single dev command:

   ```sh
   npm install   # first time only
   npm run dev
   ```

3. Open the printed URL, e.g. <http://localhost:5173/console/>.

The dev server serves the SPA with HMR on a separate port (`5173`) and proxies
the two gateway surfaces the console calls to the backend, so everything works
against live data:

- `/console/api` — the console's JSON API and the SSE live-update stream
  (`/console/api/stream`) that drives auto-refresh.
- `/v2` — the public REST API: deployments, instance creation, and the BPMN
  definition XML the Process Explorer diagram viewer fetches.

### Configuration

Both are environment variables (all optional):

| Variable        | Default                  | Purpose                                   |
| --------------- | ------------------------ | ----------------------------------------- |
| `NANO_BACKEND`  | `http://127.0.0.1:8080`  | Gateway the dev server proxies API calls to. |
| `CONSOLE_PORT`  | `5173`                   | Port the HMR dev server listens on.       |

Example pointing at a gateway on another port:

```sh
NANO_BACKEND=http://127.0.0.1:9000 npm run dev
```

## Production build

```sh
npm run build   # → console/dist, embedded by the gateway's `console` feature
```
