# Refreshing the vendored `@nanobpm/urban` types

`server/src/console/urban_types.d.ts` is a single, self-contained rollup of
`@nanobpm/urban`'s public runtime type surface. The gateway serves it at
`GET /console/api/urban-types`, and the Studio editor registers it as a Monaco
extra-lib so an Urban app's own APIs (`AppJobHandler`, `OperationHandler`,
`runFromEnv`, `selectHost`, `createNanoSdkEngineClient`, `UrbanApp`, `AppApi`, …)
light up with full IntelliSense — fully offline, no `node_modules` read.

Because it's a snapshot, refresh it whenever the embedded `@nanobpm/urban`
contract changes (a bump that adds/renames public types). It does not need to
track every patch — only surface changes developers would author against.

## Refresh command

From any directory that has `@nanobpm/urban` (and its `@nanobpm/nano-sdk` +
`@nanobpm/workflow` deps) installed under `node_modules` — e.g. a scaffolded
Urban app:

```sh
# 1. Prepare a scratch roller.
mkdir -p /tmp/urban-dts-roll && cd /tmp/urban-dts-roll
npm init -y >/dev/null
npm i -D rollup rollup-plugin-dts typescript

# 2. Point it at the installed urban dts graph. APP = the app whose
#    node_modules has @nanobpm/urban installed.
APP=/path/to/urban-app/node_modules/@nanobpm
mkdir -p node_modules/@nanobpm
ln -sfn "$APP/urban"    node_modules/@nanobpm/urban
ln -sfn "$APP/nano-sdk" node_modules/@nanobpm/nano-sdk
ln -sfn "$APP/workflow" node_modules/@nanobpm/workflow

cat > rollup.config.mjs <<EOF
import dts from "rollup-plugin-dts";
export default {
  input: "$APP/urban/dist/runtime/index.d.ts",
  output: { file: "urban.runtime.d.ts", format: "es" },
  plugins: [dts({ respectExternal: true })],
};
EOF
./node_modules/.bin/rollup -c rollup.config.mjs
```

## Install into the repo

Keep the provenance header (the first comment block) in
`server/src/console/urban_types.d.ts`, bump its version line, then replace
everything after the `// ---------------------------------------------------------------------------`
separator line (the last line of the header comment block) with the freshly
rolled `urban.runtime.d.ts`.
