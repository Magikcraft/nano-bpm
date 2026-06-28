#!/usr/bin/env bash
# Regenerate the embedded Deno namespace types used by the Console IDE.
#
# The Console's in-browser Monaco editor registers these as an extra-lib so
# worker / main.ts code that uses `Deno.env`, `Deno.readDir`, `Deno.serve`, etc.
# type-checks instead of erroring with "Cannot find name 'Deno'".
#
# We embed ONLY the `declare namespace Deno { ... }` blocks from `deno types`,
# dropping the web globals (Request/Response/WebSocket/...) so they don't collide
# with the editor's `dom` lib. The result is served verbatim by the gateway at
# `GET /console/api/deno-types` (server/src/console/deno_ns.d.ts, include_str!).
#
# Usage: scripts/gen-deno-types.sh   (requires `deno` on PATH)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/server/src/console/deno_ns.d.ts"

command -v deno >/dev/null || { echo "error: 'deno' not found on PATH" >&2; exit 1; }

tmp="$(mktemp)"
deno types > "$tmp"

deno_version="$(deno --version | head -1)"

python3 - "$tmp" "$OUT" "$deno_version" <<'PY'
import sys
src_path, out_path, deno_version = sys.argv[1], sys.argv[2], sys.argv[3]
lines = open(src_path).read().splitlines(keepends=True)
out, i, n, blocks = [], 0, len(lines), 0
while i < n:
    if lines[i].startswith('declare namespace Deno {'):
        blocks += 1
        out.append(lines[i]); i += 1
        # Each namespace block closes with a column-0 '}' line.
        while i < n and lines[i].rstrip('\n') != '}':
            out.append(lines[i]); i += 1
        if i < n:
            out.append(lines[i]); i += 1
        out.append('\n')
    else:
        i += 1
if blocks == 0:
    sys.exit('error: no `declare namespace Deno` blocks found in `deno types` output')
header = (
    "// Deno namespace ambient types for the Console IDE (in-browser Monaco).\n"
    "// Extracted from `deno types` (Deno namespace blocks only; web globals\n"
    "// like Request/Response/WebSocket are omitted so they don't collide with\n"
    "// the editor's `dom` lib). Regenerate with scripts/gen-deno-types.sh.\n"
    f"// Source: {deno_version}\n\n"
)
open(out_path, 'w').write(header + ''.join(out))
print(f"wrote {out_path} ({blocks} namespace blocks)")
PY

rm -f "$tmp"
