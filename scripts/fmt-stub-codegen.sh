#!/usr/bin/env bash
# Creates throwaway, parse-only stand-ins for the gitignored codegen inputs that
# rustfmt must resolve before `cargo fmt` can run in an unbuilt tree (fresh
# clone, the CI fmt job):
#
#   - server/src/stub_impls.rs              — a `mod` of the server crate
#   - generated/Cargo.toml + src/lib.rs     — path dep `nanobpm-gateway-rest`
#   - generated-console/Cargo.toml + src/lib.rs — path dep `nanobpm-console-api`
#
# The generated crate stubs became necessary when ADR 0064 made server/ a cargo
# workspace: `cargo fmt` runs `cargo metadata`, which resolves path-dependency
# manifests across the workspace, so the generated crates must exist on disk
# even though rustfmt never formats them.
#
# Existing real files are NEVER clobbered: only missing paths get stubs, and the
# paths created are recorded in a state file so `--cleanup` can remove exactly
# those (used by .githooks/pre-push via a trap — a leaked placeholder with a
# fresh mtime would defeat the Makefile's timestamp-based regeneration; CI
# runners are ephemeral and never clean up).
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"
state_file=$(git rev-parse --git-path fmt-stubs.created)

stub_impls=server/src/stub_impls.rs

stub_crate() { # <dir> <package-name>
	local dir=$1 name=$2
	if [ ! -f "$dir/Cargo.toml" ]; then
		mkdir -p "$dir/src"
		printf '[package]\nname = "%s"\nversion = "0.0.0"\nedition = "2021"\n' "$name" >"$dir/Cargo.toml"
		: >"$dir/src/lib.rs"
		echo "$dir" >>"$state_file"
	fi
}

create() {
	: >"$state_file"
	if [ ! -f "$stub_impls" ]; then
		printf '// fmt stub\n' >"$stub_impls"
		echo "$stub_impls" >>"$state_file"
	fi
	stub_crate generated nanobpm-gateway-rest
	stub_crate generated-console nanobpm-console-api
}

cleanup() {
	[ -f "$state_file" ] || exit 0
	while IFS= read -r path; do
		case "$path" in
		generated | generated-console) rm -rf "$path" ;;
		*) rm -f "$path" ;;
		esac
	done <"$state_file"
	rm -f "$state_file"
}

case "${1:-create}" in
create) create ;;
--cleanup | cleanup) cleanup ;;
*)
	echo "usage: $0 [create|--cleanup]" >&2
	exit 2
	;;
esac
