#!/usr/bin/env python3
"""Produce a sanitized copy of the Orchestration Cluster OpenAPI spec for the
rust-axum generator.

The source spec (zeebe/gateway-protocol/src/main/proto/v2) is the shared API
contract and must not be edited. Some constructs that the Java `spring`
generator tolerates make the beta `rust-axum` generator crash. This script
copies the whole spec tree to an output directory and rewrites only the
problematic constructs, leaving every other file byte-for-byte identical so the
transformation surface stays auditable.

Currently sanitized:
  * Schema-less request bodies, e.g. `content: { application/json: {} }`.
    These declare a body media type with no schema. rust-axum's
    `fromRequestBody` throws a NullPointerException on them
    (RustAxumServerCodegen.java:1014). An empty, schema-less media type carries
    no payload contract, so the media type is dropped; if a requestBody ends up
    with no content it is removed entirely (equivalent to "no body").

Local overlays (spec-patches/patches.yaml):
  In addition to sanitizing, this script applies a small set of *local* additions
  to the build copy of the spec. These live in `spec-patches/patches.yaml` so the
  upstream `spec/` tree stays byte-for-byte identical to the Camunda release it
  tracks. Each patch targets a file under spec/ and a dotted path inside it, and
  either deep-`merge`s a mapping or `append`s items to a list (e.g. a `required`
  array). This is how project-specific schema extensions (such as the
  `processCompleted` field on `CreateProcessInstanceResult`) are introduced
  without forking the upstream spec.

Usage:
    preprocess-spec.py <source-spec-dir> <output-spec-dir> [patches-file]
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import yaml


def _is_empty_media_type(value: object) -> bool:
    """A media type with no schema (None or an empty mapping) carries no
    payload contract and trips up the rust-axum generator."""
    if value is None:
        return True
    if isinstance(value, dict) and not value:
        return True
    return False


def _sanitize_request_body(request_body: object) -> bool:
    """Strip schema-less media types from a requestBody. Returns True if the
    requestBody mapping was modified in place."""
    if not isinstance(request_body, dict):
        return False
    content = request_body.get("content")
    if not isinstance(content, dict):
        return False

    empty_types = [mt for mt, schema in content.items() if _is_empty_media_type(schema)]
    if not empty_types:
        return False

    for media_type in empty_types:
        del content[media_type]
    if not content:
        del request_body["content"]
    return True


def _sanitize(node: object) -> bool:
    """Recursively sanitize a parsed YAML document. Returns True if anything
    changed."""
    changed = False
    if isinstance(node, dict):
        request_body = node.get("requestBody")
        if isinstance(request_body, dict):
            if _sanitize_request_body(request_body):
                changed = True
            # A requestBody with no content left is equivalent to no body.
            if "content" not in request_body:
                del node["requestBody"]
                changed = True
        for value in node.values():
            if _sanitize(value):
                changed = True
    elif isinstance(node, list):
        for item in node:
            if _sanitize(item):
                changed = True
    return changed


def _deep_merge(target: dict, addition: dict) -> bool:
    """Recursively merge `addition` into `target` (additively). Returns True if
    `target` changed. Where both sides hold a mapping the merge recurses;
    otherwise the addition's value is written (overwriting any scalar/list)."""
    changed = False
    for key, value in addition.items():
        existing = target.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            if _deep_merge(existing, value):
                changed = True
        else:
            if key not in target or target[key] != value:
                target[key] = value
                changed = True
    return changed


def _resolve_parent(document: object, dotted: str) -> tuple[object, str]:
    """Walk `dotted` (e.g. "components.schemas.Foo.required") and return the
    (parent_container, last_key). Intermediate mapping nodes are created on
    demand so a patch can introduce a brand-new branch."""
    parts = dotted.split(".")
    node = document
    for part in parts[:-1]:
        if not isinstance(node, dict):
            raise ValueError(f"cannot descend into non-mapping at '{part}' in '{dotted}'")
        if part not in node or node[part] is None:
            node[part] = {}
        node = node[part]
    return node, parts[-1]


def _apply_patch(document: object, patch: dict) -> bool:
    """Apply a single patch action to a parsed YAML document. Returns True if the
    document changed."""
    target = patch.get("target")
    if not isinstance(target, str) or not target:
        raise ValueError(f"patch is missing a string 'target': {patch!r}")

    parent, last = _resolve_parent(document, target)
    if not isinstance(parent, dict):
        raise ValueError(f"target parent of '{target}' is not a mapping")

    changed = False
    if "merge" in patch:
        addition = patch["merge"]
        if not isinstance(addition, dict):
            raise ValueError(f"'merge' for target '{target}' must be a mapping")
        node = parent.get(last)
        if node is None:
            parent[last] = {}
            node = parent[last]
            changed = True
        if not isinstance(node, dict):
            raise ValueError(f"cannot merge into non-mapping at target '{target}'")
        if _deep_merge(node, addition):
            changed = True
    elif "append" in patch:
        items = patch["append"]
        if not isinstance(items, list):
            raise ValueError(f"'append' for target '{target}' must be a list")
        node = parent.get(last)
        if node is None:
            parent[last] = []
            node = parent[last]
        if not isinstance(node, list):
            raise ValueError(f"cannot append to non-list at target '{target}'")
        for item in items:
            if item not in node:
                node.append(item)
                changed = True
    else:
        raise ValueError(f"patch for target '{target}' has neither 'merge' nor 'append'")

    return changed


def _load_patches(patches_file: Path | None) -> dict[str, list[dict]]:
    """Load spec-patches/patches.yaml and group patch actions by their `file`
    (a path relative to the spec source dir). Missing file => no patches."""
    if patches_file is None or not patches_file.is_file():
        return {}
    with patches_file.open("r", encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh)
    if loaded is None:
        return {}
    if not isinstance(loaded, list):
        raise ValueError(f"{patches_file}: expected a top-level list of patches")
    by_file: dict[str, list[dict]] = {}
    for entry in loaded:
        if not isinstance(entry, dict):
            raise ValueError(f"{patches_file}: each patch must be a mapping, got {entry!r}")
        rel = entry.get("file")
        if not isinstance(rel, str) or not rel:
            raise ValueError(f"{patches_file}: patch is missing a string 'file': {entry!r}")
        by_file.setdefault(rel, []).append(entry)
    return by_file


def _apply_patches(document: object, patches: list[dict]) -> bool:
    changed = False
    for patch in patches:
        if _apply_patch(document, patch):
            changed = True
    return changed


def main(argv: list[str]) -> int:
    if len(argv) not in (3, 4):
        print(
            f"usage: {argv[0]} <source-spec-dir> <output-spec-dir> [patches-file]",
            file=sys.stderr,
        )
        return 2

    source_dir = Path(argv[1]).resolve()
    output_dir = Path(argv[2]).resolve()
    patches_file = Path(argv[3]).resolve() if len(argv) == 4 else None

    if not source_dir.is_dir():
        print(f"source spec dir not found: {source_dir}", file=sys.stderr)
        return 1

    patches_by_file = _load_patches(patches_file)

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    modified_files: list[str] = []
    patched_files: list[str] = []
    for src in sorted(source_dir.rglob("*")):
        rel = src.relative_to(source_dir)
        dst = output_dir / rel
        if src.is_dir():
            dst.mkdir(parents=True, exist_ok=True)
            continue

        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.suffix not in (".yaml", ".yml"):
            shutil.copy2(src, dst)
            continue

        with src.open("r", encoding="utf-8") as fh:
            document = yaml.safe_load(fh)

        changed = False
        if document is not None and _sanitize(document):
            changed = True

        file_patches = patches_by_file.get(rel.as_posix())
        if document is not None and file_patches:
            if _apply_patches(document, file_patches):
                changed = True
                patched_files.append(str(rel))

        if document is not None and changed:
            with dst.open("w", encoding="utf-8") as fh:
                yaml.safe_dump(document, fh, sort_keys=False, allow_unicode=True, width=4096)
            modified_files.append(str(rel))
        else:
            # Unchanged files are copied verbatim to keep the transform minimal.
            shutil.copy2(src, dst)

    # Surface any patches whose target file was not present in the spec — likely a
    # stale or misspelled patch entry that would otherwise silently no-op.
    spec_files = {
        src.relative_to(source_dir).as_posix()
        for src in source_dir.rglob("*")
        if src.is_file()
    }
    for rel in patches_by_file:
        if rel not in spec_files:
            print(f"warning: patch targets missing spec file: {rel}", file=sys.stderr)

    if modified_files:
        print("Sanitized/overlaid spec files:")
        for name in modified_files:
            tag = " (overlay)" if name in patched_files else ""
            print(f"  {name}{tag}")
    else:
        print("No spec files required sanitization or overlays.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
