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

Usage:
    preprocess-spec.py <source-spec-dir> <output-spec-dir>
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


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(f"usage: {argv[0]} <source-spec-dir> <output-spec-dir>", file=sys.stderr)
        return 2

    source_dir = Path(argv[1]).resolve()
    output_dir = Path(argv[2]).resolve()

    if not source_dir.is_dir():
        print(f"source spec dir not found: {source_dir}", file=sys.stderr)
        return 1

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    modified_files: list[str] = []
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

        if document is not None and _sanitize(document):
            with dst.open("w", encoding="utf-8") as fh:
                yaml.safe_dump(document, fh, sort_keys=False, allow_unicode=True, width=4096)
            modified_files.append(str(rel))
        else:
            # Unchanged files are copied verbatim to keep the transform minimal.
            shutil.copy2(src, dst)

    if modified_files:
        print("Sanitized spec files:")
        for name in modified_files:
            print(f"  {name}")
    else:
        print("No spec files required sanitization.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
