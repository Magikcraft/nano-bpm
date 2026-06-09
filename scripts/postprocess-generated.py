#!/usr/bin/env python3
"""Patch known bugs in the rust-axum generated code so the crate compiles.

rust-axum is a beta generator. A few constructs in the Orchestration Cluster
spec make it emit invalid Rust. Rather than editing the shared spec (the API
contract) this script rewrites the *generated* output deterministically, so the
fixes are reapplied automatically on every regeneration.

Each fix is narrowly targeted and documented below. Run after generation:

    postprocess-generated.py <generated-crate-dir>
"""
from __future__ import annotations

import re
import sys
from pathlib import Path


def fix_oneof_datetime_variant(models: str) -> tuple[str, int]:
    """Fix the invalid enum variant `DateTime::OfUtc`.

    For a `oneOf` whose first member is an inline `string`/`date-time`, rust-axum
    derives the variant identifier from the Rust type `chrono::DateTime<Utc>` and
    emits `DateTime::OfUtc`, which is not a legal variant name (it contains `::`).
    The enum is `#[serde(untagged)]`, so the variant name is not part of the wire
    format and can be safely renamed to a valid identifier.
    """
    new = models.replace("DateTime::OfUtc", "DateTimeUtc")
    return new, models.count("DateTime::OfUtc")


def fix_optional_discriminators(models: str) -> tuple[str, list[str]]:
    """Fix discriminator helpers for structs whose discriminator field is
    `Option<String>`.

    For a model with a discriminator property (e.g. `type`), rust-axum emits
    helper code that assumes the field is a required `String`:

      * `#[serde(default = "Struct::_name_for_<prop>")]` (returns `String`)
      * `#[serde(serialize_with = "Struct::_serialize_<prop>")]` taking `&String`
      * `new()` initialising the field with `Self::_name_for_<prop>()`

    When the property is optional the field is `Option<String>`, so each of these
    fails to compile. We detect only the optional discriminators (required ones
    compile fine and are left untouched) and adapt the helpers to `Option<String>`.
    """
    # A discriminator field is one carrying a `default = "X::_name_for_<prop>"`
    # serde attribute. Capture the property name and whether the backing field is
    # optional by looking at the `pub <prop>:` declaration that follows.
    discriminator_field = re.compile(
        r'#\[serde\(default = "\w+::_name_for_(?P<prop>\w+)"\)\][\s\S]*?'
        r'pub (?P=prop): (?P<ty>Option<String>|String),'
    )

    optional_props: set[str] = set()
    for match in discriminator_field.finditer(models):
        if match.group("ty") == "Option<String>":
            optional_props.add(match.group("prop"))

    applied: list[str] = []
    for prop in sorted(optional_props):
        # 1. serialize_with helper must accept the optional reference.
        models = models.replace(
            f"fn _serialize_{prop}<S>(_: &String,",
            f"fn _serialize_{prop}<S>(_: &Option<String>,",
        )

        # 2. Point the serde default at a wrapper returning Option<String>.
        models = re.sub(
            rf'(#\[serde\(default = ")(\w+)(::)_name_for_{prop}("\)\])',
            rf"\1\2\3_default_for_{prop}\4",
            models,
        )

        # 3. Inject the `_default_for_<prop>` wrapper next to `_name_for_<prop>`.
        models = re.sub(
            rf"(    fn _name_for_{prop}\(\) -> String \{{[^}}]*\}}\n)",
            rf"\1"
            rf"    fn _default_for_{prop}() -> Option<String> {{\n"
            rf"        Some(Self::_name_for_{prop}())\n"
            rf"    }}\n",
            models,
        )

        # 4. Wrap the constructor initialisation in Some(..).
        models = models.replace(
            f"{prop}: Self::_name_for_{prop}(),",
            f"{prop}: Some(Self::_name_for_{prop}()),",
        )

        applied.append(prop)

    return models, applied


def fix_pagination_disambiguation(models: str) -> tuple[str, list[str]]:
    """Add `#[serde(deny_unknown_fields)]` to the four pagination structs.

    `SearchQueryPageRequest` is an untagged `oneOf` of `LimitPagination`,
    `OffsetPagination`, `CursorForwardPagination`, and `CursorBackwardPagination`,
    all of which have only optional fields. Without `deny_unknown_fields`, serde's
    untagged deserialization matches the first variant (`LimitPagination`) for any
    object that merely contains `limit`, silently dropping `after`/`before`/
    `from`. That makes cursor and offset pagination unreachable through the typed
    model. Denying unknown fields lets each request disambiguate to the variant
    whose exact field set it matches.
    """
    structs = [
        "LimitPagination",
        "OffsetPagination",
        "CursorForwardPagination",
        "CursorBackwardPagination",
    ]
    applied: list[str] = []
    for name in structs:
        needle = f"pub struct {name} {{"
        idx = models.find(needle)
        if idx == -1:
            continue
        if models[:idx].rstrip().endswith("deny_unknown_fields)]"):
            continue
        models = models[:idx] + "#[serde(deny_unknown_fields)]\n" + models[idx:]
        applied.append(name)
    return models, applied


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <generated-crate-dir>", file=sys.stderr)
        return 2

    crate_dir = Path(argv[1]).resolve()
    models_path = crate_dir / "src" / "models.rs"
    if not models_path.is_file():
        print(f"generated models.rs not found: {models_path}", file=sys.stderr)
        return 1

    models = models_path.read_text(encoding="utf-8")

    models, datetime_count = fix_oneof_datetime_variant(models)
    models, optional_props = fix_optional_discriminators(models)
    models, pagination_structs = fix_pagination_disambiguation(models)

    models_path.write_text(models, encoding="utf-8")

    print("Post-processed generated code:")
    print(f"  oneOf date-time variant occurrences fixed: {datetime_count}")
    if optional_props:
        print(f"  optional discriminators fixed: {', '.join(optional_props)}")
    else:
        print("  optional discriminators fixed: none")
    if pagination_structs:
        print(f"  pagination structs disambiguated: {', '.join(pagination_structs)}")
    else:
        print("  pagination structs disambiguated: none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
