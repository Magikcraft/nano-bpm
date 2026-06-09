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


def fix_html_escaped_regex_patterns(models: str) -> tuple[str, int]:
    """Unescape HTML entities the generator emits inside regex patterns.

    rust-axum's templates HTML-escape the OpenAPI `pattern` keyword, so a spec
    pattern such as `^(<default>|[\\w\\.\\-]{1,31})$` is emitted as the literal
    Rust string `^(&lt;default&gt;|[\\w\\.\\-]{1,31})$`. The compiled
    `regex::Regex` then matches the seven characters `&lt;` instead of `<`, so
    legitimately valid values fail validation with a 400 — most visibly the
    `tenantId` value `<default>`, which is sent on nearly every request.

    Unescape the entities back to their literal characters, but *only* inside
    `regex::Regex::new("...")` string literals so nothing else is touched. The
    handled entities (`&lt;` `&gt;` `&#x3D;` `&amp;`) never expand to a `"` or
    `\\`, so they cannot break the surrounding Rust string literal.
    """
    entities = {"&lt;": "<", "&gt;": ">", "&#x3D;": "=", "&amp;": "&"}
    count = 0

    def unescape(match: re.Match) -> str:
        nonlocal count
        literal = match.group(0)
        new = literal
        for entity, char in entities.items():
            new = new.replace(entity, char)
        if new != literal:
            count += 1
        return new

    # Match a `regex::Regex::new("...")` call, allowing escaped chars (e.g. `\\w`)
    # inside the string literal.
    pattern = re.compile(r'regex::Regex::new\("(?:[^"\\]|\\.)*"\)')
    models = pattern.sub(unescape, models)
    return models, count


def fix_default_tenant_xss_false_positive(models: str) -> tuple[str, int]:
    """Allow the `<default>` tenant sentinel past the generated XSS guard.

    rust-axum decorates string fields with a custom validator,
    `check_xss_string`, whose body is `if ammonia::is_html(v) { Err } else
    { Ok }`. Camunda uses the literal alias `<default>` as the default tenant
    id, and `ammonia::is_html("<default>")` is `true` (it parses as a stray
    HTML tag). Every request carrying `tenantId: "<default>"` — the common
    case — therefore fails XSS validation with a 400.

    Inject an early `Ok` for the exact `<default>` sentinel so the otherwise
    desirable XSS guard still applies to all real input. The match is anchored
    on the unique function signature, so it patches the one definition only.
    """
    needle = (
        "pub fn check_xss_string(v: &str) "
        "-> std::result::Result<(), validator::ValidationError> {\n"
    )
    guard = (
        '    if v == "<default>" {\n'
        "        return std::result::Result::Ok(());\n"
        "    }\n"
    )
    if needle not in models or guard in models:
        return models, 0
    return models.replace(needle, needle + guard, 1), 1


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
    models, regex_count = fix_html_escaped_regex_patterns(models)
    models, xss_count = fix_default_tenant_xss_false_positive(models)

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
    print(f"  HTML-escaped regex patterns unescaped: {regex_count}")
    print(f"  default-tenant XSS false positive fixed: {xss_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
