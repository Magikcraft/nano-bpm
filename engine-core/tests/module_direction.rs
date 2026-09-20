//! Dependency-direction guard: freezes `engine-core`'s single-direction module
//! layering so the big splits that follow (epic #1201, slices 5–8) cannot
//! silently reintroduce a cycle or a backward import.
//!
//! Rust happily allows intra-crate cycles — nothing in the compiler stops
//! `model` from importing `engine`, or `event` from importing `state::apply`
//! again. The layering is therefore held by a checked-in table plus this test,
//! exactly like the repo's other structural guards (golden serde-drift,
//! `console/scripts/merge-gates.test.mjs`, the `processos` spec-parity test):
//! the table below is the **single source of truth**, and CI fails on any edge
//! that does not respect it.
//!
//! ## The table (mirrors `docs/engine-architecture-analysis.md` §3)
//!
//! ```text
//! L0  model  xml  json  read_query  temporal      ← zero internal deps
//! L1  feel → model         dmn → feel, model, xml
//! L2  state → model        (state::apply additionally → event)
//! L3  event → model, state::types
//! L4  command · agent · lease → L0..L3
//! L5  validate → model      bpmn → model, xml, validate
//! L6  engine → L0..L5       ffi → engine, bpmn, json
//! ```
//!
//! A `use crate::<module>` import from a module at level *Ls* to one at level
//! *Lt* is allowed when `Lt < Ls` (strictly lower), plus three intra-level
//! edges the layering deliberately permits (`dmn → feel`, `bpmn → validate`,
//! `ffi → engine`) and the two cycle-breaking special cases that slice 3
//! established:
//!
//! * `state::apply` (and *only* `apply`) may import `event`; and
//! * `event` may import `state::types` (and *only* `types`, never bare `state`
//!   or `state::apply`).
//!
//! ## What it inspects, and what it does not
//!
//! It walks `src/**.rs`, ignores `#[cfg(test)]` modules and test-only files
//! (`#[cfg(test)] mod tests;`), strips comments and literals, and extracts
//! module-level `use crate::<module>[::<sub>]` edges. The extractor parses the
//! full use-tree after each `use crate::`, so brace use-trees
//! (`use crate::{event::Event, state::apply}`, expanded entry-by-entry) and
//! paths wrapped across newlines are handled — no common import form can slip a
//! backward edge past the guard. Inline fully-qualified paths
//! (`crate::feel::eval_bool` used *without* a `use`) are out of scope: that is
//! the idiom modules use to reach a helper *without* declaring a layering
//! dependency (see `model.rs`). A leaf whose first segment is *not* a top-level
//! module — a crate-root re-export such as `use crate::Engine` — names the
//! crate's public surface rather than a module and is ignored. That
//! module-membership filter is only safe because [`MODULES`] is held in
//! lock-step with the modules on disk by [`modules_table_matches_src_tree`], so
//! a newly added module can never be an unlisted first segment that silently
//! escapes the guard before its layer is assigned.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Every top-level module in `engine-core/src`. Anything not in this set that
/// follows `use crate::` (e.g. a crate-root re-export like `crate::Engine`) is
/// not a module edge and is ignored.
const MODULES: &[&str] = &[
    "model",
    "xml",
    "json",
    "read_query",
    "temporal",
    "feel",
    "dmn",
    "cluster_vars",
    "state",
    "event",
    "command",
    "agent",
    "lease",
    "validate",
    "bpmn",
    "engine",
    "ffi",
];

/// The layer of each module (the allowed-edges table's spine). `None` is
/// returned for a non-module, which the caller never looks up.
fn module_level(module: &str) -> Option<u32> {
    Some(match module {
        "model" | "xml" | "json" | "read_query" | "temporal" => 0,
        "feel" | "dmn" | "cluster_vars" => 1,
        "state" => 2,
        "event" => 3,
        "command" | "agent" | "lease" => 4,
        "validate" | "bpmn" => 5,
        "engine" | "ffi" => 6,
        _ => return None,
    })
}

/// The only intra-level (`Lt == Ls`) edges the layering permits. Every other
/// same-level or upward edge is a violation unless it is one of the two special
/// cases handled in [`edge_allowed`].
const SAME_LEVEL_ALLOWED: &[(&str, &str)] =
    &[("dmn", "feel"), ("bpmn", "validate"), ("ffi", "engine")];

/// A single `use crate::<tgt_module>[::<tgt_sub>]` edge declared in a source
/// file belonging to `src_module` (with `src_sub` set for sub-moduled files
/// such as `state/apply.rs`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Edge {
    src_module: String,
    src_sub: Option<String>,
    tgt_module: String,
    tgt_sub: Option<String>,
    file: String,
    line: usize,
}

/// The matcher — the guarded core. Returns `Ok(())` for an allowed edge and
/// `Err(reason)` for a backward/forbidden one. Pure over its inputs so the
/// negative self-tests can exercise it directly.
fn edge_allowed(
    src_module: &str,
    src_sub: Option<&str>,
    tgt_module: &str,
    tgt_sub: Option<&str>,
) -> Result<(), String> {
    // Cycle-breaking special case 1: only `state::apply` may reach `event`.
    if src_module == "state" && tgt_module == "event" {
        return if src_sub == Some("apply") {
            Ok(())
        } else {
            Err(format!(
                "only `state::apply` may import `event`; found in `state::{}`",
                src_sub.unwrap_or("<root>")
            ))
        };
    }
    // Cycle-breaking special case 2: `event` may reach `state::types` only.
    if src_module == "event" && tgt_module == "state" {
        return if tgt_sub == Some("types") {
            Ok(())
        } else {
            Err(format!(
                "`event` may import `state::types` only; found `state::{}`",
                tgt_sub.unwrap_or("<root>")
            ))
        };
    }

    let (ls, lt) = match (module_level(src_module), module_level(tgt_module)) {
        (Some(ls), Some(lt)) => (ls, lt),
        _ => {
            return Err(format!(
                "unknown module in edge {src_module} -> {tgt_module}"
            ))
        }
    };

    if lt < ls {
        return Ok(());
    }
    if SAME_LEVEL_ALLOWED.contains(&(src_module, tgt_module)) {
        return Ok(());
    }
    Err(format!(
        "backward/lateral edge {src_module} (L{ls}) -> {tgt_module} (L{lt}) \
         is not in the allowed-edges table"
    ))
}

/// Recursively collect every `.rs` file under `dir`.
fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
    for entry in entries {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Blank every comment and the *interior* bytes of every string/char literal,
/// replacing them with spaces (newlines preserved so line numbers stay stable)
/// while leaving real code — the literal delimiters, braces, and `use crate::`
/// paths — intact.
///
/// This is deliberately a single small Rust-literal-aware lexer rather than a
/// set of independent textual scanners. The earlier approach stripped block
/// comments, line comments, and `#[cfg(test)]` braces in separate passes that
/// each modelled only a slice of Rust's lexical grammar, so a comment marker or
/// an unbalanced brace *inside a literal* corrupted detection — e.g. a
/// `const M = "/*";` opened a spurious block comment that erased following code
/// (including a forbidden import), a fixture string containing a lone `"{"` /
/// `"}"` shifted the test-module brace balance, and a lifetime (`'a`) on a line
/// with a trailing `//` comment left the line-comment scanner stuck "in a
/// string" so the comment was never stripped. Modelling normal strings, raw
/// strings (`r#"…"#`), byte strings/chars (`b"…"`, `b'…'`), char literals vs
/// lifetimes (`'a`), and nestable block comments in one pass closes all of
/// those gaps so the layering guard stays reliable across the epic #1201 moves,
/// whose fixtures routinely embed such markers.
fn sanitize(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    // Was the previous emitted *code* byte part of an identifier/number? Needed
    // to tell a raw/byte-string prefix (`r"`, `b"`), which follows a non-ident
    // byte, from an `r`/`b` that is merely inside an identifier such as `for` or
    // `sub`.
    let mut prev_ident = false;
    fn blank(out: &mut String, byte: u8) {
        out.push(if byte == b'\n' { '\n' } else { ' ' });
    }
    while i < b.len() {
        let c = b[i];
        // Line comment: blank to end of line.
        if c == b'/' && i + 1 < b.len() && b[i + 1] == b'/' {
            while i < b.len() && b[i] != b'\n' {
                blank(&mut out, b[i]);
                i += 1;
            }
            prev_ident = false;
            continue;
        }
        // Block comment (nestable): blank the whole span, markers included.
        if c == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            let mut depth = 0usize;
            while i < b.len() {
                if i + 1 < b.len() && b[i] == b'/' && b[i + 1] == b'*' {
                    depth += 1;
                    out.push_str("  ");
                    i += 2;
                    continue;
                }
                if i + 1 < b.len() && b[i] == b'*' && b[i + 1] == b'/' {
                    depth -= 1;
                    out.push_str("  ");
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                    continue;
                }
                blank(&mut out, b[i]);
                i += 1;
            }
            prev_ident = false;
            continue;
        }
        // Raw string: `r"…"`, `r#"…"#`, or byte-raw `br"…"` — only when the
        // `r`/`br` is not part of a wider identifier.
        if !prev_ident && (c == b'r' || (c == b'b' && i + 1 < b.len() && b[i + 1] == b'r')) {
            let r_pos = if c == b'b' { i + 1 } else { i };
            let mut j = r_pos + 1;
            let mut hashes = 0usize;
            while j < b.len() && b[j] == b'#' {
                hashes += 1;
                j += 1;
            }
            if j < b.len() && b[j] == b'"' {
                // Emit the prefix and the opening quote verbatim.
                for &byte in &b[i..=j] {
                    out.push(byte as char);
                }
                i = j + 1;
                // Blank the body until the matching `"` + `hashes` `#`.
                while i < b.len() {
                    if b[i] == b'"' {
                        let mut m = 0;
                        while m < hashes && i + 1 + m < b.len() && b[i + 1 + m] == b'#' {
                            m += 1;
                        }
                        if m == hashes {
                            out.push('"');
                            for _ in 0..hashes {
                                out.push('#');
                            }
                            i += 1 + hashes;
                            break;
                        }
                    }
                    blank(&mut out, b[i]);
                    i += 1;
                }
                prev_ident = false;
                continue;
            }
            // Not actually a raw string — fall through and treat `r`/`b` as code.
        }
        // Normal or byte string: `"…"` / `b"…"`.
        if c == b'"' || (c == b'b' && i + 1 < b.len() && b[i + 1] == b'"') {
            let q_pos = if c == b'b' { i + 1 } else { i };
            for &byte in &b[i..=q_pos] {
                out.push(byte as char);
            }
            i = q_pos + 1;
            while i < b.len() {
                if b[i] == b'\\' && i + 1 < b.len() {
                    blank(&mut out, b[i]);
                    blank(&mut out, b[i + 1]);
                    i += 2;
                    continue;
                }
                if b[i] == b'"' {
                    out.push('"');
                    i += 1;
                    break;
                }
                blank(&mut out, b[i]);
                i += 1;
            }
            prev_ident = false;
            continue;
        }
        // Char literal vs lifetime/label. `'\x'…'` and `'x'` are char literals
        // (blank the interior); anything else beginning with `'` is a lifetime
        // or loop label and stays as code.
        if c == b'\'' {
            if i + 1 < b.len() && b[i + 1] == b'\\' {
                // Escaped char literal: `'\n'`, `'\''`, `'\\'`, `'\xFF'`, …
                out.push('\'');
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' && i + 1 < b.len() {
                        blank(&mut out, b[i]);
                        blank(&mut out, b[i + 1]);
                        i += 2;
                        continue;
                    }
                    if b[i] == b'\'' {
                        out.push('\'');
                        i += 1;
                        break;
                    }
                    blank(&mut out, b[i]);
                    i += 1;
                }
                prev_ident = false;
                continue;
            }
            if i + 2 < b.len() && b[i + 2] == b'\'' {
                // Simple char literal `'x'`.
                out.push('\'');
                blank(&mut out, b[i + 1]);
                out.push('\'');
                i += 3;
                prev_ident = false;
                continue;
            }
            // Lifetime / label: emit the quote, let the identifier follow.
            out.push('\'');
            i += 1;
            prev_ident = false;
            continue;
        }
        // Ordinary code byte.
        out.push(c as char);
        prev_ident = c == b'_' || c.is_ascii_alphanumeric();
        i += 1;
    }
    out
}

/// Remove inline `#[cfg(test)] mod name { ... }` blocks (brace-balanced),
/// replacing them with blank lines. External `#[cfg(test)] mod name;`
/// declarations are handled separately (they gate a whole file).
fn strip_inline_cfg_test_modules(src: &str) -> String {
    let bytes = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < bytes.len() {
        if let Some(after) = match_cfg_test_mod(bytes, i) {
            // `after` points just past `mod <name>`. Find the next `{` or `;`.
            let mut j = after;
            while j < bytes.len() && bytes[j] != b'{' && bytes[j] != b';' {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'{' {
                // Balance braces and drop the whole block.
                let mut depth = 0usize;
                let mut k = j;
                while k < bytes.len() {
                    match bytes[k] {
                        b'{' => depth += 1,
                        b'}' => {
                            depth -= 1;
                            if depth == 0 {
                                k += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                    k += 1;
                }
                for b in &bytes[i..k] {
                    out.push(if *b == b'\n' { '\n' } else { ' ' });
                }
                i = k;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// If `#[cfg(test)]` (ignoring whitespace) is followed by `mod <name>`, return
/// the byte offset just past `<name>`; otherwise `None`.
fn match_cfg_test_mod(bytes: &[u8], start: usize) -> Option<usize> {
    let tag = b"#[cfg(test)]";
    if start + tag.len() > bytes.len() || &bytes[start..start + tag.len()] != tag {
        return None;
    }
    let mut i = start + tag.len();
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let kw = b"mod";
    if i + kw.len() > bytes.len() || &bytes[i..i + kw.len()] != kw {
        return None;
    }
    i += kw.len();
    // require a word boundary
    if i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
        return None;
    }
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    let name_start = i;
    while i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
        i += 1;
    }
    if i == name_start {
        return None;
    }
    Some(i)
}

/// Read an identifier `[A-Za-z_][A-Za-z0-9_]*` starting at `i`; return the
/// identifier and the offset just past it.
fn read_ident(bytes: &[u8], mut i: usize) -> (String, usize) {
    let start = i;
    if i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphabetic()) {
        i += 1;
        while i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
            i += 1;
        }
    }
    (String::from_utf8_lossy(&bytes[start..i]).into_owned(), i)
}

/// Map a source file path (relative to `src/`) to its top-level module and, for
/// directory modules, its sub-module (file stem, or `mod` for `mod.rs`).
fn module_of(rel: &Path) -> (String, Option<String>) {
    let comps: Vec<String> = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    if comps.len() == 1 {
        // e.g. `event.rs` -> module `event`
        let stem = comps[0].trim_end_matches(".rs").to_string();
        (stem, None)
    } else {
        let module = comps[0].clone();
        let last = comps[comps.len() - 1].trim_end_matches(".rs");
        let sub = last.to_string();
        (module, Some(sub))
    }
}

/// Skip ASCII whitespace (including newlines) starting at `i`.
fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

/// Parse a single Rust *use-tree* — the grammar that follows `use crate::` —
/// expanding brace groups so every leaf becomes its own path. Returns the list
/// of leaf paths (each a vector of `::`-separated segments) and the offset just
/// past the tree. Whitespace *including newlines* between tokens is skipped, so
/// a declaration wrapped across several physical lines is parsed as one unit.
///
/// Examples (the `crate::` prefix is already consumed by the caller):
/// * `state::types::X`              -> `[[state, types, X]]`
/// * `{event::Event, state::apply}` -> `[[event, Event], [state, apply]]`
/// * `state::{types::X, apply}`     -> `[[state, types, X], [state, apply]]`
/// * `model::Thing as Alias`        -> `[[model, Thing]]` (the `as` rename is
///   irrelevant to the module edge and is left unconsumed)
fn parse_use_tree(b: &[u8], start: usize) -> (Vec<Vec<String>>, usize) {
    let mut i = skip_ws(b, start);
    // Brace group: expand each comma-separated sub-tree.
    if i < b.len() && b[i] == b'{' {
        i += 1;
        let mut out = Vec::new();
        loop {
            i = skip_ws(b, i);
            if i >= b.len() {
                break;
            }
            if b[i] == b'}' {
                i += 1;
                break;
            }
            let before = i;
            let (paths, ni) = parse_use_tree(b, i);
            i = ni;
            out.extend(paths);
            i = skip_ws(b, i);
            if i < b.len() && b[i] == b',' {
                i += 1;
                continue;
            }
            if i < b.len() && b[i] == b'}' {
                i += 1;
                break;
            }
            // Malformed input or no forward progress — stop to guarantee
            // termination rather than spin.
            if i == before {
                i += 1;
            }
            break;
        }
        return (out, i);
    }
    // Path: ident (`::` (ident | `*` | `{…}`))*
    let mut prefix: Vec<String> = Vec::new();
    loop {
        i = skip_ws(b, i);
        if i < b.len() && b[i] == b'{' {
            // `prefix::{…}` — distribute the shared prefix over each leaf.
            let (subs, ni) = parse_use_tree(b, i);
            i = ni;
            let out = subs
                .into_iter()
                .map(|s| {
                    let mut full = prefix.clone();
                    full.extend(s);
                    full
                })
                .collect();
            return (out, i);
        }
        if i < b.len() && b[i] == b'*' {
            i += 1;
            let mut full = prefix.clone();
            full.push("*".to_string());
            return (vec![full], i);
        }
        let (ident, ni) = read_ident(b, i);
        if ident.is_empty() {
            break;
        }
        prefix.push(ident);
        i = ni;
        i = skip_ws(b, i);
        if i + 1 < b.len() && b[i] == b':' && b[i + 1] == b':' {
            i += 2;
            continue;
        }
        break;
    }
    if prefix.is_empty() {
        (Vec::new(), i)
    } else {
        (vec![prefix], i)
    }
}

/// Extract every `use crate::<module>[::<sub>]` edge from already-cleaned source
/// (comments + string/char literals blanked, inline test modules stripped).
///
/// The full use-tree after each `use crate::` is parsed via [`parse_use_tree`],
/// so brace use-trees (`use crate::{a::b, c}`) are expanded entry-by-entry and
/// declarations wrapped across newlines are read as one unit — every leaf whose
/// first segment is a top-level module (and is not the source module itself)
/// becomes an edge. A leaf whose first segment is not a module (a crate-root
/// re-export such as `use crate::Engine`) is not a module edge and is skipped.
fn edges_in_source(
    cleaned: &str,
    src_module: &str,
    src_sub: Option<&str>,
    file_label: &str,
    edges: &mut Vec<Edge>,
) {
    let bytes = cleaned.as_bytes();
    const NEEDLE: &str = "use crate::";
    let mut search = 0;
    while let Some(rel) = cleaned[search..].find(NEEDLE) {
        let kw_start = search + rel;
        let after_prefix = kw_start + NEEDLE.len();
        // Require a word boundary before `use` so an identifier ending in `use`
        // never matches.
        let boundary = kw_start == 0
            || !(bytes[kw_start - 1] == b'_' || bytes[kw_start - 1].is_ascii_alphanumeric());
        if !boundary {
            search = after_prefix;
            continue;
        }
        let (paths, end) = parse_use_tree(bytes, after_prefix);
        let lineno = 1 + cleaned[..kw_start]
            .bytes()
            .filter(|&byte| byte == b'\n')
            .count();
        for path in paths {
            let Some(module) = path.first() else {
                continue;
            };
            if MODULES.contains(&module.as_str()) && module != src_module {
                let sub = path.get(1).filter(|s| s.as_str() != "*").cloned();
                edges.push(Edge {
                    src_module: src_module.to_string(),
                    src_sub: src_sub.map(str::to_string),
                    tgt_module: module.clone(),
                    tgt_sub: sub,
                    file: file_label.to_string(),
                    line: lineno,
                });
            }
        }
        search = end.max(after_prefix);
    }
}

/// Collect the set of files gated by an external `#[cfg(test)] mod name;`
/// declaration (test-only files that must be ignored).
fn collect_test_gated_files(src_root: &Path, files: &[PathBuf]) -> BTreeSet<PathBuf> {
    let mut gated = BTreeSet::new();
    for file in files {
        let raw = fs::read_to_string(file).expect("read source file");
        let src = sanitize(&raw);
        let bytes = src.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if let Some(after) = match_cfg_test_mod(bytes, i) {
                // find `{` or `;`
                let mut j = after;
                // the module name we just consumed:
                let (name, _) = {
                    // re-read the name ending at `after`
                    let mut k = after;
                    while k > 0 && (bytes[k - 1] == b'_' || bytes[k - 1].is_ascii_alphanumeric()) {
                        k -= 1;
                    }
                    (
                        String::from_utf8_lossy(&bytes[k..after]).into_owned(),
                        after,
                    )
                };
                while j < bytes.len() && bytes[j] != b'{' && bytes[j] != b';' {
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b';' {
                    // external module: `<dir>/<name>.rs` or `<dir>/<name>/mod.rs`
                    let dir = file.parent().unwrap_or(src_root);
                    let flat = dir.join(format!("{name}.rs"));
                    let nested = dir.join(&name).join("mod.rs");
                    if flat.exists() {
                        gated.insert(flat);
                    }
                    if nested.exists() {
                        gated.insert(nested);
                    }
                }
                i = after;
                continue;
            }
            i += 1;
        }
    }
    gated
}

/// Walk `engine-core/src`, apply the exclusions, and return every module edge.
fn collect_edges() -> Vec<Edge> {
    let src_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src_root, &mut files);
    files.sort();

    let gated = collect_test_gated_files(&src_root, &files);

    let mut edges = Vec::new();
    for file in &files {
        if gated.contains(file) {
            continue;
        }
        let rel = file.strip_prefix(&src_root).expect("under src");
        let (src_module, src_sub) = module_of(rel);
        if src_module == "lib" {
            continue; // crate root: declares modules, has no layering edges of its own
        }
        let raw = fs::read_to_string(file).expect("read source file");
        let cleaned = strip_inline_cfg_test_modules(&sanitize(&raw));
        edges_in_source(
            &cleaned,
            &src_module,
            src_sub.as_deref(),
            &rel.to_string_lossy(),
            &mut edges,
        );
    }
    edges
}

#[test]
fn dependency_direction_holds() {
    let edges = collect_edges();
    assert!(
        !edges.is_empty(),
        "no `use crate::` edges found — the scanner or src path is wrong"
    );

    let mut violations = Vec::new();
    for e in &edges {
        if let Err(reason) = edge_allowed(
            &e.src_module,
            e.src_sub.as_deref(),
            &e.tgt_module,
            e.tgt_sub.as_deref(),
        ) {
            violations.push(format!(
                "  {}:{}  {} -> {}{}   ({reason})",
                e.file,
                e.line,
                e.src_module,
                e.tgt_module,
                e.tgt_sub
                    .as_deref()
                    .map(|s| format!("::{s}"))
                    .unwrap_or_default(),
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "engine-core module-layering violations (see docs/engine-architecture-analysis.md §3 \
         and the table in this test):\n{}",
        violations.join("\n")
    );
}

/// [`MODULES`] (and therefore [`module_level`] and the allowed-edges table) must
/// list *exactly* the top-level modules that exist under `src/`. This is the
/// single source of truth that keeps the extractor honest: [`edges_in_source`]
/// only records an edge when the imported first segment is in `MODULES`, so a
/// module added to `src/` but *not* added here would make its imports invisible
/// to the guard — a backward edge to (or from) it could land unchecked, exactly
/// the "import of an unlisted top-level module is silently dropped" gap. Failing
/// the moment the table drifts from disk forces the new/removed module's layer
/// to be assigned *before* any of its edges can bypass evaluation.
#[test]
fn modules_table_matches_src_tree() {
    let src_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut on_disk: BTreeSet<String> = BTreeSet::new();
    for entry in fs::read_dir(&src_root).expect("read src") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            // A directory is a module only if it has a `mod.rs`.
            if path.join("mod.rs").exists() {
                if let Some(name) = path.file_name() {
                    on_disk.insert(name.to_string_lossy().into_owned());
                }
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            if let Some(stem) = path.file_stem() {
                let stem = stem.to_string_lossy().into_owned();
                if stem != "lib" {
                    on_disk.insert(stem);
                }
            }
        }
    }

    let listed: BTreeSet<String> = MODULES.iter().map(|m| (*m).to_string()).collect();
    assert_eq!(
        listed,
        on_disk,
        "MODULES has drifted from engine-core/src — update MODULES, `module_level`, and the \
         layering table so the module's edges are checked (on disk but not in MODULES: {:?}; in \
         MODULES but not on disk: {:?})",
        on_disk.difference(&listed).collect::<Vec<_>>(),
        listed.difference(&on_disk).collect::<Vec<_>>(),
    );

    // Every listed module must have a layer, or `edge_allowed` cannot classify
    // its edges (it would report "unknown module" for a real one).
    for m in MODULES {
        assert!(
            module_level(m).is_some(),
            "module `{m}` is listed in MODULES but has no layer in `module_level`"
        );
    }
}

/// Guard-the-guard: the matcher must *reject* deliberately-wrong edges. If any
/// of these silently pass, the guard above is worthless.
#[test]
fn guard_rejects_backward_and_forbidden_edges() {
    let cases: &[(&str, Option<&str>, &str, Option<&str>)] = &[
        // pure backward level edges
        ("model", None, "engine", None),
        ("model", None, "feel", None),
        ("event", None, "engine", None),
        ("state", Some("types"), "engine", None),
        // lateral edges not in the allow-list
        ("json", None, "model", None),    // L0 -> L0
        ("agent", None, "lease", None),   // L4 -> L4
        ("command", None, "agent", None), // L4 -> L4
        ("validate", None, "bpmn", None), // L5 -> L5
        // the cycle we broke must stay broken:
        ("state", Some("types"), "event", None), // only apply may import event
        ("state", None, "event", None),          // bare state may not import event
        ("event", None, "state", Some("apply")), // event may not reach apply
        ("event", None, "state", None),          // event may not reach bare state
    ];
    for (sm, ss, tm, ts) in cases {
        assert!(
            edge_allowed(sm, *ss, tm, *ts).is_err(),
            "matcher wrongly ALLOWED forbidden edge {sm}{} -> {tm}{}",
            ss.map(|s| format!("::{s}")).unwrap_or_default(),
            ts.map(|s| format!("::{s}")).unwrap_or_default(),
        );
    }
}

/// The matcher must *accept* the edges the layering deliberately permits,
/// including the intra-level and cycle-breaking special cases.
#[test]
fn guard_accepts_known_good_edges() {
    let cases: &[(&str, Option<&str>, &str, Option<&str>)] = &[
        ("engine", None, "model", None),
        ("engine", None, "state", None),
        ("bpmn", None, "agent", None), // L5 -> L4
        ("feel", None, "model", None),
        ("dmn", None, "feel", None),             // permitted intra-level
        ("bpmn", None, "validate", None),        // permitted intra-level
        ("ffi", None, "engine", None),           // permitted intra-level
        ("state", Some("apply"), "event", None), // cycle-breaking exception
        ("event", None, "state", Some("types")), // cycle-breaking exception
    ];
    for (sm, ss, tm, ts) in cases {
        assert!(
            edge_allowed(sm, *ss, tm, *ts).is_ok(),
            "matcher wrongly REJECTED allowed edge {sm}{} -> {tm}{}",
            ss.map(|s| format!("::{s}")).unwrap_or_default(),
            ts.map(|s| format!("::{s}")).unwrap_or_default(),
        );
    }
}

/// Run the real cleaning pipeline (`sanitize` → strip inline test modules →
/// per-line edge extraction) over a synthetic single-file source and return the
/// module edges it yields, so the lexer's literal-awareness can be exercised
/// directly.
#[cfg(test)]
fn edges_of_source(src_module: &str, src: &str) -> Vec<Edge> {
    let cleaned = strip_inline_cfg_test_modules(&sanitize(src));
    let mut edges = Vec::new();
    edges_in_source(&cleaned, src_module, None, "synthetic.rs", &mut edges);
    edges
}

#[cfg(test)]
fn has_edge(edges: &[Edge], tgt_module: &str) -> bool {
    edges.iter().any(|e| e.tgt_module == tgt_module)
}

/// A comment marker inside a string literal must not open a spurious block
/// comment that erases the following real code (issue: the guard could silently
/// miss a forbidden import after a `const M = "/*";` line).
#[test]
fn sanitize_ignores_comment_markers_inside_strings() {
    let src = "const MARKER: &str = \"/*\";\nuse crate::model::Thing;\n";
    let edges = edges_of_source("engine", src);
    assert!(
        has_edge(&edges, "model"),
        "block-comment marker inside a string literal erased a real `use crate::` edge: {edges:?}"
    );
}

/// A lifetime (`'a`) on a line that also carries a trailing `//` comment must
/// not wedge the scanner "inside a string" and leak the comment's text as code
/// (which would spuriously report a backward edge named in the comment).
#[test]
fn sanitize_strips_line_comment_after_lifetime() {
    let src = "fn f<'a>() {} // use crate::engine::Foo\nuse crate::model::T;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        !has_edge(&edges, "engine"),
        "text inside a `//` comment after a lifetime leaked as a real edge: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "the genuine post-comment `use crate::` edge was lost: {edges:?}"
    );
}

/// An unbalanced brace inside a string literal must not shift the
/// `#[cfg(test)] mod { … }` brace balance, which would either leak the test
/// module's `use crate::` edges or eat real code after it.
#[test]
fn sanitize_ignores_braces_inside_test_module_strings() {
    let src = "#[cfg(test)]\nmod tests {\n    let s = \"}\";\n    use crate::engine::X;\n}\nuse crate::model::Y;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        !has_edge(&edges, "engine"),
        "a `\"}}\"` string closed the test module early, leaking a test-only edge: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "an unbalanced brace inside a string swallowed real code after the test module: {edges:?}"
    );
}

/// Raw-string contents (including embedded `//`, `/*`, and `use crate::` text)
/// must be blanked, never mistaken for comments or real import edges.
#[test]
fn sanitize_blanks_raw_string_contents() {
    let src = "let q = r#\"use crate::engine::Z and /* not a comment\"#;\nuse crate::model::W;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        !has_edge(&edges, "engine"),
        "a `use crate::` inside a raw string was reported as a real edge: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "a `/*` inside a raw string opened a spurious block comment: {edges:?}"
    );
}

/// A `use crate::` declaration wrapped across newlines (the module segment on a
/// separate physical line from `use crate::`) must still be extracted. Scanning
/// one line at a time missed this form, letting a backward edge land unchecked.
#[test]
fn extracts_multiline_use_declaration() {
    let src = "use crate::\n    engine::Engine;\nuse crate::model::T;\n";
    let edges = edges_of_source("feel", src);
    assert!(
        has_edge(&edges, "engine"),
        "a `use crate::` path wrapped across newlines was not extracted: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "the single-line edge on the following line was lost: {edges:?}"
    );
}

/// A brace use-tree (`use crate::{a::b, c::d}`) must be expanded entry-by-entry
/// so every leaf module is checked. The old single-path scanner saw the `{`,
/// read an empty identifier, and dropped the whole declaration — letting a
/// forbidden edge hide inside braces (`use crate::{engine::Engine}` from a lower
/// layer would bypass the guard entirely).
#[test]
fn extracts_brace_use_tree_entries() {
    let src = "use crate::{event::Event, model::Thing};\n";
    let edges = edges_of_source("feel", src);
    assert!(
        has_edge(&edges, "event"),
        "a module edge inside a brace use-tree was ignored: {edges:?}"
    );
    assert!(
        has_edge(&edges, "model"),
        "a second brace use-tree entry was ignored: {edges:?}"
    );
}

/// A nested brace use-tree with a shared prefix (`use crate::state::{types::X,
/// apply::Y}`) must distribute the prefix over each leaf, so the cycle-breaking
/// `state::types` vs `state::apply` distinction survives the braced form.
#[test]
fn extracts_nested_brace_use_tree_with_shared_prefix() {
    let src = "use crate::state::{types::T, apply::A};\n";
    let edges = edges_of_source("event", src);
    assert!(
        edges
            .iter()
            .any(|e| e.tgt_module == "state" && e.tgt_sub.as_deref() == Some("types")),
        "the `state::types` leaf of a prefixed brace use-tree was lost: {edges:?}"
    );
    assert!(
        edges
            .iter()
            .any(|e| e.tgt_module == "state" && e.tgt_sub.as_deref() == Some("apply")),
        "the `state::apply` leaf of a prefixed brace use-tree was lost: {edges:?}"
    );
}

/// A backward edge written inside a brace use-tree must be caught by the real
/// matcher — the whole point of expanding braces. `event` importing `engine`
/// via `use crate::{engine::Engine}` is a forbidden L3 -> L6 edge and must be
/// rejected once extracted.
#[test]
fn brace_use_tree_backward_edge_is_rejected() {
    let src = "use crate::{engine::Engine};\n";
    let edges = edges_of_source("event", src);
    let engine = edges
        .iter()
        .find(|e| e.tgt_module == "engine")
        .expect("brace-wrapped backward edge should be extracted");
    assert!(
        edge_allowed(&engine.src_module, None, &engine.tgt_module, None).is_err(),
        "a backward edge hidden in a brace use-tree slipped past the matcher"
    );
}

/// A crate-root re-export (`use crate::Engine`, `use crate::{Command, Engine}`)
/// names the crate's public surface, not a module, so its first segment is not
/// in `MODULES` and it must be ignored — only real module first segments count.
#[test]
fn ignores_crate_root_reexports() {
    let src = "use crate::{Engine, Command};\nuse crate::model::T;\n";
    let edges = edges_of_source("feel", src);
    assert_eq!(
        edges.len(),
        1,
        "a crate-root re-export was mistaken for a module edge: {edges:?}"
    );
    assert!(has_edge(&edges, "model"));
}
