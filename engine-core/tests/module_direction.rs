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
//! (`#[cfg(test)] mod tests;`), strips comments, and extracts module-level
//! `use crate::<module>[::<sub>]` edges. Inline fully-qualified paths
//! (`crate::feel::eval_bool` used *without* a `use`) and crate-root re-exports
//! (`use crate::{Command, Engine}`) are intentionally out of scope: the former
//! is the idiom modules use to reach a helper *without* declaring a layering
//! dependency (see `model.rs`), and the latter names the crate's public surface
//! rather than a module, so neither encodes a textual module-direction edge.

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
const SAME_LEVEL_ALLOWED: &[(&str, &str)] = &[
    ("dmn", "feel"),
    ("bpmn", "validate"),
    ("ffi", "engine"),
];

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
    let entries = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
    for entry in entries {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Remove `/* ... */` block comments (depth-aware for Rust's nestable block
/// comments), replacing them with spaces so line numbers are preserved.
fn strip_block_comments(src: &str) -> String {
    let bytes = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    let mut depth = 0usize;
    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            depth += 1;
            out.push_str("  ");
            i += 2;
            continue;
        }
        if depth > 0 && i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
            depth -= 1;
            out.push_str("  ");
            i += 2;
            continue;
        }
        if depth > 0 {
            // Preserve newlines so line counting stays accurate.
            out.push(if bytes[i] == b'\n' { '\n' } else { ' ' });
        } else {
            out.push(bytes[i] as char);
        }
        i += 1;
    }
    out
}

/// Strip a `//`-to-end-of-line comment from a single line, ignoring `//` that
/// appears inside a string or char literal.
fn strip_line_comment(line: &str) -> String {
    let bytes = line.as_bytes();
    let mut out = String::with_capacity(line.len());
    let mut i = 0;
    let mut in_str: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = in_str {
            out.push(c as char);
            if c == b'\\' && i + 1 < bytes.len() {
                out.push(bytes[i + 1] as char);
                i += 2;
                continue;
            }
            if c == q {
                in_str = None;
            }
            i += 1;
            continue;
        }
        if c == b'"' || c == b'\'' {
            in_str = Some(c);
            out.push(c as char);
            i += 1;
            continue;
        }
        if c == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            break;
        }
        out.push(c as char);
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
    (
        String::from_utf8_lossy(&bytes[start..i]).into_owned(),
        i,
    )
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

/// Extract every `use crate::<module>[::<sub>]` edge from already-cleaned source
/// (block comments + inline test modules stripped, line comments removed).
fn edges_in_source(
    cleaned: &str,
    src_module: &str,
    src_sub: Option<&str>,
    file_label: &str,
    edges: &mut Vec<Edge>,
) {
    for (lineno, raw_line) in cleaned.lines().enumerate() {
        let line = strip_line_comment(raw_line);
        let bytes = line.as_bytes();
        let mut search = 0;
        while let Some(rel) = line[search..].find("use crate::") {
            let mut i = search + rel + "use crate::".len();
            let (module, ni) = read_ident(bytes, i);
            i = ni;
            if MODULES.contains(&module.as_str()) && module != src_module {
                // optional `::sub`
                let mut sub = None;
                if i + 1 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
                    let (s, _) = read_ident(bytes, i + 2);
                    if !s.is_empty() {
                        sub = Some(s);
                    }
                }
                edges.push(Edge {
                    src_module: src_module.to_string(),
                    src_sub: src_sub.map(str::to_string),
                    tgt_module: module,
                    tgt_sub: sub,
                    file: file_label.to_string(),
                    line: lineno + 1,
                });
            }
            search = search + rel + "use crate::".len();
        }
    }
}

/// Collect the set of files gated by an external `#[cfg(test)] mod name;`
/// declaration (test-only files that must be ignored).
fn collect_test_gated_files(src_root: &Path, files: &[PathBuf]) -> BTreeSet<PathBuf> {
    let mut gated = BTreeSet::new();
    for file in files {
        let raw = fs::read_to_string(file).expect("read source file");
        let src = strip_block_comments(&raw);
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
                    while k > 0
                        && (bytes[k - 1] == b'_' || bytes[k - 1].is_ascii_alphanumeric())
                    {
                        k -= 1;
                    }
                    (String::from_utf8_lossy(&bytes[k..after]).into_owned(), after)
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
        let cleaned = strip_inline_cfg_test_modules(&strip_block_comments(&raw));
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
        ("json", None, "model", None),   // L0 -> L0
        ("agent", None, "lease", None),  // L4 -> L4
        ("command", None, "agent", None),// L4 -> L4
        ("validate", None, "bpmn", None),// L5 -> L5
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
        ("bpmn", None, "agent", None),   // L5 -> L4
        ("feel", None, "model", None),
        ("dmn", None, "feel", None),     // permitted intra-level
        ("bpmn", None, "validate", None),// permitted intra-level
        ("ffi", None, "engine", None),   // permitted intra-level
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
