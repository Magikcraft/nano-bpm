//! Minimal, dependency-free JSON string escaping shared across the crate.
//!
//! The engine hand-writes JSON in a couple of places (the always-compiled
//! activation path in `engine::api` and the feature-gated `ffi` C-ABI surface)
//! and must stay `std`-only so the same source compiles for the mobile and
//! `wasm32-unknown-unknown` targets. Keeping the escaping in one place avoids
//! two copies drifting apart over time.

/// Appends `s` to `out` as a quoted, escaped JSON string literal, with the
/// escapes required by RFC 8259 §7.
pub(crate) fn write_json_string(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                use core::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}
