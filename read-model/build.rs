//! Build script for `nanobpmn-read-model`.
//!
//! Its ONLY job is to make the `wasm` backend link. On
//! `wasm32-unknown-unknown` the read model is built with `rusqlite`'s
//! `libsqlite3-sys` in *non-bundled* mode (no C SQLite of its own — see the
//! `wasm` feature in `Cargo.toml`): the actual `sqlite3_*` C symbols are
//! provided by the `sqlite-wasm-rs` crate instead (its compiled `libwsqlite3.a`,
//! plus an in-memory `MemoryVFS` registered as the default at load time).
//!
//! `libsqlite3-sys`'s build script does not know that, though. With no bundled
//! build and no system SQLite it cannot find on a cross target, it still emits a
//! bare `cargo:rustc-link-lib=dylib=sqlite3`, so the final `wasm-ld` link of any
//! dependent (`read-model`'s own artifacts, or the downstream `engine-wasm`
//! cdylib) fails with `error: unable to find library -lsqlite3` — even though the
//! symbols themselves are already present via `sqlite-wasm-rs`.
//!
//! We resolve that by dropping an **empty** `libsqlite3.a` archive on the link
//! search path. It satisfies the linker's file lookup for `-lsqlite3` (so the
//! "unable to find library" error goes away) while contributing zero objects and
//! therefore zero symbols — so it cannot collide with, or shadow, the real
//! `sqlite3_*` implementations `sqlite-wasm-rs` links in. A `rustc-link-search`
//! emitted here propagates to the final link of every crate that depends on
//! `read-model`, which is exactly where the `-lsqlite3` request is resolved.
//!
//! This is scoped tightly: it only runs for the `wasm` feature *and* a
//! `wasm32` target. The default `native` build (which uses `rusqlite/bundled`
//! and never emits a bare `-lsqlite3`) is left completely untouched, so this
//! adds nothing to the server's build.

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let wasm_feature = std::env::var_os("CARGO_FEATURE_WASM").is_some();
    let native_feature = std::env::var_os("CARGO_FEATURE_NATIVE").is_some();
    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();

    // Only the wasm-backend-on-wasm32 configuration needs the stub. When `native`
    // is also enabled the backend seam selects `native` (real bundled SQLite), so
    // the `-lsqlite3` request is not the wasm one and must not be shadowed.
    if !(wasm_feature && !native_feature && target_arch == "wasm32") {
        return;
    }

    let out_dir = std::env::var("OUT_DIR").expect("cargo always sets OUT_DIR for a build script");
    let stub = Path::new(&out_dir).join("libsqlite3.a");
    write_empty_archive(&stub);

    println!("cargo:rustc-link-search=native={out_dir}");
}

/// Writes a valid, empty `ar` archive (the classic Unix `!<arch>\n` magic with no
/// members). `wasm-ld` accepts it as satisfying `-lsqlite3` yet it defines no
/// symbols, so the real `sqlite3_*` symbols still resolve against
/// `sqlite-wasm-rs`.
fn write_empty_archive(path: &Path) {
    std::fs::write(path, b"!<arch>\n").expect("write empty libsqlite3.a stub");
}
