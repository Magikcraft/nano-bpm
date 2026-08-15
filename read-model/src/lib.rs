//! `nanobpmn-read-model` — the SQLite-backed read model (CQRS query side) of
//! nanobpmn, extracted from the gateway `server` crate so the *same* projection
//! and read-query surface can compile two ways behind a single backend seam:
//!
//! * feature `native` (default) — a real C SQLite via `rusqlite`'s `bundled`
//!   build. This is byte-for-byte what the `server` gateway uses today; the
//!   server-only orchestration it layers on top (partition sharding, WAL
//!   checkpointing, adaptive pruning, on-disk file paths) is either kept in the
//!   `server` crate or gated behind `#[cfg(feature = "native")]` here.
//! * feature `wasm` — an in-memory SQLite for `wasm32-unknown-unknown` via the
//!   optional `sqlite-wasm-rs` dependency (MemoryVFS). Declared now with a
//!   compiling stub backend ([`backend::wasm`]); a sibling task fills the body in.
//!
//! The public projection/query API ([`ReadStore`] and the `*Row` result types)
//! is backend-agnostic — it speaks `rusqlite` regardless of which SQLite
//! implementation backs the connection. Only [`backend::open_connection`] differs
//! per backend.

mod backend;
mod store;

pub use store::*;
