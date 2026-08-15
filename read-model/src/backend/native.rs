//! The `native` backend: a real, file-or-memory C SQLite via `rusqlite`'s
//! `bundled` build. This is byte-for-byte the connection setup the gateway
//! `server` has always used for its read-model store.

use std::path::Path;

use rusqlite::Connection;

/// WAL autocheckpoint threshold in pages for the read-model store. Default 12288
/// (~48 MiB at the 4 KiB page size), 12x SQLite's built-in 1000, chosen to
/// coalesce repeated dirties of hot pages before they are copied back into the
/// main DB. Acts as the always-safe backstop bound on the WAL (works even when no
/// adaptive pruner runs). `NANOBPMN_READ_WAL_AUTOCHECKPOINT` overrides; 0 disables
/// SQLite's automatic checkpoint entirely (only safe when a pruner checkpoints).
fn read_wal_autocheckpoint_pages() -> i64 {
    std::env::var("NANOBPMN_READ_WAL_AUTOCHECKPOINT")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|v| *v >= 0)
        .unwrap_or(12288)
}

/// Opens (and tunes) the read-model connection at `path`, or an in-memory
/// database when `path` is `None`.
///
/// For a file-backed store the read model is a *derived* projection: on boot it
/// is rebuilt from the journal (the source of truth) by replaying events past
/// `exported_position`, so its own fsync durability is not load-bearing. We
/// therefore run WAL + `synchronous=NORMAL`: this removes the per-commit fsync
/// media barrier from the exporter's hot loop (the single per-node projection
/// thread all partitions funnel through) while still surviving app crashes; an
/// OS/power loss at worst rewinds the projection, which boot catch-up rebuilds.
/// A tie in these numbers is the read-model exporter's throughput ceiling, so
/// this is the cheapest lever on it. `synchronous` can be overridden via
/// `NANOBPMN_READ_SYNC` (e.g. FULL to restore the old durability, OFF to isolate
/// fsync cost in a spike). In-memory stores skip this (no journal file).
pub(crate) fn open_connection(path: Option<&Path>) -> rusqlite::Result<Connection> {
    let conn = match path {
        Some(p) => Connection::open(p)?,
        None => Connection::open_in_memory()?,
    };
    if path.is_some() {
        let sync = std::env::var("NANOBPMN_READ_SYNC").unwrap_or_else(|_| "NORMAL".into());
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", &sync)?;
        // WAL autocheckpoint threshold (pages). SQLite's built-in default is
        // 1000 (~4 MiB), which under a sustained projection flood checkpoints
        // hot pages back into the multi-hundred-MB main DB extremely often —
        // rewriting the same index / freelist / recently-inserted leaf pages
        // over and over and amplifying *physical* disk writes ~100x over the
        // logical change. A larger window coalesces repeated dirties of a page
        // into a single copy-back, cutting checkpoint write volume (and thus
        // disk saturation and the fsync-latency it inflates on a shared disk)
        // roughly in proportion. This is the backstop bound; when the adaptive
        // pruner runs it checkpoints off the exporter thread at a lower
        // threshold (see `maybe_checkpoint_wal`) so this rarely fires inline.
        // `NANOBPMN_READ_WAL_AUTOCHECKPOINT` overrides (0 disables auto).
        conn.pragma_update(None, "wal_autocheckpoint", read_wal_autocheckpoint_pages())?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
    }
    Ok(conn)
}
