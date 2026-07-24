//! Shared SQLite on-disk space accounting and reclamation helpers.
//!
//! Both the read-model store ([`crate::readstore`]) and the destructive var-spill
//! cache ([`crate::varspill`]) sit on SQLite files whose *live* row set is kept
//! bounded (adaptive retention / destructive reads + terminal eviction), but whose
//! *file* only ever grows to the freelist high-water mark: SQLite reuses freed
//! pages for later inserts and never returns them to the OS on its own. A single
//! backlog spike therefore inflates the file for the lifetime of the process.
//!
//! This module centralises the two primitives that address that so neither store
//! duplicates the knowledge:
//!
//! * [`page_stats`] — O(1) `(file_bytes, live_bytes)` accounting from the header.
//! * [`enable_incremental_auto_vacuum`] — puts a store into `INCREMENTAL`
//!   auto-vacuum mode so freed pages land on a reclaimable freelist.
//! * [`reclaim_freelist`] — returns freelist pages to the OS via
//!   `incremental_vacuum` and truncates the WAL, gated on a byte threshold so it
//!   runs only when there is meaningful space to reclaim.

use rusqlite::Connection;

/// Reads a SQLite database's size as `(file_bytes, live_bytes)` from its header:
/// `file_bytes = page_count × page_size` (the whole allocated file, freelist
/// included) and `live_bytes = (page_count − freelist_count) × page_size` (the
/// pages holding actual data). All three PRAGMAs are O(1) header reads, so this is
/// cheap enough for a hot loop.
pub fn page_stats(conn: &Connection) -> (u64, u64) {
    let page_count: i64 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap_or(0);
    let freelist: i64 = conn
        .query_row("PRAGMA freelist_count", [], |r| r.get(0))
        .unwrap_or(0);
    let page_size: i64 = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap_or(4096);
    let ps = page_size.max(0) as u64;
    let file = page_count.max(0) as u64 * ps;
    let live = (page_count - freelist).max(0) as u64 * ps;
    (file, live)
}

/// Puts `conn` into `INCREMENTAL` auto-vacuum mode so that deleted rows move their
/// pages onto a freelist that [`reclaim_freelist`] can later return to the OS.
///
/// Setting the pragma only *records* the request; the accompanying `VACUUM`
/// rewrites the database to install the auto-vacuum pointer map and actually switch
/// mode (a new database is fine before any table exists, but an existing one needs
/// the VACUUM). On a fresh or just-wiped (empty) database this VACUUM is effectively
/// free, so this is intended for wipe-on-open caches like [`crate::varspill`];
/// callers holding large persistent data should instead set the pragma at creation
/// time to avoid a full rewrite.
pub fn enable_incremental_auto_vacuum(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("PRAGMA auto_vacuum=INCREMENTAL; VACUUM;")?;
    Ok(())
}

/// Returns freelist pages to the OS when at least `threshold_bytes` are reclaimable,
/// otherwise a no-op. Runs `PRAGMA incremental_vacuum` (moves freelist pages out of
/// the main database file — requires [`enable_incremental_auto_vacuum`]) followed by
/// a `wal_checkpoint(TRUNCATE)` so the freed space actually leaves the `-wal`
/// sidecar and the main file shrinks on disk. Returns the number of bytes reclaimed
/// from the file (0 if the gate was not met).
///
/// Gating avoids a per-delete `incremental_vacuum`/checkpoint storm: it fires only
/// once enough space has accumulated to be worth a full copy-back, so in steady
/// state (bounded live set) it rarely runs at all, and it caps the file at roughly
/// `live + threshold` rather than the historical peak.
pub fn reclaim_freelist(conn: &Connection, threshold_bytes: u64) -> rusqlite::Result<u64> {
    let (file_before, live) = page_stats(conn);
    if file_before.saturating_sub(live) < threshold_bytes {
        return Ok(0);
    }
    // In WAL mode the pages freed by the just-committed eviction transaction live
    // in the `-wal` sidecar until a checkpoint copies them into the main database;
    // `incremental_vacuum` can only truncate pages that are already in the main
    // file. So checkpoint first (land the frees), then vacuum (move freed pages off
    // the end and shrink the main file), then checkpoint again to flush the
    // truncation out of the WAL. Checkpoints are best-effort — a concurrent reader
    // can hold TRUNCATE back, which is fine: the next reclaim retries.
    let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| {
        Ok::<_, rusqlite::Error>(())
    });
    // Reclaim the whole freelist. `PRAGMA incremental_vacuum` frees pages as the
    // statement is *stepped*, and rusqlite's `execute*`/`execute_batch` step a
    // no-result pragma only once (freeing a single page). Driving a prepared
    // statement to completion frees every reclaimable page in one call, the way the
    // sqlite3 CLI's exec loop does.
    {
        let mut stmt = conn.prepare("PRAGMA incremental_vacuum")?;
        let mut rows = stmt.query([])?;
        while rows.next()?.is_some() {}
    }
    let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| {
        Ok::<_, rusqlite::Error>(())
    });
    let (file_after, _) = page_stats(conn);
    Ok(file_before.saturating_sub(file_after))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_incremental() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        enable_incremental_auto_vacuum(&conn).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT NOT NULL);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn page_stats_reports_growth_and_freelist() {
        let conn = open_incremental();
        let (file0, _live0) = page_stats(&conn);
        let blob = "x".repeat(4096);
        for k in 0..2000 {
            conn.execute("INSERT INTO t (k, v) VALUES (?1, ?2)", (k, &blob))
                .unwrap();
        }
        let (file1, live1) = page_stats(&conn);
        assert!(file1 > file0, "file should grow after inserts");
        assert!(live1 > 0);
        // Delete everything: pages move onto the freelist, file stays at high-water.
        conn.execute("DELETE FROM t", []).unwrap();
        let (file2, live2) = page_stats(&conn);
        assert!(
            file2.saturating_sub(live2) > 0,
            "deleted pages should sit on the freelist"
        );
    }

    #[test]
    fn reclaim_shrinks_file_when_gate_met() {
        let conn = open_incremental();
        let blob = "x".repeat(4096);
        for k in 0..2000 {
            conn.execute("INSERT INTO t (k, v) VALUES (?1, ?2)", (k, &blob))
                .unwrap();
        }
        let (file_peak, _) = page_stats(&conn);
        conn.execute("DELETE FROM t", []).unwrap();
        // Gate at 1 byte so any freelist triggers the reclaim.
        let reclaimed = reclaim_freelist(&conn, 1).unwrap();
        let (file_after, live_after) = page_stats(&conn);
        assert!(reclaimed > 0, "should reclaim freed pages to the OS");
        // The whole freelist must be drained, not a single page: the file collapses
        // back toward the (now empty) live set rather than holding the high-water.
        assert!(
            file_after < file_peak / 4,
            "file should collapse after reclaim, not free one page (peak={file_peak}, after={file_after})"
        );
        let freelist_after = file_after.saturating_sub(live_after);
        assert!(
            freelist_after < file_peak / 4,
            "freelist should be drained after reclaim (leftover={freelist_after})"
        );
    }

    #[test]
    fn reclaim_is_noop_below_threshold() {
        let conn = open_incremental();
        let blob = "x".repeat(4096);
        for k in 0..100 {
            conn.execute("INSERT INTO t (k, v) VALUES (?1, ?2)", (k, &blob))
                .unwrap();
        }
        conn.execute("DELETE FROM t", []).unwrap();
        // Huge threshold: nothing should be reclaimed.
        let reclaimed = reclaim_freelist(&conn, 1 << 30).unwrap();
        assert_eq!(reclaimed, 0);
    }
}
