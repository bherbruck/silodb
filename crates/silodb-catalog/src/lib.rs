//! `_silodb_catalog`: the source of truth for which Parquet files make up
//! each logical cold table and what timestamp range each covers.
//!
//! Lives in the *hot* SQLite database so it inherits that database's
//! transactional guarantees — a compaction is durable exactly when the
//! transaction that deletes hot rows and inserts the catalog row commits.
//! Both `silodb-vtab` (read: which files can this query touch?) and
//! `silodb-compact` (write: record the file I just produced) depend on this
//! crate; neither depends on the other.
//!
//! Depends on `rusqlite` only — no `parquet`/`arrow` here.

use rusqlite::{params, Connection, OptionalExtension, Result, Row};

/// Name of the catalog table.
pub const CATALOG_TABLE: &str = "_silodb_catalog";

/// One catalog row: one immutable Parquet file belonging to one logical
/// table, covering `[range_start, range_end)` in the hot table's timestamp
/// domain (epoch microseconds by silodb convention — see `silodb-schema`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    pub logical_table: String,
    /// Path as given to `compact_bucket` — absolute, or relative to the
    /// embedding application's working directory. Stored verbatim.
    pub path: String,
    pub range_start: i64,
    pub range_end: i64,
    pub row_count: Option<i64>,
    /// Epoch seconds, stamped by SQLite at insert time.
    pub created_at: i64,
    /// Reserved for future retention/eviction use; always 'active' for now.
    pub status: String,
}

/// Create the catalog table and its range index if they don't exist yet.
/// Idempotent; call it before any other operation in this crate.
pub fn ensure_catalog(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _silodb_catalog (
            logical_table TEXT NOT NULL,
            path          TEXT NOT NULL,
            range_start   INTEGER NOT NULL,
            range_end     INTEGER NOT NULL,
            row_count     INTEGER,
            created_at    INTEGER NOT NULL,
            status        TEXT NOT NULL DEFAULT 'active',
            PRIMARY KEY (logical_table, path)
        );
        CREATE INDEX IF NOT EXISTS idx_silodb_catalog_range
          ON _silodb_catalog(logical_table, range_start, range_end);",
    )
}

/// True if the catalog table exists in this database.
pub fn catalog_exists(conn: &Connection) -> Result<bool> {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [CATALOG_TABLE],
        |_| Ok(()),
    )
    .optional()
    .map(|o| o.is_some())
}

/// Insert the row for a freshly compacted file. `created_at` is stamped by
/// SQLite (`unixepoch()`); the passed entry's `created_at`/`status` fields
/// are ignored on insert. Runs in the caller's ambient transaction if one
/// is open — `compact_bucket` relies on that.
pub fn insert_entry(conn: &Connection, entry: &CatalogEntry) -> Result<()> {
    conn.execute(
        "INSERT INTO _silodb_catalog
           (logical_table, path, range_start, range_end, row_count, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, unixepoch())",
        params![
            entry.logical_table,
            entry.path,
            entry.range_start,
            entry.range_end,
            entry.row_count,
        ],
    )?;
    Ok(())
}

fn entry_from_row(row: &Row<'_>) -> Result<CatalogEntry> {
    Ok(CatalogEntry {
        logical_table: row.get(0)?,
        path: row.get(1)?,
        range_start: row.get(2)?,
        range_end: row.get(3)?,
        row_count: row.get(4)?,
        created_at: row.get(5)?,
        status: row.get(6)?,
    })
}

const SELECT_COLS: &str =
    "logical_table, path, range_start, range_end, row_count, created_at, status";

/// The entry for one specific file, if present.
pub fn entry_for_path(
    conn: &Connection,
    logical_table: &str,
    path: &str,
) -> Result<Option<CatalogEntry>> {
    conn.query_row(
        &format!(
            "SELECT {SELECT_COLS} FROM _silodb_catalog
             WHERE logical_table = ?1 AND path = ?2"
        ),
        params![logical_table, path],
        entry_from_row,
    )
    .optional()
}

/// Active files for a logical table whose range may contain a timestamp in
/// `[lo, hi]` (inclusive query bounds; pass `i64::MIN`/`i64::MAX` for an
/// unbounded side). Entry ranges are half-open `[range_start, range_end)` —
/// that exclusivity is part of the catalog contract (`compact_bucket`'s
/// `bucket_end` is exclusive), so `range_end == lo` is a provable
/// non-overlap, not a boundary case to keep.
/// Ordered by `range_start` so scans read oldest bucket first.
pub fn entries_overlapping(
    conn: &Connection,
    logical_table: &str,
    lo: i64,
    hi: i64,
) -> Result<Vec<CatalogEntry>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {SELECT_COLS} FROM _silodb_catalog
         WHERE logical_table = ?1 AND status = 'active'
           AND range_start <= ?3 AND range_end > ?2
         ORDER BY range_start, path"
    ))?;
    let rows = stmt.query_map(params![logical_table, lo, hi], entry_from_row)?;
    rows.collect()
}

/// All active files for a logical table, oldest bucket first.
pub fn entries_for_table(conn: &Connection, logical_table: &str) -> Result<Vec<CatalogEntry>> {
    entries_overlapping(conn, logical_table, i64::MIN, i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(table: &str, path: &str, start: i64, end: i64) -> CatalogEntry {
        CatalogEntry {
            logical_table: table.into(),
            path: path.into(),
            range_start: start,
            range_end: end,
            row_count: Some(4),
            created_at: 0,
            status: "active".into(),
        }
    }

    fn setup() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        ensure_catalog(&conn).unwrap();
        conn
    }

    #[test]
    fn ensure_catalog_is_idempotent() {
        let conn = setup();
        assert!(catalog_exists(&conn).unwrap());
        ensure_catalog(&conn).unwrap();
    }

    #[test]
    fn insert_and_lookup_round_trip() {
        let conn = setup();
        insert_entry(&conn, &entry("t", "a.parquet", 0, 1000)).unwrap();
        let got = entry_for_path(&conn, "t", "a.parquet").unwrap().unwrap();
        assert_eq!(got.range_start, 0);
        assert_eq!(got.range_end, 1000);
        assert_eq!(got.row_count, Some(4));
        assert_eq!(got.status, "active");
        assert!(got.created_at > 0, "created_at stamped by SQLite");
        assert!(entry_for_path(&conn, "t", "b.parquet").unwrap().is_none());
        assert!(entry_for_path(&conn, "other", "a.parquet").unwrap().is_none());
    }

    #[test]
    fn duplicate_path_for_same_table_is_rejected() {
        let conn = setup();
        insert_entry(&conn, &entry("t", "a.parquet", 0, 1000)).unwrap();
        assert!(insert_entry(&conn, &entry("t", "a.parquet", 5, 6)).is_err());
        // Same path under a different logical table is fine.
        insert_entry(&conn, &entry("t2", "a.parquet", 0, 1000)).unwrap();
    }

    #[test]
    fn overlap_query_selects_only_touching_ranges() {
        let conn = setup();
        insert_entry(&conn, &entry("t", "a.parquet", 0, 1000)).unwrap();
        insert_entry(&conn, &entry("t", "b.parquet", 1000, 2000)).unwrap();
        insert_entry(&conn, &entry("t", "c.parquet", 2000, 3000)).unwrap();
        insert_entry(&conn, &entry("u", "d.parquet", 0, 3000)).unwrap();

        let paths = |lo: i64, hi: i64| -> Vec<String> {
            entries_overlapping(&conn, "t", lo, hi)
                .unwrap()
                .into_iter()
                .map(|e| e.path)
                .collect()
        };

        assert_eq!(paths(1200, 1800), vec!["b.parquet"]);
        assert_eq!(paths(500, 1500), vec!["a.parquet", "b.parquet"]);
        assert_eq!(
            paths(i64::MIN, i64::MAX),
            vec!["a.parquet", "b.parquet", "c.parquet"]
        );
        assert_eq!(paths(3500, 4000), Vec::<String>::new());
        // Half-open boundary: b covers [1000, 2000) — ts 2000 is c's, not
        // b's. hi exactly at c's range_start keeps c.
        assert_eq!(paths(2000, 2000), vec!["c.parquet"]);
        assert_eq!(paths(1999, 1999), vec!["b.parquet"]);
    }

    #[test]
    fn entries_for_table_orders_by_range_start() {
        let conn = setup();
        insert_entry(&conn, &entry("t", "z-late.parquet", 5000, 6000)).unwrap();
        insert_entry(&conn, &entry("t", "a-early.parquet", 0, 1000)).unwrap();
        let got: Vec<String> = entries_for_table(&conn, "t")
            .unwrap()
            .into_iter()
            .map(|e| e.path)
            .collect();
        assert_eq!(got, vec!["a-early.parquet", "z-late.parquet"]);
    }
}
