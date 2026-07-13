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

/// Files whose range is exactly `[start, end)` — i.e. previous compactions
/// of this precise bucket. More than one entry means late rows were
/// compacted into follow-up files. Ordered by path (paths embed a sequence
/// number, so this is creation order).
pub fn entries_for_bucket(
    conn: &Connection,
    logical_table: &str,
    start: i64,
    end: i64,
) -> Result<Vec<CatalogEntry>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {SELECT_COLS} FROM _silodb_catalog
         WHERE logical_table = ?1 AND status = 'active'
           AND range_start = ?2 AND range_end = ?3
         ORDER BY path"
    ))?;
    let rows = stmt.query_map(params![logical_table, start, end], entry_from_row)?;
    rows.collect()
}

/// How many rows — of ANY status — exist for exactly `[start, end)`.
/// This is the sequence-number source for new file names: counting only
/// active rows would reuse a superseded file's name, and a later GC of
/// the superseded row would then delete the new live file.
pub fn bucket_seq(conn: &Connection, logical_table: &str, start: i64, end: i64) -> Result<i64> {
    conn.query_row(
        "SELECT count(*) FROM _silodb_catalog
         WHERE logical_table = ?1 AND range_start = ?2 AND range_end = ?3",
        params![logical_table, start, end],
        |r| r.get(0),
    )
}

/// Active files lying entirely inside `[start, end)` — merge candidates
/// for tier promotion, *including* a previously merged window-sized file
/// (so a late straggler re-consolidates with it instead of accumulating
/// beside it). Ordered by (range_start, path) so concatenation preserves
/// time order for non-overlapping children.
pub fn entries_within(
    conn: &Connection,
    logical_table: &str,
    start: i64,
    end: i64,
) -> Result<Vec<CatalogEntry>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {SELECT_COLS} FROM _silodb_catalog
         WHERE logical_table = ?1 AND status = 'active'
           AND range_start >= ?2 AND range_end <= ?3
         ORDER BY range_start, path"
    ))?;
    let rows = stmt.query_map(params![logical_table, start, end], entry_from_row)?;
    rows.collect()
}

/// Flip one file to `status = 'superseded'` (invisible to readers, file
/// awaiting GC). Runs in the caller's ambient transaction.
pub fn supersede_entry(conn: &Connection, logical_table: &str, path: &str) -> Result<()> {
    conn.execute(
        "UPDATE _silodb_catalog SET status = 'superseded'
         WHERE logical_table = ?1 AND path = ?2",
        params![logical_table, path],
    )?;
    Ok(())
}

/// Rows awaiting file GC (merge children and retention-evicted files).
pub fn gc_entries(conn: &Connection, logical_table: &str) -> Result<Vec<CatalogEntry>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {SELECT_COLS} FROM _silodb_catalog
         WHERE logical_table = ?1 AND status IN ('superseded', 'evicted')
         ORDER BY path"
    ))?;
    let rows = stmt.query_map([logical_table], entry_from_row)?;
    rows.collect()
}

/// Remove one catalog row (after its file has been unlinked by GC).
pub fn delete_entry(conn: &Connection, logical_table: &str, path: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM _silodb_catalog WHERE logical_table = ?1 AND path = ?2",
        params![logical_table, path],
    )?;
    Ok(())
}

/// Per-table maintenance policy, set at init and read by `maintain()`.
/// `tiers_us` ascending, each dividing the next; buckets/windows are
/// epoch-aligned multiples of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePolicy {
    pub logical_table: String,
    /// Tier window sizes in microseconds, finest first.
    pub tiers_us: Vec<i64>,
    /// Don't touch data newer than now - margin.
    pub safety_margin_us: i64,
    /// Retention: evict cold files entirely older than now - retain.
    /// `None` = keep forever.
    pub retain_us: Option<i64>,
}

/// Create the policy table if needed, migrating older layouts. Idempotent.
pub fn ensure_policy_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _silodb_policy (
            logical_table     TEXT PRIMARY KEY,
            tiers_us          TEXT NOT NULL,  -- comma-separated i64 µs
            safety_margin_us  INTEGER NOT NULL,
            retain_us         INTEGER         -- NULL = keep forever
        );",
    )?;
    // Migration for policy tables created before retain_us existed.
    match conn.execute_batch("ALTER TABLE _silodb_policy ADD COLUMN retain_us INTEGER") {
        Ok(()) => Ok(()),
        Err(e) if e.to_string().contains("duplicate column") => Ok(()),
        Err(e) => Err(e),
    }
}

/// Insert or replace a table's policy.
pub fn set_policy(conn: &Connection, policy: &TablePolicy) -> Result<()> {
    ensure_policy_table(conn)?;
    let tiers = policy
        .tiers_us
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    conn.execute(
        "INSERT OR REPLACE INTO _silodb_policy VALUES (?1, ?2, ?3, ?4)",
        params![
            policy.logical_table,
            tiers,
            policy.safety_margin_us,
            policy.retain_us
        ],
    )?;
    Ok(())
}

/// A table's policy, if one was set.
pub fn get_policy(conn: &Connection, logical_table: &str) -> Result<Option<TablePolicy>> {
    let table_exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_silodb_policy'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if table_exists.is_none() {
        return Ok(None);
    }
    ensure_policy_table(conn)?; // migrate before reading retain_us
    conn.query_row(
        "SELECT logical_table, tiers_us, safety_margin_us, retain_us FROM _silodb_policy
         WHERE logical_table = ?1",
        [logical_table],
        |r| {
            let tiers: String = r.get(1)?;
            Ok(TablePolicy {
                logical_table: r.get(0)?,
                tiers_us: tiers
                    .split(',')
                    .filter_map(|t| t.parse().ok())
                    .collect(),
                safety_margin_us: r.get(2)?,
                retain_us: r.get(3)?,
            })
        },
    )
    .optional()
}

/// Flip every active file entirely older than `cutoff` to
/// `status = 'evicted'` (retention). Whole-file granularity: a file
/// straddling the cutoff is untouched until all of it has expired.
/// Returns the flipped entries. Runs in the caller's ambient transaction.
pub fn evict_older_than(
    conn: &Connection,
    logical_table: &str,
    cutoff: i64,
) -> Result<Vec<CatalogEntry>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {SELECT_COLS} FROM _silodb_catalog
         WHERE logical_table = ?1 AND status = 'active' AND range_end <= ?2
         ORDER BY range_start, path"
    ))?;
    let expired: Vec<CatalogEntry> = stmt
        .query_map(params![logical_table, cutoff], entry_from_row)?
        .collect::<Result<_>>()?;
    for e in &expired {
        conn.execute(
            "UPDATE _silodb_catalog SET status = 'evicted'
             WHERE logical_table = ?1 AND path = ?2",
            params![logical_table, e.path],
        )?;
    }
    Ok(expired)
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
