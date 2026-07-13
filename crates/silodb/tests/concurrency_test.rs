//! Concurrency: a second connection reads the single-name view while the
//! first one inserts and compacts. WAL mode — the deployment reality on a
//! device where the query path and the compaction schedule are separate
//! from the ingest path.
//!
//! Invariants checked from the reader's side:
//! - no errors, ever (busy_timeout absorbs write locks)
//! - the view's row count is monotonically non-decreasing (rows move
//!   between tiers but are never transiently lost or doubled)

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rusqlite::{params, Connection};
use silodb::CompactOutcome;

const SCHEMA: &str = "ts TIMESTAMP, value REAL";
const BUCKETS: i64 = 30;
const ROWS_PER_BUCKET: i64 = 25;

fn open(db: &std::path::Path) -> Connection {
    let conn = Connection::open(db).unwrap();
    conn.busy_timeout(std::time::Duration::from_secs(10)).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    silodb::load_module(&conn).unwrap();
    conn
}

#[test]
fn reader_sees_consistent_counts_while_writer_compacts() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cold");
    let db = dir.path().join("hot.db");

    let writer = open(&db);
    silodb::init_table(&writer, "readings", SCHEMA, &base).unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let reader_done = done.clone();
    let reader_db = db.clone();
    let reader = std::thread::spawn(move || -> (u64, i64) {
        let conn = open(&reader_db);
        let mut last: i64 = 0;
        let mut reads: u64 = 0;
        while !reader_done.load(Ordering::Relaxed) {
            let n: i64 = conn
                .query_row("SELECT count(*) FROM readings", [], |r| r.get(0))
                .expect("reader must never error");
            assert!(
                n >= last,
                "view count went backwards ({last} -> {n}): rows transiently lost"
            );
            // Rows must never be double-counted either: the ceiling is
            // everything the writer could possibly have inserted.
            assert!(n <= BUCKETS * ROWS_PER_BUCKET, "double-counted rows: {n}");
            last = n;
            reads += 1;
        }
        (reads, last)
    });

    for b in 0..BUCKETS {
        let start = b * 1000;
        for i in 0..ROWS_PER_BUCKET {
            writer
                .execute(
                    "INSERT INTO readings VALUES (?1, ?2)",
                    params![start + i, i as f64],
                )
                .unwrap();
        }
        let outcome =
            silodb::compact_table(&writer, "readings", start, start + 1000, &base).unwrap();
        assert!(
            matches!(outcome, CompactOutcome::Compacted { rows, .. } if rows == ROWS_PER_BUCKET as usize),
            "{outcome:?}"
        );
    }
    done.store(true, Ordering::Relaxed);
    let (reads, last_seen) = reader.join().unwrap();

    assert!(reads > 0, "reader never got a look in");
    assert!(last_seen <= BUCKETS * ROWS_PER_BUCKET);
    // Final state, from a fresh connection: everything present exactly once.
    let check = open(&db);
    let total: i64 = check
        .query_row("SELECT count(*) FROM readings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total, BUCKETS * ROWS_PER_BUCKET);
    let hot: i64 = check
        .query_row("SELECT count(*) FROM readings_hot", [], |r| r.get(0))
        .unwrap();
    assert_eq!(hot, 0, "all buckets compacted");
}
