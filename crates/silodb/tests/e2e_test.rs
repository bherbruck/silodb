//! End-to-end through the facade: hot writes → compaction → one connection
//! querying hot + cold as a single view. This is the product story; if this
//! passes, the read and write paths actually connect.

use rusqlite::{params, Connection};
use silodb::{compact_bucket, BucketSpec, CompactOutcome};

fn spec(start: i64, end: i64) -> BucketSpec<'static> {
    BucketSpec {
        hot_table: "readings",
        logical_table: "readings",
        ts_column: "ts",
        bucket_start: start,
        bucket_end: end,
    }
}

#[test]
fn hot_and_cold_union_view_over_compacted_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let cold_dir = dir.path().join("readings");
    std::fs::create_dir(&cold_dir).unwrap();

    let conn = Connection::open_in_memory().unwrap();
    silodb::catalog::ensure_catalog(&conn).unwrap();
    silodb::load_module(&conn).unwrap();
    conn.execute_batch(
        "CREATE TABLE readings (ts INTEGER NOT NULL, value REAL, name TEXT)",
    )
    .unwrap();

    // 3 buckets of history plus rows still hot.
    for i in 0..40i64 {
        conn.execute(
            "INSERT INTO readings VALUES (?1, ?2, ?3)",
            params![i * 100, i as f64, format!("s{i}")],
        )
        .unwrap();
    }

    // Compact the first three closed buckets ([0,1000), [1000,2000),
    // [2000,3000)); ts 3000.. stays hot.
    for b in 0..3i64 {
        let start = b * 1000;
        let out = cold_dir.join(format!("bucket-{start}.parquet"));
        let outcome = compact_bucket(&conn, &spec(start, start + 1000), &out).unwrap();
        assert_eq!(outcome, CompactOutcome::Compacted { rows: 10 });
    }

    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE cold USING silodb('{}');
         CREATE VIEW all_readings AS
           SELECT ts, value, name FROM readings
           UNION ALL
           SELECT ts, value, name FROM cold;",
        cold_dir.display()
    ))
    .unwrap();

    // Every row is visible exactly once through the view.
    let total: i64 = conn
        .query_row("SELECT count(*) FROM all_readings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total, 40);
    let hot: i64 = conn
        .query_row("SELECT count(*) FROM readings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(hot, 10, "only the unclosed bucket stays hot");

    // A range query spanning the cold/hot boundary comes back whole and
    // ordered, through the view.
    let ts: Vec<i64> = conn
        .prepare("SELECT ts FROM all_readings WHERE ts >= 2500 AND ts < 3500 ORDER BY ts")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(ts, (25..35).map(|i| i * 100).collect::<Vec<_>>());

    // The cold side pruned: only the file covering [2000,3000) was a
    // candidate for ts >= 2500.
    let stats = silodb::last_scan_stats().unwrap();
    assert_eq!(stats.total_files, 3);
    assert_eq!(stats.candidate_files, 1);

    // A bucket compacted after the vtab and view exist shows up without DDL.
    let out = cold_dir.join("bucket-3000.parquet");
    assert_eq!(
        compact_bucket(&conn, &spec(3000, 4000), &out).unwrap(),
        CompactOutcome::Compacted { rows: 10 }
    );
    let total: i64 = conn
        .query_row("SELECT count(*) FROM all_readings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(total, 40, "still 40 — moved, not duplicated");
    let hot: i64 = conn
        .query_row("SELECT count(*) FROM readings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(hot, 0);
}
