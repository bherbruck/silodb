//! End-to-end through the facade: hot writes → compaction → one connection
//! querying hot + cold as a single view. This is the product story; if this
//! passes, the read and write paths actually connect.
//!
//! Deliberately runs in the day-zero order: the vtab and view are created
//! BEFORE anything is compacted (before the catalog even exists).

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

fn view_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT count(*) FROM all_readings", [], |r| r.get(0))
        .unwrap()
}

#[test]
fn hot_and_cold_union_view_over_compacted_buckets() {
    let base = tempfile::tempdir().unwrap();
    let cold_dir = base.path().join("readings");
    std::fs::create_dir(&cold_dir).unwrap();

    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    conn.execute_batch(
        "CREATE TABLE readings (ts INTEGER NOT NULL, value REAL, name TEXT)",
    )
    .unwrap();

    // Day zero: vtab + view exist before any compaction, before the
    // catalog exists. Schema comes from the hot table.
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE cold USING silodb('{}', table=readings);
         CREATE VIEW all_readings AS
           SELECT ts, value, name FROM readings
           UNION ALL
           SELECT ts, value, name FROM cold;",
        base.path().display()
    ))
    .unwrap();
    assert_eq!(view_count(&conn), 0);

    // 3 buckets of history plus rows still hot.
    for i in 0..40i64 {
        conn.execute(
            "INSERT INTO readings VALUES (?1, ?2, ?3)",
            params![i * 100, i as f64, format!("s{i}")],
        )
        .unwrap();
    }
    assert_eq!(view_count(&conn), 40);

    // Compact the first three closed buckets ([0,1000), [1000,2000),
    // [2000,3000)); ts 3000.. stays hot. The view total never changes.
    for b in 0..3i64 {
        let start = b * 1000;
        let outcome = compact_bucket(&conn, &spec(start, start + 1000), &cold_dir).unwrap();
        assert!(
            matches!(outcome, CompactOutcome::Compacted { rows: 10, .. }),
            "{outcome:?}"
        );
        assert_eq!(view_count(&conn), 40, "moved, never duplicated or lost");
    }
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

    // A bucket compacted later shows up without DDL.
    assert!(matches!(
        compact_bucket(&conn, &spec(3000, 4000), &cold_dir).unwrap(),
        CompactOutcome::Compacted { rows: 10, .. }
    ));
    assert_eq!(view_count(&conn), 40);
    let hot: i64 = conn
        .query_row("SELECT count(*) FROM readings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(hot, 0);

    // Late rows into a compacted bucket: still invisible to the user —
    // compact again, view stays consistent throughout.
    conn.execute(
        "INSERT INTO readings VALUES (1500, 99.0, 'late')",
        [],
    )
    .unwrap();
    assert_eq!(view_count(&conn), 41);
    assert!(matches!(
        compact_bucket(&conn, &spec(1000, 2000), &cold_dir).unwrap(),
        CompactOutcome::Compacted { rows: 1, .. }
    ));
    assert_eq!(view_count(&conn), 41);
}
