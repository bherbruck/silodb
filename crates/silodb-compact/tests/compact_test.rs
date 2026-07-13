//! Phase 3 acceptance: temp-file/fsync/rename/transactional-delete
//! sequencing, idempotency across every calling pattern (specv2: no
//! user-visible failure modes short of real data loss), and the
//! bounded-cost invariant.

use std::fs::File;
use std::path::Path;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rusqlite::{params, Connection};
use silodb_compact::{compact_bucket, BucketSpec, CompactError, CompactOutcome};

fn hot_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE readings (
            ts      INTEGER NOT NULL,
            value   REAL,
            name    TEXT,
            payload BLOB
        );",
    )
    .unwrap();
    conn
}

fn insert_row(conn: &Connection, ts: i64, value: Option<f64>, name: Option<&str>) {
    conn.execute(
        "INSERT INTO readings (ts, value, name, payload) VALUES (?1, ?2, ?3, ?4)",
        params![ts, value, name, name.map(str::as_bytes)],
    )
    .unwrap();
}

fn spec(start: i64, end: i64) -> BucketSpec<'static> {
    BucketSpec {
        hot_table: "readings",
        logical_table: "readings",
        ts_column: "ts",
        bucket_start: start,
        bucket_end: end,
    }
}

fn hot_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT count(*) FROM readings", [], |r| r.get(0))
        .unwrap()
}

fn compacted(outcome: CompactOutcome) -> (usize, std::path::PathBuf) {
    match outcome {
        CompactOutcome::Compacted { rows, path } => (rows, path),
        other => panic!("expected Compacted, got {other:?}"),
    }
}

/// ts values read straight from a Parquet file.
fn parquet_ts(path: &Path) -> Vec<i64> {
    use arrow::array::{Array, TimestampMicrosecondArray};
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let ts = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for i in 0..ts.len() {
            out.push(ts.value(i));
        }
    }
    out
}

#[test]
fn round_trip_moves_exactly_the_bucket() {
    let conn = hot_db();
    // In-bucket rows (with NULLs), plus rows before/after the bucket.
    insert_row(&conn, 500, Some(0.5), Some("before"));
    for i in 0..10 {
        let ts = 1000 + i * 100;
        insert_row(
            &conn,
            ts,
            (i != 3).then_some(i as f64),
            (i != 7).then(|| format!("s{i}")).as_deref(),
        );
    }
    insert_row(&conn, 2500, Some(9.9), Some("after"));

    let dir = tempfile::tempdir().unwrap();
    let (rows, path) = compacted(compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap());
    assert_eq!(rows, 10);
    assert_eq!(
        path.file_name().unwrap().to_str().unwrap(),
        "bucket-1000-2000-0.parquet",
        "range and sequence encoded for human debuggability"
    );

    // Parquet holds exactly the bucket, ordered by ts.
    let ts = parquet_ts(&path);
    assert_eq!(ts, (0..10).map(|i| 1000 + i * 100).collect::<Vec<_>>());
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        1,
        "no temp file left behind"
    );

    // Hot table keeps only out-of-bucket rows.
    assert_eq!(hot_count(&conn), 2);

    // Catalog row committed with the right range and count.
    let entry = silodb_catalog::entry_for_path(&conn, "readings", &path.display().to_string())
        .unwrap()
        .unwrap();
    assert_eq!(entry.range_start, 1000);
    assert_eq!(entry.range_end, 2000);
    assert_eq!(entry.row_count, Some(10));
}

#[test]
fn null_and_type_round_trip_through_parquet() {
    use arrow::array::{Array, BinaryArray, Float64Array, StringArray};
    let conn = hot_db();
    insert_row(&conn, 1000, None, Some("a"));
    insert_row(&conn, 1100, Some(1.5), None);

    let dir = tempfile::tempdir().unwrap();
    let (_, path) = compacted(compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap());

    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let value = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(value.is_null(0));
    assert_eq!(value.value(1), 1.5);
    let name = batch
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(0), "a");
    assert!(name.is_null(1));
    let payload = batch
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(payload.value(0), b"a");
    assert!(payload.is_null(1));
}

#[test]
fn empty_bucket_is_a_clean_noop() {
    let conn = hot_db();
    insert_row(&conn, 5000, Some(1.0), Some("elsewhere"));
    let dir = tempfile::tempdir().unwrap();
    let outcome = compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap();
    assert_eq!(outcome, CompactOutcome::EmptyBucket);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    assert_eq!(hot_count(&conn), 1);
}

#[test]
fn rerun_after_success_is_a_noop() {
    let conn = hot_db();
    insert_row(&conn, 1000, Some(1.0), Some("x"));
    let dir = tempfile::tempdir().unwrap();
    let (_, path) = compacted(compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap());
    let bytes_before = std::fs::read(&path).unwrap();

    let outcome = compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap();
    assert_eq!(outcome, CompactOutcome::AlreadyCompacted);
    assert_eq!(std::fs::read(&path).unwrap(), bytes_before, "file untouched");

    // Still exactly one catalog row.
    let n: i64 = conn
        .query_row("SELECT count(*) FROM _silodb_catalog", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
}

/// The spec's simulated-crash case: the process dies between the rename and
/// the delete+catalog transaction. On disk: a complete Parquet file. In the
/// DB: hot rows intact, no catalog row. Re-running must land on the same
/// sequence — producing the same file — and commit. No duplication.
#[test]
fn rerun_after_crash_between_rename_and_commit() {
    // Build the crashed state directly: run a full compaction on a throwaway
    // DB to obtain the exact file a finished run produces...
    let dir = tempfile::tempdir().unwrap();
    let make_hot = || {
        let conn = hot_db();
        for i in 0..5 {
            insert_row(&conn, 1000 + i * 10, Some(i as f64), Some("r"));
        }
        insert_row(&conn, 9000, Some(1.0), Some("outside"));
        conn
    };
    let throwaway = make_hot();
    let (_, path) = compacted(compact_bucket(&throwaway, &spec(1000, 2000), dir.path()).unwrap());
    let file_from_finished_run = std::fs::read(&path).unwrap();

    // ...and pair that file with a fresh DB whose transaction "never ran".
    let conn = make_hot();
    assert_eq!(hot_count(&conn), 6);

    let (rows, rerun_path) =
        compacted(compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap());
    assert_eq!(rows, 5);
    assert_eq!(rerun_path, path, "same sequence number recomputed");
    assert_eq!(
        std::fs::read(&rerun_path).unwrap(),
        file_from_finished_run,
        "re-run must produce an identical file"
    );
    assert_eq!(hot_count(&conn), 1, "bucket rows deleted exactly once");
    let entry =
        silodb_catalog::entry_for_path(&conn, "readings", &rerun_path.display().to_string())
            .unwrap()
            .unwrap();
    assert_eq!(entry.row_count, Some(5));
}

/// Late rows landing in an already-compacted bucket are not an error: the
/// next run writes a follow-up file (next sequence number) for the same
/// range, invisible to the caller.
#[test]
fn late_rows_get_a_follow_up_file() {
    let conn = hot_db();
    insert_row(&conn, 1000, Some(1.0), Some("x"));
    let dir = tempfile::tempdir().unwrap();
    let (_, first) = compacted(compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap());

    // A write lands inside the already-compacted bucket.
    insert_row(&conn, 1500, Some(2.0), Some("late"));
    let (rows, second) = compacted(compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap());
    assert_eq!(rows, 1);
    assert_eq!(
        second.file_name().unwrap().to_str().unwrap(),
        "bucket-1000-2000-1.parquet"
    );
    assert_ne!(first, second);
    assert_eq!(parquet_ts(&second), vec![1500]);
    assert_eq!(hot_count(&conn), 0, "late row aged out too");

    // Both files are catalog-committed for the same range.
    let entries = silodb_catalog::entries_for_bucket(&conn, "readings", 1000, 2000).unwrap();
    assert_eq!(entries.len(), 2);

    // And a further re-run is back to a no-op.
    assert_eq!(
        compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap(),
        CompactOutcome::AlreadyCompacted
    );
}

#[test]
fn refuses_when_catalog_references_missing_file() {
    let conn = hot_db();
    insert_row(&conn, 1000, Some(1.0), Some("x"));
    let dir = tempfile::tempdir().unwrap();
    let (_, path) = compacted(compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap());
    std::fs::remove_file(&path).unwrap();

    let err = compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap_err();
    assert!(matches!(err, CompactError::MissingCompactedFile { .. }));
}

#[test]
fn type_mismatch_aborts_without_side_effects() {
    let conn = hot_db();
    // SQLite's flexible typing lets TEXT into an INTEGER column; compaction
    // must refuse rather than guess.
    conn.execute(
        "INSERT INTO readings (ts, value) VALUES (1000, 'not a number')",
        [],
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let err = compact_bucket(&conn, &spec(1000, 2000), dir.path()).unwrap_err();
    assert!(matches!(err, CompactError::TypeMismatch { .. }), "{err}");
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        0,
        "no file, no tmp"
    );
    assert_eq!(hot_count(&conn), 1, "nothing deleted");
}

#[test]
fn unsupported_declared_type_is_rejected() {
    let conn = hot_db();
    conn.execute_batch("CREATE TABLE weird (ts INTEGER, x NUMERIC)")
        .unwrap();
    conn.execute("INSERT INTO weird VALUES (1000, 1)", []).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let err = compact_bucket(
        &conn,
        &BucketSpec {
            hot_table: "weird",
            logical_table: "weird",
            ts_column: "ts",
            bucket_start: 0,
            bucket_end: 2000,
        },
        dir.path(),
    )
    .unwrap_err();
    assert!(matches!(err, CompactError::UnsupportedDecl { .. }), "{err}");
}

#[test]
fn bad_timestamp_column_is_rejected() {
    let conn = hot_db();
    insert_row(&conn, 1000, Some(1.0), Some("x"));
    let dir = tempfile::tempdir().unwrap();
    let err = compact_bucket(
        &conn,
        &BucketSpec {
            ts_column: "name", // TEXT column
            ..spec(1000, 2000)
        },
        dir.path(),
    )
    .unwrap_err();
    assert!(matches!(err, CompactError::BadTimestampColumn { .. }), "{err}");
}

/// specv2's bounded-cost invariant: run compaction over ever-growing
/// history (100 buckets fed in over a loop) and assert each run's work
/// stays flat — measured in rows touched via SQLite's change counter and
/// rows written per outcome, not wall clock.
#[test]
fn compaction_cost_stays_flat_as_history_accumulates() {
    let conn = hot_db();
    let dir = tempfile::tempdir().unwrap();
    const ROWS_PER_BUCKET: usize = 50;

    let mut rows_per_run = Vec::new();
    let mut changes_per_run = Vec::new();
    for b in 0..100i64 {
        let start = b * 1000;
        // New bucket's writes arrive...
        for i in 0..ROWS_PER_BUCKET as i64 {
            insert_row(&conn, start + i, Some(i as f64), Some("s"));
        }
        // ...then the now-closed bucket is compacted.
        let changes_before: i64 = conn
            .query_row("SELECT total_changes()", [], |r| r.get(0))
            .unwrap();
        let (rows, _) = compacted(compact_bucket(&conn, &spec(start, start + 1000), dir.path()).unwrap());
        let changes_after: i64 = conn
            .query_row("SELECT total_changes()", [], |r| r.get(0))
            .unwrap();

        rows_per_run.push(rows);
        changes_per_run.push(changes_after - changes_before);

        // Hot table never accumulates history.
        assert_eq!(hot_count(&conn), 0, "hot table drained after bucket {b}");
    }

    // Work per run must be exactly flat: same rows written, same DB rows
    // touched, on run 100 as on run 1 — no dependence on accumulated
    // history.
    assert!(
        rows_per_run.iter().all(|&r| r == ROWS_PER_BUCKET),
        "rows written crept: {rows_per_run:?}"
    );
    let first = changes_per_run[0];
    assert!(
        changes_per_run.iter().all(|&c| c == first),
        "DB rows touched per run crept: {changes_per_run:?}"
    );
}
