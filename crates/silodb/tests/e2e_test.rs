//! End-to-end through the facade's single-name surface: the app only ever
//! touches `readings` — inserts land hot, reads span hot + cold, compaction
//! moves rows underneath without anything observable changing.

use rusqlite::{params, Connection};
use silodb::{compact_table, CompactOutcome, InitError};

const SCHEMA: &str = "ts INTEGER, value REAL, name TEXT";

fn boot(db: &std::path::Path, base: &std::path::Path) -> Connection {
    let conn = Connection::open(db).unwrap();
    silodb::load_module(&conn).unwrap();
    silodb::init_table(&conn, "readings", SCHEMA, base).unwrap();
    conn
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

#[test]
fn single_name_surface_hides_the_hot_cold_split() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cold");
    let db = dir.path().join("hot.db");
    let conn = boot(&db, &base);

    // Day zero: one name, empty, no cold/ dir on disk yet.
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 0);
    assert!(!base.exists(), "everything on disk is lazy");

    // The app writes and reads ONE name.
    for i in 0..40i64 {
        conn.execute(
            "INSERT INTO readings VALUES (?1, ?2, ?3)",
            params![i * 100, i as f64, format!("s{i}")],
        )
        .unwrap();
    }
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 40);

    // Compact three closed buckets; the app-visible table never changes.
    for b in 0..3i64 {
        let outcome = compact_table(&conn, "readings", b * 1000, (b + 1) * 1000, &base).unwrap();
        assert!(matches!(outcome, CompactOutcome::Compacted { rows: 10, .. }));
        assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 40);
    }
    assert_eq!(count(&conn, "SELECT count(*) FROM readings_hot"), 10);
    assert!(base.join("readings").is_dir(), "created by first compaction");

    // Range query spanning the cold/hot boundary, through the one name.
    let ts: Vec<i64> = conn
        .prepare("SELECT ts FROM readings WHERE ts >= 2500 AND ts < 3500 ORDER BY ts")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(ts, (25..35).map(|i| i * 100).collect::<Vec<_>>());

    // File-level pruning happened under the hood.
    let stats = silodb::last_scan_stats().unwrap();
    assert_eq!(stats.total_files, 3);
    assert_eq!(stats.candidate_files, 1);

    // Late row into an already-compacted bucket: insert through the one
    // name, recompact, still seamless.
    conn.execute("INSERT INTO readings VALUES (1500, 99.0, 'late')", [])
        .unwrap();
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 41);
    assert!(matches!(
        compact_table(&conn, "readings", 1000, 2000, &base).unwrap(),
        CompactOutcome::Compacted { rows: 1, .. }
    ));
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 41);
}

#[test]
fn second_boot_is_a_noop_and_data_survives() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cold");
    let db = dir.path().join("hot.db");
    {
        let conn = boot(&db, &base);
        conn.execute("INSERT INTO readings VALUES (100, 1.0, 'a')", [])
            .unwrap();
        compact_table(&conn, "readings", 0, 1000, &base).unwrap();
        conn.execute("INSERT INTO readings VALUES (2000, 2.0, 'b')", [])
            .unwrap();
    }
    // Fresh process: same two boot lines, everything already exists.
    let conn = boot(&db, &base);
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 2);
    assert_eq!(count(&conn, "SELECT count(*) FROM readings_hot"), 1);
}

#[test]
fn cold_only_database_keeps_working_without_the_hot_table() {
    // A retired table: history compacted, hot table dropped. The vtab's
    // schema= is baked into its DDL, so reads keep working; the view needs
    // its hot arm replaced (or the empty-hot-table placeholder kept — this
    // test takes the drop path on purpose).
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cold");
    let db = dir.path().join("hot.db");
    {
        let conn = boot(&db, &base);
        conn.execute("INSERT INTO readings VALUES (100, 1.0, 'a')", [])
            .unwrap();
        compact_table(&conn, "readings", 0, 1000, &base).unwrap();
        conn.execute_batch(
            "DROP TRIGGER readings_insert;
             DROP VIEW readings;
             DROP TABLE readings_hot;",
        )
        .unwrap();
    }
    let conn = Connection::open(&db).unwrap();
    silodb::load_module(&conn).unwrap();
    // No init, no hot table — the cold vtab alone still serves history.
    assert_eq!(count(&conn, "SELECT count(*) FROM readings_cold"), 1);
}

#[test]
fn init_table_detects_schema_drift() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cold");
    let db = dir.path().join("hot.db");
    boot(&db, &base);

    let conn = Connection::open(&db).unwrap();
    silodb::load_module(&conn).unwrap();
    let err = silodb::init_table(&conn, "readings", "ts INTEGER, other REAL", &base).unwrap_err();
    assert!(matches!(err, InitError::SchemaDrift { .. }), "{err}");
}

#[test]
fn init_table_requires_a_resolvable_ts_column() {
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    // No TIMESTAMP column and nothing named ts.
    let err = silodb::init_table(&conn, "readings", "value REAL", "cold/").unwrap_err();
    assert!(matches!(err, InitError::BadSchema(_)), "{err}");
    // Two TIMESTAMP columns: refuse to guess the bucket axis.
    let err = silodb::init_table(
        &conn,
        "readings",
        "a TIMESTAMP, b DATETIME, value REAL",
        "cold/",
    )
    .unwrap_err();
    assert!(matches!(err, InitError::BadSchema(_)), "{err}");
}

/// The TIMESTAMP declared type is the whole story: any column name, real
/// dates in the exported parquet, helpers for humans — no ts_column
/// argument anywhere.
#[test]
fn timestamp_typed_column_drives_everything_by_type() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cold");
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    silodb::init_table(
        &conn,
        "sensor",
        "stamped_at TIMESTAMP, seq INTEGER, value REAL",
        &base,
    )
    .unwrap();

    // Insert via the helpers: silodb_ts parses ISO text.
    conn.execute(
        "INSERT INTO sensor VALUES (silodb_ts('2026-07-13T10:00:00Z'), 1, 21.5)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sensor VALUES (silodb_ts('2026-07-13T11:30:00Z'), 2, 22.0)",
        [],
    )
    .unwrap();

    // Compact the 10:00 hour — bucket axis discovered by type, any name.
    let hour = 3_600_000_000i64;
    let start = silodb_schema_ts("2026-07-13T10:00:00Z");
    let outcome = compact_table(&conn, "sensor", start, start + hour, &base).unwrap();
    let CompactOutcome::Compacted { rows: 1, path } = outcome else {
        panic!("{outcome:?}");
    };

    // The parquet file carries a real UTC timestamp type on stamped_at.
    let file = std::fs::File::open(&path).unwrap();
    let meta =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let field = meta.schema().field_with_name("stamped_at").unwrap();
    assert_eq!(
        *field.data_type(),
        arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into())
        )
    );

    // Query across hot+cold with helpers both directions.
    let (n, rendered): (i64, String) = conn
        .query_row(
            "SELECT count(*), silodb_datetime(min(stamped_at)) FROM sensor
             WHERE stamped_at >= silodb_ts('2026-07-13')",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(rendered, "2026-07-13T10:00:00Z");
}

fn silodb_schema_ts(s: &str) -> i64 {
    silodb_schema::parse_timestamp_micros(s).unwrap()
}

#[test]
fn silodb_ts_helper_rejects_garbage_and_passes_integers() {
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    let v: i64 = conn
        .query_row("SELECT silodb_ts(12345)", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, 12345, "integers pass through");
    let err = conn
        .query_row("SELECT silodb_ts('not a date')", [], |r| r.get::<_, i64>(0))
        .unwrap_err();
    assert!(err.to_string().contains("unparseable"), "{err}");
}
