//! Phase 1 acceptance (directory-mode): scanning `fixtures/basic.parquet`
//! (10 rows, 3 row groups) through a catalog-registered bucket file returns
//! exactly the rows the fixture generator wrote.

mod common;

use common::{cold_env, fixture_basic, ColdEnv};
use rusqlite::types::Value;
use rusqlite::Connection;

/// Env with the basic fixture copied in as bucket `[1000, 10001)`.
fn env_with_fixture() -> ColdEnv {
    let env = cold_env();
    let dest = env.table_dir.join("bucket-1000.parquet");
    std::fs::copy(fixture_basic(), &dest).unwrap();
    env.register(&dest, 1000, 10_001, 10);
    env.create_vtab();
    env
}

#[test]
fn full_scan_returns_all_rows_in_order() {
    let env = env_with_fixture();

    type Row = (i64, i64, Option<f64>, Option<String>, Option<Vec<u8>>, i64);
    let rows: Vec<Row> = env
        .conn
        .prepare("SELECT id, ts, value, name, payload, flag FROM cold")
        .unwrap()
        .query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 10);
    for (i, row) in rows.iter().enumerate() {
        let n = (i + 1) as i64;
        assert_eq!(row.0, n, "id");
        assert_eq!(row.1, n * 1000, "ts as raw microseconds");
        if n == 3 {
            assert_eq!(row.2, None, "value NULL at id 3");
        } else {
            assert_eq!(row.2, Some(n as f64 * 0.5), "value");
        }
        if n == 7 {
            assert_eq!(row.3, None, "name NULL at id 7");
        } else {
            assert_eq!(row.3.as_deref(), Some(format!("sensor-{n}").as_str()));
        }
        if n == 5 {
            assert_eq!(row.4, None, "payload NULL at id 5");
        } else {
            assert_eq!(row.4.as_deref(), Some(&[n as u8; 3][..]));
        }
        assert_eq!(row.5, i64::from(n % 2 == 0), "flag as INTEGER 0/1");
    }
}

#[test]
fn aggregates_and_filters_work_through_sqlite() {
    let env = env_with_fixture();

    let count: i64 = env
        .conn
        .query_row("SELECT count(*) FROM cold", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 10);

    let in_range: i64 = env
        .conn
        .query_row(
            "SELECT count(*) FROM cold WHERE ts > 4500 AND ts < 9500",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(in_range, 5); // ts 5000..=9000

    let null_values: i64 = env
        .conn
        .query_row("SELECT count(*) FROM cold WHERE value IS NULL", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(null_values, 1);
}

#[test]
fn declared_column_affinities_match_schema_mapping() {
    let env = env_with_fixture();

    let mut stmt = env
        .conn
        .prepare("SELECT id, ts, value, name, payload FROM cold LIMIT 1")
        .unwrap();
    let row: (Value, Value, Value, Value, Value) = stmt
        .query_row([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .unwrap();
    assert!(matches!(row.0, Value::Integer(_)));
    assert!(matches!(row.1, Value::Integer(_)), "timestamp is INTEGER");
    assert!(matches!(row.2, Value::Real(_)));
    assert!(matches!(row.3, Value::Text(_)));
    assert!(matches!(row.4, Value::Blob(_)));
}

#[test]
fn create_errors_when_no_schema_source_exists() {
    // No catalog, no cold files, and no hot table to borrow a schema from.
    let conn = Connection::open_in_memory().unwrap();
    silodb_vtab::load_module(&conn).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let err = conn
        .execute_batch(&format!(
            "CREATE VIRTUAL TABLE cold USING silodb('{}')",
            dir.path().display()
        ))
        .unwrap_err();
    assert!(err.to_string().contains("no schema source"), "{err}");
}

#[test]
fn day_zero_vtab_works_before_any_compaction() {
    // Hot table exists, catalog doesn't even exist yet: CREATE VIRTUAL
    // TABLE must work (schema borrowed from the hot table) and scans must
    // be empty, not errors.
    let conn = Connection::open_in_memory().unwrap();
    silodb_vtab::load_module(&conn).unwrap();
    conn.execute_batch("CREATE TABLE sensor (ts INTEGER NOT NULL, value REAL)")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE cold USING silodb('{}', table=sensor, hot_table=sensor)",
        dir.path().display()
    ))
    .unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM cold", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0);
    // Constrained scans too (exercises filter's no-catalog path).
    let n: i64 = conn
        .query_row("SELECT count(*) FROM cold WHERE ts > 5", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0);
}

#[test]
fn create_errors_on_missing_directory() {
    let env = cold_env();
    let err = env
        .conn
        .execute_batch("CREATE VIRTUAL TABLE cold USING silodb('/no/such/dir/')")
        .unwrap_err();
    assert!(err.to_string().contains("not a directory"), "{err}");
}

#[test]
fn second_connection_reconnects_to_persisted_vtab() {
    // xConnect path: table definition + catalog persisted in a db file.
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("sensor");
    std::fs::create_dir(&table_dir).unwrap();
    let dest = table_dir.join("bucket-1000.parquet");
    std::fs::copy(fixture_basic(), &dest).unwrap();
    let db_path = dir.path().join("hot.db");

    {
        let conn = Connection::open(&db_path).unwrap();
        silodb_catalog::ensure_catalog(&conn).unwrap();
        silodb_vtab::load_module(&conn).unwrap();
        silodb_catalog::insert_entry(
            &conn,
            &silodb_catalog::CatalogEntry {
                logical_table: "sensor".into(),
                path: dest.display().to_string(),
                range_start: 1000,
                range_end: 10_001,
                row_count: Some(10),
                created_at: 0,
                status: "active".into(),
            },
        )
        .unwrap();
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE cold USING silodb('{}', table=sensor)",
            dir.path().display()
        ))
        .unwrap();
    }
    {
        let conn = Connection::open(&db_path).unwrap();
        silodb_vtab::load_module(&conn).unwrap();
        let count: i64 = conn
            .query_row("SELECT count(*) FROM cold", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 10);
    }
}
