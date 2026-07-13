//! Phase 1 acceptance: full scan over `fixtures/basic.parquet` (10 rows,
//! 3 row groups) returns exactly the rows the fixture generator wrote.

use rusqlite::types::Value;
use rusqlite::Connection;

fn fixture_path(name: &str) -> String {
    format!("{}/../../fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

fn conn_with_vtab(fixture: &str) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    silodb_vtab::load_module(&conn).unwrap();
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE cold USING silodb('{}')",
        fixture_path(fixture)
    ))
    .unwrap();
    conn
}

#[test]
fn full_scan_returns_all_rows_in_order() {
    let conn = conn_with_vtab("basic.parquet");

    let rows: Vec<(i64, i64, Option<f64>, Option<String>, Option<Vec<u8>>, i64)> = conn
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
    let conn = conn_with_vtab("basic.parquet");

    let count: i64 = conn
        .query_row("SELECT count(*) FROM cold", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 10);

    // SQLite applies the WHERE itself in Phase 1 (no pushdown yet) — results
    // must still be right.
    let in_range: i64 = conn
        .query_row(
            "SELECT count(*) FROM cold WHERE ts > 4500 AND ts < 9500",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(in_range, 5); // ts 5000..=9000

    let null_values: i64 = conn
        .query_row("SELECT count(*) FROM cold WHERE value IS NULL", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(null_values, 1);
}

#[test]
fn declared_column_affinities_match_schema_mapping() {
    let conn = conn_with_vtab("basic.parquet");

    let mut stmt = conn
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
fn missing_file_errors_cleanly() {
    let conn = Connection::open_in_memory().unwrap();
    silodb_vtab::load_module(&conn).unwrap();
    let err = conn
        .execute_batch("CREATE VIRTUAL TABLE cold USING silodb('/no/such/file.parquet')")
        .unwrap_err();
    assert!(err.to_string().contains("silodb"), "{err}");
}

#[test]
fn second_connection_reconnects_to_persisted_vtab() {
    // xConnect path: table definition persisted in a db file, reopened.
    let dir = std::env::temp_dir().join(format!("silodb-vtab-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("reconnect.db");
    let _ = std::fs::remove_file(&db_path);

    {
        let conn = Connection::open(&db_path).unwrap();
        silodb_vtab::load_module(&conn).unwrap();
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE cold USING silodb('{}')",
            fixture_path("basic.parquet")
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
    let _ = std::fs::remove_file(&db_path);
}
