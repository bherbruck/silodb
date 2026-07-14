//! The SQL-only front door (create_hypertable precedent): plain DDL, then
//! silodb_create_table() converts in place — existing rows survive — and
//! silodb_maintain() runs the policy. No Rust API calls after load_module.

use rusqlite::{params, Connection};

const HOUR: i64 = 3600 * 1_000_000;
const DAY: i64 = 24 * HOUR;
const MARGIN: i64 = 2 * HOUR;

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

#[test]
fn convert_a_populated_table_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cold");
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();

    // Plain table with pre-existing data — the create_hypertable scenario.
    conn.execute_batch(
        "CREATE TABLE readings (ts TIMESTAMP, device TEXT, value REAL)",
    )
    .unwrap();
    for h in 0..48 {
        conn.execute(
            "INSERT INTO readings VALUES (?1, 'a', ?2)",
            params![h * HOUR, h as f64],
        )
        .unwrap();
    }

    let converted: String = conn
        .query_row(
            &format!(
                "SELECT silodb_create_table('readings', '{}', '1d,7d')",
                base.display()
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(converted, "readings");

    // Same name, same data, now tiered infrastructure underneath.
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 48);
    assert_eq!(count(&conn, "SELECT count(*) FROM readings_hot"), 48);

    // Maintain via SQL: day 0+1 compact.
    let actions: i64 = conn
        .query_row(
            &format!(
                "SELECT silodb_maintain('readings', '{}', {})",
                base.display(),
                2 * DAY + MARGIN + 1
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(actions >= 2, "{actions}");
    assert_eq!(count(&conn, "SELECT count(*) FROM readings_hot"), 0);
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 48, "moved, not lost");

    // Inserts keep flowing through the converted name.
    conn.execute(
        "INSERT INTO readings VALUES (?1, 'b', 1.0)",
        params![49 * HOUR],
    )
    .unwrap();
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 49);

    // Idempotent re-convert (already a view) — no error, nothing broken.
    let again: String = conn
        .query_row(
            &format!(
                "SELECT silodb_create_table('readings', '{}', '1d,7d')",
                base.display()
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(again, "readings");
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 49);
}

#[test]
fn convert_errors_are_clean() {
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    // Nonexistent table.
    let err = conn
        .query_row(
            "SELECT silodb_create_table('nope', 'cold/', '1d')",
            [],
            |r| r.get::<_, String>(0),
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("no table"), "{err}");
    // Bad tiers surface the policy validation.
    conn.execute_batch("CREATE TABLE t (ts TIMESTAMP)").unwrap();
    let err = conn
        .query_row(
            "SELECT silodb_create_table('t', 'cold/', '7d,30d')",
            [],
            |r| r.get::<_, String>(0),
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("multiple"), "{err}");
    // Failed conversion must not strand the table — 't' is still 't'.
    let kind: String = conn
        .query_row(
            "SELECT type FROM sqlite_master WHERE name = 't'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(kind, "table", "table untouched after failed convert");
}
