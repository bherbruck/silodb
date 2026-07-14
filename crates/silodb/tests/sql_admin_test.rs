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
                "SELECT silodb_create_table('readings', NULL, '1d,7d', '{}')",
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
            &format!("SELECT silodb_maintain('readings', {})", 2 * DAY + MARGIN + 1),
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
                "SELECT silodb_create_table('readings', NULL, '1d,7d', '{}')",
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
            "SELECT silodb_create_table('nope', NULL, '1d', 'cold/')",
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
            "SELECT silodb_create_table('t', NULL, '7d,30d', 'cold/')",
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

#[test]
fn default_dir_chain() {
    // 1. Explicit beats everything; 2. db-level default; 3. <dbfile>.silodb/
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hot.db");
    let conn = Connection::open(&db).unwrap();
    silodb::load_module(&conn).unwrap();

    // Derived default: <dbfile>.silodb/
    conn.execute_batch("CREATE TABLE a (ts TIMESTAMP, v REAL)").unwrap();
    conn.query_row("SELECT silodb_create_table('a')", [], |r| r.get::<_, String>(0))
        .unwrap();
    let p = silodb::catalog::get_policy(&conn, "a").unwrap().unwrap();
    assert!(
        p.base_dir.ends_with("hot.db.silodb"),
        "derived from the db file: {}",
        p.base_dir
    );

    // db default wins over derivation once set...
    let custom = dir.path().join("elsewhere");
    conn.query_row(
        &format!("SELECT silodb_set_default_dir('{}')", custom.display()),
        [],
        |r| r.get::<_, String>(0),
    )
    .unwrap();
    conn.execute_batch("CREATE TABLE b (ts TIMESTAMP, v REAL)").unwrap();
    conn.query_row("SELECT silodb_create_table('b')", [], |r| r.get::<_, String>(0))
        .unwrap();
    let p = silodb::catalog::get_policy(&conn, "b").unwrap().unwrap();
    assert_eq!(p.base_dir, custom.display().to_string());

    // ...but table 'a' keeps its frozen dir (defaults never move tables).
    let p = silodb::catalog::get_policy(&conn, "a").unwrap().unwrap();
    assert!(p.base_dir.ends_with("hot.db.silodb"));

    // Explicit ts column persists in the policy (create_hypertable slot 2).
    conn.execute_batch("CREATE TABLE c (observed_at TIMESTAMP, noted_at TIMESTAMP, v REAL)")
        .unwrap();
    conn.query_row(
        "SELECT silodb_create_table('c', 'observed_at')",
        [],
        |r| r.get::<_, String>(0),
    )
    .unwrap();
    let p = silodb::catalog::get_policy(&conn, "c").unwrap().unwrap();
    assert_eq!(p.ts_column.as_deref(), Some("observed_at"));
    // And end-to-end: the ambiguous two-TIMESTAMP table works.
    conn.execute("INSERT INTO c VALUES (1000, 2000, 1.0)", []).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM c", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);

    // In-memory db with no explicit/default dir errors clearly.
    let mem = Connection::open_in_memory().unwrap();
    silodb::load_module(&mem).unwrap();
    mem.execute_batch("CREATE TABLE t (ts TIMESTAMP)").unwrap();
    let err = mem
        .query_row("SELECT silodb_create_table('t')", [], |r| r.get::<_, String>(0))
        .unwrap_err()
        .to_string();
    assert!(err.contains("no base directory"), "{err}");
}

#[test]
fn retention_is_a_separate_changeable_policy() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hot.db");
    let conn = Connection::open(&db).unwrap();
    silodb::load_module(&conn).unwrap();
    conn.execute_batch("CREATE TABLE r (ts TIMESTAMP, v REAL)").unwrap();
    conn.query_row("SELECT silodb_create_table('r', NULL, '1d,7d')", [], |r| {
        r.get::<_, String>(0)
    })
    .unwrap();
    // No retention at create.
    assert_eq!(
        silodb::catalog::get_policy(&conn, "r").unwrap().unwrap().retain_us,
        None
    );

    // Two weeks of data, compacted.
    for h in 0..14 * 24 {
        conn.execute("INSERT INTO r VALUES (?1, 1.0)", params![h * HOUR])
            .unwrap();
    }
    let now = 15 * DAY;
    silodb::maintain(&conn, "r", now).unwrap();
    let before: i64 = conn
        .query_row("SELECT count(*) FROM r", [], |r| r.get(0))
        .unwrap();
    assert_eq!(before, 14 * 24);

    // Retroactively add retention (Timescale add_retention_policy style):
    // the very next maintain evicts the expired week.
    conn.query_row("SELECT silodb_set_retention('r', '7d')", [], |r| {
        r.get::<_, String>(0)
    })
    .unwrap();
    silodb::maintain(&conn, "r", now).unwrap();
    let after: i64 = conn
        .query_row("SELECT count(*) FROM r", [], |r| r.get(0))
        .unwrap();
    assert!(after < before, "expired data evicted: {after} < {before}");

    // Clear it (NULL = keep forever); nothing further evicts.
    conn.query_row("SELECT silodb_set_retention('r', NULL)", [], |r| {
        r.get::<_, String>(0)
    })
    .unwrap();
    silodb::maintain(&conn, "r", now + 300 * DAY).unwrap();
    let kept: i64 = conn
        .query_row("SELECT count(*) FROM r", [], |r| r.get(0))
        .unwrap();
    assert_eq!(kept, after, "cleared retention keeps everything");

    // Shorter than the largest tier is refused.
    let err = conn
        .query_row("SELECT silodb_set_retention('r', '3d')", [], |r| {
            r.get::<_, String>(0)
        })
        .unwrap_err()
        .to_string();
    assert!(err.contains("largest tier"), "{err}");
}
