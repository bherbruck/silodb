//! ADD COLUMN schema evolution — the one supported hypertable edit.
//! Old cold files stay untouched; history reads back NULL in the new
//! column; merges NULL-pad old files as tiers converge; rollups, stats
//! and both table surfaces (init-style and managed) follow the schema.

use rusqlite::{params, Connection, OptionalExtension};

const HOUR: i64 = 3600 * 1_000_000;
const DAY: i64 = 24 * HOUR;
const MARGIN: i64 = 2 * HOUR;

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

/// init-style table, two days compacted, then ADD COLUMN, then more data.
fn widened_env() -> (Connection, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let conn = Connection::open(dir.path().join("hot.db")).unwrap();
    silodb::load_module(&conn).unwrap();
    silodb::init_table_tiered_at(
        &conn,
        "readings",
        "ts TIMESTAMP, device TEXT, value REAL",
        "1d,7d",
        dir.path().join("cold"),
    )
    .unwrap();
    for h in 0..48 {
        conn.execute(
            "INSERT INTO readings VALUES (?1, 'a', ?2)",
            params![h * HOUR, h as f64],
        )
        .unwrap();
    }
    silodb::maintain(&conn, "readings", 2 * DAY + MARGIN + 1).unwrap();
    assert_eq!(count(&conn, "SELECT count(*) FROM readings_hot"), 0);

    silodb::alter_table_add_column(&conn, "readings", "humidity REAL").unwrap();

    for h in 48..72 {
        conn.execute(
            "INSERT INTO readings VALUES (?1, 'a', ?2, ?3)",
            params![h * HOUR, h as f64, (h * 2) as f64],
        )
        .unwrap();
    }
    (conn, dir)
}

#[test]
fn history_reads_null_and_new_rows_read_values() {
    let (conn, _dir) = widened_env();
    // All 72 rows visible through one name, 4 columns wide.
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 72);
    assert_eq!(
        count(&conn, "SELECT count(*) FROM readings WHERE humidity IS NULL"),
        48,
        "pre-ALTER history is NULL in the new column"
    );
    let (n, sum): (i64, f64) = conn
        .query_row(
            "SELECT count(humidity), sum(humidity) FROM readings",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(n, 24);
    assert_eq!(sum, (48..72).map(|h| (h * 2) as f64).sum::<f64>());
}

#[test]
fn cross_boundary_compaction_and_merge_harmonize() {
    let (conn, _dir) = widened_env();
    // Compact the post-ALTER day, then age everything into the 7d tier —
    // the merge must NULL-pad the two narrow files up to the wide schema.
    silodb::maintain(&conn, "readings", 10 * DAY).unwrap();
    assert_eq!(count(&conn, "SELECT count(*) FROM readings_hot"), 0);
    let active: i64 = count(
        &conn,
        "SELECT count(*) FROM _silodb_catalog WHERE status='active'",
    );
    assert_eq!(active, 1, "three daily files merged into one weekly file");

    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 72);
    assert_eq!(
        count(&conn, "SELECT count(*) FROM readings WHERE humidity IS NOT NULL"),
        24,
        "new-column values survive the merge; history stays NULL"
    );
    // The merged file carries the widest schema: a constraint on the new
    // column scans it (no phantom pruning) and answers correctly.
    let hot: f64 = conn
        .query_row(
            "SELECT max(humidity) FROM readings WHERE humidity > 100",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(hot, 142.0);
}

#[test]
fn constraint_on_new_column_prunes_pre_alter_files() {
    let (conn, _dir) = widened_env();
    // Compact post-ALTER data so cold has both narrow and wide files.
    silodb::maintain(&conn, "readings", 3 * DAY + MARGIN + 1).unwrap();
    let n = count(&conn, "SELECT count(*) FROM readings WHERE humidity >= 96");
    assert_eq!(n, 24);
    let stats = silodb::last_scan_stats().unwrap();
    assert!(
        stats.scanned_files < stats.total_files,
        "files that predate the column can't match a pushed constraint on \
         it and must be skipped: {stats:?}"
    );
}

#[test]
fn rollup_follows_the_new_column() {
    let (conn, _dir) = widened_env();
    silodb::create_rollup(&conn, "readings", "1h").unwrap();
    silodb::create_rollup_view(&conn, "readings", "1h").unwrap();
    // Widen again with a rollup registered: value+humidity stats coexist.
    silodb::alter_table_add_column(&conn, "readings", "pressure REAL").unwrap();
    conn.execute(
        "INSERT INTO readings VALUES (?1, 'a', 1.0, 2.0, 3.0)",
        params![72 * HOUR],
    )
    .unwrap();
    silodb::maintain(&conn, "readings", 4 * DAY + MARGIN + 1).unwrap();

    // View equivalence on the new columns: rollup view vs direct scan.
    let (vn, vs): (i64, Option<f64>) = conn
        .query_row(
            "SELECT sum(humidity_count), sum(humidity_sum) FROM readings_1h",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let (dn, ds): (i64, Option<f64>) = conn
        .query_row(
            "SELECT count(humidity), sum(humidity) FROM readings",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((vn, vs), (dn, ds), "rollup view matches a direct scan");
    let pn: i64 = count(&conn, "SELECT sum(pressure_count) FROM readings_1h");
    assert_eq!(pn, 1);
}

#[test]
fn managed_table_alter() {
    let dir = tempfile::tempdir().unwrap();
    let conn = Connection::open(dir.path().join("hot.db")).unwrap();
    silodb::load_module(&conn).unwrap();
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE readings USING silodb('{}',
            schema='ts TIMESTAMP, device TEXT, value REAL', tiers='1d,7d')",
        dir.path().join("cold").display()
    ))
    .unwrap();
    for h in 0..24 {
        conn.execute(
            "INSERT INTO readings VALUES (?1, 'a', ?2)",
            params![h * HOUR, h as f64],
        )
        .unwrap();
    }
    silodb::maintain(&conn, "readings", DAY + MARGIN + 1).unwrap();
    silodb::set_retention(&conn, "readings", Some("30d")).unwrap();

    conn.query_row(
        "SELECT silodb_add_column('readings', 'humidity REAL')",
        [],
        |r| r.get::<_, String>(0),
    )
    .unwrap();

    // Hot rows and cold history both survived the vtab re-create.
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 24);
    conn.execute(
        "INSERT INTO readings VALUES (?1, 'a', 1.0, 55.5)",
        params![25 * HOUR],
    )
    .unwrap();
    assert_eq!(
        count(&conn, "SELECT count(*) FROM readings WHERE humidity = 55.5"),
        1
    );
    assert_eq!(
        count(&conn, "SELECT count(*) FROM readings WHERE humidity IS NULL"),
        24
    );
    // Policy survived intact — retention was set via function, the
    // re-created vtab's policy string has no retain=, and it must not
    // have been clobbered.
    let p = silodb::catalog::get_policy(&conn, "readings").unwrap().unwrap();
    assert_eq!(p.retain_us, silodb_schema::parse_duration_micros("30d"));
    assert_eq!(p.tiers_us.len(), 2);
}

#[test]
fn axis_frozen_before_second_timestamp_column() {
    let (conn, _dir) = widened_env();
    // Policy had no explicit ts; the ALTER froze it, so adding a second
    // TIMESTAMP column is unambiguous.
    let p = silodb::catalog::get_policy(&conn, "readings").unwrap().unwrap();
    assert_eq!(p.ts_column.as_deref(), Some("ts"), "axis frozen by first alter");
    silodb::alter_table_add_column(&conn, "readings", "observed_at TIMESTAMP").unwrap();
    conn.execute(
        "INSERT INTO readings VALUES (?1, 'a', 1.0, 2.0, ?2)",
        params![73 * HOUR, 999],
    )
    .unwrap();
    silodb::maintain(&conn, "readings", 5 * DAY).unwrap();
    assert_eq!(count(&conn, "SELECT count(*) FROM readings"), 73);
    let ob: i64 = conn
        .query_row(
            "SELECT observed_at FROM readings WHERE observed_at IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(ob, 999, "secondary timestamp round-trips through parquet");
}

#[test]
fn refusals_are_loud_and_leave_nothing_half_done() {
    let (conn, _dir) = widened_env();
    for (coldef, msg) in [
        ("humidity REAL", "already exists"),
        ("a REAL, b REAL", "one column at a time"),
        ("naked", "needs a declared type"),
    ] {
        let err = silodb::alter_table_add_column(&conn, "readings", coldef)
            .unwrap_err()
            .to_string();
        assert!(err.contains(msg), "{coldef}: {err}");
    }
    // Not a silodb table.
    conn.execute_batch("CREATE TABLE plain (x INTEGER)").unwrap();
    let err = silodb::alter_table_add_column(&conn, "plain", "y REAL")
        .unwrap_err()
        .to_string();
    assert!(err.contains("not a silodb table"), "{err}");
    // Hot table shape untouched by all of the above.
    let cols: i64 = conn
        .query_row("SELECT count(*) FROM pragma_table_info('readings_hot')", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(cols, 4, "refused alters changed nothing");
}

#[test]
fn boot_reinit_with_pre_alter_schema_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hot.db");
    let boot = |schema: &str| {
        let conn = Connection::open(&db).unwrap();
        silodb::load_module(&conn).unwrap();
        silodb::init_table_tiered(&conn, "r", schema, "1d,7d").unwrap();
        conn
    };
    let conn = boot("ts TIMESTAMP, v REAL");
    conn.execute("INSERT INTO r VALUES (0, 1.0)", []).unwrap();
    silodb::alter_table_add_column(&conn, "r", "w REAL").unwrap();
    drop(conn);

    // App reboots with its OLD schema string: init must accept the wider
    // table (prefix rule) and leave the wide surface in place.
    let conn = boot("ts TIMESTAMP, v REAL");
    let cols: i64 = conn
        .query_row("SELECT count(*) FROM pragma_table_info('r_hot')", [], |r| r.get(0))
        .unwrap();
    assert_eq!(cols, 3);
    conn.execute("INSERT INTO r VALUES (1, 2.0, 3.0)", []).unwrap();
    assert_eq!(count(&conn, "SELECT count(*) FROM r WHERE w = 3.0"), 1);
    // But genuinely different columns still drift loudly.
    let conn2 = Connection::open(&db).unwrap();
    silodb::load_module(&conn2).unwrap();
    let err = silodb::init_table_tiered(&conn2, "r", "ts TIMESTAMP, other TEXT", "1d,7d")
        .unwrap_err()
        .to_string();
    assert!(err.contains("drift") || err.contains("exists with columns"), "{err}");
}

#[test]
fn stats_table_follows_and_prunes_by_series() {
    let (conn, _dir) = widened_env();
    silodb::maintain(&conn, "readings", 3 * DAY + MARGIN + 1).unwrap();
    // The stats table gained humidity slots; wide files have real stats
    // rows for it, narrow files' rows read NULL.
    let has: Option<i64> = conn
        .query_row(
            "SELECT count(*) FROM pragma_table_info('readings_stats') \
             WHERE name = 'humidity_sum'",
            [],
            |r| r.get(0),
        )
        .optional()
        .unwrap();
    assert_eq!(has, Some(1), "stats table widened");
    let n: i64 = count(
        &conn,
        "SELECT count(*) FROM readings_stats WHERE humidity_count IS NOT NULL",
    );
    assert!(n >= 1, "post-alter file has humidity stats");
}
