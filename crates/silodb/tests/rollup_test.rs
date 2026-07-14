//! Continuous aggregates: declare-anytime backfill, compaction-txn deltas,
//! late-data additivity, origin alignment, recursion — all anchored by the
//! equivalence property: the rollup view must agree exactly with a raw
//! GROUP BY silodb_bucket over the same data.

use rusqlite::{params, Connection};

const HOUR: i64 = 3600 * 1_000_000;
const DAY: i64 = 24 * HOUR;
const MARGIN: i64 = 2 * HOUR;

struct Env {
    conn: Connection,
    base: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

fn env(tiers: &str) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cold");
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    silodb::init_table_tiered(
        &conn,
        "readings",
        "ts TIMESTAMP, device TEXT, value REAL",
        &base,
        tiers,
    )
    .unwrap();
    Env {
        conn,
        base,
        _dir: dir,
    }
}

impl Env {
    /// Deterministic rows: every 20 minutes per device, some NULL values.
    fn fill_days(&self, from: i64, days: i64) {
        for d in from..from + days {
            for slot in 0..72 {
                let ts = d * DAY + slot * 20 * 60 * 1_000_000;
                for dev in ["a", "b"] {
                    let v: Option<f64> = if slot % 7 == 3 {
                        None
                    } else {
                        Some(((d * 72 + slot) as f64 * 0.25) % 50.0)
                    };
                    self.conn
                        .execute(
                            "INSERT INTO readings VALUES (?1, ?2, ?3)",
                            params![ts, dev, v],
                        )
                        .unwrap();
                }
            }
        }
    }

    fn maintain(&self, now: i64) {
        silodb::maintain(&self.conn, "readings", &self.base, now).unwrap();
    }

    /// The equivalence property: rollup view ≡ raw GROUP BY silodb_bucket.
    fn assert_equivalent(&self, origin: i64) {
        type Row = (i64, String, i64, f64, Option<f64>, Option<f64>, Option<f64>);
        let read = |sql: &str| -> Vec<Row> {
            self.conn
                .prepare(sql)
                .unwrap()
                .query_map([], |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get::<_, Option<f64>>(3)?.unwrap_or(0.0),
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                    ))
                })
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        let via_rollup = read(
            "SELECT ts, device, value_count, value_sum, value_min, value_max, value_avg
             FROM readings_1h WHERE value_count > 0 ORDER BY ts, device",
        );
        let via_raw = read(&format!(
            "SELECT silodb_bucket('1h', ts, {origin}) AS b, device, count(value),
                    sum(value), min(value), max(value),
                    CAST(sum(value) AS REAL) / nullif(count(value), 0)
             FROM readings GROUP BY 1, 2 HAVING count(value) > 0 ORDER BY 1, 2",
        ));
        assert_eq!(via_rollup.len(), via_raw.len(), "row counts differ");
        for (a, b) in via_rollup.iter().zip(&via_raw) {
            assert_eq!((a.0, &a.1, a.2), (b.0, &b.1, b.2), "bucket/series/count");
            assert!((a.3 - b.3).abs() <= 1e-9 * a.3.abs().max(1.0), "sum");
            assert_eq!(a.4, b.4, "min");
            assert_eq!(a.5, b.5, "max");
        }
    }
}

#[test]
fn backfill_then_forward_deltas_match_raw() {
    let e = env("1d,7d");
    e.fill_days(0, 3);
    e.maintain(3 * DAY + MARGIN + 1); // compact days 0..2 — no rollup yet

    // Declare AFTER data exists: backfill from cold files.
    silodb::create_rollup(&e.conn, "readings", "1h").unwrap();
    silodb::create_rollup_view(&e.conn, "readings", "1h").unwrap();
    e.assert_equivalent(0);

    // Forward path: new data flows through compaction deltas.
    e.fill_days(3, 2);
    e.maintain(5 * DAY + MARGIN + 1);
    e.assert_equivalent(0);

    // Hot tail: uncompacted rows served by the live arm of the view.
    e.fill_days(5, 1);
    e.assert_equivalent(0);
}

#[test]
fn late_data_deltas_stay_exact() {
    let e = env("1d,7d");
    e.fill_days(0, 2);
    let now = 2 * DAY + MARGIN + 1;
    e.maintain(now);
    silodb::create_rollup(&e.conn, "readings", "1h").unwrap();
    silodb::create_rollup_view(&e.conn, "readings", "1h").unwrap();

    // A late row lands in an already-compacted, already-backfilled bucket.
    e.conn
        .execute(
            "INSERT INTO readings VALUES (?1, 'a', 42.0)",
            params![DAY / 2 + 123],
        )
        .unwrap();
    e.assert_equivalent(0); // visible via the live arm immediately
    e.maintain(now); // late row compacts into a follow-up file + delta row
    e.assert_equivalent(0); // additive delta rows re-aggregate exactly
}

#[test]
fn origin_aligns_rollup_buckets() {
    // Monday 2024-01-01 as origin; 7d tier0 would be too coarse for 1h
    // grain? No — grain must divide tier0: 1d works. Use origin on 1d.
    let origin = silodb_schema::parse_timestamp_micros("2024-01-01").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cold");
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    silodb::init_table_tiered(
        &conn,
        "readings",
        "ts TIMESTAMP, device TEXT, value REAL",
        &base,
        &format!("1d,7d,origin={origin}"),
    )
    .unwrap();
    let e = Env {
        conn,
        base,
        _dir: dir,
    };
    // Data around the origin (before and after).
    for i in -30i64..30 {
        e.conn
            .execute(
                "INSERT INTO readings VALUES (?1, 'a', ?2)",
                params![origin + i * HOUR + 17, i as f64],
            )
            .unwrap();
    }
    e.maintain(origin + 40 * HOUR + MARGIN);
    silodb::create_rollup(&e.conn, "readings", "1h").unwrap();
    silodb::create_rollup_view(&e.conn, "readings", "1h").unwrap();
    e.assert_equivalent(origin);

    // Every materialized bucket sits on the origin-anchored hour grid.
    let misaligned: i64 = e
        .conn
        .query_row(
            &format!(
                "SELECT count(*) FROM readings_rollup_1h WHERE (ts - {origin}) % {HOUR} != 0"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(misaligned, 0);

    // Origin is immutable: re-init with a different one is refused.
    let err = silodb::init_table_tiered(
        &e.conn,
        "readings",
        "ts TIMESTAMP, device TEXT, value REAL",
        &e.base,
        "1d,7d,origin=0",
    )
    .unwrap_err();
    assert!(err.to_string().contains("origin"), "{err}");
}

#[test]
fn rollup_validation_and_drop() {
    let e = env("1d,7d");
    // Grain must divide tier 0.
    for bad in ["7h", "2d", "5h"] {
        assert!(
            silodb::create_rollup(&e.conn, "readings", bad).is_err(),
            "{bad} accepted"
        );
    }
    e.fill_days(0, 1);
    e.maintain(DAY + MARGIN + 1);
    silodb::create_rollup(&e.conn, "readings", "1h").unwrap();
    let n: i64 = e
        .conn
        .query_row("SELECT count(*) FROM readings_rollup_1h", [], |r| r.get(0))
        .unwrap();
    assert!(n > 0);

    silodb::drop_rollup(&e.conn, "readings", "1h").unwrap();
    assert!(e
        .conn
        .prepare("SELECT count(*) FROM readings_rollup_1h")
        .is_err());
    // Compaction after drop writes no deltas (registry empty) — and works.
    e.fill_days(1, 1);
    e.maintain(2 * DAY + MARGIN + 1);
}

/// The recursion: tier the rollup table itself, so hourly history lives in
/// its own parquet buckets under its own policy.
#[test]
fn tiered_rollup_recursion() {
    let e = env("1d,7d");
    // Rollup's single-name surface FIRST (schema = the rollup layout)...
    silodb::init_table_tiered(
        &e.conn,
        "readings_rollup_1h",
        "ts TIMESTAMP, device TEXT, value_count INTEGER, value_sum REAL, \
         value_sumsq REAL, value_min REAL, value_max REAL",
        &e.base,
        "7d,28d",
    )
    .unwrap();
    // ...then registration detects and uses it.
    e.fill_days(0, 9);
    let now = 9 * DAY + MARGIN + 1;
    e.maintain(now);
    silodb::create_rollup(&e.conn, "readings", "1h").unwrap();
    silodb::create_rollup_view(&e.conn, "readings", "1h").unwrap();
    e.assert_equivalent(0);

    // Maintain the rollup table itself: its hourly rows compact into its
    // own cold files.
    silodb::maintain(&e.conn, "readings_rollup_1h", &e.base, now).unwrap();
    let rollup_files = silodb::catalog::entries_for_table(&e.conn, "readings_rollup_1h")
        .unwrap()
        .len();
    assert!(rollup_files > 0, "rollup history tiered into parquet");
    // Equivalence still holds — rollup reads now span its hot + cold.
    e.assert_equivalent(0);
}

#[test]
fn two_grains_coexist_and_both_stay_exact() {
    let e = env("1d,7d");
    e.fill_days(0, 2);
    e.maintain(2 * DAY + MARGIN + 1);
    silodb::create_rollup(&e.conn, "readings", "1h").unwrap();
    silodb::create_rollup(&e.conn, "readings", "4h").unwrap();
    silodb::create_rollup_view(&e.conn, "readings", "1h").unwrap();
    silodb::create_rollup_view(&e.conn, "readings", "4h").unwrap();

    // More data through the forward path feeds BOTH accumulators.
    e.fill_days(2, 1);
    e.maintain(3 * DAY + MARGIN + 1);
    e.assert_equivalent(0); // 1h view

    // 4h view checked against raw at its own grain.
    let (n_4h, sum_4h): (i64, f64) = e
        .conn
        .query_row(
            "SELECT sum(value_count), sum(value_sum) FROM readings_4h",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let (n_raw, sum_raw): (i64, f64) = e
        .conn
        .query_row("SELECT count(value), sum(value) FROM readings", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(n_4h, n_raw);
    assert!((sum_4h - sum_raw).abs() <= 1e-9 * sum_raw.abs().max(1.0));
}

#[test]
fn rollup_rows_follow_source_retention() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cold");
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    silodb::init_table_tiered(
        &conn,
        "readings",
        "ts TIMESTAMP, device TEXT, value REAL",
        &base,
        "1d,7d,retain=7d",
    )
    .unwrap();
    let e = Env {
        conn,
        base,
        _dir: dir,
    };
    e.fill_days(0, 3);
    e.maintain(3 * DAY + MARGIN + 1);
    silodb::create_rollup(&e.conn, "readings", "1h").unwrap();
    let before: i64 = e
        .conn
        .query_row("SELECT count(*) FROM readings_rollup_1h", [], |r| r.get(0))
        .unwrap();
    assert!(before > 0);

    // Advance the clock past retention for days 0..2: raw files evict AND
    // their rollup buckets are trimmed in the same maintain call.
    let now = 10 * DAY + MARGIN + 1; // cutoff = day 3+: everything expires
    e.maintain(now);
    let after: i64 = e
        .conn
        .query_row("SELECT count(*) FROM readings_rollup_1h", [], |r| r.get(0))
        .unwrap();
    assert_eq!(after, 0, "rollup rows past retain trimmed with the raw data");
    let raw: i64 = e
        .conn
        .query_row("SELECT count(*) FROM readings", [], |r| r.get(0))
        .unwrap();
    assert_eq!(raw, 0);
}

#[test]
fn silodb_bucket_function_contract() {
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    let q = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };

    assert_eq!(q("SELECT silodb_bucket('1h', 3_700_000_000)"), 3_600_000_000);
    assert_eq!(q("SELECT silodb_bucket(3600000000, 3_700_000_000)"), 3_600_000_000);
    // Text timestamps accepted like silodb_ts.
    assert_eq!(
        q("SELECT silodb_bucket('1d', '2026-07-14T10:42:00Z')"),
        silodb_schema::parse_timestamp_micros("2026-07-14").unwrap()
    );
    // Origin shifts the grid: Thursday-epoch weeks vs Monday weeks.
    let monday = silodb_schema::parse_timestamp_micros("2024-01-01").unwrap();
    let wed = silodb_schema::parse_timestamp_micros("2024-01-03T12:00:00Z").unwrap();
    assert_eq!(q(&format!("SELECT silodb_bucket('7d', {wed}, {monday})")), monday);
    // Pre-origin timestamps floor correctly (euclidean).
    let prev_monday = silodb_schema::parse_timestamp_micros("2023-12-25").unwrap();
    let sun = silodb_schema::parse_timestamp_micros("2023-12-31").unwrap();
    assert_eq!(q(&format!("SELECT silodb_bucket('7d', {sun}, {monday})")), prev_monday);
    // Garbage errors, doesn't panic.
    assert!(conn
        .query_row("SELECT silodb_bucket('nope', 0)", [], |r| r.get::<_, i64>(0))
        .is_err());
    assert!(conn
        .query_row("SELECT silodb_bucket('1h', 'not a date')", [], |r| r
            .get::<_, i64>(0))
        .is_err());
}
