//! Per-(file, series) statistics: always-on, transactionally consistent
//! with file lifecycle, powering series-aware file pruning and free
//! whole-chunk aggregates.

use rusqlite::{params, Connection};

const HOUR: i64 = 3600 * 1_000_000;
const DAY: i64 = 24 * HOUR;
const MARGIN: i64 = 2 * HOUR;

struct Env {
    conn: Connection,
    _dir: tempfile::TempDir,
}

fn env(tiers: &str) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("cold");
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    silodb::init_table_tiered_at(&conn, "readings", "ts TIMESTAMP, device TEXT, value REAL", tiers, &base)
    .unwrap();
    let _ = base;
    Env { conn, _dir: dir }
}

impl Env {
    /// `rare` only emits rows on `rare_day`; `common` emits every day.
    fn fill_sparse(&self, days: i64, rare_day: i64) {
        for d in 0..days {
            for h in 0..24 {
                let ts = d * DAY + h * HOUR;
                self.conn
                    .execute(
                        "INSERT INTO readings VALUES (?1, 'common', ?2)",
                        params![ts, (d * 24 + h) as f64],
                    )
                    .unwrap();
                if d == rare_day {
                    self.conn
                        .execute(
                            "INSERT INTO readings VALUES (?1, 'rare', 1.0)",
                            params![ts],
                        )
                        .unwrap();
                }
            }
        }
    }

    fn maintain(&self, now: i64) {
        silodb::maintain(&self.conn, "readings", now).unwrap();
    }

    fn count(&self, sql: &str) -> i64 {
        self.conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }
}

#[test]
fn stats_rows_track_file_lifecycle_and_match_raw_totals() {
    let e = env("1d,7d");
    e.fill_sparse(9, 3);
    e.maintain(9 * DAY + MARGIN + 1); // compaction AND weekly merge happen

    // Stats totals equal raw totals — the "free whole-chunk aggregate".
    let (stats_n, stats_sum): (i64, f64) = e
        .conn
        .query_row(
            "SELECT sum(value_count), sum(value_sum) FROM readings_stats",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let (raw_n, raw_sum): (i64, f64) = e
        .conn
        .query_row("SELECT count(value), sum(value) FROM readings", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(stats_n, raw_n);
    assert!((stats_sum - raw_sum).abs() <= 1e-9 * raw_sum.abs().max(1.0));

    // Every stats row points at an ACTIVE file (children were replaced by
    // the merge atomically), and every active file has stats.
    let orphan = e.count(
        "SELECT count(*) FROM readings_stats
         WHERE path NOT IN (SELECT path FROM _silodb_catalog WHERE status='active')",
    );
    assert_eq!(orphan, 0, "no stats for dead files");
    let missing = e.count(
        "SELECT count(*) FROM _silodb_catalog WHERE status='active'
         AND logical_table='readings'
         AND path NOT IN (SELECT path FROM readings_stats)",
    );
    assert_eq!(missing, 0, "every active file has stats");

    // Per-file aggregate for the merged weekly chunk: one row read.
    let weekly_common: f64 = e
        .conn
        .query_row(
            "SELECT s.value_sum FROM readings_stats s
             JOIN _silodb_catalog c ON c.path = s.path
             WHERE s.device = 'common' AND c.range_start = 0 AND c.range_end = ?1",
            [7 * DAY],
            |r| r.get(0),
        )
        .unwrap();
    let raw_weekly: f64 = e
        .conn
        .query_row(
            "SELECT sum(value) FROM readings WHERE device='common' AND ts < ?1",
            [7 * DAY],
            |r| r.get(0),
        )
        .unwrap();
    assert!((weekly_common - raw_weekly).abs() <= 1e-9 * raw_weekly.abs().max(1.0));
}

#[test]
fn series_pruning_skips_files_without_the_series() {
    let e = env("1d");
    e.fill_sparse(6, 2); // 'rare' exists only on day 2
    e.maintain(6 * DAY + MARGIN + 1); // 6 daily files, no merge tier

    let got = e.count("SELECT count(*) FROM readings WHERE device = 'rare'");
    assert_eq!(got, 24);
    let stats = silodb::last_scan_stats().unwrap();
    assert_eq!(stats.candidate_files, 6, "time pruning can't help here");
    assert_eq!(
        stats.series_pruned_files, 5,
        "stats prune the 5 files with no 'rare' rows: {stats:?}"
    );
    assert_eq!(stats.scanned_files, 1);

    // Common series: nothing pruned, everything still correct.
    let got = e.count("SELECT count(*) FROM readings WHERE device = 'common'");
    assert_eq!(got, 6 * 24);
    assert_eq!(silodb::last_scan_stats().unwrap().series_pruned_files, 0);
}

#[test]
fn missing_stats_degrade_to_no_pruning_and_self_heal() {
    let e = env("1d");
    e.fill_sparse(4, 1);
    let now = 4 * DAY + MARGIN + 1;
    e.maintain(now);

    // Simulate a pre-stats dataset (upgrade path): drop all stats rows.
    e.conn.execute("DELETE FROM readings_stats", []).unwrap();

    // Conservative: no pruning, correct results, no errors.
    let got = e.count("SELECT count(*) FROM readings WHERE device = 'rare'");
    assert_eq!(got, 24);
    assert_eq!(silodb::last_scan_stats().unwrap().series_pruned_files, 0);

    // maintain() self-heals by re-reading the files once...
    e.maintain(now);
    let healed = e.count("SELECT count(*) FROM readings_stats");
    assert!(healed > 0);
    // ...and pruning works again.
    let got = e.count("SELECT count(*) FROM readings WHERE device = 'rare'");
    assert_eq!(got, 24);
    assert_eq!(silodb::last_scan_stats().unwrap().series_pruned_files, 3);
}

#[test]
fn eviction_removes_stats_rows() {
    let e = env("1d,7d");
    silodb::set_retention(&e.conn, "readings", Some("7d")).unwrap();
    e.fill_sparse(3, 0);
    e.maintain(3 * DAY + MARGIN + 1);
    assert!(e.count("SELECT count(*) FROM readings_stats") > 0);

    e.maintain(30 * DAY); // everything past retention
    assert_eq!(e.count("SELECT count(*) FROM readings"), 0);
    assert_eq!(
        e.count("SELECT count(*) FROM readings_stats"),
        0,
        "stats die with their files"
    );
}
