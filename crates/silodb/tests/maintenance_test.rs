//! Tiered maintenance: `maintain()` as a convergence function — tier-0
//! compaction, window promotion with supersede, GC, late-data re-merge,
//! crash idempotency, and view invariance throughout.

use rusqlite::{params, Connection};
use silodb::MaintainAction;

const DAY: i64 = 86_400 * 1_000_000;
const MARGIN: i64 = 2 * 3600 * 1_000_000;

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
    silodb::init_table_tiered(&conn, "readings", "ts TIMESTAMP, value REAL", &base, tiers)
        .unwrap();
    Env {
        conn,
        base,
        _dir: dir,
    }
}

impl Env {
    /// One row per hour for `days` days starting at day `from`.
    fn fill_days(&self, from: i64, days: i64) {
        for d in from..from + days {
            for h in 0..24 {
                self.conn
                    .execute(
                        "INSERT INTO readings VALUES (?1, ?2)",
                        params![d * DAY + h * 3600 * 1_000_000, (d * 24 + h) as f64],
                    )
                    .unwrap();
            }
        }
    }

    fn view_count(&self) -> i64 {
        self.conn
            .query_row("SELECT count(*) FROM readings", [], |r| r.get(0))
            .unwrap()
    }

    fn active_files(&self) -> Vec<(i64, i64, String)> {
        silodb::catalog::entries_for_table(&self.conn, "readings")
            .unwrap()
            .into_iter()
            .map(|e| (e.range_start, e.range_end, e.path))
            .collect()
    }

    fn maintain(&self, now: i64) -> Vec<MaintainAction> {
        silodb::maintain(&self.conn, "readings", &self.base, now).unwrap()
    }
}

#[test]
fn tier0_compaction_is_driven_by_the_clock() {
    let e = env("1d");
    e.fill_days(0, 3); // days 0,1,2
    // Clock inside day 2: only days 0 and 1 are closed + past margin.
    let actions = e.maintain(2 * DAY + MARGIN + 1);
    let compacted: Vec<_> = actions
        .iter()
        .filter(|a| matches!(a, MaintainAction::Compacted { .. }))
        .collect();
    assert_eq!(compacted.len(), 2);
    assert_eq!(e.view_count(), 72, "nothing lost");
    // Idempotent: nothing further due.
    assert!(e.maintain(2 * DAY + MARGIN + 1).is_empty());
    // Clock moves past day 2 → one more.
    let actions = e.maintain(3 * DAY + MARGIN + 1);
    assert_eq!(actions.len(), 1);
}

#[test]
fn weekly_promotion_merges_and_gcs_daily_files() {
    let e = env("1d,7d");
    e.fill_days(0, 8); // one full week + one day into the next
    let now = 8 * DAY + MARGIN + 1;
    let actions = e.maintain(now);

    let merges: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            MaintainAction::Merged {
                window, children, rows, ..
            } => Some((*window, *children, *rows)),
            _ => None,
        })
        .collect();
    assert_eq!(merges, vec![((0, 7 * DAY), 7, 7 * 24)]);
    let gcs = actions
        .iter()
        .filter(|a| matches!(a, MaintainAction::Gc { .. }))
        .count();
    assert_eq!(gcs, 7, "daily children unlinked");

    // Active files: one weekly + day 7's daily.
    let mut files = e.active_files();
    files.sort();
    assert_eq!(files.len(), 2);
    assert_eq!((files[0].0, files[0].1), (0, 7 * DAY));
    assert_eq!((files[1].0, files[1].1), (7 * DAY, 8 * DAY));
    // Superseded files really gone from disk.
    let on_disk = std::fs::read_dir(e.base.join("readings")).unwrap().count();
    assert_eq!(on_disk, 2);

    assert_eq!(e.view_count(), 8 * 24, "data identical through it all");
    assert!(e.maintain(now).is_empty(), "converged");
}

#[test]
fn three_tiers_converge_over_a_simulated_month() {
    let e = env("1d,7d,28d");
    // Simulate 30 days arriving one day at a time, maintaining as we go —
    // the steady-state path, not a backlog.
    for d in 0..30 {
        e.fill_days(d, 1);
        e.maintain((d + 1) * DAY + MARGIN + 1);
    }
    let now = 30 * DAY + MARGIN + 1;
    e.maintain(now);
    assert_eq!(e.view_count(), 30 * 24);

    let mut files = e.active_files();
    files.sort();
    let spans: Vec<(i64, i64)> = files.iter().map(|f| (f.0 / DAY, f.1 / DAY)).collect();
    // One 28d file + dailies for days 28, 29 (the 7d window 28..35 isn't
    // closed yet at day 30).
    assert_eq!(spans, vec![(0, 28), (28, 29), (29, 30)]);

    // Every remaining file exists and nothing extra lingers on disk.
    let on_disk = std::fs::read_dir(e.base.join("readings")).unwrap().count();
    assert_eq!(on_disk, files.len());
}

#[test]
fn late_rows_after_promotion_remerge_convergently() {
    let e = env("1d,7d");
    e.fill_days(0, 7);
    let now = 8 * DAY;
    e.maintain(now);
    assert_eq!(e.active_files().len(), 1, "one weekly file");

    // A late row lands inside the already-promoted week.
    e.conn
        .execute(
            "INSERT INTO readings VALUES (?1, 999.0)",
            params![3 * DAY + 12 * 3600 * 1_000_000 + 1],
        )
        .unwrap();
    assert_eq!(e.view_count(), 7 * 24 + 1);

    let actions = e.maintain(now);
    // Late row compacts into a small day-3 file, then the mixed window
    // (weekly + straggler) re-merges into a new weekly seq.
    assert!(actions
        .iter()
        .any(|a| matches!(a, MaintainAction::Compacted { .. })));
    assert!(actions
        .iter()
        .any(|a| matches!(a, MaintainAction::Merged { children: 2, .. })));
    assert_eq!(e.view_count(), 7 * 24 + 1, "row visible exactly once");
    assert_eq!(e.active_files().len(), 1, "back to one weekly file");
    assert!(e.maintain(now).is_empty(), "converged");
}

/// Crash between merge rename and its catalog transaction: file on disk,
/// children still active. Re-running maintain must converge with no
/// duplication and a byte-identical merged file.
#[test]
fn merge_crash_rerun_is_idempotent() {
    let e = env("1d,7d");
    e.fill_days(0, 7);
    let now = 8 * DAY;
    // Compact the dailies only (tier0), leaving promotion undone.
    for d in 0..7 {
        silodb::compact_table(&e.conn, "readings", d * DAY, (d + 1) * DAY, &e.base).unwrap();
    }

    // Do the real merge once to learn the exact file a finished run makes.
    let done = silodb_compact::merge_window(&e.conn, "readings", &e.base, 0, 7 * DAY).unwrap();
    let silodb_compact::MergeOutcome::Merged { path, .. } = done else {
        panic!("{done:?}");
    };
    let finished_bytes = std::fs::read(&path).unwrap();

    // Reconstruct the "crashed" state: children active again, merged row
    // gone, merged FILE still on disk (rename happened, commit didn't).
    e.conn
        .execute(
            "UPDATE _silodb_catalog SET status='active' WHERE range_end - range_start < ?1",
            [7 * DAY],
        )
        .unwrap();
    silodb::catalog::delete_entry(&e.conn, "readings", &path.display().to_string()).unwrap();

    let actions = e.maintain(now);
    assert!(actions
        .iter()
        .any(|a| matches!(a, MaintainAction::Merged { children: 7, .. })));
    assert_eq!(
        std::fs::read(&path).unwrap(),
        finished_bytes,
        "same seq recomputed, byte-identical rewrite"
    );
    assert_eq!(e.view_count(), 7 * 24);
    assert!(e.maintain(now).is_empty());
}

#[test]
fn misaligned_tiers_are_rejected_at_init() {
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    let err = silodb::init_table_tiered(
        &conn,
        "readings",
        "ts TIMESTAMP, value REAL",
        "cold/",
        "1d,7d,30d", // 30 % 7 != 0
    )
    .unwrap_err();
    assert!(err.to_string().contains("multiple"), "{err}");
    for bad in ["", "0d", "-1d", "1x", "7d,1d"] {
        assert!(
            silodb::init_table_tiered(&conn, "t", "ts TIMESTAMP", "cold/", bad).is_err(),
            "{bad} should be rejected"
        );
    }
}

#[test]
fn maintain_without_policy_errors() {
    let conn = Connection::open_in_memory().unwrap();
    silodb::load_module(&conn).unwrap();
    let err = silodb::maintain(&conn, "nope", "cold/", 0).unwrap_err();
    assert!(matches!(err, silodb::MaintainError::NoPolicy(_)));
}
