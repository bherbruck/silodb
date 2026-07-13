//! Phase 2 acceptance: constrained queries skip row groups, asserted on the
//! scan-stats counter (not wall-clock), against fixture row-group boundaries
//! pinned by fixtures/gen:
//!
//!   basic.parquet ts (µs): rg0 = [1000, 4000], rg1 = [5000, 8000],
//!                          rg2 = [9000, 10000]
//!   basic.parquet value:   rg0 max 2.0, rg1 max 4.0, rg2 = [4.5, 5.0]

use rusqlite::Connection;
use silodb_vtab::{last_scan_stats, ScanStats};

fn conn_with_vtab() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    silodb_vtab::load_module(&conn).unwrap();
    conn.execute_batch(&format!(
        "CREATE VIRTUAL TABLE cold USING silodb('{}/../../fixtures/basic.parquet')",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    conn
}

fn ids(conn: &Connection, sql: &str) -> Vec<i64> {
    conn.prepare(sql)
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn assert_scanned(scanned: usize) {
    assert_eq!(
        last_scan_stats(),
        Some(ScanStats {
            total_row_groups: 3,
            scanned_row_groups: scanned,
        })
    );
}

#[test]
fn unconstrained_scan_reads_all_row_groups() {
    let conn = conn_with_vtab();
    assert_eq!(ids(&conn, "SELECT id FROM cold").len(), 10);
    assert_scanned(3);
}

#[test]
fn ts_range_skips_outer_row_groups() {
    let conn = conn_with_vtab();
    let got = ids(
        &conn,
        "SELECT id FROM cold WHERE ts > 4500 AND ts < 8500",
    );
    assert_eq!(got, vec![5, 6, 7, 8]);
    assert_scanned(1); // only rg1
}

#[test]
fn ts_upper_bound_skips_later_row_groups() {
    let conn = conn_with_vtab();
    let got = ids(&conn, "SELECT id FROM cold WHERE ts <= 4000");
    assert_eq!(got, vec![1, 2, 3, 4]);
    assert_scanned(1); // only rg0
}

#[test]
fn ts_equality_hits_one_row_group() {
    let conn = conn_with_vtab();
    let got = ids(&conn, "SELECT id FROM cold WHERE ts = 9000");
    assert_eq!(got, vec![9]);
    assert_scanned(1); // only rg2
}

#[test]
fn ts_boundary_value_keeps_the_boundary_group() {
    let conn = conn_with_vtab();
    // ts >= 8000: rg1's max is exactly 8000 — must NOT be pruned.
    let got = ids(&conn, "SELECT id FROM cold WHERE ts >= 8000");
    assert_eq!(got, vec![8, 9, 10]);
    assert_scanned(2); // rg1 + rg2

    // ts > 8000: now rg1 is provably out.
    let got = ids(&conn, "SELECT id FROM cold WHERE ts > 8000");
    assert_eq!(got, vec![9, 10]);
    assert_scanned(1);
}

#[test]
fn real_column_constraint_prunes_too() {
    let conn = conn_with_vtab();
    let got = ids(&conn, "SELECT id FROM cold WHERE value > 4.0");
    assert_eq!(got, vec![9, 10]);
    assert_scanned(1); // only rg2
}

#[test]
fn empty_result_can_skip_every_row_group() {
    let conn = conn_with_vtab();
    let got = ids(&conn, "SELECT id FROM cold WHERE ts > 99999");
    assert!(got.is_empty());
    assert_scanned(0);
}

#[test]
fn text_constraint_does_not_prune_but_still_filters() {
    let conn = conn_with_vtab();
    // TEXT column isn't prunable; results must still be correct.
    let got = ids(&conn, "SELECT id FROM cold WHERE name = 'sensor-4'");
    assert_eq!(got, vec![4]);
}

#[test]
fn pruned_and_full_scan_agree_on_fixture() {
    let conn = conn_with_vtab();
    for (lo, hi) in [
        (0, 11_000),
        (1000, 1000),
        (999, 1001),
        (4000, 5000),
        (8000, 9000),
        (10_000, 10_000),
        (10_001, 20_000),
    ] {
        let pruned = ids(
            &conn,
            &format!("SELECT id FROM cold WHERE ts >= {lo} AND ts <= {hi}"),
        );
        // +0 defeats pushdown (expression, not a bare column constraint),
        // giving a genuinely unpruned reference plan.
        let full = ids(
            &conn,
            &format!("SELECT id FROM cold WHERE ts+0 >= {lo} AND ts+0 <= {hi}"),
        );
        assert_eq!(pruned, full, "range [{lo}, {hi}]");
    }
}
