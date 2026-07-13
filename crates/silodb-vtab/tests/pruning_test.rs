//! Phase 2 acceptance (row-group layer): constrained queries skip row
//! groups, asserted on the scan-stats counters (not wall-clock), against
//! fixture row-group boundaries pinned by fixtures/gen:
//!
//!   basic.parquet ts (µs): rg0 = [1000, 4000], rg1 = [5000, 8000],
//!                          rg2 = [9000, 10000]
//!   basic.parquet value:   rg0 max 2.0, rg1 max 4.0, rg2 = [4.5, 5.0]

mod common;

use common::{cold_env, fixture_basic, ColdEnv};
use silodb_vtab::last_scan_stats;

fn env_with_fixture() -> ColdEnv {
    let env = cold_env();
    let dest = env.table_dir.join("bucket-1000.parquet");
    std::fs::copy(fixture_basic(), &dest).unwrap();
    env.register(&dest, 1000, 10_001, 10);
    env.create_vtab(common::ColdEnv::FIXTURE_SCHEMA);
    env
}

fn assert_row_groups(scanned: usize) {
    let stats = last_scan_stats().unwrap();
    assert_eq!(stats.total_row_groups, 3, "{stats:?}");
    assert_eq!(stats.scanned_row_groups, scanned, "{stats:?}");
}

#[test]
fn unconstrained_scan_reads_all_row_groups() {
    let env = env_with_fixture();
    assert_eq!(env.ids("SELECT id FROM cold").len(), 10);
    assert_row_groups(3);
}

#[test]
fn ts_range_skips_outer_row_groups() {
    let env = env_with_fixture();
    let got = env.ids("SELECT id FROM cold WHERE ts > 4500 AND ts < 8500");
    assert_eq!(got, vec![5, 6, 7, 8]);
    assert_row_groups(1); // only rg1
}

#[test]
fn ts_upper_bound_skips_later_row_groups() {
    let env = env_with_fixture();
    let got = env.ids("SELECT id FROM cold WHERE ts <= 4000");
    assert_eq!(got, vec![1, 2, 3, 4]);
    assert_row_groups(1); // only rg0
}

#[test]
fn ts_equality_hits_one_row_group() {
    let env = env_with_fixture();
    let got = env.ids("SELECT id FROM cold WHERE ts = 9000");
    assert_eq!(got, vec![9]);
    assert_row_groups(1); // only rg2
}

#[test]
fn ts_boundary_value_keeps_the_boundary_group() {
    let env = env_with_fixture();
    // ts >= 8000: rg1's max is exactly 8000 — must NOT be pruned.
    let got = env.ids("SELECT id FROM cold WHERE ts >= 8000");
    assert_eq!(got, vec![8, 9, 10]);
    assert_row_groups(2); // rg1 + rg2

    // ts > 8000: now rg1 is provably out.
    let got = env.ids("SELECT id FROM cold WHERE ts > 8000");
    assert_eq!(got, vec![9, 10]);
    assert_row_groups(1);
}

#[test]
fn real_column_constraint_prunes_too() {
    let env = env_with_fixture();
    let got = env.ids("SELECT id FROM cold WHERE value > 4.0");
    assert_eq!(got, vec![9, 10]);
    assert_row_groups(1); // only rg2
}

#[test]
fn empty_result_can_skip_every_row_group() {
    let env = env_with_fixture();
    let got = env.ids("SELECT id FROM cold WHERE ts > 10500 AND ts < 10800");
    assert!(got.is_empty());
    // The file's bucket range [1000, 10001) doesn't reach 10500 either, so
    // even the catalog layer may drop it — either way, zero row groups read.
    assert_eq!(last_scan_stats().unwrap().scanned_row_groups, 0);
}

#[test]
fn text_constraint_does_not_prune_but_still_filters() {
    let env = env_with_fixture();
    // TEXT column isn't prunable; results must still be correct.
    let got = env.ids("SELECT id FROM cold WHERE name = 'sensor-4'");
    assert_eq!(got, vec![4]);
}

#[test]
fn pruned_and_full_scan_agree_on_fixture() {
    let env = env_with_fixture();
    for (lo, hi) in [
        (0, 11_000),
        (1000, 1000),
        (999, 1001),
        (4000, 5000),
        (8000, 9000),
        (10_000, 10_000),
        (10_001, 20_000),
    ] {
        let pruned = env.ids(&format!(
            "SELECT id FROM cold WHERE ts >= {lo} AND ts <= {hi}"
        ));
        // +0 defeats pushdown (expression, not a bare column constraint),
        // giving a genuinely unpruned reference plan.
        let full = env.ids(&format!(
            "SELECT id FROM cold WHERE ts+0 >= {lo} AND ts+0 <= {hi}"
        ));
        assert_eq!(pruned, full, "range [{lo}, {hi}]");
    }
}
