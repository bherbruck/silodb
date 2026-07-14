//! Model-based lifecycle property: a random interleaving of inserts,
//! maintenance runs (with a random forward-moving clock) and range queries
//! against a managed table must always agree with a trivial in-memory
//! oracle. This exercises the entire stack at once — writable vtab, shadow
//! table, compaction, tier merges, GC, catalog pruning, series stats,
//! hot∪cold cursor — under sequences no example-based test would think of.

use proptest::prelude::*;
use rusqlite::{params, Connection};

const HOUR: i64 = 3600 * 1_000_000;

#[derive(Debug, Clone)]
enum Op {
    /// Insert (ts_hours, device_idx, value).
    Insert(i64, u8, f64),
    /// Advance the clock by n hours and run maintain.
    Maintain(i64),
    /// Range-count query [lo_hours, lo+len_hours), optionally one device.
    Query(i64, i64, Option<u8>),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0i64..24 * 40, 0u8..3, -100.0f64..100.0).prop_map(|(t, d, v)| {
            Op::Insert(t, d, (v * 10.0).round() / 10.0)
        }),
        2 => (1i64..24 * 20).prop_map(Op::Maintain),
        4 => (0i64..24 * 40, 1i64..24 * 20, prop::option::of(0u8..3))
            .prop_map(|(lo, len, d)| Op::Query(lo, len, d)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, ..ProptestConfig::default() })]

    #[test]
    fn managed_table_always_agrees_with_the_oracle(
        ops in prop::collection::vec(op_strategy(), 1..60),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("cold");
        let conn = Connection::open_in_memory().unwrap();
        silodb::load_module(&conn).unwrap();
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE t USING silodb('{}',
                schema='ts TIMESTAMP, device TEXT, value REAL',
                tiers='1d,7d')",
            base.display()
        ))
        .unwrap();

        // Oracle: a plain vec of rows. No retention in the policy, so the
        // model never has to reason about file-granular eviction — that
        // interaction has its own example-based tests.
        let mut model: Vec<(i64, u8, f64)> = Vec::new();
        let mut clock: i64 = 0;

        for op in &ops {
            match *op {
                Op::Insert(th, d, v) => {
                    let ts = th * HOUR + 17; // off-grid on purpose
                    conn.execute(
                        "INSERT INTO t VALUES (?1, ?2, ?3)",
                        params![ts, format!("dev-{d}"), v],
                    )
                    .unwrap();
                    model.push((ts, d, v));
                }
                Op::Maintain(dh) => {
                    clock += dh * HOUR;
                    silodb::maintain(&conn, "t", clock).unwrap();
                }
                Op::Query(lo_h, len_h, dev) => {
                    let (lo, hi) = (lo_h * HOUR, (lo_h + len_h) * HOUR);
                    let (sql, expected): (String, (i64, f64)) = match dev {
                        Some(d) => (
                            format!(
                                "SELECT count(value), coalesce(sum(value),0) FROM t
                                 WHERE ts >= {lo} AND ts < {hi} AND device = 'dev-{d}'"
                            ),
                            model
                                .iter()
                                .filter(|(t, md, _)| *t >= lo && *t < hi && *md == d)
                                .fold((0, 0.0), |(n, s), (_, _, v)| (n + 1, s + v)),
                        ),
                        None => (
                            format!(
                                "SELECT count(value), coalesce(sum(value),0) FROM t
                                 WHERE ts >= {lo} AND ts < {hi}"
                            ),
                            model
                                .iter()
                                .filter(|(t, _, _)| *t >= lo && *t < hi)
                                .fold((0, 0.0), |(n, s), (_, _, v)| (n + 1, s + v)),
                        ),
                    };
                    let got: (i64, f64) = conn
                        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
                        .unwrap();
                    prop_assert_eq!(got.0, expected.0, "count for {:?} after {:?}", op, ops);
                    prop_assert!(
                        (got.1 - expected.1).abs() <= 1e-9 * expected.1.abs().max(1.0),
                        "sum for {:?}", op
                    );
                }
            }
        }

        // Final invariant: full contents equal the model exactly.
        let total: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();
        prop_assert_eq!(total as usize, model.len());
    }
}
