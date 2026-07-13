//! Property test (spec: "the test that actually catches 'pruning silently
//! drops a row it shouldn't'"): random data split across random bucket
//! files + random range constraints; the doubly-pruned (catalog file level
//! plus row-group level) vtab scan must return exactly the rows a straight
//! in-memory filter of the same data returns.

mod common;

use common::{cold_env, write_id_ts_file};
use proptest::prelude::*;

#[derive(Debug, Clone, Copy)]
enum Op {
    Gt,
    Ge,
    Lt,
    Le,
    Eq,
}

impl Op {
    fn sql(self) -> &'static str {
        match self {
            Op::Gt => ">",
            Op::Ge => ">=",
            Op::Lt => "<",
            Op::Le => "<=",
            Op::Eq => "=",
        }
    }

    fn eval(self, x: i64, v: i64) -> bool {
        match self {
            Op::Gt => x > v,
            Op::Ge => x >= v,
            Op::Lt => x < v,
            Op::Le => x <= v,
            Op::Eq => x == v,
        }
    }
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        Just(Op::Gt),
        Just(Op::Ge),
        Just(Op::Lt),
        Just(Op::Le),
        Just(Op::Eq),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        ..ProptestConfig::default()
    })]

    #[test]
    fn pruned_scan_equals_in_memory_filter(
        // Unsorted on purpose: min/max pruning must be correct regardless of
        // row order. Values cluster in a narrow band so constraints actually
        // bite; a few extremes probe overflow-ish edges.
        ts in prop::collection::vec(
            prop_oneof![
                9 => -10_000i64..10_000,
                1 => prop_oneof![Just(i64::MIN), Just(i64::MAX), Just(0)],
            ],
            1..200,
        ),
        rows_per_file in 1usize..40,
        row_group_size in 1usize..8,
        constraints in prop::collection::vec(
            (op_strategy(), -10_000i64..10_000),
            1..3,
        ),
    ) {
        let env = cold_env();

        // Split rows into bucket files. Catalog ranges are the true per-file
        // min/max (end exclusive), mirroring what compaction records.
        for (f, chunk) in ts.chunks(rows_per_file).enumerate() {
            let base = (f * rows_per_file) as i64;
            let rows: Vec<(i64, i64)> = chunk
                .iter()
                .enumerate()
                .map(|(i, &t)| (base + i as i64, t))
                .collect();
            let path = env.table_dir.join(format!("bucket-{f}.parquet"));
            write_id_ts_file(&path, &rows, row_group_size);
            let min = *chunk.iter().min().unwrap();
            let max = *chunk.iter().max().unwrap();
            env.register(&path, min, max.saturating_add(1), rows.len() as i64);
        }
        env.create_vtab(common::ColdEnv::ID_TS_SCHEMA);

        let where_clause = constraints
            .iter()
            .map(|(op, v)| format!("ts {} {v}", op.sql()))
            .collect::<Vec<_>>()
            .join(" AND ");
        let got: Vec<i64> = env
            .conn
            .prepare(&format!(
                "SELECT id FROM cold WHERE {where_clause} ORDER BY id"
            ))
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        let expected: Vec<i64> = ts
            .iter()
            .enumerate()
            .filter(|&(_, &x)| constraints.iter().all(|(op, v)| op.eval(x, *v)))
            .map(|(i, _)| i as i64)
            .collect();

        prop_assert_eq!(got, expected);
    }
}
