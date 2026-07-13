//! Property test (spec: "the test that actually catches 'pruning silently
//! drops a row it shouldn't'"): random data + random range constraints;
//! the pruned vtab scan must return exactly the rows a straight in-memory
//! filter of the same data returns.

use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use proptest::prelude::*;
use rusqlite::Connection;

/// Write (id, ts) rows to a Parquet file with small row groups so a typical
/// case spans several groups.
fn write_parquet(path: &std::path::Path, ts: &[i64], row_group_size: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let ids: Vec<i64> = (0..ts.len() as i64).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(ts.to_vec())),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_size))
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

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
        row_group_size in 1usize..16,
        constraints in prop::collection::vec(
            (op_strategy(), -10_000i64..10_000),
            1..3,
        ),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prop.parquet");
        write_parquet(&path, &ts, row_group_size);

        let conn = Connection::open_in_memory().unwrap();
        silodb_vtab::load_module(&conn).unwrap();
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE cold USING silodb('{}')",
            path.display()
        ))
        .unwrap();

        let where_clause = constraints
            .iter()
            .map(|(op, v)| format!("ts {} {v}", op.sql()))
            .collect::<Vec<_>>()
            .join(" AND ");
        let got: Vec<i64> = conn
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
