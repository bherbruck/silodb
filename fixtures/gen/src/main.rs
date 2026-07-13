//! Regenerates the checked-in fixture Parquet files, deterministically.
//! Run from the repo root: `cargo run --manifest-path fixtures/gen/Cargo.toml`
//!
//! Row-group boundaries are load-bearing: the vtab test suites assert on
//! *which* row groups get pruned, so the files pin `max_row_group_size` and
//! fixed data. Don't edit the .parquet files by hand; edit this and rerun.

use std::fs::File;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn main() {
    write_basic("fixtures/basic.parquet");
    println!("fixtures regenerated");
}

/// 10 rows, max_row_group_size = 4 → row groups of 4 / 4 / 2 rows.
///
/// ts runs 1_000..=10_000 µs step 1000, strictly increasing, so row-group
/// ts ranges are: rg0 = [1000, 4000], rg1 = [5000, 8000], rg2 = [9000, 10000].
/// NULLs planted in value/name/payload to exercise NULL handling.
fn write_basic(path: &str) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
        Field::new("flag", DataType::Boolean, false),
    ]));

    let ids: Vec<i64> = (1..=10).collect();
    let ts: Vec<i64> = (1..=10).map(|i| i * 1000).collect();
    let values: Vec<Option<f64>> = (1..=10)
        .map(|i| if i == 3 { None } else { Some(i as f64 * 0.5) })
        .collect();
    let names: Vec<Option<String>> = (1..=10)
        .map(|i| {
            if i == 7 {
                None
            } else {
                Some(format!("sensor-{i}"))
            }
        })
        .collect();
    let payloads: Vec<Option<Vec<u8>>> = (1..=10u8)
        .map(|i| if i == 5 { None } else { Some(vec![i, i, i]) })
        .collect();
    let flags: Vec<bool> = (1..=10).map(|i| i % 2 == 0).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(ts)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(
                names
                    .iter()
                    .map(|o| o.as_deref())
                    .collect::<Vec<Option<&str>>>(),
            )),
            Arc::new(BinaryArray::from(
                payloads
                    .iter()
                    .map(|o| o.as_deref())
                    .collect::<Vec<Option<&[u8]>>>(),
            )),
            Arc::new(BooleanArray::from(flags)),
        ],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    let meta = writer.close().unwrap();
    assert_eq!(meta.row_groups().len(), 3, "expected exactly 3 row groups");
}
