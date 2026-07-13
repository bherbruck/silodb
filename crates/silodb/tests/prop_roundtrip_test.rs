//! Whole-pipeline property: random rows (all four storage classes, NULLs)
//! inserted through the single-name surface, random bucket boundaries
//! compacted in random order — the view must always equal the inserted
//! data, byte for byte, no matter how rows are split between hot and cold.

use proptest::prelude::*;
use rusqlite::{params, Connection};

type Row = (i64, Option<i64>, Option<f64>, Option<String>, Option<Vec<u8>>);

fn row_strategy() -> impl Strategy<Value = Row> {
    (
        -50_000i64..50_000,                                   // ts (µs, synthetic)
        prop::option::of(any::<i64>()),                        // seq INTEGER
        prop::option::of(-1e12f64..1e12),                      // value REAL (finite)
        prop::option::of("[a-zA-Z0-9 ,;'\"\\-]{0,20}"),        // name TEXT (incl. quotes)
        prop::option::of(prop::collection::vec(any::<u8>(), 0..24)), // payload BLOB
    )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    #[test]
    fn view_equals_inserted_data_across_any_compaction_split(
        mut rows in prop::collection::vec(row_strategy(), 1..120),
        // Bucket width decides how many files the data shatters into.
        bucket_width in 1_000i64..40_000,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("cold");
        let conn = Connection::open_in_memory().unwrap();
        silodb::load_module(&conn).unwrap();
        silodb::init_table(
            &conn,
            "readings",
            "ts TIMESTAMP, seq INTEGER, value REAL, name TEXT, payload BLOB",
            &base,
        )
        .unwrap();

        for (ts, seq, value, name, payload) in &rows {
            conn.execute(
                "INSERT INTO readings VALUES (?1, ?2, ?3, ?4, ?5)",
                params![ts, seq, value, name, payload],
            )
            .unwrap();
        }

        // Compact every bucket the data spans.
        let lo = rows.iter().map(|r| r.0).min().unwrap();
        let hi = rows.iter().map(|r| r.0).max().unwrap();
        let first = lo.div_euclid(bucket_width);
        let last = hi.div_euclid(bucket_width);
        for b in first..=last {
            silodb::compact_table(
                &conn,
                "readings",
                b * bucket_width,
                (b + 1) * bucket_width,
                &base,
            )
            .unwrap();
        }
        let hot: i64 = conn
            .query_row("SELECT count(*) FROM readings_hot", [], |r| r.get(0))
            .unwrap();
        prop_assert_eq!(hot, 0, "every row aged out");

        // The view returns exactly what was inserted.
        let mut got: Vec<Row> = conn
            .prepare("SELECT ts, seq, value, name, payload FROM readings")
            .unwrap()
            .query_map([], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        let key = |r: &Row| (r.0, r.1, r.2.map(f64::to_bits), r.3.clone(), r.4.clone());
        got.sort_by_key(key);
        rows.sort_by_key(key);
        prop_assert_eq!(got, rows);
    }
}
