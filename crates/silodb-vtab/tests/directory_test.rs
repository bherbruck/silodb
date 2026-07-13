//! Phase 2.5 acceptance: catalog-driven multi-file tables — file-level
//! range pruning, catalog-as-source-of-truth (stray files invisible, new
//! files visible without DDL), footer cache behavior, and failure modes.

mod common;

use common::{cold_env, write_id_ts_file, ColdEnv};
use silodb_vtab::last_scan_stats;

/// Three bucket files, 4 rows each, one row group per 2 rows:
///   bucket-0:    ids 0..4,  ts 0, 100, 200, 300      range [0, 1000)
///   bucket-1000: ids 4..8,  ts 1000, 1100, 1200, 1300 range [1000, 2000)
///   bucket-2000: ids 8..12, ts 2000, 2100, 2200, 2300 range [2000, 3000)
fn env_with_three_buckets() -> ColdEnv {
    let env = cold_env();
    for b in 0..3i64 {
        let start = b * 1000;
        let rows: Vec<(i64, i64)> = (0..4)
            .map(|i| (b * 4 + i, start + i * 100))
            .collect();
        let path = env.table_dir.join(format!("bucket-{start}.parquet"));
        write_id_ts_file(&path, &rows, 2);
        env.register(&path, start, start + 1000, 4);
    }
    env.create_vtab();
    env
}

#[test]
fn scan_spans_all_files_in_bucket_order() {
    let env = env_with_three_buckets();
    let got = env.ids("SELECT id FROM cold");
    assert_eq!(got, (0..12).collect::<Vec<i64>>());
    let stats = last_scan_stats().unwrap();
    assert_eq!(stats.total_files, 3);
    assert_eq!(stats.candidate_files, 3);
    assert_eq!(stats.scanned_files, 3);
}

#[test]
fn ts_range_prunes_whole_files_via_catalog() {
    let env = env_with_three_buckets();
    let got = env.ids("SELECT id FROM cold WHERE ts >= 1000 AND ts < 1250");
    assert_eq!(got, vec![4, 5, 6]);
    let stats = last_scan_stats().unwrap();
    assert_eq!(stats.total_files, 3);
    assert_eq!(
        stats.candidate_files, 1,
        "catalog range query must drop the other two files: {stats:?}"
    );
    // Within the surviving file, row-group pruning still applies:
    // rows (1000,1100 | 1200,1300) → ts < 1250 keeps both groups,
    // ts >= 1000 keeps both → 2 groups scanned of 2.
    assert_eq!(stats.total_row_groups, 2);
}

#[test]
fn row_group_pruning_composes_with_file_pruning() {
    let env = env_with_three_buckets();
    let got = env.ids("SELECT id FROM cold WHERE ts >= 1200 AND ts <= 2100");
    assert_eq!(got, vec![6, 7, 8, 9]);
    let stats = last_scan_stats().unwrap();
    assert_eq!(stats.candidate_files, 2);
    // bucket-1000: rg(1000,1100) pruned, rg(1200,1300) kept.
    // bucket-2000: rg(2000,2100) kept, rg(2200,2300) pruned.
    assert_eq!(stats.total_row_groups, 4);
    assert_eq!(stats.scanned_row_groups, 2);
}

#[test]
fn stray_parquet_file_without_catalog_row_is_invisible() {
    let env = env_with_three_buckets();
    // Simulates a compaction that crashed between rename and commit: file
    // on disk, no catalog row — its rows are still in the hot table, so
    // reading it would double-count.
    let stray = env.table_dir.join("bucket-9000.parquet");
    write_id_ts_file(&stray, &[(999, 9000), (998, 9100)], 2);

    let got = env.ids("SELECT id FROM cold");
    assert_eq!(got, (0..12).collect::<Vec<i64>>(), "stray rows must not appear");
    assert_eq!(last_scan_stats().unwrap().total_files, 3);
}

#[test]
fn file_added_after_create_is_visible_without_ddl() {
    let env = env_with_three_buckets();
    assert_eq!(env.ids("SELECT id FROM cold").len(), 12);

    let path = env.table_dir.join("bucket-3000.parquet");
    write_id_ts_file(&path, &[(12, 3000), (13, 3100)], 2);
    env.register(&path, 3000, 4000, 2);

    let got = env.ids("SELECT id FROM cold");
    assert_eq!(got, (0..14).collect::<Vec<i64>>());
    assert_eq!(last_scan_stats().unwrap().total_files, 4);
}

#[test]
fn footer_cache_hits_on_repeat_queries() {
    let env = env_with_three_buckets();
    env.ids("SELECT id FROM cold");
    assert_eq!(last_scan_stats().unwrap().metadata_cache_hits, 0);
    env.ids("SELECT id FROM cold");
    assert_eq!(
        last_scan_stats().unwrap().metadata_cache_hits,
        3,
        "second scan must reuse all three cached footers"
    );
}

#[test]
fn catalog_entry_with_missing_file_errors_loudly() {
    let env = env_with_three_buckets();
    let ghost = env.table_dir.join("bucket-5000.parquet");
    env.register(&ghost, 5000, 6000, 1);
    let err = env
        .conn
        .prepare("SELECT id FROM cold")
        .unwrap()
        .query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();
    assert!(err.to_string().contains("data loss"), "{err}");
}

#[test]
fn schema_mismatch_across_files_errors() {
    let env = env_with_three_buckets();
    // A file whose schema differs from the declared (first file's) schema.
    let odd = env.table_dir.join("bucket-7000.parquet");
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("something_else", arrow::datatypes::DataType::Int64, false),
    ]));
    let batch = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(arrow::array::Int64Array::from(vec![1])) as _],
    )
    .unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&odd).unwrap(),
        schema,
        None,
    )
    .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    env.register(&odd, 7000, 8000, 1);

    let err = env
        .conn
        .prepare("SELECT id FROM cold")
        .unwrap()
        .query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();
    assert!(err.to_string().contains("schema"), "{err}");
}

#[test]
fn logical_table_and_ts_column_overrides() {
    let env = cold_env();
    // Vtab name ≠ logical table; overridden via table=.
    let path = env.table_dir.join("bucket-0.parquet");
    write_id_ts_file(&path, &[(1, 0), (2, 100)], 2);
    silodb_catalog::insert_entry(
        &env.conn,
        &silodb_catalog::CatalogEntry {
            logical_table: "other_name".into(),
            path: path.display().to_string(),
            range_start: 0,
            range_end: 1000,
            row_count: Some(2),
            created_at: 0,
            status: "active".into(),
        },
    )
    .unwrap();
    env.conn
        .execute_batch(&format!(
            "CREATE VIRTUAL TABLE cold USING silodb('{}', table=other_name, ts_column=ts)",
            env.dir.path().display()
        ))
        .unwrap();
    assert_eq!(env.ids("SELECT id FROM cold"), vec![1, 2]);
}

#[test]
fn vtab_name_is_the_default_logical_table() {
    let env = cold_env();
    let path = env.table_dir.join("bucket-0.parquet");
    write_id_ts_file(&path, &[(1, 0), (2, 100)], 2);
    env.register(&path, 0, 1000, 2);
    // No table= argument: the vtab's own name selects the logical table —
    // one base dir serves every cold table.
    env.conn
        .execute_batch(&format!(
            "CREATE VIRTUAL TABLE sensor USING silodb('{}')",
            env.dir.path().display()
        ))
        .unwrap();
    let ids: Vec<i64> = env
        .conn
        .prepare("SELECT id FROM sensor")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(ids, vec![1, 2]);
}
