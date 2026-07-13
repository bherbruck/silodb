//! Shared test scaffolding: a hot in-memory DB with a catalog, a temp cold
//! directory, and helpers to mint deterministic Parquet bucket files.
//!
//! Compiled once per test binary, so any single binary uses only a subset —
//! dead_code warnings here are noise.
#![allow(dead_code)]

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rusqlite::Connection;
use silodb_catalog::CatalogEntry;

pub struct ColdEnv {
    pub conn: Connection,
    pub dir: tempfile::TempDir,
    pub table_dir: PathBuf,
}

/// Hot DB (in-memory, catalog ensured, module loaded) + `<tmp>/sensor/`
/// cold directory. Logical table name: `sensor`.
pub fn cold_env() -> ColdEnv {
    let conn = Connection::open_in_memory().unwrap();
    silodb_catalog::ensure_catalog(&conn).unwrap();
    silodb_vtab::load_module(&conn).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("sensor");
    std::fs::create_dir(&table_dir).unwrap();
    ColdEnv {
        conn,
        dir,
        table_dir,
    }
}

impl ColdEnv {
    pub fn create_vtab(&self) {
        // Base dir + explicit table= (the vtab's name, `cold`, differs from
        // the logical table). The vtab-name-default path is covered in
        // directory_test.
        self.conn
            .execute_batch(&format!(
                "CREATE VIRTUAL TABLE cold USING silodb('{}', table=sensor)",
                self.dir.path().display()
            ))
            .unwrap();
    }

    /// Register `path` in the catalog for logical table `sensor`.
    pub fn register(&self, path: &Path, range_start: i64, range_end: i64, rows: i64) {
        silodb_catalog::insert_entry(
            &self.conn,
            &CatalogEntry {
                logical_table: "sensor".into(),
                path: path.display().to_string(),
                range_start,
                range_end,
                row_count: Some(rows),
                created_at: 0,
                status: "active".into(),
            },
        )
        .unwrap();
    }

    pub fn ids(&self, sql: &str) -> Vec<i64> {
        self.conn
            .prepare(sql)
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }
}

/// Write an (id, ts) Parquet bucket file. Row groups of `rg_size` rows.
pub fn write_id_ts_file(path: &Path, rows: &[(i64, i64)], rg_size: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(rg_size))
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Path of the checked-in hand-built fixture.
pub fn fixture_basic() -> PathBuf {
    PathBuf::from(format!(
        "{}/../../fixtures/basic.parquet",
        env!("CARGO_MANIFEST_DIR")
    ))
}
