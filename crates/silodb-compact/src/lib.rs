//! Compaction: move one closed bucket of rows from the hot SQLite table
//! into a brand-new, immutable Parquet file, and record it in
//! `_silodb_catalog`.
//!
//! Sequence (specv2 Phase 3):
//! 1. select hot rows in `[bucket_start, bucket_end)`, ordered by timestamp
//! 2. write them to `<file>.tmp` inside `out_dir`
//! 3. fsync the temp file, atomically rename into place, fsync the dir
//! 4. **one transaction**: DELETE the hot rows AND INSERT the catalog row
//!
//! Files are named by this crate, not the caller:
//! `bucket-<start>-<end>-<seq>.parquet`, where `seq` counts prior committed
//! compactions of the exact same bucket. That makes every calling pattern
//! idempotent with no error paths to handle:
//!
//! - **Re-run after success**: no hot rows left, an entry exists → no-op
//!   (`AlreadyCompacted`).
//! - **Re-run after a crash between rename and commit**: the file exists
//!   but has no catalog row (so it's invisible to the vtab) and the rows
//!   are still hot; the same seq is computed again, the file is rewritten
//!   byte-identically, and the transaction commits. No duplication.
//! - **Late rows landing in an already-compacted bucket**: next call gets
//!   the next seq and writes an additional file for the same range. The
//!   catalog handles overlapping ranges naturally.
//!
//! A catalog row is the *only* thing that makes a file real.
//!
//! Cost invariant: one call reads/writes one bucket's worth of hot rows.
//! This crate never opens a previously written Parquet file and never scans
//! the hot table beyond the bucket's index range.
//!
//! Deciding *when* to call this is the embedding application's job — see
//! the trigger-logic contract in `docs/specv2.md`.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rusqlite::types::Value;
use rusqlite::{params, Connection};
use silodb_catalog::CatalogEntry;
use silodb_schema::SqliteType;

/// Rows per Parquet row group. Spec: start with 10–20k and revisit only if
/// row-group pruning shows it matters.
const ROW_GROUP_ROWS: usize = 16_384;

/// What `compact_bucket` needs to know about one compaction unit.
#[derive(Debug, Clone)]
pub struct BucketSpec<'a> {
    /// Hot table to age rows out of.
    pub hot_table: &'a str,
    /// Catalog key; usually the cold directory's basename (must match the
    /// vtab's `table=` argument).
    pub logical_table: &'a str,
    /// INTEGER column holding epoch microseconds (silodb convention).
    pub ts_column: &'a str,
    /// Bucket bounds: `[bucket_start, bucket_end)` — end exclusive, like
    /// the catalog's `range_end`.
    pub bucket_start: i64,
    pub bucket_end: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactOutcome {
    /// Rows written to `path`, deleted from the hot table, catalog row
    /// committed.
    Compacted { rows: usize, path: std::path::PathBuf },
    /// The bucket has no hot rows left and at least one committed file —
    /// a previous run finished. Nothing was touched.
    AlreadyCompacted,
    /// No hot rows in the bucket and it was never compacted; no file
    /// written, no catalog row.
    EmptyBucket,
}

#[derive(Debug)]
pub enum CompactError {
    Sqlite(rusqlite::Error),
    Parquet(parquet::errors::ParquetError),
    Io(std::io::Error),
    /// A hot-table column has a declared type we refuse to guess a storage
    /// class for.
    UnsupportedDecl { column: String, decl: String },
    /// A value's runtime type doesn't match its column's storage class.
    TypeMismatch { column: String, value: &'static str },
    /// `ts_column` is missing from the hot table or not INTEGER-declared.
    BadTimestampColumn { column: String },
    /// The catalog says this file exists but it's gone from disk.
    MissingCompactedFile { path: String },
}

impl std::fmt::Display for CompactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(e) => write!(f, "sqlite error: {e}"),
            Self::Parquet(e) => write!(f, "parquet error: {e}"),
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::UnsupportedDecl { column, decl } => write!(
                f,
                "column '{column}' has unsupported declared type '{decl}'"
            ),
            Self::TypeMismatch { column, value } => write!(
                f,
                "column '{column}' holds a {value} value incompatible with its declared type"
            ),
            Self::BadTimestampColumn { column } => write!(
                f,
                "timestamp column '{column}' missing or not INTEGER-declared"
            ),
            Self::MissingCompactedFile { path } => write!(
                f,
                "catalog references '{path}' but the file is missing (possible data loss)"
            ),
        }
    }
}

impl std::error::Error for CompactError {}

impl From<rusqlite::Error> for CompactError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e)
    }
}
impl From<parquet::errors::ParquetError> for CompactError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::Parquet(e)
    }
}
impl From<std::io::Error> for CompactError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, CompactError>;

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

struct HotColumn {
    name: String,
    ty: SqliteType,
}

/// Hot-table columns in declaration order, mapped to storage classes.
fn hot_columns(conn: &Connection, table: &str) -> Result<Vec<HotColumn>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_ident(table)))?;
    let cols = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    cols.into_iter()
        .map(|(name, decl)| {
            let ty = SqliteType::from_decl(&decl).ok_or(CompactError::UnsupportedDecl {
                column: name.clone(),
                decl,
            })?;
            Ok(HotColumn { name, ty })
        })
        .collect()
}

/// Column-major accumulator that keeps SQLite's dynamic typing honest on
/// the way into strictly-typed Arrow arrays.
enum ColBuf {
    Ts(TimestampMicrosecondBuilder),
    Int(Int64Builder),
    Real(Float64Builder),
    Text(StringBuilder),
    Blob(BinaryBuilder),
}

impl ColBuf {
    fn new(ty: SqliteType, is_ts: bool) -> Self {
        if is_ts {
            return Self::Ts(TimestampMicrosecondBuilder::new());
        }
        match ty {
            SqliteType::Integer => Self::Int(Int64Builder::new()),
            SqliteType::Real => Self::Real(Float64Builder::new()),
            SqliteType::Text => Self::Text(StringBuilder::new()),
            SqliteType::Blob => Self::Blob(BinaryBuilder::new()),
        }
    }

    fn push(&mut self, column: &str, v: Value) -> Result<()> {
        let mismatch = |value| CompactError::TypeMismatch {
            column: column.to_owned(),
            value,
        };
        match self {
            Self::Ts(b) => match v {
                Value::Integer(i) => b.append_value(i),
                // NULL timestamps can't be bucketed; the WHERE range
                // excludes them, so one here is a logic error.
                _ => return Err(mismatch(value_kind(&v))),
            },
            Self::Int(b) => match v {
                Value::Integer(i) => b.append_value(i),
                Value::Null => b.append_null(),
                _ => return Err(mismatch(value_kind(&v))),
            },
            Self::Real(b) => match v {
                Value::Real(f) => b.append_value(f),
                // SQLite happily stores INTEGER values in REAL columns.
                Value::Integer(i) => b.append_value(i as f64),
                Value::Null => b.append_null(),
                _ => return Err(mismatch(value_kind(&v))),
            },
            Self::Text(b) => match v {
                Value::Text(s) => b.append_value(s),
                Value::Null => b.append_null(),
                _ => return Err(mismatch(value_kind(&v))),
            },
            Self::Blob(b) => match v {
                Value::Blob(bytes) => b.append_value(bytes),
                Value::Null => b.append_null(),
                _ => return Err(mismatch(value_kind(&v))),
            },
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Ts(b) => Arc::new(b.finish()),
            Self::Int(b) => Arc::new(b.finish()),
            Self::Real(b) => Arc::new(b.finish()),
            Self::Text(b) => Arc::new(b.finish()),
            Self::Blob(b) => Arc::new(b.finish()),
        }
    }
}

fn value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "NULL",
        Value::Integer(_) => "INTEGER",
        Value::Real(_) => "REAL",
        Value::Text(_) => "TEXT",
        Value::Blob(_) => "BLOB",
    }
}

/// Compact `[spec.bucket_start, spec.bucket_end)` from the hot table into a
/// new Parquet file inside `out_dir` (named `bucket-<start>-<end>-<seq>
/// .parquet` by this function), then — in one transaction — delete the hot
/// rows and insert the catalog row. Idempotent under every calling pattern;
/// see module docs.
pub fn compact_bucket(
    conn: &Connection,
    spec: &BucketSpec<'_>,
    out_dir: &Path,
) -> Result<CompactOutcome> {
    silodb_catalog::ensure_catalog(conn)?;

    // Validate the hot table's shape before anything else — a bad
    // ts_column must be an error, not a silently empty bucket.
    let columns = hot_columns(conn, spec.hot_table)?;
    let ts_idx = columns
        .iter()
        .position(|c| c.name == spec.ts_column)
        .filter(|&i| columns[i].ty == SqliteType::Integer)
        .ok_or_else(|| CompactError::BadTimestampColumn {
            column: spec.ts_column.to_owned(),
        })?;

    let rows_in_bucket: i64 = conn.query_row(
        &format!(
            "SELECT count(*) FROM {} WHERE {} >= ?1 AND {} < ?2",
            quote_ident(spec.hot_table),
            quote_ident(spec.ts_column),
            quote_ident(spec.ts_column),
        ),
        params![spec.bucket_start, spec.bucket_end],
        |r| r.get(0),
    )?;

    // Committed files for this exact bucket. Their count is the sequence
    // number of the file this run would create — recomputed identically on
    // a post-crash re-run, so the rewrite lands on the same name.
    let committed = silodb_catalog::entries_for_bucket(
        conn,
        spec.logical_table,
        spec.bucket_start,
        spec.bucket_end,
    )?;
    for entry in &committed {
        if !Path::new(&entry.path).is_file() {
            return Err(CompactError::MissingCompactedFile {
                path: entry.path.clone(),
            });
        }
    }

    if rows_in_bucket == 0 {
        return Ok(if committed.is_empty() {
            CompactOutcome::EmptyBucket
        } else {
            CompactOutcome::AlreadyCompacted
        });
    }

    let out_path = out_dir.join(format!(
        "bucket-{}-{}-{}.parquet",
        spec.bucket_start,
        spec.bucket_end,
        committed.len(),
    ));
    let out_str = out_path.display().to_string();

    let arrow_schema = Arc::new(silodb_schema::bucket_arrow_schema(
        &columns
            .iter()
            .map(|c| (c.name.clone(), c.ty))
            .collect::<Vec<_>>(),
        ts_idx,
    ));

    let col_list = columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = conn.prepare(&format!(
        "SELECT {col_list} FROM {} WHERE {} >= ?1 AND {} < ?2 ORDER BY {}",
        quote_ident(spec.hot_table),
        quote_ident(spec.ts_column),
        quote_ident(spec.ts_column),
        quote_ident(spec.ts_column),
    ))?;

    // Stream rows into the writer in row-group-sized batches so memory is
    // bounded by one row group, not one bucket.
    let tmp_path = {
        let mut p = out_path.as_os_str().to_owned();
        p.push(".tmp");
        std::path::PathBuf::from(p)
    };
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .build();
    let mut writer: Option<ArrowWriter<File>> = None;
    let mut bufs: Vec<ColBuf> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| ColBuf::new(c.ty, i == ts_idx))
        .collect();
    let mut buffered = 0usize;
    let mut total_rows = 0usize;

    let write_result: Result<usize> = (|| {
        let mut rows = stmt.query(params![spec.bucket_start, spec.bucket_end])?;
        loop {
            let row = rows.next()?;
            if let Some(row) = row {
                for (i, (buf, col)) in bufs.iter_mut().zip(&columns).enumerate() {
                    buf.push(&col.name, row.get::<_, Value>(i)?)?;
                }
                buffered += 1;
                total_rows += 1;
            }
            let flush_now = buffered > 0 && (buffered >= ROW_GROUP_ROWS || row.is_none());
            if flush_now {
                let arrays: Vec<ArrayRef> = bufs.iter_mut().map(ColBuf::finish).collect();
                let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
                    .map_err(|e| CompactError::Parquet(e.into()))?;
                let w = match writer.as_mut() {
                    Some(w) => w,
                    None => {
                        let file = File::create(&tmp_path)?;
                        writer = Some(ArrowWriter::try_new(
                            file,
                            arrow_schema.clone(),
                            Some(props.clone()),
                        )?);
                        writer.as_mut().unwrap()
                    }
                };
                w.write(&batch)?;
                buffered = 0;
            }
            if row.is_none() {
                break;
            }
        }
        Ok(total_rows)
    })();

    let total_rows = match write_result {
        Ok(n) => n,
        Err(e) => {
            // Don't leave a half-written temp file behind.
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
    };

    let Some(writer) = writer else {
        return Ok(CompactOutcome::EmptyBucket);
    };

    // fsync temp file, atomic rename, fsync directory — the file is durable
    // and complete before anything references it.
    let file = writer.into_inner()?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp_path, &out_path)?;
    File::open(out_dir)?.sync_all()?;

    // The one transaction that makes it real: hot rows out, catalog row in.
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let txn: Result<()> = (|| {
        conn.execute(
            &format!(
                "DELETE FROM {} WHERE {} >= ?1 AND {} < ?2",
                quote_ident(spec.hot_table),
                quote_ident(spec.ts_column),
                quote_ident(spec.ts_column),
            ),
            params![spec.bucket_start, spec.bucket_end],
        )?;
        silodb_catalog::insert_entry(
            conn,
            &CatalogEntry {
                logical_table: spec.logical_table.to_owned(),
                path: out_str.clone(),
                range_start: spec.bucket_start,
                range_end: spec.bucket_end,
                row_count: Some(total_rows as i64),
                created_at: 0,
                status: "active".into(),
            },
        )?;
        Ok(())
    })();
    match txn {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            Ok(CompactOutcome::Compacted {
                rows: total_rows,
                path: out_path,
            })
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}
