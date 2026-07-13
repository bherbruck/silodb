//! Compaction: move one closed bucket of rows from the hot SQLite table
//! into a brand-new, immutable Parquet file, and record it in
//! `_silodb_catalog`.
//!
//! Sequence (docs/spec.md, write path):
//! 1. select hot rows in `[bucket_start, bucket_end)`, ordered by timestamp
//! 2. write them to `<file>.tmp` inside `<base_dir>/<logical_table>/`
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
//! the trigger-logic contract in `docs/spec.md`.

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
    /// Bucket axis override. `None` → discovered by declared type
    /// (`silodb_schema::resolve_ts_index` precedence: explicit name, else
    /// exactly one TIMESTAMP/DATETIME column, else an INTEGER `ts`).
    pub ts_column: Option<&'a str>,
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
    /// No usable bucket axis: the explicit `ts_column` is missing or not
    /// INTEGER-class, or type-driven discovery found zero/multiple
    /// TIMESTAMP columns.
    BadTimestampColumn { reason: silodb_schema::TsResolveError },
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
            Self::BadTimestampColumn { reason } => {
                write!(f, "no usable timestamp column: {reason}")
            }
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
impl From<arrow::error::ArrowError> for CompactError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Parquet(e.into())
    }
}

pub type Result<T> = std::result::Result<T, CompactError>;

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Hot-table columns in declaration order, parsed (storage class +
/// TIMESTAMP marker) through `silodb-schema`'s shared rules.
fn hot_columns(conn: &Connection, table: &str) -> Result<Vec<silodb_schema::ColumnDecl>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_ident(table)))?;
    let cols = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    cols.into_iter()
        .map(|(name, decl)| {
            silodb_schema::ColumnDecl::parse(&name, &decl).ok_or(CompactError::UnsupportedDecl {
                column: name,
                decl,
            })
        })
        .collect()
}

/// Column-major accumulator that keeps SQLite's dynamic typing honest on
/// the way into strictly-typed Arrow arrays.
enum ColBuf {
    /// `allow_null` is false only for the bucket axis (its arrow field is
    /// non-nullable; the WHERE range excludes NULLs anyway). Secondary
    /// TIMESTAMP-declared columns are nullable like everything else.
    Ts {
        b: TimestampMicrosecondBuilder,
        allow_null: bool,
    },
    Int(Int64Builder),
    Real(Float64Builder),
    Text(StringBuilder),
    Blob(BinaryBuilder),
}

impl ColBuf {
    fn new(col: &silodb_schema::ColumnDecl, is_axis: bool) -> Self {
        if is_axis || col.declared_timestamp {
            // Timezone must match silodb_schema::timestamp_arrow_type() or
            // RecordBatch construction rejects the array.
            return Self::Ts {
                b: TimestampMicrosecondBuilder::new().with_timezone("UTC"),
                allow_null: !is_axis,
            };
        }
        match col.ty {
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
            Self::Ts { b, allow_null } => match v {
                Value::Integer(i) => b.append_value(i),
                Value::Null if *allow_null => b.append_null(),
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
            Self::Ts { b, .. } => Arc::new(b.finish()),
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
/// new Parquet file under `base_dir` — the one directory the application
/// configures, shared by every cold table. The per-table directory
/// `<base_dir>/<logical_table>/` and the catalog itself are created lazily
/// here, on the first compaction that actually writes; nothing else in the
/// system ever creates them. The file is named
/// `bucket-<start>-<end>-<seq>.parquet` internally. After the
/// rename, one transaction deletes the hot rows and inserts the catalog
/// row. Idempotent under every calling pattern; see module docs.
pub fn compact_bucket(
    conn: &Connection,
    spec: &BucketSpec<'_>,
    base_dir: &Path,
) -> Result<CompactOutcome> {
    silodb_catalog::ensure_catalog(conn)?;

    // Validate the hot table's shape before anything else — a bad
    // ts_column must be an error, not a silently empty bucket.
    let columns = hot_columns(conn, spec.hot_table)?;
    let ts_idx = silodb_schema::resolve_ts_index(&columns, spec.ts_column)
        .map_err(|reason| CompactError::BadTimestampColumn { reason })?;
    let ts_name = columns[ts_idx].name.clone();

    let rows_in_bucket: i64 = conn.query_row(
        &format!(
            "SELECT count(*) FROM {} WHERE {} >= ?1 AND {} < ?2",
            quote_ident(spec.hot_table),
            quote_ident(&ts_name),
            quote_ident(&ts_name),
        ),
        params![spec.bucket_start, spec.bucket_end],
        |r| r.get(0),
    )?;

    // Active files for this exact bucket (drives the no-op/already-done
    // logic). The file *name* sequence comes from `bucket_seq` — a count
    // over rows of any status, so a superseded file's name is never
    // reused (GC of the old row would otherwise delete the new file).
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

    let seq = silodb_catalog::bucket_seq(
        conn,
        spec.logical_table,
        spec.bucket_start,
        spec.bucket_end,
    )?;
    let table_dir = base_dir.join(spec.logical_table);
    std::fs::create_dir_all(&table_dir)?;
    let out_path = table_dir.join(format!(
        "bucket-{}-{}-{seq}.parquet",
        spec.bucket_start, spec.bucket_end,
    ));
    let out_str = out_path.display().to_string();

    let arrow_schema = Arc::new(silodb_schema::bucket_arrow_schema(&columns, ts_idx));

    let col_list = columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = conn.prepare(&format!(
        "SELECT {col_list} FROM {} WHERE {} >= ?1 AND {} < ?2 ORDER BY {}",
        quote_ident(spec.hot_table),
        quote_ident(&ts_name),
        quote_ident(&ts_name),
        quote_ident(&ts_name),
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
        .map(|(i, c)| ColBuf::new(c, i == ts_idx))
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
    File::open(&table_dir)?.sync_all()?;

    // The one transaction that makes it real: hot rows out, catalog row in.
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let txn: Result<()> = (|| {
        conn.execute(
            &format!(
                "DELETE FROM {} WHERE {} >= ?1 AND {} < ?2",
                quote_ident(spec.hot_table),
                quote_ident(&ts_name),
                quote_ident(&ts_name),
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

/// Outcome of a tier-promotion merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeOutcome {
    /// Children rewritten into one file, catalog updated in one
    /// transaction (children flipped to `superseded`, not yet unlinked —
    /// GC is the caller's step).
    Merged {
        children: usize,
        rows: usize,
        path: std::path::PathBuf,
    },
    /// Fewer than one strictly-finer active file inside the window.
    NothingToMerge,
}

/// Merge every active file lying strictly inside `[window_start,
/// window_end)` (each narrower than the window) into a single new file
/// covering the window, then — in one transaction — insert the new
/// catalog row and mark the children `superseded`. Child *files* are left
/// on disk for the caller to GC after commit.
///
/// This is the one write-path operation that reads Parquet — it copies
/// its own children (batch-wise, memory bounded by one batch) and never
/// touches the hot table. `compact_bucket`'s never-reads-Parquet
/// invariant is per-function and unaffected.
///
/// Idempotent like compaction: a crash between rename and commit leaves
/// an invisible file; a re-run recomputes the same children and sequence
/// and rewrites it identically.
pub fn merge_window(
    conn: &Connection,
    logical_table: &str,
    base_dir: &Path,
    window_start: i64,
    window_end: i64,
) -> Result<MergeOutcome> {
    silodb_catalog::ensure_catalog(conn)?;
    let children =
        silodb_catalog::entries_within(conn, logical_table, window_start, window_end)?;
    // Nothing to do when the window is empty, or already exactly one file
    // covering the whole window (that IS the tier's end state).
    let already_converged = children.len() == 1
        && children[0].range_start == window_start
        && children[0].range_end == window_end;
    if children.is_empty() || already_converged {
        return Ok(MergeOutcome::NothingToMerge);
    }
    for c in &children {
        if !Path::new(&c.path).is_file() {
            return Err(CompactError::MissingCompactedFile {
                path: c.path.clone(),
            });
        }
    }

    let seq = silodb_catalog::bucket_seq(conn, logical_table, window_start, window_end)?;
    let table_dir = base_dir.join(logical_table);
    std::fs::create_dir_all(&table_dir)?;
    let out_path = table_dir.join(format!(
        "bucket-{window_start}-{window_end}-{seq}.parquet"
    ));
    let tmp_path = {
        let mut p = out_path.as_os_str().to_owned();
        p.push(".tmp");
        std::path::PathBuf::from(p)
    };

    let write_result: Result<usize> = (|| {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let mut writer: Option<ArrowWriter<File>> = None;
        let mut schema: Option<std::sync::Arc<arrow::datatypes::Schema>> = None;
        let mut total_rows = 0usize;
        // Children are ordered by (range_start, path); non-overlapping
        // ranges concatenate in time order. Overlapping late-arrival
        // files may interleave out of order — harmless: row-group
        // statistics stay exact per group, which is all pruning needs.
        for child in &children {
            let file = File::open(&child.path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
            for batch in reader {
                let batch = batch?;
                let schema = schema.get_or_insert_with(|| batch.schema());
                if batch.schema() != *schema {
                    return Err(CompactError::Parquet(
                        parquet::errors::ParquetError::General(format!(
                            "merge children disagree on schema at '{}'",
                            child.path
                        )),
                    ));
                }
                let w = match writer.as_mut() {
                    Some(w) => w,
                    None => {
                        let props = WriterProperties::builder()
                            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
                            .build();
                        writer = Some(ArrowWriter::try_new(
                            File::create(&tmp_path)?,
                            schema.clone(),
                            Some(props),
                        )?);
                        writer.as_mut().unwrap()
                    }
                };
                total_rows += batch.num_rows();
                w.write(&batch)?;
            }
        }
        let Some(writer) = writer else {
            return Ok(0);
        };
        let file = writer.into_inner()?;
        file.sync_all()?;
        Ok(total_rows)
    })();

    let total_rows = match write_result {
        Ok(0) => {
            // All children empty (shouldn't happen — compaction never
            // writes empty files) — treat as nothing to do.
            let _ = std::fs::remove_file(&tmp_path);
            return Ok(MergeOutcome::NothingToMerge);
        }
        Ok(n) => n,
        Err(e) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
    };

    std::fs::rename(&tmp_path, &out_path)?;
    File::open(&table_dir)?.sync_all()?;

    conn.execute_batch("BEGIN IMMEDIATE")?;
    let txn: Result<()> = (|| {
        silodb_catalog::insert_entry(
            conn,
            &CatalogEntry {
                logical_table: logical_table.to_owned(),
                path: out_path.display().to_string(),
                range_start: window_start,
                range_end: window_end,
                row_count: Some(total_rows as i64),
                created_at: 0,
                status: "active".into(),
            },
        )?;
        for c in &children {
            silodb_catalog::supersede_entry(conn, logical_table, &c.path)?;
        }
        Ok(())
    })();
    match txn {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            Ok(MergeOutcome::Merged {
                children: children.len(),
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
