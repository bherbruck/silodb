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

    // Registered continuous aggregates get their deltas computed from this
    // same stream and committed in this compaction's transaction; per-file
    // series statistics are always-on.
    let plans = rollup_plans(conn, spec.logical_table, &columns, ts_idx)?;
    let mut accs: Vec<RollupAcc<'_>> = plans.iter().map(RollupAcc::new).collect();
    let stats_plan = FileStatsPlan::new(spec.logical_table, &columns, ts_idx);
    let mut stats_acc = FileStatsAcc::new(&stats_plan);
    let mut row_vals: Vec<Value> = Vec::with_capacity(columns.len());

    let write_result: Result<usize> = (|| {
        let mut rows = stmt.query(params![spec.bucket_start, spec.bucket_end])?;
        loop {
            let row = rows.next()?;
            if let Some(row) = row {
                row_vals.clear();
                for i in 0..columns.len() {
                    row_vals.push(row.get::<_, Value>(i)?);
                }
                stats_acc.add_row(&row_vals);
                for acc in &mut accs {
                    acc.add_row(&row_vals)?;
                }
                for (buf, (col, v)) in
                    bufs.iter_mut().zip(columns.iter().zip(row_vals.drain(..)))
                {
                    buf.push(&col.name, v)?;
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
        // Rollup deltas + file stats commit with the migration: exact by
        // construction, no invalidation machinery — every row enters cold
        // exactly once.
        for acc in accs {
            acc.flush(conn)?;
        }
        stats_acc.flush(conn, &out_str)?;
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

    // Stats plan from the first child's footer (all children share the
    // writer's schema; mismatches error below anyway).
    let stats_plan = {
        use parquet::arrow::arrow_reader::ArrowReaderMetadata;
        let f = File::open(&children[0].path)?;
        let meta = ArrowReaderMetadata::load(&f, Default::default())?;
        let (decls, ts) = decls_from_arrow(meta.schema());
        FileStatsPlan::new(logical_table, &decls, ts)
    };
    let mut stats_acc = FileStatsAcc::new(&stats_plan);

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
                stats_acc.add_batch(&batch);
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
        // The merged file's stats replace its children's, atomically.
        let child_paths: Vec<String> = children.iter().map(|c| c.path.clone()).collect();
        delete_stats_for_paths(conn, logical_table, &child_paths)?;
        stats_acc.flush(conn, &out_path.display().to_string())?;
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

// --- continuous-aggregate rollups ----------------------------------------

/// Sufficient statistics for one REAL column in one (bucket, series) cell.
/// avg/stddev are derived at query time (sum/count etc.) — materializing
/// them would make re-aggregation inexact (avg-of-avg).
#[derive(Clone, Copy)]
struct Aggs {
    count: i64,
    sum: f64,
    sumsq: f64,
    min: f64,
    max: f64,
}

impl Aggs {
    fn new() -> Self {
        Aggs {
            count: 0,
            sum: 0.0,
            sumsq: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    fn add(&mut self, v: f64) {
        self.count += 1;
        self.sum += v;
        self.sumsq += v * v;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
    }
}

/// Hashable series-identity key (group columns are never REAL, so no f64).
#[derive(Clone, PartialEq, Eq, Hash)]
enum KeyVal {
    Null,
    Int(i64),
    Text(String),
    Blob(Vec<u8>),
}

/// Everything needed to compute and store one rollup's deltas: which
/// column is the axis, which are series identity, which get aggregated,
/// and the INSERT statement for the rollup table.
pub struct RollupPlan {
    pub spec: silodb_catalog::RollupSpec,
    pub origin_us: i64,
    ts_idx: usize,
    group_idxs: Vec<usize>,
    agg_idxs: Vec<usize>,
    insert_sql: String,
}

impl RollupPlan {
    /// Column classification: axis = `ts_idx`; aggregated = REAL columns;
    /// series identity = everything else (INTEGER/TEXT/BLOB, including
    /// secondary timestamp columns).
    pub fn new(
        spec: silodb_catalog::RollupSpec,
        columns: &[silodb_schema::ColumnDecl],
        ts_idx: usize,
        origin_us: i64,
    ) -> Self {
        let mut group_idxs = Vec::new();
        let mut agg_idxs = Vec::new();
        for (i, c) in columns.iter().enumerate() {
            if i == ts_idx {
                continue;
            } else if c.ty == SqliteType::Real {
                agg_idxs.push(i);
            } else {
                group_idxs.push(i);
            }
        }
        let n_params = 1 + group_idxs.len() + agg_idxs.len() * 5;
        let placeholders = (1..=n_params)
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql = format!(
            "INSERT INTO {} VALUES ({placeholders})",
            quote_ident(&spec.rollup_table)
        );
        RollupPlan {
            spec,
            origin_us,
            ts_idx,
            group_idxs,
            agg_idxs,
            insert_sql,
        }
    }

    /// DDL for a fresh (plain-table) rollup target. `ts` keeps the
    /// TIMESTAMP decl so the rollup table can itself be tiered
    /// (recursion); group columns use their storage-class decls.
    pub fn rollup_ddl(&self, columns: &[silodb_schema::ColumnDecl]) -> String {
        let mut cols = vec!["ts TIMESTAMP".to_owned()];
        for &i in &self.group_idxs {
            cols.push(format!(
                "{} {}",
                quote_ident(&columns[i].name),
                columns[i].ty.decl()
            ));
        }
        for &i in &self.agg_idxs {
            let n = &columns[i].name;
            for (suffix, ty) in [
                ("count", "INTEGER"),
                ("sum", "REAL"),
                ("sumsq", "REAL"),
                ("min", "REAL"),
                ("max", "REAL"),
            ] {
                cols.push(format!("{} {ty}", quote_ident(&format!("{n}_{suffix}"))));
            }
        }
        format!(
            "CREATE TABLE {} ({})",
            quote_ident(&self.spec.rollup_table),
            cols.join(", ")
        )
    }
}

/// Delta accumulator for one plan over one stream of rows.
pub struct RollupAcc<'a> {
    plan: &'a RollupPlan,
    cells: std::collections::HashMap<(i64, Vec<KeyVal>), Vec<Aggs>>,
}

impl<'a> RollupAcc<'a> {
    pub fn new(plan: &'a RollupPlan) -> Self {
        RollupAcc {
            plan,
            cells: std::collections::HashMap::new(),
        }
    }

    fn cell(&mut self, ts: i64, key: Vec<KeyVal>) -> Option<&mut Vec<Aggs>> {
        let bucket =
            silodb_schema::bucket_floor(self.plan.spec.grain_us, ts, self.plan.origin_us)?;
        Some(
            self.cells
                .entry((bucket, key))
                .or_insert_with(|| vec![Aggs::new(); self.plan.agg_idxs.len()]),
        )
    }

    /// Accumulate one hot-table row (compaction path).
    pub fn add_row(&mut self, values: &[Value]) -> Result<()> {
        let Value::Integer(ts) = values[self.plan.ts_idx] else {
            return Ok(()); // axis rows are validated upstream
        };
        let key: Vec<KeyVal> = self
            .plan
            .group_idxs
            .iter()
            .map(|&i| match &values[i] {
                Value::Null => KeyVal::Null,
                Value::Integer(v) => KeyVal::Int(*v),
                Value::Text(s) => KeyVal::Text(s.clone()),
                Value::Blob(b) => KeyVal::Blob(b.clone()),
                Value::Real(f) => KeyVal::Int(f.to_bits() as i64), // unreachable: Real cols aggregate
            })
            .collect();
        let agg_idxs = self.plan.agg_idxs.clone();
        let Some(aggs) = self.cell(ts, key) else {
            return Ok(());
        };
        for (slot, &i) in agg_idxs.iter().enumerate() {
            match values[i] {
                Value::Real(v) => aggs[slot].add(v),
                Value::Integer(v) => aggs[slot].add(v as f64),
                _ => {} // NULLs don't count (SQL aggregate semantics)
            }
        }
        Ok(())
    }

    /// Accumulate a whole Arrow batch (backfill path, reading files this
    /// crate wrote — types follow `bucket_arrow_schema`).
    pub fn add_batch(&mut self, batch: &arrow::array::RecordBatch) -> Result<()> {
        use arrow::array::{Array, AsArray};
        use arrow::datatypes::{Float64Type, Int64Type, TimestampMicrosecondType};

        let ts_col = batch
            .column(self.plan.ts_idx)
            .as_primitive::<TimestampMicrosecondType>();
        for row in 0..batch.num_rows() {
            let key: Vec<KeyVal> = self
                .plan
                .group_idxs
                .iter()
                .map(|&i| {
                    let col = batch.column(i);
                    if col.is_null(row) {
                        return KeyVal::Null;
                    }
                    match col.data_type() {
                        arrow::datatypes::DataType::Int64 => {
                            KeyVal::Int(col.as_primitive::<Int64Type>().value(row))
                        }
                        arrow::datatypes::DataType::Timestamp(_, _) => KeyVal::Int(
                            col.as_primitive::<TimestampMicrosecondType>().value(row),
                        ),
                        arrow::datatypes::DataType::Utf8 => {
                            KeyVal::Text(col.as_string::<i32>().value(row).to_owned())
                        }
                        arrow::datatypes::DataType::Binary => {
                            KeyVal::Blob(col.as_binary::<i32>().value(row).to_vec())
                        }
                        _ => KeyVal::Null,
                    }
                })
                .collect();
            let agg_idxs = self.plan.agg_idxs.clone();
            let Some(aggs) = self.cell(ts_col.value(row), key) else {
                continue;
            };
            for (slot, &i) in agg_idxs.iter().enumerate() {
                let col = batch.column(i).as_primitive::<Float64Type>();
                if !col.is_null(row) {
                    aggs[slot].add(col.value(row));
                }
            }
        }
        Ok(())
    }

    /// INSERT the accumulated deltas. Runs in the caller's ambient
    /// transaction — compaction calls this inside its delete+catalog txn.
    pub fn flush(self, conn: &Connection) -> Result<usize> {
        let mut stmt = conn.prepare(&self.plan.insert_sql)?;
        let n = self.cells.len();
        for ((bucket, key), aggs) in self.cells {
            let mut params: Vec<Value> = Vec::with_capacity(1 + key.len() + aggs.len() * 5);
            params.push(Value::Integer(bucket));
            for k in key {
                params.push(match k {
                    KeyVal::Null => Value::Null,
                    KeyVal::Int(v) => Value::Integer(v),
                    KeyVal::Text(s) => Value::Text(s),
                    KeyVal::Blob(b) => Value::Blob(b),
                });
            }
            for a in aggs {
                params.push(Value::Integer(a.count));
                params.push(Value::Real(a.sum));
                params.push(Value::Real(a.sumsq));
                if a.count == 0 {
                    params.push(Value::Null);
                    params.push(Value::Null);
                } else {
                    params.push(Value::Real(a.min));
                    params.push(Value::Real(a.max));
                }
            }
            stmt.execute(rusqlite::params_from_iter(params))?;
        }
        Ok(n)
    }
}

/// Load the rollup plans registered for a table (empty when none).
pub fn rollup_plans(
    conn: &Connection,
    logical_table: &str,
    columns: &[silodb_schema::ColumnDecl],
    ts_idx: usize,
) -> Result<Vec<RollupPlan>> {
    let specs = silodb_catalog::rollups_for_table(conn, logical_table)?;
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    let origin = silodb_catalog::get_policy(conn, logical_table)?
        .map(|p| p.origin_us)
        .unwrap_or(0);
    Ok(specs
        .into_iter()
        .map(|s| RollupPlan::new(s, columns, ts_idx, origin))
        .collect())
}

/// Backfill one plan from existing cold files, accumulating across all of
/// them and flushing once. Caller wraps this (plus the registry insert) in
/// one transaction so a crash leaves no half-registered rollup.
pub fn rollup_backfill(
    conn: &Connection,
    plan: &RollupPlan,
    entries: &[silodb_catalog::CatalogEntry],
) -> Result<usize> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut acc = RollupAcc::new(plan);
    for e in entries {
        let file = File::open(&e.path).map_err(|_| CompactError::MissingCompactedFile {
            path: e.path.clone(),
        })?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        for batch in reader {
            acc.add_batch(&batch?)?;
        }
    }
    acc.flush(conn)
}

// --- per-(file, series) statistics -----------------------------------------

/// Always-on file statistics: one row per (cold file, series) in
/// `<logical_table>_stats`, holding count/sum/sumsq/min/max for every REAL
/// column. Computed for free from the compaction/merge streams, committed
/// in their transactions, and deleted when the file leaves `active`.
///
/// Two things they buy: series-aware file pruning in the vtab (a query
/// filtering on a series column skips files with no rows for it), and
/// free whole-file aggregates — an aggregate that fully covers a chunk is
/// one stats-row read, no parquet.
pub struct FileStatsPlan {
    pub stats_table: String,
    group_idxs: Vec<usize>,
    agg_idxs: Vec<usize>,
    ddl: String,
    insert_sql: String,
}

/// Name of the stats table for a logical table.
pub fn stats_table_name(logical_table: &str) -> String {
    format!("{logical_table}_stats")
}

/// Reconstruct the column classification from an Arrow schema of a file we
/// wrote: the bucket axis is the (unique) **non-nullable** Timestamp field
/// (`bucket_arrow_schema` guarantees that), Float64 fields aggregate,
/// everything else is series identity.
fn decls_from_arrow(
    schema: &arrow::datatypes::Schema,
) -> (Vec<silodb_schema::ColumnDecl>, usize) {
    let mut ts_idx = 0;
    let decls: Vec<silodb_schema::ColumnDecl> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let is_axis = matches!(f.data_type(), arrow::datatypes::DataType::Timestamp(_, _))
                && !f.is_nullable();
            if is_axis {
                ts_idx = i;
            }
            let ty = silodb_schema::sqlite_type_for(f.data_type())
                .unwrap_or(silodb_schema::SqliteType::Blob);
            silodb_schema::ColumnDecl {
                name: f.name().clone(),
                ty,
                declared_timestamp: is_axis,
            }
        })
        .collect();
    (decls, ts_idx)
}

impl FileStatsPlan {
    pub fn new(
        logical_table: &str,
        columns: &[silodb_schema::ColumnDecl],
        ts_idx: usize,
    ) -> Self {
        let stats_table = stats_table_name(logical_table);
        let mut group_idxs = Vec::new();
        let mut agg_idxs = Vec::new();
        let mut cols = vec!["path TEXT NOT NULL".to_owned()];
        for (i, c) in columns.iter().enumerate() {
            if i == ts_idx {
                continue;
            } else if c.ty == SqliteType::Real {
                agg_idxs.push(i);
            } else {
                group_idxs.push(i);
                cols.push(format!("{} {}", quote_ident(&c.name), c.ty.decl()));
            }
        }
        for &i in &agg_idxs {
            let n = &columns[i].name;
            for (suffix, ty) in [
                ("count", "INTEGER"),
                ("sum", "REAL"),
                ("sumsq", "REAL"),
                ("min", "REAL"),
                ("max", "REAL"),
            ] {
                cols.push(format!("{} {ty}", quote_ident(&format!("{n}_{suffix}"))));
            }
        }
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {} ({});
             CREATE INDEX IF NOT EXISTS {} ON {} (path);",
            quote_ident(&stats_table),
            cols.join(", "),
            quote_ident(&format!("{stats_table}_path")),
            quote_ident(&stats_table),
        );
        let n_params = 1 + group_idxs.len() + agg_idxs.len() * 5;
        let placeholders = (1..=n_params)
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql = format!(
            "INSERT INTO {} VALUES ({placeholders})",
            quote_ident(&stats_table)
        );
        FileStatsPlan {
            stats_table,
            group_idxs,
            agg_idxs,
            ddl,
            insert_sql,
        }
    }
}

/// Accumulator for one file's stats (compaction: sqlite rows; merge and
/// backfill: arrow batches).
pub struct FileStatsAcc<'a> {
    plan: &'a FileStatsPlan,
    cells: std::collections::HashMap<Vec<KeyVal>, Vec<Aggs>>,
}

impl<'a> FileStatsAcc<'a> {
    pub fn new(plan: &'a FileStatsPlan) -> Self {
        FileStatsAcc {
            plan,
            cells: std::collections::HashMap::new(),
        }
    }

    pub fn add_row(&mut self, values: &[Value]) {
        let key: Vec<KeyVal> = self
            .plan
            .group_idxs
            .iter()
            .map(|&i| match &values[i] {
                Value::Null => KeyVal::Null,
                Value::Integer(v) => KeyVal::Int(*v),
                Value::Text(s) => KeyVal::Text(s.clone()),
                Value::Blob(b) => KeyVal::Blob(b.clone()),
                Value::Real(f) => KeyVal::Int(f.to_bits() as i64), // unreachable
            })
            .collect();
        let aggs = self
            .cells
            .entry(key)
            .or_insert_with(|| vec![Aggs::new(); self.plan.agg_idxs.len()]);
        for (slot, &i) in self.plan.agg_idxs.iter().enumerate() {
            match values[i] {
                Value::Real(v) => aggs[slot].add(v),
                Value::Integer(v) => aggs[slot].add(v as f64),
                _ => {}
            }
        }
    }

    pub fn add_batch(&mut self, batch: &arrow::array::RecordBatch) {
        use arrow::array::{Array, AsArray};
        use arrow::datatypes::{Float64Type, Int64Type, TimestampMicrosecondType};
        for row in 0..batch.num_rows() {
            let key: Vec<KeyVal> = self
                .plan
                .group_idxs
                .iter()
                .map(|&i| {
                    let col = batch.column(i);
                    if col.is_null(row) {
                        return KeyVal::Null;
                    }
                    match col.data_type() {
                        arrow::datatypes::DataType::Int64 => {
                            KeyVal::Int(col.as_primitive::<Int64Type>().value(row))
                        }
                        arrow::datatypes::DataType::Timestamp(_, _) => KeyVal::Int(
                            col.as_primitive::<TimestampMicrosecondType>().value(row),
                        ),
                        arrow::datatypes::DataType::Utf8 => {
                            KeyVal::Text(col.as_string::<i32>().value(row).to_owned())
                        }
                        arrow::datatypes::DataType::Binary => {
                            KeyVal::Blob(col.as_binary::<i32>().value(row).to_vec())
                        }
                        _ => KeyVal::Null,
                    }
                })
                .collect();
            let aggs = self
                .cells
                .entry(key)
                .or_insert_with(|| vec![Aggs::new(); self.plan.agg_idxs.len()]);
            for (slot, &i) in self.plan.agg_idxs.iter().enumerate() {
                let col = batch.column(i).as_primitive::<Float64Type>();
                if !col.is_null(row) {
                    aggs[slot].add(col.value(row));
                }
            }
        }
    }

    /// Ensure the stats table exists, clear any prior rows for `path`
    /// (idempotent re-runs), and insert. Caller's ambient transaction.
    pub fn flush(self, conn: &Connection, path: &str) -> Result<usize> {
        conn.execute_batch(&self.plan.ddl)?;
        conn.execute(
            &format!(
                "DELETE FROM {} WHERE path = ?1",
                quote_ident(&self.plan.stats_table)
            ),
            [path],
        )?;
        let mut stmt = conn.prepare(&self.plan.insert_sql)?;
        let n = self.cells.len();
        for (key, aggs) in self.cells {
            let mut params: Vec<Value> = Vec::with_capacity(1 + key.len() + aggs.len() * 5);
            params.push(Value::Text(path.to_owned()));
            for k in key {
                params.push(match k {
                    KeyVal::Null => Value::Null,
                    KeyVal::Int(v) => Value::Integer(v),
                    KeyVal::Text(s) => Value::Text(s),
                    KeyVal::Blob(b) => Value::Blob(b),
                });
            }
            for a in aggs {
                params.push(Value::Integer(a.count));
                params.push(Value::Real(a.sum));
                params.push(Value::Real(a.sumsq));
                if a.count == 0 {
                    params.push(Value::Null);
                    params.push(Value::Null);
                } else {
                    params.push(Value::Real(a.min));
                    params.push(Value::Real(a.max));
                }
            }
            stmt.execute(rusqlite::params_from_iter(params))?;
        }
        Ok(n)
    }
}

/// Delete stats rows for files that are no longer active. Safe if the
/// stats table doesn't exist yet.
pub fn delete_stats_for_paths(
    conn: &Connection,
    logical_table: &str,
    paths: &[String],
) -> Result<()> {
    let table = stats_table_name(logical_table);
    let exists: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name = ?1",
        [&table],
        |r| r.get(0),
    )?;
    if exists == 0 {
        return Ok(());
    }
    let mut stmt = conn.prepare(&format!(
        "DELETE FROM {} WHERE path = ?1",
        quote_ident(&table)
    ))?;
    for p in paths {
        stmt.execute([p])?;
    }
    Ok(())
}

/// Compute + store stats for existing files that predate the stats table
/// (upgrade path / self-heal). Returns how many files were backfilled.
pub fn stats_backfill_missing(
    conn: &Connection,
    logical_table: &str,
    hot_table: &str,
) -> Result<usize> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let columns = hot_columns(conn, hot_table)?;
    let ts_idx = silodb_schema::resolve_ts_index(&columns, None)
        .map_err(|reason| CompactError::BadTimestampColumn { reason })?;
    let plan = FileStatsPlan::new(logical_table, &columns, ts_idx);
    conn.execute_batch(&plan.ddl)?;

    let mut done = 0;
    for e in silodb_catalog::entries_for_table(conn, logical_table)? {
        let has: i64 = conn.query_row(
            &format!(
                "SELECT count(*) FROM {} WHERE path = ?1",
                quote_ident(&plan.stats_table)
            ),
            [&e.path],
            |r| r.get(0),
        )?;
        if has > 0 {
            continue;
        }
        let file = File::open(&e.path).map_err(|_| CompactError::MissingCompactedFile {
            path: e.path.clone(),
        })?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
        let mut acc = FileStatsAcc::new(&plan);
        for batch in reader {
            acc.add_batch(&batch?);
        }
        conn.execute_batch("BEGIN IMMEDIATE")?;
        match acc.flush(conn, &e.path) {
            Ok(_) => conn.execute_batch("COMMIT")?,
            Err(err) => {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(err);
            }
        }
        done += 1;
    }
    Ok(done)
}
