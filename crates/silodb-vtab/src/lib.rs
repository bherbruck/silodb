//! `silodb` SQLite virtual table: read-only queries over Parquet bucket
//! files, driven by the `_silodb_catalog` table in the same (hot) database.
//!
//! ```sql
//! -- one base directory shared by ALL cold tables; the vtab's own name is
//! -- the logical table (files live in cold/sensor_a/, managed entirely by
//! -- compaction — this side never creates or requires directories):
//! CREATE VIRTUAL TABLE sensor_a USING silodb('cold/');
//! -- every part is overridable, incl. an explicit column list for
//! -- databases that have no hot table to borrow the schema from:
//! CREATE VIRTUAL TABLE cold USING silodb('cold/', table=sensor_a,
//!     ts_column=ts, schema='ts INTEGER, value REAL, name TEXT');
//! SELECT * FROM sensor_a WHERE ts > ?1 AND ts < ?2;
//! ```
//!
//! One immutable Parquet file per compacted bucket. The catalog — not a
//! directory glob — decides which files exist: a Parquet file with no
//! catalog row (e.g. a compaction that crashed before its commit) is
//! invisible here, and its rows are still in the hot table.
//!
//! `xConnect` does **zero file I/O** (spec): columns come from the
//! `schema=` argument when given, otherwise from the hot table (one PRAGMA
//! against the same database — the authoritative schema, so nothing is
//! restated or drifts). Both routes feed
//! `silodb_schema::bucket_arrow_schema`, the same mapping compaction
//! writes files with. Connect works identically whether the base directory
//! has a thousand files, none, or doesn't exist yet.
//!
//! Pruning happens in two layers at `xFilter` time:
//! 1. **File level**: an indexed range query against `_silodb_catalog`
//!    drops whole files whose bucket range can't overlap the query's
//!    timestamp constraints. New bucket files become visible to the very
//!    next query — no DDL needed.
//! 2. **Row-group level** (unchanged from Phase 2): footer min/max
//!    statistics drop row groups within the surviving files. Footers are
//!    parsed once and cached per `(path, mtime, size)` — files are
//!    immutable, so a cache entry never goes stale.
//!
//! SQLite re-checks every constraint on returned rows (`omit` stays false),
//! so both layers only ever need to be conservative, never exact.

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ffi::{c_int, CStr, CString};
use std::fs::File;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt8Array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use rusqlite::ffi;
use rusqlite::types::{Null, ValueRef};
use rusqlite::vtab::{
    Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, Module, VTab, VTabConnection,
    VTabCursor, VTabKind,
};
use rusqlite::{Connection, Error, OptionalExtension, Result};

const MODULE_NAME: &CStr = c"silodb";

/// Register the `silodb` module on a connection.
pub fn load_module(conn: &Connection) -> Result<()> {
    const MODULE: Module<SiloTab> = Module::read_only_module();
    let aux: Option<()> = None;
    conn.create_module(MODULE_NAME, &MODULE, aux)
}

/// Pruning outcome of the most recent `xFilter` on this thread.
///
/// Diagnostic hook for tests and logging — the acceptance criteria for both
/// pruning layers are "reads fewer files / row groups", which needs
/// counters, not wall-clock timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScanStats {
    /// Active catalog entries for the logical table.
    pub total_files: usize,
    /// Files surviving the catalog range query.
    pub candidate_files: usize,
    /// Files actually opened for reading (≥ 1 row group survived).
    pub scanned_files: usize,
    /// Row groups across all candidate files.
    pub total_row_groups: usize,
    /// Row groups handed to the Parquet readers.
    pub scanned_row_groups: usize,
    /// Candidate files whose footer came from the `(path, mtime, size)`
    /// cache instead of being re-parsed.
    pub metadata_cache_hits: usize,
}

thread_local! {
    static LAST_SCAN: Cell<Option<ScanStats>> = const { Cell::new(None) };
}

/// Stats for the most recent silodb table scan started on this thread.
pub fn last_scan_stats() -> Option<ScanStats> {
    LAST_SCAN.with(Cell::get)
}

fn module_err(e: impl std::fmt::Display) -> Error {
    Error::ModuleError(format!("silodb: {e}"))
}

#[derive(Clone)]
struct CachedMeta {
    mtime: SystemTime,
    size: u64,
    meta: ArrowReaderMetadata,
}

/// An instance of the silodb virtual table: one logical cold table.
#[repr(C)]
pub struct SiloTab {
    /// Base class. Must be first.
    base: ffi::sqlite3_vtab,
    /// Raw handle of the (hot) database this vtab lives in; used to query
    /// `_silodb_catalog` from inside `filter`. Never closed through here.
    db: *mut ffi::sqlite3,
    /// Base directory shared by all cold tables. Convention only — file
    /// locations come from the catalog verbatim.
    base_dir: PathBuf,
    logical_table: String,
    /// Index of the timestamp column in `schema`, if it exists — drives
    /// catalog file-level range pruning.
    ts_col: Option<usize>,
    schema: SchemaRef,
    /// Footer metadata per file, keyed by path; entries validated against
    /// `(mtime, size)` on every use.
    meta_cache: RefCell<HashMap<PathBuf, CachedMeta>>,
}

impl SiloTab {
    /// Non-owning view of the hot database connection. The returned
    /// `Connection` must be dropped before the enclosing callback returns
    /// and must never be handed out beyond it.
    fn hot_db(&self) -> Result<Connection> {
        unsafe { Connection::from_handle(self.db) }
    }

    /// Footer metadata for one catalog file, from cache when `(mtime,
    /// size)` still match. Returns `(meta, was_cache_hit)`.
    fn file_meta(&self, path: &Path) -> Result<(ArrowReaderMetadata, bool)> {
        let stat = std::fs::metadata(path).map_err(|e| {
            module_err(format!(
                "catalog lists '{}' but it cannot be read: {e} \
                 (cold file missing or unreadable — possible data loss)",
                path.display()
            ))
        })?;
        let mtime = stat.modified().map_err(module_err)?;
        let size = stat.len();

        if let Some(hit) = self.meta_cache.borrow().get(path)
            && hit.mtime == mtime
            && hit.size == size
        {
            return Ok((hit.meta.clone(), true));
        }

        let file = File::open(path).map_err(module_err)?;
        let meta = ArrowReaderMetadata::load(&file, Default::default()).map_err(module_err)?;
        self.meta_cache.borrow_mut().insert(
            path.to_path_buf(),
            CachedMeta {
                mtime,
                size,
                meta: meta.clone(),
            },
        );
        Ok((meta, false))
    }
}

fn parse_str_arg(arg: &[u8]) -> Result<String> {
    let s = std::str::from_utf8(arg)
        .map_err(|_| module_err("argument is not UTF-8"))?
        .trim();
    let s = s
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .or_else(|| s.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
        .unwrap_or(s);
    Ok(s.to_owned())
}

struct TabArgs {
    base_dir: PathBuf,
    logical_table: Option<String>,
    /// Explicit bucket-axis override. `None` → type-driven discovery
    /// (`silodb_schema::resolve_ts_index` precedence).
    ts_column: Option<String>,
    hot_table: Option<String>,
    schema: Option<String>,
}

/// First argument: the base directory shared by every cold table (quoted).
/// Optional `key=value` arguments: `table=<logical table>` (default: the
/// virtual table's own name), `ts_column=<name>` (default: discover by
/// TIMESTAMP/DATETIME declared type, else an INTEGER `ts`),
/// `schema='name TYPE, ...'` (explicit column list; default: borrow the
/// hot table's), and `hot_table=<name>` (which table to borrow from;
/// default: the logical table name).
fn parse_args(args: &[&[u8]]) -> Result<TabArgs> {
    let [dir_arg, rest @ ..] = args else {
        return Err(module_err(
            "expected a base directory argument: USING silodb('cold/')",
        ));
    };
    let dir_str = parse_str_arg(dir_arg)?;
    if dir_str.is_empty() {
        return Err(module_err("empty directory path"));
    }

    let mut logical_table = None;
    let mut ts_column = None;
    let mut hot_table = None;
    let mut schema = None;
    for arg in rest {
        let s = parse_str_arg(arg)?;
        let (key, value) = s
            .split_once('=')
            .ok_or_else(|| module_err(format!("unrecognized argument '{s}'")))?;
        let value = value.trim().trim_matches('\'').trim_matches('"');
        match key.trim() {
            "table" => logical_table = Some(value.to_owned()),
            "ts_column" => ts_column = Some(value.to_owned()),
            "hot_table" => hot_table = Some(value.to_owned()),
            "schema" => schema = Some(value.to_owned()),
            other => return Err(module_err(format!("unrecognized parameter '{other}'"))),
        }
    }

    Ok(TabArgs {
        base_dir: PathBuf::from(dir_str),
        logical_table,
        ts_column,
        hot_table,
        schema,
    })
}

/// Parse a `schema='name TYPE, ...'` argument into the shared bucket
/// schema plus the resolved bucket-axis index. Declared types go through
/// the same `silodb-schema` affinity rules the hot-table route uses;
/// TIMESTAMP/DATETIME columns are discovered by type when `ts_column=`
/// isn't given.
fn schema_from_arg(arg: &str, ts_column: Option<&str>) -> Result<(SchemaRef, usize)> {
    let cols = arg
        .split(',')
        .map(|part| {
            let part = part.trim();
            let (name, decl) = part
                .split_once(char::is_whitespace)
                .unwrap_or((part, ""));
            let name = name.trim_matches('"').trim_matches('`');
            if name.is_empty() {
                return Err(module_err(format!("bad schema column '{part}'")));
            }
            silodb_schema::ColumnDecl::parse(name, decl.trim()).ok_or_else(|| {
                module_err(format!(
                    "schema column '{name}' has unsupported declared type '{}'",
                    decl.trim()
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let ts_idx = silodb_schema::resolve_ts_index(&cols, ts_column)
        .map_err(|e| module_err(format!("schema argument: {e}")))?;
    Ok((
        std::sync::Arc::new(silodb_schema::bucket_arrow_schema(&cols, ts_idx)),
        ts_idx,
    ))
}

/// Declared schema borrowed from the hot table — the authoritative source,
/// mapped exactly the way `compact_bucket` maps it when writing files, so
/// the vtab's columns can't drift from the cold files.
fn schema_from_hot_table(
    hot: &Connection,
    hot_table: &str,
    ts_column: Option<&str>,
) -> Result<Option<(SchemaRef, usize)>> {
    // Only a real (non-virtual) table can be a schema source. This also
    // protects against recursion: with no table= argument the default
    // logical/hot table name is the vtab's own name, and running PRAGMA
    // table_info on the vtab mid-construction would re-enter xCreate.
    let is_real_table: Option<i64> = hot
        .query_row(
            "SELECT 1 FROM sqlite_master
             WHERE type = 'table' AND name = ?1 AND sql NOT LIKE 'CREATE VIRTUAL%'",
            [hot_table],
            |r| r.get(0),
        )
        .optional()
        .map_err(module_err)?;
    if is_real_table.is_none() {
        return Ok(None);
    }
    let mut stmt = hot
        .prepare(&format!(
            "PRAGMA table_info(\"{}\")",
            hot_table.replace('"', "\"\"")
        ))
        .map_err(module_err)?;
    let cols = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })
        .map_err(module_err)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(module_err)?;
    if cols.is_empty() {
        return Ok(None); // no such hot table
    }
    let mapped = cols
        .into_iter()
        .map(|(name, decl)| {
            silodb_schema::ColumnDecl::parse(&name, &decl).ok_or_else(|| {
                module_err(format!(
                    "hot table '{hot_table}' column '{name}' has unsupported \
                     declared type '{decl}'"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let ts_idx = silodb_schema::resolve_ts_index(&mapped, ts_column)
        .map_err(|e| module_err(format!("hot table '{hot_table}': {e}")))?;
    Ok(Some((
        std::sync::Arc::new(silodb_schema::bucket_arrow_schema(&mapped, ts_idx)),
        ts_idx,
    )))
}

unsafe impl<'vtab> VTab<'vtab> for SiloTab {
    type Aux = ();
    type Cursor = SiloCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        table_name: &[u8],
        args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let parsed = parse_args(args)?;
        // No existence check on base_dir: nothing on disk is required (or
        // even consulted) to connect. Compaction creates <dir>/<table>
        // lazily on its first run for a table.
        let logical_table = match parsed.logical_table {
            Some(t) => t,
            None => std::str::from_utf8(table_name)
                .map_err(|_| module_err("table name is not UTF-8"))?
                .to_owned(),
        };

        let handle = unsafe { db.handle() };

        // Column declaration — zero file I/O, by design (see docs/spec.md). Either an
        // explicit schema= argument, or one PRAGMA against the hot table in
        // this same database (the authoritative schema, nothing restated).
        let ts_arg = parsed.ts_column.as_deref();
        let (schema, ts_idx) = match &parsed.schema {
            Some(arg) => schema_from_arg(arg, ts_arg)?,
            None => {
                let hot = unsafe { Connection::from_handle(handle) }?;
                let hot_table = parsed.hot_table.as_deref().unwrap_or(&logical_table);
                schema_from_hot_table(&hot, hot_table, ts_arg)?.ok_or_else(|| {
                    module_err(format!(
                        "no schema source for '{logical_table}': there is no real \
                         table named '{hot_table}' to borrow columns from — pass \
                         hot_table=<name> or an explicit schema='col TYPE, ...'"
                    ))
                })?
            }
        };
        let ts_col = Some(ts_idx);
        let sql = silodb_schema::create_table_sql(&schema).map_err(module_err)?;
        // No VTabConfig trust flag on purpose. DirectOnly would forbid the
        // intended `hot UNION ALL cold` view pattern; Innocuous would
        // overclaim for a module that reads files off disk. The default
        // (usable in views under SQLite's default trusted-schema mode) is
        // the honest middle.

        let vtab = Self {
            base: ffi::sqlite3_vtab::default(),
            db: handle,
            base_dir: parsed.base_dir,
            logical_table,
            ts_col,
            schema,
            meta_cache: RefCell::new(HashMap::new()),
        };
        Ok((Cow::Owned(CString::new(sql)?), vtab))
    }

    /// Offer to consume EQ/GT/GE/LT/LE constraints on prunable columns.
    /// The (column, op) list is encoded into `idx_str`; the constraint
    /// values arrive positionally in `filter`'s args.
    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        let mut idx_str = String::new();
        let mut n_args = 0;
        for (constraint, mut usage) in info.constraints_and_usages() {
            if !constraint.is_usable() {
                continue;
            }
            let col = constraint.column();
            if col < 0 || prunable_class(self.schema.field(col as usize).data_type()).is_none() {
                continue;
            }
            let op = match constraint.operator() {
                IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ => 'E',
                IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_GT => 'G',
                IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_GE => 'g',
                IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_LT => 'L',
                IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_LE => 'l',
                _ => continue,
            };
            n_args += 1;
            usage.set_argv_index(n_args);
            // omit stays false: SQLite re-tests the constraint on each row,
            // so pruning can't cause wrong results, only wasted I/O.
            if !idx_str.is_empty() {
                idx_str.push(';');
            }
            idx_str.push(op);
            idx_str.push_str(&col.to_string());
        }

        if n_args > 0 {
            info.set_idx_str(&idx_str);
            info.set_estimated_cost(100_000.0);
        } else {
            info.set_estimated_cost(1_000_000.0);
        }
        Ok(true)
    }

    fn open(&'vtab mut self) -> Result<SiloCursor<'vtab>> {
        Ok(SiloCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            files: Vec::new(),
            next_file: 0,
            reader: None,
            batch: None,
            row_in_batch: 0,
            rowid: 0,
            phantom: PhantomData,
        })
    }
}

impl CreateVTab<'_> for SiloTab {
    const KIND: VTabKind = VTabKind::Default;
}

/// Column classes pruning understands. Unsigned ints are left out: their
/// Parquet statistics involve sign-reinterpretation subtleties that aren't
/// worth handling for a filter pattern we don't have.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PruneClass {
    Int,
    Real,
}

fn prunable_class(dt: &DataType) -> Option<PruneClass> {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64 => Some(PruneClass::Int),
        DataType::Float32 | DataType::Float64 => Some(PruneClass::Real),
        _ => None,
    }
}

/// One pushed-down constraint, decoded from `idx_str` + filter args.
struct Pushed {
    col: usize,
    op: char,
    value: PushedValue,
}

enum PushedValue {
    Int(i64),
    Real(f64),
}

fn decode_pushed(idx_str: &str, args: &Filters<'_>) -> Result<Vec<Pushed>> {
    let mut out = Vec::new();
    for (spec, value) in idx_str.split(';').zip(args.iter()) {
        let mut chars = spec.chars();
        let op = chars
            .next()
            .ok_or_else(|| module_err("corrupt idx_str"))?;
        let col: usize = chars
            .as_str()
            .parse()
            .map_err(|_| module_err("corrupt idx_str column"))?;
        // Non-numeric RHS (e.g. a TEXT bind against the column) can't be
        // used for stats pruning; skip it — SQLite still applies the test.
        let value = match value {
            ValueRef::Integer(i) => PushedValue::Int(i),
            ValueRef::Real(f) => PushedValue::Real(f),
            _ => continue,
        };
        out.push(Pushed { col, op, value });
    }
    Ok(out)
}

/// Inclusive [lo, hi] bounds on the timestamp column implied by the pushed
/// constraints, for the catalog's file-level range query. Bounds stay
/// conservative: GT uses the value itself (not value+1), Real-typed values
/// on the integer ts column contribute nothing.
fn ts_bounds(pushed: &[Pushed], ts_col: usize) -> (i64, i64) {
    let mut lo = i64::MIN;
    let mut hi = i64::MAX;
    for p in pushed {
        if p.col != ts_col {
            continue;
        }
        let PushedValue::Int(v) = p.value else {
            continue;
        };
        match p.op {
            'E' => {
                lo = lo.max(v);
                hi = hi.min(v);
            }
            'G' | 'g' => lo = lo.max(v),
            'L' | 'l' => hi = hi.min(v),
            _ => {}
        }
    }
    (lo, hi)
}

/// Extract a row group's (min, max) for a column, in the i64 domain.
fn int_min_max(rg: &RowGroupMetaData, col: usize) -> Option<(i64, i64)> {
    match rg.column(col).statistics()? {
        Statistics::Int64(s) => Some((*s.min_opt()?, *s.max_opt()?)),
        Statistics::Int32(s) => Some((i64::from(*s.min_opt()?), i64::from(*s.max_opt()?))),
        _ => None,
    }
}

/// Extract a row group's (min, max) for a column, in the f64 domain.
fn real_min_max(rg: &RowGroupMetaData, col: usize) -> Option<(f64, f64)> {
    match rg.column(col).statistics()? {
        Statistics::Double(s) => Some((*s.min_opt()?, *s.max_opt()?)),
        Statistics::Float(s) => Some((f64::from(*s.min_opt()?), f64::from(*s.max_opt()?))),
        _ => None,
    }
}

/// Can any value in [min, max] satisfy `x <op> v`? NULLs never satisfy these
/// operators, so ignoring them (as Parquet min/max stats do) is sound.
fn range_may_match<T: PartialOrd>(min: T, max: T, op: char, v: T) -> bool {
    match op {
        'E' => min <= v && v <= max,
        'G' => max > v,
        'g' => max >= v,
        'L' => min < v,
        'l' => min <= v,
        _ => true,
    }
}

/// A row group survives unless some constraint provably excludes it.
/// Missing statistics, unsupported stats layout, or a cross-domain
/// comparison we can't do exactly → keep the group.
fn row_group_may_match(rg: &RowGroupMetaData, schema: &SchemaRef, pushed: &[Pushed]) -> bool {
    pushed.iter().all(|p| {
        let class = match prunable_class(schema.field(p.col).data_type()) {
            Some(c) => c,
            None => return true,
        };
        match (class, &p.value) {
            (PruneClass::Int, PushedValue::Int(v)) => int_min_max(rg, p.col)
                .is_none_or(|(min, max)| range_may_match(min, max, p.op, *v)),
            (PruneClass::Real, PushedValue::Real(v)) => real_min_max(rg, p.col)
                .is_none_or(|(min, max)| range_may_match(min, max, p.op, *v)),
            (PruneClass::Real, PushedValue::Int(v)) => real_min_max(rg, p.col)
                .is_none_or(|(min, max)| range_may_match(min, max, p.op, *v as f64)),
            // Real-valued constraint against an INTEGER column: i64→f64 is
            // lossy above 2^53 (e.g. nanosecond timestamps), so don't prune.
            (PruneClass::Int, PushedValue::Real(_)) => true,
        }
    })
}

/// One file the cursor will read: pre-pruned row groups, footer already
/// parsed.
struct ScanFile {
    path: PathBuf,
    meta: ArrowReaderMetadata,
    row_groups: Vec<usize>,
}

/// Cursor over the candidate files' record batches. `batch == None` with no
/// files left means EOF.
#[repr(C)]
pub struct SiloCursor<'vtab> {
    /// Base class. Must be first.
    base: ffi::sqlite3_vtab_cursor,
    files: Vec<ScanFile>,
    next_file: usize,
    reader: Option<ParquetRecordBatchReader>,
    batch: Option<RecordBatch>,
    row_in_batch: usize,
    rowid: i64,
    phantom: PhantomData<&'vtab SiloTab>,
}

impl SiloCursor<'_> {
    fn vtab(&self) -> &SiloTab {
        unsafe { &*(self.base.pVtab as *const SiloTab) }
    }

    /// Pull batches — moving through files as needed — until one has rows
    /// or everything is exhausted.
    fn advance_batch(&mut self) -> Result<()> {
        self.batch = None;
        self.row_in_batch = 0;
        loop {
            if let Some(reader) = self.reader.as_mut() {
                for batch in reader {
                    let batch = batch.map_err(module_err)?;
                    if batch.num_rows() > 0 {
                        self.batch = Some(batch);
                        return Ok(());
                    }
                }
                self.reader = None;
            }
            let Some(next) = self.files.get(self.next_file) else {
                return Ok(());
            };
            self.next_file += 1;
            let file = File::open(&next.path).map_err(module_err)?;
            let reader =
                ParquetRecordBatchReaderBuilder::new_with_metadata(file, next.meta.clone())
                    .with_row_groups(next.row_groups.clone())
                    .build()
                    .map_err(module_err)?;
            self.reader = Some(reader);
        }
    }
}

unsafe impl VTabCursor for SiloCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        idx_str: Option<&str>,
        args: &Filters<'_>,
    ) -> Result<()> {
        let vtab = self.vtab();
        let pushed = match idx_str {
            Some(s) if !s.is_empty() => decode_pushed(s, args)?,
            _ => Vec::new(),
        };

        // Layer 1: catalog range query — whole-file pruning, and the point
        // where files compacted after CREATE VIRTUAL TABLE become visible.
        // No catalog table yet (nothing ever compacted) → empty cold table.
        let hot = vtab.hot_db()?;
        let (total_files, candidates) = if silodb_catalog::catalog_exists(&hot)? {
            let total = silodb_catalog::entries_for_table(&hot, &vtab.logical_table)?.len();
            let (lo, hi) = match vtab.ts_col {
                Some(ts) => ts_bounds(&pushed, ts),
                None => (i64::MIN, i64::MAX),
            };
            (
                total,
                silodb_catalog::entries_overlapping(&hot, &vtab.logical_table, lo, hi)?,
            )
        } else {
            (0, Vec::new())
        };
        drop(hot);

        // Layer 2: row-group pruning within each candidate (Phase 2 logic).
        let mut stats = ScanStats {
            total_files,
            candidate_files: candidates.len(),
            ..Default::default()
        };
        let mut files = Vec::new();
        for entry in &candidates {
            let path = PathBuf::from(&entry.path);
            let (meta, cache_hit) = vtab.file_meta(&path)?;
            stats.metadata_cache_hits += usize::from(cache_hit);

            // Column names and order must line up with the declaration —
            // xColumn maps by position. Arrow types may differ (hand-built
            // files, older bucket layouts): cell conversion is driven by
            // each file's own types, so that's fine.
            let file_fields = meta.schema().fields();
            let decl_fields = vtab.schema.fields();
            if file_fields.len() != decl_fields.len()
                || file_fields
                    .iter()
                    .zip(decl_fields.iter())
                    .any(|(f, d)| f.name() != d.name())
            {
                return Err(module_err(format!(
                    "'{}' has different columns than this table declares \
                     (file: [{}], declared: [{}])",
                    entry.path,
                    file_fields
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    decl_fields
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                )));
            }

            let total = meta.metadata().num_row_groups();
            stats.total_row_groups += total;
            let keep: Vec<usize> = (0..total)
                .filter(|&i| {
                    pushed.is_empty()
                        || row_group_may_match(meta.metadata().row_group(i), &vtab.schema, &pushed)
                })
                .collect();
            stats.scanned_row_groups += keep.len();
            if !keep.is_empty() {
                stats.scanned_files += 1;
                files.push(ScanFile {
                    path,
                    meta,
                    row_groups: keep,
                });
            }
        }
        LAST_SCAN.with(|c| c.set(Some(stats)));

        self.files = files;
        self.next_file = 0;
        self.reader = None;
        self.rowid = 0;
        self.advance_batch()
    }

    fn next(&mut self) -> Result<()> {
        self.rowid += 1;
        self.row_in_batch += 1;
        let in_batch = self.batch.as_ref().map_or(0, RecordBatch::num_rows);
        if self.row_in_batch >= in_batch {
            self.advance_batch()?;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.batch.is_none()
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        let batch = self
            .batch
            .as_ref()
            .ok_or_else(|| module_err("column() called at EOF"))?;
        let array = batch.column(i as usize);
        set_result_from_array(ctx, array.as_ref(), self.row_in_batch)
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}

/// Convert one Arrow array cell to a SQLite result value, per the mapping
/// in `silodb-schema` (timestamps/dates as raw INTEGER in their own unit,
/// booleans as 0/1).
fn set_result_from_array(ctx: &mut Context, array: &dyn Array, row: usize) -> Result<()> {
    if array.is_null(row) {
        return ctx.set_result(&Null);
    }

    fn down<T: 'static>(array: &dyn Array) -> Result<&T> {
        array
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| module_err("array downcast mismatch"))
    }

    match array.data_type() {
        DataType::Boolean => ctx.set_result(&(down::<BooleanArray>(array)?.value(row) as i64)),
        DataType::Int8 => ctx.set_result(&(down::<Int8Array>(array)?.value(row) as i64)),
        DataType::Int16 => ctx.set_result(&(down::<Int16Array>(array)?.value(row) as i64)),
        DataType::Int32 => ctx.set_result(&(down::<Int32Array>(array)?.value(row) as i64)),
        DataType::Int64 => ctx.set_result(&down::<Int64Array>(array)?.value(row)),
        DataType::UInt8 => ctx.set_result(&(down::<UInt8Array>(array)?.value(row) as i64)),
        DataType::UInt16 => ctx.set_result(&(down::<UInt16Array>(array)?.value(row) as i64)),
        DataType::UInt32 => ctx.set_result(&(down::<UInt32Array>(array)?.value(row) as i64)),
        DataType::Timestamp(unit, _) => {
            let v = match unit {
                TimeUnit::Second => down::<TimestampSecondArray>(array)?.value(row),
                TimeUnit::Millisecond => down::<TimestampMillisecondArray>(array)?.value(row),
                TimeUnit::Microsecond => down::<TimestampMicrosecondArray>(array)?.value(row),
                TimeUnit::Nanosecond => down::<TimestampNanosecondArray>(array)?.value(row),
            };
            ctx.set_result(&v)
        }
        DataType::Date32 => ctx.set_result(&(down::<Date32Array>(array)?.value(row) as i64)),
        DataType::Date64 => ctx.set_result(&down::<Date64Array>(array)?.value(row)),
        DataType::Float32 => ctx.set_result(&(down::<Float32Array>(array)?.value(row) as f64)),
        DataType::Float64 => ctx.set_result(&down::<Float64Array>(array)?.value(row)),
        DataType::Utf8 => ctx.set_result(&down::<StringArray>(array)?.value(row)),
        DataType::LargeUtf8 => ctx.set_result(&down::<LargeStringArray>(array)?.value(row)),
        DataType::Binary => ctx.set_result(&down::<BinaryArray>(array)?.value(row)),
        DataType::LargeBinary => ctx.set_result(&down::<LargeBinaryArray>(array)?.value(row)),
        DataType::FixedSizeBinary(_) => {
            ctx.set_result(&down::<FixedSizeBinaryArray>(array)?.value(row))
        }
        other => Err(module_err(format!("unsupported column type {other}"))),
    }
}
