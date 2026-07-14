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
    Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, Inserts, Module, UpdateVTab,
    Updates, VTab, VTabConnection, VTabCursor, VTabKind,
};
use rusqlite::{Connection, Error, OptionalExtension, Result};

const MODULE_NAME: &CStr = c"silodb";

/// Register the `silodb` module on a connection.
pub fn load_module(conn: &Connection) -> Result<()> {
    const MODULE: Module<SiloTab> = Module::update_module();
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
    /// Candidate files skipped because the per-file series statistics
    /// prove they hold no rows for the queried series (EQ constraints on
    /// series columns).
    pub series_pruned_files: usize,
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
    /// Managed mode (`tiers=` in the DDL): the vtab owns this shadow hot
    /// table, routes INSERTs into it, and serves hot ∪ cold itself.
    shadow: Option<String>,
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
    /// Presence turns on managed mode: the vtab owns a `<name>_data`
    /// shadow table, accepts INSERTs, and serves hot ∪ cold itself.
    tiers: Option<String>,
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
    let mut tiers = None;
    let mut columns = Vec::new();
    for arg in rest {
        let s = parse_str_arg(arg)?;
        // `key=value` where the key is a bare word → a parameter.
        // Anything else (`device TEXT`) is a column definition, exactly
        // like FTS5 declares columns as bare arguments.
        let param = s
            .split_once('=')
            .filter(|(key, _)| !key.trim().is_empty() && !key.contains(char::is_whitespace));
        match param {
            Some((key, value)) => {
                let value = value.trim().trim_matches('\'').trim_matches('"');
                match key.trim() {
                    "table" => logical_table = Some(value.to_owned()),
                    "ts_column" => ts_column = Some(value.to_owned()),
                    "hot_table" => hot_table = Some(value.to_owned()),
                    "schema" => schema = Some(value.to_owned()),
                    "tiers" => tiers = Some(value.to_owned()),
                    other => {
                        return Err(module_err(format!("unrecognized parameter '{other}'")))
                    }
                }
            }
            None => columns.push(s),
        }
    }
    if !columns.is_empty() {
        if schema.is_some() {
            return Err(module_err(
                "give columns either inline (device TEXT, ...) or via \
                 schema='...', not both",
            ));
        }
        schema = Some(columns.join(", "));
    }

    Ok(TabArgs {
        base_dir: PathBuf::from(dir_str),
        logical_table,
        ts_column,
        hot_table,
        schema,
        tiers,
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
        let vtab_name = std::str::from_utf8(table_name)
            .map_err(|_| module_err("table name is not UTF-8"))?
            .to_owned();
        if parsed.tiers.is_some() {
            // Managed mode: the logical table IS the vtab name — an alias
            // would orphan the shadow table from maintain()'s view.
            if parsed.logical_table.is_some() || parsed.hot_table.is_some() {
                return Err(module_err(
                    "managed mode (tiers=) does not take table= or hot_table= — \
                     the vtab's own name is the logical table",
                ));
            }
            if parsed.schema.is_none() {
                return Err(module_err(
                    "managed mode (tiers=) requires an explicit schema='col TYPE, ...'",
                ));
            }
        }
        let logical_table = match parsed.logical_table {
            Some(t) => t,
            None => vtab_name.clone(),
        };
        let shadow = parsed
            .tiers
            .as_ref()
            .map(|_| format!("{vtab_name}_data"));

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
            shadow,
        };
        Ok((Cow::Owned(CString::new(sql)?), vtab))
    }

    /// Offer to consume EQ/GT/GE/LT/LE constraints on prunable columns,
    /// and capture SQLite's column-usage mask so `filter` can project the
    /// Parquet read down to the columns the statement actually touches
    /// (decoding a dictionary-encoded TEXT column nobody asked for costs
    /// more than the aggregation itself — measured in silodb-bench).
    /// `idx_str` = `<colUsed hex>|<op><col>;<op><col>...`; constraint
    /// values arrive positionally in `filter`'s args.
    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        let mut idx_str = format!("{:x}|", info.col_used());
        let mut n_args = 0;
        for (constraint, mut usage) in info.constraints_and_usages() {
            if !constraint.is_usable() {
                continue;
            }
            let col = constraint.column();
            if col < 0 {
                continue;
            }
            let dt = self.schema.field(col as usize).data_type();
            let is_eq = matches!(
                constraint.operator(),
                IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ
            );
            // Numeric/timestamp columns take every range op (stats
            // pruning); TEXT columns take EQ only, feeding per-file
            // series pruning.
            let text_eq = is_eq
                && matches!(
                    dt,
                    DataType::Utf8 | DataType::LargeUtf8
                );
            if prunable_class(dt).is_none() && !text_eq {
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
            if !idx_str.ends_with('|') {
                idx_str.push(';');
            }
            idx_str.push(op);
            idx_str.push_str(&col.to_string());
        }

        info.set_idx_str(&idx_str);
        info.set_estimated_cost(if n_args > 0 { 100_000.0 } else { 1_000_000.0 });
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
            hot_rows: Vec::new(),
            hot_idx: 0,
            col_map: Vec::new(),
            projection: Vec::new(),
            phantom: PhantomData,
        })
    }
}

impl<'vtab> CreateVTab<'vtab> for SiloTab {
    const KIND: VTabKind = VTabKind::Default;

    /// CREATE VIRTUAL TABLE. In managed mode this is where the shadow hot
    /// table and the maintenance policy come to exist — one DDL statement
    /// defines the entire system.
    fn create(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        module_name: &[u8],
        database_name: &[u8],
        table_name: &[u8],
        args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let (sql, vtab) = Self::connect(db, aux, module_name, database_name, table_name, args)?;
        if let Some(shadow) = &vtab.shadow {
            let parsed = parse_args(args)?; // cheap; re-read schema/tiers text
            let schema_str = parsed.schema.expect("checked in connect");
            let tiers_str = parsed.tiers.expect("managed");
            let hot = vtab.hot_db()?;

            // Policy first (validates tiers/retain/origin before any DDL);
            // origin immutability across re-creates.
            let mut policy =
                silodb_catalog::parse_policy_string(&vtab.logical_table, &tiers_str)
                    .map_err(module_err)?;
            policy.base_dir = vtab.base_dir.display().to_string();
            policy.ts_column = parsed.ts_column.clone();
            if let Some(existing) = silodb_catalog::get_policy(&hot, &vtab.logical_table)
                .map_err(module_err)?
            {
                if existing.origin_us != policy.origin_us {
                    return Err(module_err(format!(
                        "origin changed for '{}' ({} -> {}); the window grid is \
                         immutable once files exist",
                        vtab.logical_table, existing.origin_us, policy.origin_us
                    )));
                }
                // Re-creating the vtab (boot, or DROP + CREATE around an
                // ALTER) must not clobber a retention set via
                // silodb_set_retention — the tiers string can't carry
                // retention, so what's stored always survives.
                policy.retain_us = existing.retain_us;
            }
            silodb_catalog::set_policy(&hot, &policy).map_err(module_err)?;

            // Shadow hot table, verbatim decls (TIMESTAMP markers must
            // survive for compaction's schema read), plus the bucket-axis
            // index compaction depends on.
            let cols = parse_verbatim_schema(&schema_str)?;
            let ts_name = {
                let decls: Vec<silodb_schema::ColumnDecl> =
                    cols.iter().map(|(d, _)| d.clone()).collect();
                let idx = silodb_schema::resolve_ts_index(&decls, parsed.ts_column.as_deref())
                    .map_err(|e| module_err(format!("schema argument: {e}")))?;
                decls[idx].name.clone()
            };
            let col_defs = cols
                .iter()
                .map(|(d, verbatim)| format!("{} {verbatim}", quote_ident(&d.name)))
                .collect::<Vec<_>>()
                .join(", ");
            hot.execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {sh} ({col_defs});
                 CREATE INDEX IF NOT EXISTS {idx} ON {sh} ({ts});",
                sh = quote_ident(shadow),
                idx = quote_ident(&format!("{shadow}_ts")),
                ts = quote_ident(&ts_name),
            ))
            .map_err(module_err)?;
        }
        Ok((sql, vtab))
    }

    /// DROP TABLE. **Nothing is destroyed** — not the shadow hot table,
    /// not the catalog, files, stats, or policy. DDL detaches the name;
    /// re-creating the vtab reattaches everything, hot rows included
    /// (create's `CREATE TABLE IF NOT EXISTS` adopts the surviving
    /// shadow). Destroying history is retention's job, never DDL's.
    fn destroy(&self) -> Result<()> {
        Ok(())
    }
}

impl UpdateVTab<'_> for SiloTab {
    fn insert(&mut self, args: &Inserts<'_>) -> Result<i64> {
        let Some(shadow) = &self.shadow else {
            return Err(module_err(
                "this silodb table is read-only (no tiers= in its definition); \
                 INSERT into the hot table instead",
            ));
        };
        let n_cols = self.schema.fields().len();
        // args[0..2] are the rowid slots; column values follow.
        let values: Vec<rusqlite::types::Value> = args
            .iter()
            .skip(2)
            .map(rusqlite::types::Value::try_from)
            .collect::<std::result::Result<_, _>>()
            .map_err(module_err)?;
        if values.len() != n_cols {
            return Err(module_err(format!(
                "expected {n_cols} values, got {}",
                values.len()
            )));
        }
        let placeholders = (1..=n_cols)
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let hot = self.hot_db()?;
        hot.execute(
            &format!(
                "INSERT INTO {} VALUES ({placeholders})",
                quote_ident(shadow)
            ),
            rusqlite::params_from_iter(values),
        )?;
        Ok(hot.last_insert_rowid())
    }

    fn delete(&mut self, _arg: ValueRef<'_>) -> Result<()> {
        Err(module_err(immutable_msg(self)))
    }

    fn update(&mut self, _args: &Updates<'_>) -> Result<()> {
        Err(module_err(immutable_msg(self)))
    }
}

fn immutable_msg(t: &SiloTab) -> String {
    match &t.shadow {
        Some(shadow) => format!(
            "compacted history is immutable; for hot-only changes mutate the \
             shadow table '{shadow}' directly"
        ),
        None => "this silodb table is read-only".to_owned(),
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('\"', "\"\""))
}

/// Parse `schema=` keeping verbatim decl text (shadow-table DDL needs
/// TIMESTAMP markers as written).
fn parse_verbatim_schema(
    schema: &str,
) -> Result<Vec<(silodb_schema::ColumnDecl, String)>> {
    schema
        .split(',')
        .map(|part| {
            let part = part.trim();
            let (name, decl) = part
                .split_once(char::is_whitespace)
                .unwrap_or((part, ""));
            let name = name.trim_matches('\"').trim_matches('`');
            let decl = decl.trim();
            silodb_schema::ColumnDecl::parse(name, decl)
                .map(|d| (d, decl.to_owned()))
                .ok_or_else(|| module_err(format!("bad schema column '{part}'")))
        })
        .collect()
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
    Text(String),
}

/// Split `idx_str` into (colUsed mask, constraint list).
fn decode_idx_str(idx_str: &str) -> Result<(u64, &str)> {
    let (mask_hex, constraints) = idx_str
        .split_once('|')
        .ok_or_else(|| module_err("corrupt idx_str: missing mask"))?;
    let mask = u64::from_str_radix(mask_hex, 16)
        .map_err(|_| module_err("corrupt idx_str mask"))?;
    Ok((mask, constraints))
}

/// Which schema columns SQLite will actually request, per the xBestIndex
/// colUsed mask. Bit 63 means "column 63 or beyond".
fn used_columns(mask: u64, n_cols: usize) -> Vec<usize> {
    (0..n_cols)
        .filter(|&i| {
            let bit = i.min(63);
            mask & (1u64 << bit) != 0
        })
        .collect()
}

fn decode_pushed(constraints: &str, args: &Filters<'_>) -> Result<Vec<Pushed>> {
    let mut out = Vec::new();
    if constraints.is_empty() {
        return Ok(out);
    }
    for (spec, value) in constraints.split(';').zip(args.iter()) {
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
            ValueRef::Text(t) => match std::str::from_utf8(t) {
                Ok(s) => PushedValue::Text(s.to_owned()),
                Err(_) => continue,
            },
            _ => continue,
        };
        out.push(Pushed { col, op, value });
    }
    Ok(out)
}

/// Paths that HAVE stats rows but NONE matching every EQ constraint —
/// provably empty for the queried series. Errors and missing tables
/// degrade to "prune nothing".
fn series_pruned_paths(
    hot: &Connection,
    logical_table: &str,
    eqs: &[(&str, &PushedValue)],
) -> Result<std::collections::HashSet<String>> {
    let stats_table = format!("{logical_table}_stats");
    let exists: i64 = hot
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [&stats_table],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if exists == 0 {
        return Ok(Default::default());
    }
    // Only constraints on columns the stats table actually has (series
    // columns) participate; unknown columns would be SQL errors.
    let stat_cols: std::collections::HashSet<String> = hot
        .prepare("SELECT name FROM pragma_table_info(?1)")?
        .query_map([&stats_table], |r| r.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();
    let usable: Vec<&(&str, &PushedValue)> =
        eqs.iter().filter(|(n, _)| stat_cols.contains(*n)).collect();
    if usable.is_empty() {
        return Ok(Default::default());
    }

    let quote = |n: &str| format!("\"{}\"", n.replace('"', "\"\""));
    let preds = usable
        .iter()
        .enumerate()
        .map(|(i, (n, _))| format!("{} = ?{}", quote(n), i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    let params: Vec<rusqlite::types::Value> = usable
        .iter()
        .map(|(_, v)| match v {
            PushedValue::Int(i) => rusqlite::types::Value::Integer(*i),
            PushedValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
            PushedValue::Real(f) => rusqlite::types::Value::Real(*f),
        })
        .collect();

    // Files with stats minus files with a matching series row.
    let mut stmt = hot.prepare(&format!(
        "SELECT DISTINCT path FROM {st}
         WHERE path NOT IN (SELECT path FROM {st} WHERE {preds})",
        st = quote(&stats_table),
    ))?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params), |r| {
        r.get::<_, String>(0)
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
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
            // Text constraints don't participate in row-group stats pruning
            // (they exist for file-level series pruning).
            (_, PushedValue::Text(_)) => true,
        }
    })
}

/// One file the cursor will read: pre-pruned row groups, footer already
/// parsed.
struct ScanFile {
    path: PathBuf,
    meta: ArrowReaderMetadata,
    row_groups: Vec<usize>,
    /// Schema indices this file actually has AND the statement uses —
    /// its projection mask (files may be a prefix of the declared schema
    /// after ADD COLUMN evolution).
    projection: Vec<usize>,
    /// Declared-schema index → position in this file's projected batch;
    /// `None` = not requested or missing from the file (serve NULL).
    col_map: Vec<Option<usize>>,
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
    /// Managed mode: shadow-table rows served before the cold files,
    /// materialized at filter time (the hot tier is small by design).
    hot_rows: Vec<Vec<rusqlite::types::Value>>,
    hot_idx: usize,
    /// Schema column index → position in the projected batch. `None` =
    /// column not requested by this statement (xColumn answers NULL,
    /// defensively — SQLite said it wouldn't ask).
    col_map: Vec<Option<usize>>,
    /// Schema indices to actually decode, from xBestIndex's colUsed mask.
    projection: Vec<usize>,
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
            self.col_map = next.col_map.clone();
            let file = File::open(&next.path).map_err(module_err)?;
            let mask = parquet::arrow::ProjectionMask::roots(
                next.meta.metadata().file_metadata().schema_descr(),
                next.projection.iter().copied(),
            );
            let reader =
                ParquetRecordBatchReaderBuilder::new_with_metadata(file, next.meta.clone())
                    .with_row_groups(next.row_groups.clone())
                    .with_projection(mask)
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
        let n_cols = vtab.schema.fields().len();
        let (used, pushed) = match idx_str {
            Some(s) if !s.is_empty() => {
                let (mask, constraints) = decode_idx_str(s)?;
                (used_columns(mask, n_cols), decode_pushed(constraints, args)?)
            }
            // No idx_str (shouldn't happen — best_index always sets one):
            // read everything.
            _ => ((0..n_cols).collect(), Vec::new()),
        };
        let col_map = {
            let mut map = vec![None; n_cols];
            for (proj_pos, &schema_idx) in used.iter().enumerate() {
                map[schema_idx] = Some(proj_pos);
            }
            map
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

        // Layer 1.5: per-file series statistics. EQ constraints on series
        // columns skip whole files that provably hold no rows for that
        // series — before any footer work. Conservative: a file with no
        // stats rows at all (pre-upgrade data, not yet healed by
        // maintain()) is kept.
        let series_skip: std::collections::HashSet<String> = {
            let eqs: Vec<(&str, &PushedValue)> = pushed
                .iter()
                .filter(|p| p.op == 'E' && Some(p.col) != vtab.ts_col)
                .filter(|p| matches!(p.value, PushedValue::Text(_) | PushedValue::Int(_)))
                .map(|p| (vtab.schema.field(p.col).name().as_str(), &p.value))
                .collect();
            if eqs.is_empty() || candidates.is_empty() {
                Default::default()
            } else {
                series_pruned_paths(&vtab.hot_db()?, &vtab.logical_table, &eqs)
                    .unwrap_or_default()
            }
        };

        // Layer 2: row-group pruning within each candidate (Phase 2 logic).
        let mut stats = ScanStats {
            total_files,
            candidate_files: candidates.len(),
            ..Default::default()
        };
        let mut files = Vec::new();
        for entry in &candidates {
            if series_skip.contains(&entry.path) {
                stats.series_pruned_files += 1;
                continue;
            }
            let path = PathBuf::from(&entry.path);
            let (meta, cache_hit) = vtab.file_meta(&path)?;
            stats.metadata_cache_hits += usize::from(cache_hit);

            // A file's columns must be a PREFIX of the declared columns
            // (by name, in order): identical for freshly written files,
            // shorter for files predating ADD COLUMN evolution — the
            // missing tail reads as NULL. Anything else is real drift.
            let file_fields = meta.schema().fields();
            let decl_fields = vtab.schema.fields();
            let is_prefix = file_fields.len() <= decl_fields.len()
                && file_fields
                    .iter()
                    .zip(decl_fields.iter())
                    .all(|(f, d)| f.name() == d.name());
            if !is_prefix {
                return Err(module_err(format!(
                    "'{}' has columns incompatible with this table \
                     (file: [{}], declared: [{}]; files must be a prefix — \
                     only ADD COLUMN evolution is supported)",
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
            let n_file_cols = file_fields.len();

            // A pushed EQ/range constraint on a column this file doesn't
            // have can never match (the column is all-NULL here): skip the
            // whole file. SQLite re-checks rows, so this is pure savings.
            if pushed.iter().any(|p| p.col >= n_file_cols) {
                stats.series_pruned_files += 1;
                continue;
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
                let file_projection: Vec<usize> =
                    used.iter().copied().filter(|&i| i < n_file_cols).collect();
                let mut file_map = vec![None; n_cols];
                for (pos, &schema_idx) in file_projection.iter().enumerate() {
                    file_map[schema_idx] = Some(pos);
                }
                files.push(ScanFile {
                    path,
                    meta,
                    row_groups: keep,
                    projection: file_projection,
                    col_map: file_map,
                });
            }
        }
        LAST_SCAN.with(|c| c.set(Some(stats)));

        // Managed mode: the hot arm — shadow rows matching the pushed
        // constraints (SQLite re-checks everything, so this is best-effort
        // narrowing, not correctness).
        self.hot_rows = match &vtab.shadow {
            Some(shadow) => fetch_hot_rows(&vtab.hot_db()?, shadow, &vtab.schema, &pushed)?,
            None => Vec::new(),
        };
        self.hot_idx = 0;

        let _ = col_map; // per-file maps supersede the scan-level one
        self.col_map = Vec::new();
        self.projection = used;
        self.files = files;
        self.next_file = 0;
        self.reader = None;
        self.rowid = 0;
        if self.hot_rows.is_empty() {
            self.advance_batch()
        } else {
            // Cold batches start once the hot arm is exhausted.
            self.batch = None;
            self.row_in_batch = 0;
            Ok(())
        }
    }

    fn next(&mut self) -> Result<()> {
        self.rowid += 1;
        if self.hot_idx < self.hot_rows.len() {
            self.hot_idx += 1;
            if self.hot_idx == self.hot_rows.len() {
                self.advance_batch()?;
            }
            return Ok(());
        }
        self.row_in_batch += 1;
        let in_batch = self.batch.as_ref().map_or(0, RecordBatch::num_rows);
        if self.row_in_batch >= in_batch {
            self.advance_batch()?;
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.hot_idx >= self.hot_rows.len() && self.batch.is_none()
    }

    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        if let Some(row) = self.hot_rows.get(self.hot_idx) {
            // Hot arm carries full rows in schema order — no projection.
            let v = row
                .get(i as usize)
                .ok_or_else(|| module_err("column index out of range"))?;
            return set_result_from_value(ctx, v);
        }
        let batch = self
            .batch
            .as_ref()
            .ok_or_else(|| module_err("column() called at EOF"))?;
        let Some(Some(proj_pos)) = self.col_map.get(i as usize) else {
            // SQLite's colUsed said this column wouldn't be requested, so
            // it wasn't decoded. Answer NULL rather than erroring if it
            // asks anyway.
            return ctx.set_result(&Null);
        };
        let array = batch.column(*proj_pos);
        set_result_from_array(ctx, array.as_ref(), self.row_in_batch)
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}

/// Shadow-table rows matching the pushed constraints, full columns in
/// schema order.
fn fetch_hot_rows(
    hot: &Connection,
    shadow: &str,
    schema: &SchemaRef,
    pushed: &[Pushed],
) -> Result<Vec<Vec<rusqlite::types::Value>>> {
    let cols = schema
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let mut preds = Vec::new();
    let mut params: Vec<rusqlite::types::Value> = Vec::new();
    for p in pushed {
        let op = match p.op {
            'E' => "=",
            'G' => ">",
            'g' => ">=",
            'L' => "<",
            'l' => "<=",
            _ => continue,
        };
        preds.push(format!(
            "{} {op} ?{}",
            quote_ident(schema.field(p.col).name()),
            params.len() + 1
        ));
        params.push(match &p.value {
            PushedValue::Int(v) => rusqlite::types::Value::Integer(*v),
            PushedValue::Real(v) => rusqlite::types::Value::Real(*v),
            PushedValue::Text(v) => rusqlite::types::Value::Text(v.clone()),
        });
    }
    let where_clause = if preds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", preds.join(" AND "))
    };
    let n_cols = schema.fields().len();
    let mut stmt = hot.prepare(&format!(
        "SELECT {cols} FROM {}{where_clause}",
        quote_ident(shadow)
    ))?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params), |r| {
        (0..n_cols)
            .map(|i| r.get::<_, rusqlite::types::Value>(i))
            .collect::<std::result::Result<Vec<_>, _>>()
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(module_err)
}

fn set_result_from_value(ctx: &mut Context, v: &rusqlite::types::Value) -> Result<()> {
    use rusqlite::types::Value as V;
    match v {
        V::Null => ctx.set_result(&Null),
        V::Integer(i) => ctx.set_result(i),
        V::Real(f) => ctx.set_result(f),
        V::Text(s) => ctx.set_result(&s.as_str()),
        V::Blob(b) => ctx.set_result(&b.as_slice()),
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
