//! `silodb` SQLite virtual table: read-only queries over a Parquet file.
//!
//! ```sql
//! CREATE VIRTUAL TABLE cold USING silodb('path/to/file.parquet');
//! SELECT * FROM cold WHERE ts > ?1 AND ts < ?2;
//! ```
//!
//! Rows stream out batch-by-batch — the file is never fully materialized.
//! `xBestIndex` forwards range/equality constraints to the cursor, which
//! skips row groups whose min/max statistics prove they can't match
//! (Phase 2). SQLite still re-checks every constraint on returned rows
//! (`omit` is left false), so pruning only ever has to be conservative,
//! never exact.

use std::borrow::Cow;
use std::cell::Cell;
use std::ffi::{c_int, CStr, CString};
use std::fs::File;
use std::marker::PhantomData;
use std::path::PathBuf;

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
    Context, CreateVTab, Filters, IndexConstraintOp, IndexInfo, Module, VTab, VTabConfig,
    VTabConnection, VTabCursor, VTabKind,
};
use rusqlite::{Connection, Error, Result};

const MODULE_NAME: &CStr = c"silodb";

/// Register the `silodb` module on a connection.
pub fn load_module(conn: &Connection) -> Result<()> {
    const MODULE: Module<SiloTab> = Module::read_only_module();
    let aux: Option<()> = None;
    conn.create_module(MODULE_NAME, &MODULE, aux)
}

/// Row-group pruning outcome of the most recent `xFilter` on this thread.
///
/// Diagnostic hook for tests and logging — Phase 2's acceptance criterion is
/// "fewer row groups read", which needs a counter, not wall-clock timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanStats {
    /// Row groups in the file.
    pub total_row_groups: usize,
    /// Row groups actually handed to the Parquet reader.
    pub scanned_row_groups: usize,
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

/// An instance of the silodb virtual table: one Parquet file.
#[repr(C)]
pub struct SiloTab {
    /// Base class. Must be first.
    base: ffi::sqlite3_vtab,
    path: PathBuf,
    schema: SchemaRef,
    /// Footer metadata (incl. row-group statistics), parsed once at connect.
    reader_meta: ArrowReaderMetadata,
}

/// The single positional argument arrives verbatim, quotes included:
/// `USING silodb('file.parquet')` → `'file.parquet'`.
fn parse_path_arg(arg: &[u8]) -> Result<PathBuf> {
    let s = std::str::from_utf8(arg)
        .map_err(|_| module_err("path argument is not UTF-8"))?
        .trim();
    let s = s
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .or_else(|| s.strip_prefix('"').and_then(|s| s.strip_suffix('"')))
        .unwrap_or(s);
    if s.is_empty() {
        return Err(module_err("empty Parquet file path"));
    }
    Ok(PathBuf::from(s))
}

unsafe impl<'vtab> VTab<'vtab> for SiloTab {
    type Aux = ();
    type Cursor = SiloCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&()>,
        _module_name: &[u8],
        _database_name: &[u8],
        _table_name: &[u8],
        args: &[&[u8]],
    ) -> Result<(Cow<'static, CStr>, Self)> {
        let [arg] = args else {
            return Err(module_err(
                "expected exactly one argument: USING silodb('path/to/file.parquet')",
            ));
        };
        let path = parse_path_arg(arg)?;

        let file = File::open(&path)
            .map_err(|e| module_err(format!("cannot open '{}': {e}", path.display())))?;
        let reader_meta =
            ArrowReaderMetadata::load(&file, Default::default()).map_err(module_err)?;
        let schema = reader_meta.schema().clone();

        let sql = silodb_schema::create_table_sql(&schema).map_err(module_err)?;
        db.config(VTabConfig::DirectOnly)?;

        let vtab = Self {
            base: ffi::sqlite3_vtab::default(),
            path,
            schema,
            reader_meta,
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
            // so row-group pruning can't cause wrong results, only wasted I/O.
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

/// Column classes row-group pruning understands. Unsigned ints are left out:
/// their Parquet statistics involve sign-reinterpretation subtleties that
/// aren't worth handling for a filter pattern we don't have.
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
fn row_group_may_match(
    rg: &RowGroupMetaData,
    schema: &SchemaRef,
    pushed: &[Pushed],
) -> bool {
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

/// Cursor over record batches. `batch == None` after `filter` means EOF.
#[repr(C)]
pub struct SiloCursor<'vtab> {
    /// Base class. Must be first.
    base: ffi::sqlite3_vtab_cursor,
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

    /// Pull batches until one has rows or the reader is exhausted.
    fn advance_batch(&mut self) -> Result<()> {
        self.batch = None;
        self.row_in_batch = 0;
        let Some(reader) = self.reader.as_mut() else {
            return Ok(());
        };
        for batch in reader {
            let batch = batch.map_err(module_err)?;
            if batch.num_rows() > 0 {
                self.batch = Some(batch);
                return Ok(());
            }
        }
        Ok(())
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
        let meta = vtab.reader_meta.clone();
        let total = meta.metadata().num_row_groups();

        let pushed = match idx_str {
            Some(s) if !s.is_empty() => decode_pushed(s, args)?,
            _ => Vec::new(),
        };
        let keep: Vec<usize> = (0..total)
            .filter(|&i| {
                pushed.is_empty()
                    || row_group_may_match(meta.metadata().row_group(i), &vtab.schema, &pushed)
            })
            .collect();

        LAST_SCAN.with(|c| {
            c.set(Some(ScanStats {
                total_row_groups: total,
                scanned_row_groups: keep.len(),
            }))
        });

        let file = File::open(&vtab.path).map_err(module_err)?;
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, meta)
            .with_row_groups(keep)
            .build()
            .map_err(module_err)?;
        self.reader = Some(reader);
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

    fn down<'a, T: 'static>(array: &'a dyn Array) -> Result<&'a T> {
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
