//! `silodb` SQLite virtual table: read-only queries over a Parquet file.
//!
//! ```sql
//! CREATE VIRTUAL TABLE cold USING silodb('path/to/file.parquet');
//! SELECT * FROM cold WHERE ts > ?1 AND ts < ?2;
//! ```
//!
//! Phase 1: full scan, iterating record batches so the file is never fully
//! materialized in memory. Constraint pushdown lands in Phase 2.

use std::borrow::Cow;
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
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use rusqlite::ffi;
use rusqlite::types::Null;
use rusqlite::vtab::{
    Context, CreateVTab, Filters, IndexInfo, Module, VTab, VTabConfig, VTabConnection, VTabCursor,
    VTabKind,
};
use rusqlite::{Connection, Error, Result};

const MODULE_NAME: &CStr = c"silodb";

/// Register the `silodb` module on a connection.
pub fn load_module(conn: &Connection) -> Result<()> {
    const MODULE: Module<SiloTab> = Module::read_only_module();
    let aux: Option<()> = None;
    conn.create_module(MODULE_NAME, &MODULE, aux)
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
}

impl SiloTab {
    fn reader_builder(&self) -> Result<ParquetRecordBatchReaderBuilder<File>> {
        let file = File::open(&self.path).map_err(module_err)?;
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(module_err)
    }
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
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(module_err)?;
        let schema = builder.schema().clone();

        let sql = silodb_schema::create_table_sql(&schema).map_err(module_err)?;
        db.config(VTabConfig::DirectOnly)?;

        let vtab = Self {
            base: ffi::sqlite3_vtab::default(),
            path,
            schema,
        };
        Ok((Cow::Owned(CString::new(sql)?), vtab))
    }

    /// Phase 1: full scan only.
    fn best_index(&self, info: &mut IndexInfo) -> Result<bool> {
        info.set_estimated_cost(1_000_000.);
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
        _idx_str: Option<&str>,
        _args: &Filters<'_>,
    ) -> Result<()> {
        let reader = self.vtab().reader_builder()?.build().map_err(module_err)?;
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
