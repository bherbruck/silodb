//! Facade crate: the only crate the supervisory binary should depend on.
//!
//! The intended surface is one name per table — the application inserts
//! into and selects from `readings` and never sees the hot/cold split:
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let conn = rusqlite::Connection::open("hot.db")?;
//!
//! // Every boot (module registration is per-connection):
//! silodb::load_module(&conn)?;
//! // Idempotent; creates readings_hot / readings_cold / the readings view
//! // + insert trigger on first boot, no-ops after:
//! silodb::init_table(&conn, "readings", "ts INTEGER, value REAL, name TEXT", "cold/")?;
//!
//! // The app's whole world is now one name:
//! conn.execute("INSERT INTO readings VALUES (?1, ?2, ?3)",
//!              rusqlite::params![1_700_000_000_000_000i64, 21.5, "boiler"])?;
//! let n: i64 = conn.query_row(
//!     "SELECT count(*) FROM readings WHERE ts > ?1", [0i64], |r| r.get(0))?;
//!
//! // Aging a closed bucket out is one call; the view's contents never
//! // change, rows just move from SQLite pages to a Parquet file:
//! silodb::compact_table(&conn, "readings", 0, 3_600_000_000, "cold/")?;
//! # Ok(()) }
//! ```
//!
//! Everything on disk is lazy: `cold/` and the catalog table appear on the
//! first compaction that writes, not at init. Dropping to the lower-level
//! pieces (`compact_bucket` with a custom [`BucketSpec`], hand-written
//! `CREATE VIRTUAL TABLE ... USING silodb(...)`, the [`catalog`] API) is
//! always possible; `init_table` is convention, not a requirement.

use std::path::Path;

use rusqlite::functions::FunctionFlags;
use rusqlite::Connection;

pub use silodb_compact::{compact_bucket, BucketSpec, CompactError, CompactOutcome};
pub use silodb_vtab::{last_scan_stats, ScanStats};

/// Catalog schema and operations (`_silodb_catalog` in the hot database).
pub use silodb_catalog as catalog;

/// Register the `silodb` vtab module plus the timestamp helper functions
/// on a connection. Call on every open (registrations are per-connection,
/// not persisted).
///
/// Helpers (pure logic in `silodb-schema`, UTC, no locale surprises):
/// - `silodb_ts(x)` → epoch microseconds. TEXT parses ISO 8601
///   (`'2026-07-13'`, `'2026-07-13 10:42:00'`, `'...T10:42:00.5Z'`);
///   INTEGER passes through, so `WHERE ts > silodb_ts(?1)` accepts either.
/// - `silodb_datetime(µs)` → ISO 8601 UTC text, the reverse.
pub fn load_module(conn: &Connection) -> rusqlite::Result<()> {
    silodb_vtab::load_module(conn)?;
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    conn.create_scalar_function("silodb_ts", 1, flags, |ctx| {
        match ctx.get_raw(0) {
            rusqlite::types::ValueRef::Integer(i) => Ok(i),
            rusqlite::types::ValueRef::Text(t) => {
                let s = std::str::from_utf8(t)
                    .map_err(|e| rusqlite::Error::UserFunctionError(e.into()))?;
                silodb_schema::parse_timestamp_micros(s).ok_or_else(|| {
                    rusqlite::Error::UserFunctionError(
                        format!("silodb_ts: unparseable datetime '{s}'").into(),
                    )
                })
            }
            other => Err(rusqlite::Error::UserFunctionError(
                format!("silodb_ts: expected TEXT or INTEGER, got {}", other.data_type()).into(),
            )),
        }
    })?;
    conn.create_scalar_function("silodb_datetime", 1, flags, |ctx| {
        let us: i64 = ctx.get(0)?;
        Ok(silodb_schema::format_timestamp_micros(us))
    })?;
    Ok(())
}

/// Errors from [`init_table`].
#[derive(Debug)]
pub enum InitError {
    Sqlite(rusqlite::Error),
    /// The schema argument couldn't be parsed or uses an unsupported
    /// declared type.
    BadSchema(String),
    /// `<table>_hot` already exists with different columns than the schema
    /// argument — the app's schema string changed between releases.
    /// Migration is deliberately not attempted.
    SchemaDrift {
        table: String,
        existing: String,
        requested: String,
    },
}

impl std::fmt::Display for InitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(e) => write!(f, "sqlite error: {e}"),
            Self::BadSchema(m) => write!(f, "bad schema argument: {m}"),
            Self::SchemaDrift {
                table,
                existing,
                requested,
            } => write!(
                f,
                "schema drift on '{table}': table has [{existing}] but init \
                 requested [{requested}]; migrate manually before re-initializing"
            ),
        }
    }
}

impl std::error::Error for InitError {}

impl From<rusqlite::Error> for InitError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e)
    }
}

/// Parse a `"name TYPE, name TYPE"` schema string, keeping each column's
/// verbatim declared type (it goes into the hot table's CREATE TABLE, so
/// `TIMESTAMP` etc. must survive as written) alongside the parsed
/// [`silodb_schema::ColumnDecl`].
fn parse_schema(
    schema: &str,
) -> Result<Vec<(silodb_schema::ColumnDecl, String)>, InitError> {
    let cols = schema
        .split(',')
        .map(|part| {
            let part = part.trim();
            let (name, decl) = part
                .split_once(char::is_whitespace)
                .unwrap_or((part, ""));
            let name = name.trim_matches('"').trim_matches('`');
            let decl = decl.trim();
            if name.is_empty() {
                return Err(InitError::BadSchema(format!("empty column in '{part}'")));
            }
            let parsed = silodb_schema::ColumnDecl::parse(name, decl).ok_or_else(|| {
                InitError::BadSchema(format!(
                    "column '{name}' has unsupported declared type '{decl}'"
                ))
            })?;
            Ok((parsed, decl.to_owned()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if cols.is_empty() {
        return Err(InitError::BadSchema("no columns".into()));
    }
    Ok(cols)
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Set up the single-name surface for one time-series table. Idempotent —
/// call it on every boot, right after [`load_module`].
///
/// Creates (all `IF NOT EXISTS`):
/// - `<table>_hot` — real SQLite table with the given columns; writes land
///   here and compaction drains it
/// - `<table>_cold` — silodb vtab over `<base_dir>/<table>/`, with the
///   schema baked into its DDL so it reconnects with zero dependencies (no
///   hot table, no files, no catalog required — a cold-only archive
///   database keeps working)
/// - `<table>` — view unioning both, with an `INSTEAD OF INSERT` trigger
///   forwarding writes to `<table>_hot`
///
/// The app then uses `<table>` for everything: `INSERT INTO readings ...`,
/// `SELECT ... FROM readings`. UPDATE/DELETE through the view are not
/// wired: compacted history is immutable, and silently mutating only the
/// hot subset would misbehave — run them on `<table>_hot` explicitly if
/// hot-only mutation is really intended.
///
/// The bucket axis is discovered by declared type — one TIMESTAMP/DATETIME
/// column (any name), or the legacy INTEGER `ts`; use the lower-level
/// pieces directly if a table needs a different timestamp column.
///
/// Fails with [`InitError::SchemaDrift`] if `<table>_hot` exists with
/// different columns than `schema` — nothing is touched in that case.
pub fn init_table(
    conn: &Connection,
    table: &str,
    schema: &str,
    base_dir: impl AsRef<Path>,
) -> Result<(), InitError> {
    let cols = parse_schema(schema)?;
    // A bucket axis must be resolvable: TIMESTAMP-typed column (preferred),
    // or the legacy INTEGER `ts` name.
    let decls: Vec<silodb_schema::ColumnDecl> = cols.iter().map(|(c, _)| c.clone()).collect();
    silodb_schema::resolve_ts_index(&decls, None)
        .map_err(|e| InitError::BadSchema(e.to_string()))?;

    let hot = format!("{table}_hot");
    let cold = format!("{table}_cold");

    // Drift check before any DDL. Compared at the ColumnDecl level (name,
    // storage class, timestamp marker) — cosmetic decl spelling may vary.
    let existing: Vec<(String, String)> = conn
        .prepare(&format!("PRAGMA table_info({})", quote_ident(&hot)))?
        .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
        .collect::<Result<_, _>>()?;
    if !existing.is_empty() {
        let existing_mapped: Vec<silodb_schema::ColumnDecl> = existing
            .iter()
            .map(|(n, d)| {
                silodb_schema::ColumnDecl::parse(n, d).ok_or_else(|| {
                    InitError::BadSchema(format!("existing column '{n}' has decl '{d}'"))
                })
            })
            .collect::<Result<_, _>>()?;
        if existing_mapped != decls {
            return Err(InitError::SchemaDrift {
                table: table.to_owned(),
                existing: existing
                    .iter()
                    .map(|(n, d)| format!("{n} {d}"))
                    .collect::<Vec<_>>()
                    .join(", "),
                requested: schema.to_owned(),
            });
        }
    }

    // Verbatim decls go into the hot table so TIMESTAMP/DATETIME markers
    // survive for compaction's own schema read.
    let col_defs = cols
        .iter()
        .map(|(c, decl)| format!("{} {decl}", quote_ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let col_names = cols
        .iter()
        .map(|(c, _)| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let new_refs = cols
        .iter()
        .map(|(c, _)| format!("NEW.{}", quote_ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");

    let ts_name = {
        let idx = silodb_schema::resolve_ts_index(&decls, None).expect("validated above");
        decls[idx].name.clone()
    };
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {hot_q} ({col_defs});
         -- The bucket axis must be indexed: compaction selects, counts and
         -- deletes by ts range, and without this every compact_bucket call
         -- scans the whole hot table (quadratic over a backlog — measured
         -- 10x throughput loss at 2M rows in silodb-bench).
         CREATE INDEX IF NOT EXISTS {ts_idx_q} ON {hot_q} ({ts_q});
         CREATE VIRTUAL TABLE IF NOT EXISTS {cold_q} USING silodb('{base}',
             table={table}, schema='{schema_esc}');
         CREATE VIEW IF NOT EXISTS {table_q} AS
           SELECT {col_names} FROM {hot_q}
           UNION ALL
           SELECT {col_names} FROM {cold_q};
         CREATE TRIGGER IF NOT EXISTS {trigger_q}
           INSTEAD OF INSERT ON {table_q}
           BEGIN
             INSERT INTO {hot_q} ({col_names}) VALUES ({new_refs});
           END;",
        hot_q = quote_ident(&hot),
        cold_q = quote_ident(&cold),
        table_q = quote_ident(table),
        trigger_q = quote_ident(&format!("{table}_insert")),
        ts_idx_q = quote_ident(&format!("{table}_hot_ts")),
        ts_q = quote_ident(&ts_name),
        base = base_dir.as_ref().display(),
        schema_esc = schema.replace('\'', "''"),
    ))?;
    Ok(())
}

/// Compact one closed bucket of `<table>` (as set up by [`init_table`]):
/// `[bucket_start, bucket_end)` epoch microseconds, out of `<table>_hot`,
/// into `<base_dir>/<table>/`. Same idempotency guarantees as
/// [`compact_bucket`], which this merely parameterizes by convention.
pub fn compact_table(
    conn: &Connection,
    table: &str,
    bucket_start: i64,
    bucket_end: i64,
    base_dir: impl AsRef<Path>,
) -> Result<CompactOutcome, CompactError> {
    let hot = format!("{table}_hot");
    compact_bucket(
        conn,
        &BucketSpec {
            hot_table: &hot,
            logical_table: table,
            ts_column: None,
            bucket_start,
            bucket_end,
        },
        base_dir.as_ref(),
    )
}
