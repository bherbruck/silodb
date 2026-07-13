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

use rusqlite::Connection;

pub use silodb_compact::{compact_bucket, BucketSpec, CompactError, CompactOutcome};
pub use silodb_vtab::{last_scan_stats, load_module, ScanStats};

/// Catalog schema and operations (`_silodb_catalog` in the hot database).
pub use silodb_catalog as catalog;

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

/// Parse a `"name TYPE, name TYPE"` schema string into (name, storage
/// class) pairs, validating every declared type through the same affinity
/// rules the rest of silodb uses.
fn parse_schema(schema: &str) -> Result<Vec<(String, silodb_schema::SqliteType)>, InitError> {
    let cols = schema
        .split(',')
        .map(|part| {
            let part = part.trim();
            let (name, decl) = part
                .split_once(char::is_whitespace)
                .unwrap_or((part, ""));
            let name = name.trim_matches('"').trim_matches('`');
            if name.is_empty() {
                return Err(InitError::BadSchema(format!("empty column in '{part}'")));
            }
            let ty = silodb_schema::SqliteType::from_decl(decl.trim()).ok_or_else(|| {
                InitError::BadSchema(format!(
                    "column '{name}' has unsupported declared type '{}'",
                    decl.trim()
                ))
            })?;
            Ok((name.to_owned(), ty))
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
/// `ts_column` is fixed to `ts` by convention here; use the lower-level
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
    if !cols
        .iter()
        .any(|(n, t)| n == "ts" && *t == silodb_schema::SqliteType::Integer)
    {
        return Err(InitError::BadSchema(
            "schema must include an INTEGER 'ts' column (epoch microseconds)".into(),
        ));
    }

    let hot = format!("{table}_hot");
    let cold = format!("{table}_cold");

    // Drift check before any DDL.
    let existing: Vec<(String, String)> = conn
        .prepare(&format!("PRAGMA table_info({})", quote_ident(&hot)))?
        .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
        .collect::<Result<_, _>>()?;
    if !existing.is_empty() {
        let existing_mapped: Vec<(String, silodb_schema::SqliteType)> = existing
            .iter()
            .map(|(n, d)| {
                silodb_schema::SqliteType::from_decl(d)
                    .map(|t| (n.clone(), t))
                    .ok_or_else(|| {
                        InitError::BadSchema(format!("existing column '{n}' has decl '{d}'"))
                    })
            })
            .collect::<Result<_, _>>()?;
        if existing_mapped != cols {
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

    let col_defs = cols
        .iter()
        .map(|(n, t)| format!("{} {}", quote_ident(n), t.decl()))
        .collect::<Vec<_>>()
        .join(", ");
    let col_names = cols
        .iter()
        .map(|(n, _)| quote_ident(n))
        .collect::<Vec<_>>()
        .join(", ");
    let new_refs = cols
        .iter()
        .map(|(n, _)| format!("NEW.{}", quote_ident(n)))
        .collect::<Vec<_>>()
        .join(", ");

    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {hot_q} ({col_defs});
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
            ts_column: "ts",
            bucket_start,
            bucket_end,
        },
        base_dir.as_ref(),
    )
}
