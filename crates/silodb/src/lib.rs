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
//! // + insert trigger on first boot, no-ops after. Cold files land in
//! // hot.db.silodb/ unless set_default_dir / init_table_at says otherwise:
//! silodb::init_table_tiered(&conn, "readings",
//!     "ts TIMESTAMP, value REAL, name TEXT", "1d,7d,28d")?;
//! silodb::set_retention(&conn, "readings", Some("2y"))?;
//!
//! // The app's whole world is now one name:
//! conn.execute("INSERT INTO readings VALUES (?1, ?2, ?3)",
//!              rusqlite::params![1_700_000_000_000_000i64, 21.5, "boiler"])?;
//! let n: i64 = conn.query_row(
//!     "SELECT count(*) FROM readings WHERE ts > ?1", [0i64], |r| r.get(0))?;
//!
//! // Storage management is one call on a dumb timer; the dir and policy
//! // live in the database, never repeated at call sites:
//! silodb::maintain(&conn, "readings", 1_700_000_000_000_000)?;
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
use rusqlite::{Connection, OptionalExtension};

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
    // silodb_bucket(width, ts[, origin]) — floor ts to its epoch-aligned
    // (or origin-anchored) window start. Same argument order as
    // time_bucket() in TimescaleDB/DuckDB; deliberately NOT named
    // time_bucket: SQLite's function namespace is global/flat, and ours
    // returns integer µs, not a timestamp type. width is a duration
    // string ('1h') or integer µs; ts and origin accept integer µs or ISO
    // text (silodb_ts semantics).
    for n_args in [2, 3] {
        conn.create_scalar_function("silodb_bucket", n_args, flags, move |ctx| {
            let err = |m: String| rusqlite::Error::UserFunctionError(m.into());
            let as_us = |v: rusqlite::types::ValueRef<'_>, what: &str| -> rusqlite::Result<i64> {
                match v {
                    rusqlite::types::ValueRef::Integer(i) => Ok(i),
                    rusqlite::types::ValueRef::Text(t) => {
                        let s = std::str::from_utf8(t)
                            .map_err(|e| rusqlite::Error::UserFunctionError(e.into()))?;
                        silodb_schema::parse_timestamp_micros(s)
                            .ok_or_else(|| err(format!("silodb_bucket: bad {what} '{s}'")))
                    }
                    other => Err(err(format!(
                        "silodb_bucket: {what} must be INTEGER or TEXT, got {}",
                        other.data_type()
                    ))),
                }
            };
            let width = match ctx.get_raw(0) {
                rusqlite::types::ValueRef::Integer(i) => i,
                rusqlite::types::ValueRef::Text(t) => {
                    let s = std::str::from_utf8(t)
                        .map_err(|e| rusqlite::Error::UserFunctionError(e.into()))?;
                    silodb_schema::parse_duration_micros(s)
                        .ok_or_else(|| err(format!("silodb_bucket: bad width '{s}'")))?
                }
                other => {
                    return Err(err(format!(
                        "silodb_bucket: width must be a duration string or INTEGER µs, got {}",
                        other.data_type()
                    )))
                }
            };
            let ts = as_us(ctx.get_raw(1), "ts")?;
            let origin = if n_args == 3 {
                as_us(ctx.get_raw(2), "origin")?
            } else {
                0
            };
            silodb_schema::bucket_floor(width, ts, origin)
                .ok_or_else(|| err("silodb_bucket: overflow or non-positive width".into()))
        })?;
    }
    register_admin_functions(conn)?;
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


/// The hot tier's table for a logical table: `<t>_hot` (init_table
/// convention) or `<t>_data` (managed-vtab shadow). `None` when neither
/// exists (cold-only database).
pub fn resolve_hot_table(conn: &Connection, table: &str) -> rusqlite::Result<Option<String>> {
    for cand in [format!("{table}_hot"), format!("{table}_data")] {
        let found: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                [&cand],
                |r| r.get(0),
            )
            .optional()?;
        if found.is_some() {
            return Ok(Some(cand));
        }
    }
    Ok(None)
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
) -> Result<(), InitError> {
    init_impl(conn, table, schema, "1d", None, None)
}

/// [`init_table`] with an explicit base directory (otherwise resolved:
/// db-level default from [`set_default_dir`], else `<dbfile>.silodb/`).
pub fn init_table_at(
    conn: &Connection,
    table: &str,
    schema: &str,
    base_dir: impl AsRef<Path>,
) -> Result<(), InitError> {
    init_impl(
        conn,
        table,
        schema,
        "1d",
        Some(&base_dir.as_ref().display().to_string()),
        None,
    )
}

/// [`init_table`] plus an explicit compaction-tier policy, e.g.
/// `"1d, 7d, 28d"`: hot rows compact into 1-day bucket files; once a
/// 7-day window is fully in the past its daily files merge into one
/// weekly file; likewise weekly → 28-day. Units: `s m h d w y` (y = 365d).
/// Each tier must be an exact multiple of the previous one (windows are
/// epoch-aligned; `30d` after `7d` would strand straddling files — use
/// `28d`).
///
/// The tiers string carries only tier windows (plus an optional
/// `origin=`); retention is its own policy — set it with
/// [`set_retention`] (never expressed at create time, so a boot re-init
/// can't fight a later policy change). Without one, history is kept
/// forever. The policy persists in `_silodb_policy`; [`maintain`]
/// executes it.
pub fn init_table_tiered(
    conn: &Connection,
    table: &str,
    schema: &str,
    tiers: &str,
) -> Result<(), InitError> {
    init_impl(conn, table, schema, tiers, None, None)
}

/// [`init_table_tiered`] with an explicit base directory.
pub fn init_table_tiered_at(
    conn: &Connection,
    table: &str,
    schema: &str,
    tiers: &str,
    base_dir: impl AsRef<Path>,
) -> Result<(), InitError> {
    init_impl(
        conn,
        table,
        schema,
        tiers,
        Some(&base_dir.as_ref().display().to_string()),
        None,
    )
}

/// The base-dir resolution chain: explicit > db default (`_silodb_config`
/// `default_dir`, see [`set_default_dir`]) > `<database file>.silodb/`.
/// In-memory/temp databases have no file, so they require an explicit dir.
fn resolve_base_dir(
    conn: &Connection,
    explicit: Option<&str>,
) -> Result<String, InitError> {
    if let Some(d) = explicit {
        return Ok(d.to_owned());
    }
    if let Some(d) = catalog::get_config(conn, "default_dir")? {
        return Ok(d);
    }
    match conn.path() {
        Some(p) if !p.is_empty() => {
            let derived = format!("{p}.silodb");
            Ok(std::path::absolute(&derived)
                .map(|p| p.display().to_string())
                .unwrap_or(derived))
        }
        _ => Err(InitError::BadSchema(
            "no base directory: this database has no file (in-memory?) — pass \
             an explicit dir or SELECT silodb_set_default_dir(...)"
                .into(),
        )),
    }
}

/// Persist the db-level default cold-storage directory (used when neither
/// an explicit dir nor a table policy specifies one). Never moves existing
/// tables — each table's dir is frozen in its policy at create time.
pub fn set_default_dir(conn: &Connection, dir: impl AsRef<Path>) -> Result<(), InitError> {
    catalog::set_config(conn, "default_dir", &dir.as_ref().display().to_string())?;
    Ok(())
}

fn init_impl(
    conn: &Connection,
    table: &str,
    schema: &str,
    tiers: &str,
    explicit_dir: Option<&str>,
    ts_column: Option<&str>,
) -> Result<(), InitError> {
    let mut policy = catalog::parse_policy_string(table, tiers)
        .map_err(InitError::BadSchema)?;
    let existing = catalog::get_policy(conn, table)?;
    // Origin is immutable once set: every written file's windows are
    // anchored to it, and moving it would misalign all of them. Detect a
    // changed origin loudly instead of silently re-anchoring. The dir is
    // frozen the same way: once a table has one, re-inits keep it.
    if let Some(existing) = &existing
        && existing.origin_us != policy.origin_us
    {
        return Err(InitError::BadSchema(format!(
            "origin changed for '{table}' ({} -> {}); the window grid is \
             immutable once files exist — re-aligning is a migration, not a \
             knob",
            existing.origin_us, policy.origin_us
        )));
    }
    policy.base_dir = match existing.as_ref().map(|e| e.base_dir.as_str()) {
        Some(dir) if !dir.is_empty() => dir.to_owned(),
        _ => resolve_base_dir(conn, explicit_dir)?,
    };
    // Retention is set_retention()'s alone (the tiers string can't carry
    // it) — boot-time re-init always preserves what's stored.
    policy.retain_us = existing.as_ref().and_then(|e| e.retain_us);
    policy.ts_column = ts_column
        .map(str::to_owned)
        .or(existing.and_then(|e| e.ts_column));
    policy.logical_table = table.to_owned();
    catalog::set_policy(conn, &policy)?;
    init_table_inner(conn, table, schema, &policy.base_dir, policy.ts_column.as_deref())
}

fn init_table_inner(
    conn: &Connection,
    table: &str,
    schema: &str,
    base_dir: impl AsRef<Path>,
    ts_column: Option<&str>,
) -> Result<(), InitError> {
    let cols = parse_schema(schema)?;
    // A bucket axis must be resolvable: explicit choice (persisted in the
    // policy), else TIMESTAMP-typed column, else the legacy INTEGER `ts`.
    let decls: Vec<silodb_schema::ColumnDecl> = cols.iter().map(|(c, _)| c.clone()).collect();
    silodb_schema::resolve_ts_index(&decls, ts_column)
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
        // Exact match, or the requested schema is a proper prefix of what
        // exists — that's a boot re-init running with the pre-ALTER schema
        // string after alter_table_add_column widened the table. Every
        // statement below is IF NOT EXISTS, so nothing gets narrowed; the
        // wide view/vtab/trigger stay as the ALTER left them.
        let requested_is_prefix = decls.len() < existing_mapped.len()
            && existing_mapped[..decls.len()] == decls[..];
        if existing_mapped != decls && !requested_is_prefix {
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
        let idx = silodb_schema::resolve_ts_index(&decls, ts_column).expect("validated above");
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
             table={table}, schema='{schema_esc}'{ts_arg});
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
        ts_arg = ts_column
            .map(|t| format!(", ts_column={t}"))
            .unwrap_or_default(),
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
) -> Result<CompactOutcome, CompactError> {
    let hot = resolve_hot_table(conn, table)
        .map_err(CompactError::Sqlite)?
        .unwrap_or_else(|| format!("{table}_hot"));
    let policy = silodb_catalog::get_policy(conn, table).map_err(CompactError::Sqlite)?;
    let (dir, ts) = match &policy {
        Some(p) if !p.base_dir.is_empty() => (p.base_dir.clone(), p.ts_column.clone()),
        _ => {
            return Err(CompactError::Sqlite(rusqlite::Error::ModuleError(format!(
                "no policy/base_dir for '{table}' — init/create it first"
            ))))
        }
    };
    compact_bucket(
        conn,
        &BucketSpec {
            hot_table: &hot,
            logical_table: table,
            ts_column: ts.as_deref(),
            bucket_start,
            bucket_end,
        },
        Path::new(&dir),
    )
}

/// One thing [`maintain`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintainAction {
    /// Hot rows for a closed tier-0 bucket aged out to a new file.
    Compacted {
        window: (i64, i64),
        rows: usize,
        path: std::path::PathBuf,
    },
    /// Finer files promoted into one tier-N window file (children
    /// superseded).
    Merged {
        window: (i64, i64),
        children: usize,
        rows: usize,
        path: std::path::PathBuf,
    },
    /// Retention: an active file entirely older than `now - retain`
    /// flipped to `evicted` (its file is unlinked by the GC step of the
    /// same call).
    Evicted { window: (i64, i64), path: String },
    /// A superseded/evicted file unlinked and its catalog row removed.
    Gc { path: String },
}

#[derive(Debug)]
pub enum MaintainError {
    /// No `_silodb_policy` row for this table — init it with
    /// [`init_table`]/[`init_table_tiered`] first.
    NoPolicy(String),
    Compact(CompactError),
    Sqlite(rusqlite::Error),
}

impl std::fmt::Display for MaintainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoPolicy(t) => write!(f, "no maintenance policy for table '{t}'"),
            Self::Compact(e) => write!(f, "{e}"),
            Self::Sqlite(e) => write!(f, "sqlite error: {e}"),
        }
    }
}
impl std::error::Error for MaintainError {}
impl From<CompactError> for MaintainError {
    fn from(e: CompactError) -> Self {
        Self::Compact(e)
    }
}
impl From<rusqlite::Error> for MaintainError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e)
    }
}

/// Converge one table's storage toward its tier policy: compact every
/// closed tier-0 bucket out of the hot table, promote finer files into
/// every higher-tier window that is fully in the past, and GC superseded
/// files. Idempotent — call it on a dumb timer and at boot; when nothing
/// is due it costs a few indexed queries and returns an empty report.
///
/// `now_us` is the clock (epoch µs): pass real time in production; tests
/// pass whatever they like. Nothing newer than `now - safety_margin` is
/// ever touched. Contract: one maintainer process at a time (same as the
/// compaction scheduling contract).
pub fn maintain(
    conn: &Connection,
    table: &str,
    now_us: i64,
) -> Result<Vec<MaintainAction>, MaintainError> {
    let policy = catalog::get_policy(conn, table)?
        .ok_or_else(|| MaintainError::NoPolicy(table.to_owned()))?;
    if policy.base_dir.is_empty() {
        return Err(MaintainError::NoPolicy(format!(
            "{table} (policy has no base_dir — re-run init/create to upgrade it)"
        )));
    }
    let base = Path::new(&policy.base_dir);
    // Idempotent; maintain is a writer by definition, and the promotion/GC
    // steps below query the catalog even when nothing was ever compacted
    // (found by the model-based lifecycle proptest: maintain-before-data).
    catalog::ensure_catalog(conn)?;
    let cutoff = now_us.saturating_sub(policy.safety_margin_us);
    let t0 = policy.tiers_us[0];
    let mut actions = Vec::new();

    // --- tier 0: age closed buckets out of the hot table -------------
    let hot_resolved = resolve_hot_table(conn, table)?;
    let hot = hot_resolved
        .clone()
        .unwrap_or_else(|| format!("{table}_hot"));
    let hot_exists: Option<i64> = hot_resolved.as_ref().map(|_| 1);
    if hot_exists.is_some() {
        // The ts column: same discovery compact_bucket uses.
        let cols: Vec<silodb_schema::ColumnDecl> = conn
            .prepare(&format!("PRAGMA table_info({})", quote_ident(&hot)))?
            .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
            .filter_map(|r| r.ok().and_then(|(n, d)| silodb_schema::ColumnDecl::parse(&n, &d)))
            .collect();
        if let Ok(ts_idx) = silodb_schema::resolve_ts_index(&cols, policy.ts_column.as_deref()) {
            let ts_q = quote_ident(&cols[ts_idx].name);
            let (lo, hi) = conn.query_row(
                &format!(
                    "SELECT min({ts_q}), max({ts_q}) FROM {} WHERE {ts_q} < ?1",
                    quote_ident(&hot)
                ),
                [cutoff],
                |r| Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, Option<i64>>(1)?)),
            )?;
            let bounds = lo.zip(hi);
            if let Some((lo, hi)) = bounds {
                let first = (lo - policy.origin_us).div_euclid(t0);
                let last = (hi - policy.origin_us).div_euclid(t0);
                for b in first..=last {
                    let (start, end) =
                        (b * t0 + policy.origin_us, (b + 1) * t0 + policy.origin_us);
                    if end > cutoff {
                        break; // bucket still open (or inside the margin)
                    }
                    match compact_table(conn, table, start, end)? {
                        CompactOutcome::Compacted { rows, path } => {
                            actions.push(MaintainAction::Compacted {
                                window: (start, end),
                                rows,
                                path,
                            })
                        }
                        CompactOutcome::AlreadyCompacted | CompactOutcome::EmptyBucket => {}
                    }
                }
            }
        }
    }

    // --- retention: evict whole files past the retain window ---------
    // Before promotions, so soon-to-die data isn't pointlessly merged.
    if let Some(retain) = policy.retain_us {
        let retain_cutoff = now_us.saturating_sub(retain);
        let evicted = catalog::evict_older_than(conn, table, retain_cutoff)?;
        let evicted_paths: Vec<String> = evicted.iter().map(|e| e.path.clone()).collect();
        silodb_compact::delete_stats_for_paths(conn, table, &evicted_paths)?;
        for e in evicted {
            actions.push(MaintainAction::Evicted {
                window: (e.range_start, e.range_end),
                path: e.path,
            });
        }
        // Plain-table rollups follow the source's retention (whole grain
        // buckets only). A tiered rollup (its own policy) governs itself.
        for spec in catalog::rollups_for_table(conn, table)? {
            if catalog::get_policy(conn, &spec.rollup_table)?.is_some() {
                continue;
            }
            conn.execute(
                &format!(
                    "DELETE FROM {} WHERE ts + ?1 <= ?2",
                    quote_ident(&spec.rollup_table)
                ),
                rusqlite::params![spec.grain_us, retain_cutoff],
            )?;
        }
    }

    // --- higher tiers: promote finer files into closed windows -------
    for &w in &policy.tiers_us[1..] {
        // Candidate windows = distinct epoch-aligned windows containing
        // active files strictly finer than w, where the window itself is
        // fully behind the cutoff.
        let entries = catalog::entries_for_table(conn, table)?;
        let mut windows: Vec<i64> = entries
            .iter()
            .filter(|e| (e.range_end - e.range_start) < w)
            .map(|e| (e.range_start - policy.origin_us).div_euclid(w))
            .collect();
        windows.sort_unstable();
        windows.dedup();
        for win in windows {
            let (start, end) = (win * w + policy.origin_us, (win + 1) * w + policy.origin_us);
            if end > cutoff {
                continue;
            }
            match silodb_compact::merge_window(conn, table, base, start, end)? {
                silodb_compact::MergeOutcome::Merged {
                    children,
                    rows,
                    path,
                } => actions.push(MaintainAction::Merged {
                    window: (start, end),
                    children,
                    rows,
                    path,
                }),
                silodb_compact::MergeOutcome::NothingToMerge => {}
            }
        }
    }

    // --- self-heal: per-file series stats for files that predate them ---
    // (upgrade path; bounded by the active file count, once per file ever.)
    if hot_exists.is_some() {
        let _ = silodb_compact::stats_backfill_missing(conn, table, &hot);
    }

    // --- GC superseded + evicted files ---------------------------------
    for entry in catalog::gc_entries(conn, table)? {
        match std::fs::remove_file(&entry.path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(MaintainError::Compact(e.into())),
        }
        catalog::purge_entry(conn, table, &entry.path)?;
        actions.push(MaintainAction::Gc { path: entry.path });
    }

    Ok(actions)
}

// --- continuous aggregates (rollups) ---------------------------------------

/// Register a continuous aggregate for `table` at `grain` (e.g. `"1h"`)
/// and backfill it from all existing cold files — Timescale-style
/// declare-anytime semantics. One transaction covers registration +
/// backfill, so a crash leaves no half-registered rollup.
///
/// The rollup target is `<table>_rollup_<grain>`, holding sufficient
/// statistics per (grain bucket, series columns): `<col>_count/_sum/
/// _sumsq/_min/_max` for every REAL column, grouped by every other
/// column. avg/stddev derive at query time — nothing inexact (no
/// avg-of-avg) is ever materialized.
///
/// If a table **or silodb single-name view** with that name already
/// exists (schema-compatible), it's used as the target — that's the
/// recursion: `init_table_tiered` the rollup name first and the rollup's
/// own history gets tiered/retained under its own policy.
///
/// Going forward, compaction computes deltas from its own stream and
/// commits them in the tier-migration transaction. Requirements: the
/// table has a policy (grain must divide tier 0) and a hot table to read
/// the schema from.
pub fn create_rollup(conn: &Connection, table: &str, grain: &str) -> Result<(), InitError> {
    let bad = InitError::BadSchema;
    let grain_us = silodb_schema::parse_duration_micros(grain)
        .ok_or_else(|| bad(format!("bad grain '{grain}'")))?;
    let policy = catalog::get_policy(conn, table)?
        .ok_or_else(|| bad(format!("no policy for '{table}' — init_table first")))?;
    let t0 = policy.tiers_us[0];
    if grain_us > t0 || t0 % grain_us != 0 {
        return Err(bad(format!(
            "grain '{grain}' must divide tier 0 ({}s) so every compaction \
             bucket contains whole grain buckets",
            t0 / 1_000_000
        )));
    }

    let hot_name = resolve_hot_table(conn, table)?
        .ok_or_else(|| InitError::BadSchema(format!("no hot table for '{table}'")))?;
    let columns = hot_decls(conn, &hot_name)?;
    let decls: Vec<silodb_schema::ColumnDecl> = columns.clone();
    let ts_idx = silodb_schema::resolve_ts_index(&decls, policy.ts_column.as_deref())
        .map_err(|e| bad(format!("hot table: {e}")))?;

    let rollup_table = format!("{table}_rollup_{grain}");
    let spec = catalog::RollupSpec {
        logical_table: table.to_owned(),
        grain_us,
        rollup_table: rollup_table.clone(),
    };
    let plan = silodb_compact::RollupPlan::new(spec.clone(), &columns, ts_idx, policy.origin_us);

    let existing: Option<String> = conn
        .query_row(
            "SELECT type FROM sqlite_master WHERE name = ?1 AND type IN ('table','view')",
            [&rollup_table],
            |r| r.get(0),
        )
        .optional()?;

    conn.execute_batch("BEGIN IMMEDIATE")?;
    let txn: Result<(), InitError> = (|| {
        if existing.is_none() {
            conn.execute_batch(&plan.rollup_ddl(&columns))?;
            conn.execute_batch(&format!(
                "CREATE INDEX {} ON {} (ts)",
                quote_ident(&format!("{rollup_table}_ts")),
                quote_ident(&rollup_table)
            ))?;
        }
        catalog::insert_rollup(conn, &spec)?;
        let entries = catalog::entries_for_table(conn, table)?;
        silodb_compact::rollup_backfill(conn, &plan, &entries)
            .map_err(|e| bad(format!("backfill: {e}")))?;
        Ok(())
    })();
    match txn {
        Ok(()) => {
            conn.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

/// Unregister a rollup and drop its plain table (a recursive/tiered rollup
/// target is left in place — it has its own lifecycle; drop it like any
/// silodb table).
pub fn drop_rollup(conn: &Connection, table: &str, grain: &str) -> Result<(), InitError> {
    let grain_us = silodb_schema::parse_duration_micros(grain)
        .ok_or_else(|| InitError::BadSchema(format!("bad grain '{grain}'")))?;
    let rollup_table = format!("{table}_rollup_{grain}");
    catalog::delete_rollup(conn, table, grain_us)?;
    let is_plain_table: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE name = ?1 AND type = 'table'",
            [&rollup_table],
            |r| r.get(0),
        )
        .optional()?;
    if is_plain_table.is_some() {
        conn.execute_batch(&format!("DROP TABLE {}", quote_ident(&rollup_table)))?;
    }
    Ok(())
}

fn hot_decls(conn: &Connection, hot: &str) -> Result<Vec<silodb_schema::ColumnDecl>, InitError> {
    let cols: Vec<silodb_schema::ColumnDecl> = conn
        .prepare(&format!("PRAGMA table_info({})", quote_ident(hot)))?
        .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
        .filter_map(|r| r.ok().and_then(|(n, d)| silodb_schema::ColumnDecl::parse(&n, &d)))
        .collect();
    if cols.is_empty() {
        return Err(InitError::BadSchema(format!("no hot table '{hot}'")));
    }
    Ok(cols)
}

/// Create the standard real-time view `<table>_<grain>`: materialized
/// sufficient statistics UNION'd with a live aggregation of the hot tail,
/// re-aggregated so late-data delta rows combine, with `<col>_avg`
/// convenience columns. Requires [`load_module`] on querying connections
/// (uses `silodb_bucket`).
pub fn create_rollup_view(conn: &Connection, table: &str, grain: &str) -> Result<(), InitError> {
    let bad = InitError::BadSchema;
    let grain_us = silodb_schema::parse_duration_micros(grain)
        .ok_or_else(|| bad(format!("bad grain '{grain}'")))?;
    let specs = catalog::rollups_for_table(conn, table)?;
    let spec = specs
        .iter()
        .find(|s| s.grain_us == grain_us)
        .ok_or_else(|| bad(format!("no '{grain}' rollup registered for '{table}'")))?;
    let origin = catalog::get_policy(conn, table)?
        .map(|p| p.origin_us)
        .unwrap_or(0);

    let hot_name = resolve_hot_table(conn, table)?
        .ok_or_else(|| InitError::BadSchema(format!("no hot table for '{table}'")))?;
    let columns = hot_decls(conn, &hot_name)?;
    let policy_ts = catalog::get_policy(conn, table)?.and_then(|p| p.ts_column);
    let ts_idx = silodb_schema::resolve_ts_index(&columns, policy_ts.as_deref())
        .map_err(|e| bad(format!("hot table: {e}")))?;
    let ts_name = quote_ident(&columns[ts_idx].name);
    let mut group = Vec::new();
    let mut aggs = Vec::new();
    for (i, c) in columns.iter().enumerate() {
        if i == ts_idx {
            continue;
        } else if c.ty == silodb_schema::SqliteType::Real {
            aggs.push(quote_ident(&c.name));
        } else {
            group.push(quote_ident(&c.name));
        }
    }

    let group_list = group.join(", ");
    let comma_group = if group.is_empty() {
        String::new()
    } else {
        format!(", {group_list}")
    };
    // Outer arm re-aggregates so additive delta rows (late data) combine.
    let outer_stats = aggs
        .iter()
        .map(|a| {
            let a = a.trim_matches('"');
            format!(
                "sum(\"{a}_count\") AS \"{a}_count\", sum(\"{a}_sum\") AS \"{a}_sum\", \
                 sum(\"{a}_sumsq\") AS \"{a}_sumsq\", min(\"{a}_min\") AS \"{a}_min\", \
                 max(\"{a}_max\") AS \"{a}_max\""
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    // Inner rollup arm passes the materialized columns through untouched.
    let inner_pass = aggs
        .iter()
        .map(|a| {
            let a = a.trim_matches('"');
            format!("\"{a}_count\", \"{a}_sum\", \"{a}_sumsq\", \"{a}_min\", \"{a}_max\"")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let live_stats = aggs
        .iter()
        .map(|a| {
            let raw = a.clone();
            let a = a.trim_matches('"');
            format!(
                "count({raw}) AS \"{a}_count\", sum({raw}) AS \"{a}_sum\", \
                 sum({raw}*{raw}) AS \"{a}_sumsq\", min({raw}) AS \"{a}_min\", \
                 max({raw}) AS \"{a}_max\""
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let avg_cols = aggs
        .iter()
        .map(|a| {
            let a = a.trim_matches('"');
            format!(
                "CAST(sum(\"{a}_sum\") AS REAL) / nullif(sum(\"{a}_count\"), 0) AS \"{a}_avg\""
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    conn.execute_batch(&format!(
        "CREATE VIEW IF NOT EXISTS {view} AS
         SELECT ts{comma_group}, {outer_stats}, {avg_cols}
         FROM (
           SELECT ts{comma_group}, {inner_pass} FROM {rollup}
           UNION ALL
           SELECT silodb_bucket('{grain}', {ts_name}, {origin}) AS ts{comma_group}, {live_stats}
           FROM {hot} GROUP BY 1{live_group}
         )
         GROUP BY ts{comma_group}",
        view = quote_ident(&format!("{table}_{grain}")),
        rollup = quote_ident(&spec.rollup_table),
        hot = quote_ident(&hot_name),
        live_group = (2..=group.len() + 1)
            .map(|i| format!(", {i}"))
            .collect::<String>(),
    ))?;
    Ok(())
}

/// Register the Timescale-style admin functions (`create_hypertable`
/// precedent) on a connection — the SQL-only front door:
///
/// ```sql
/// CREATE TABLE readings (ts TIMESTAMP, device TEXT, value REAL);
/// SELECT silodb_create_table('readings', 'cold/', '1d,7d,28d');
/// SELECT silodb_set_retention('readings', '2y');
/// SELECT silodb_maintain('readings', 'cold/', unixepoch()*1000000);
/// ```
///
/// `silodb_create_table` converts a plain table **in place** — existing
/// rows survive (they stay hot until maintenance ages them out), exactly
/// like `create_hypertable`. Idempotent: converting an already-converted
/// table just re-runs the idempotent init. `silodb_maintain` returns the
/// number of actions taken.
///
/// Registered `SQLITE_DIRECTONLY`: callable only from top-level SQL,
/// never from views/triggers — side effects don't hide.
/// Called automatically by [`load_module`].
fn register_admin_functions(conn: &Connection) -> rusqlite::Result<()> {
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DIRECTONLY;
    // silodb_create_table(table[, ts_column[, tiers[, dir]]]) — ts_column
    // is slot #2 like create_hypertable; NULL infers (one TIMESTAMP column,
    // else INTEGER 'ts'). tiers default '1d'. dir default: db-level
    // default (silodb_set_default_dir), else <dbfile>.silodb/.
    for n_args in 1..=4 {
        conn.create_scalar_function("silodb_create_table", n_args, flags, move |ctx| {
            let table: String = ctx.get(0)?;
            let opt_text = |i: usize| -> rusqlite::Result<Option<String>> {
                if i >= n_args as usize {
                    return Ok(None);
                }
                match ctx.get_raw(i) {
                    rusqlite::types::ValueRef::Null => Ok(None),
                    rusqlite::types::ValueRef::Text(t) => Ok(Some(
                        std::str::from_utf8(t)
                            .map_err(|e| rusqlite::Error::UserFunctionError(e.into()))?
                            .to_owned(),
                    )),
                    other => Err(rusqlite::Error::UserFunctionError(
                        format!("expected TEXT or NULL, got {}", other.data_type()).into(),
                    )),
                }
            };
            let ts = opt_text(1)?;
            let tiers = opt_text(2)?.unwrap_or_else(|| "1d".to_owned());
            let dir = opt_text(3)?;
            let conn = unsafe { ctx.get_connection()? };
            convert_table(&conn, &table, ts.as_deref(), &tiers, dir.as_deref())
                .map_err(|e| rusqlite::Error::UserFunctionError(e.to_string().into()))?;
            Ok(table)
        })?;
    }
    conn.create_scalar_function("silodb_maintain", 2, flags, |ctx| {
        let table: String = ctx.get(0)?;
        let now: i64 = ctx.get(1)?;
        let conn = unsafe { ctx.get_connection()? };
        let actions = maintain(&conn, &table, now)
            .map_err(|e| rusqlite::Error::UserFunctionError(e.to_string().into()))?;
        Ok(actions.len() as i64)
    })?;
    // silodb_set_retention(table, duration|NULL) — Timescale's
    // add_retention_policy shape: separate from create, changeable
    // anytime, NULL clears (keep forever).
    conn.create_scalar_function("silodb_set_retention", 2, flags, |ctx| {
        let table: String = ctx.get(0)?;
        let retain: Option<String> = match ctx.get_raw(1) {
            rusqlite::types::ValueRef::Null => None,
            rusqlite::types::ValueRef::Text(t) => Some(
                std::str::from_utf8(t)
                    .map_err(|e| rusqlite::Error::UserFunctionError(e.into()))?
                    .to_owned(),
            ),
            other => {
                return Err(rusqlite::Error::UserFunctionError(
                    format!("expected TEXT or NULL, got {}", other.data_type()).into(),
                ))
            }
        };
        let conn = unsafe { ctx.get_connection()? };
        set_retention(&conn, &table, retain.as_deref())
            .map_err(|e| rusqlite::Error::UserFunctionError(e.to_string().into()))?;
        Ok(table)
    })?;
    // silodb_add_column(table, 'col TYPE') — the one supported schema
    // evolution; see alter_table_add_column.
    conn.create_scalar_function("silodb_add_column", 2, flags, |ctx| {
        let table: String = ctx.get(0)?;
        let coldef: String = ctx.get(1)?;
        let conn = unsafe { ctx.get_connection()? };
        alter_table_add_column(&conn, &table, &coldef)
            .map_err(|e| rusqlite::Error::UserFunctionError(e.to_string().into()))?;
        Ok(table)
    })?;
    conn.create_scalar_function("silodb_set_default_dir", 1, flags, |ctx| {
        let dir: String = ctx.get(0)?;
        let conn = unsafe { ctx.get_connection()? };
        set_default_dir(&conn, &dir)
            .map_err(|e| rusqlite::Error::UserFunctionError(e.to_string().into()))?;
        Ok(dir)
    })?;
    Ok(())
}

/// The `create_hypertable` move: turn a plain table into a silodb table
/// in place. Renames `<table>` to `<table>_hot` (rows intact) and builds
/// the standard surface around it.
fn convert_table(
    conn: &Connection,
    table: &str,
    ts_column: Option<&str>,
    tiers: &str,
    base_dir: Option<&str>,
) -> Result<(), InitError> {
    // Validate the policy BEFORE any DDL — a bad tiers string must not
    // leave the table stranded mid-rename.
    catalog::parse_policy_string(table, tiers).map_err(InitError::BadSchema)?;
    let kind: Option<String> = conn
        .query_row(
            "SELECT type FROM sqlite_master WHERE name = ?1 AND type IN ('table','view')",
            [table],
            |r| r.get(0),
        )
        .optional()?;
    let hot = format!("{table}_hot");
    match kind.as_deref() {
        // Plain table → rename to the hot slot, then init builds the rest.
        Some("table") => {
            let hot_exists: Option<i64> = conn
                .query_row(
                    "SELECT 1 FROM sqlite_master WHERE name = ?1",
                    [&hot],
                    |r| r.get(0),
                )
                .optional()?;
            if hot_exists.is_some() {
                return Err(InitError::BadSchema(format!(
                    "both '{table}' and '{hot}' exist — ambiguous; move one aside"
                )));
            }
            conn.execute_batch(&format!(
                "ALTER TABLE {} RENAME TO {}",
                quote_ident(table),
                quote_ident(&hot)
            ))?;
        }
        // Already converted (the single-name view) → idempotent re-init.
        Some("view") => {}
        Some(_) | None => {
            return Err(InitError::BadSchema(format!(
                "no table named '{table}' to convert"
            )))
        }
    }
    // Reconstruct the schema string from the hot table's verbatim decls.
    let schema = conn
        .prepare(&format!("PRAGMA table_info({})", quote_ident(&hot)))?
        .query_map([], |r| {
            Ok(format!(
                "{} {}",
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    init_impl(conn, table, &schema, tiers, base_dir, ts_column)
}

/// Set (or clear, with `None`) a table's retention — the
/// `add_retention_policy` precedent: retention is its own policy call,
/// not part of table creation, and unlike origin/tiers it is safe to
/// change at any time (it never affects window alignment). The next
/// [`maintain`] applies it. `retain` must be at least the largest tier
/// window (files merge into windows that must be evictable whole).
///
/// This is the only way to set retention — the tiers/policy string
/// rejects `retain=` so create-time DDL can never fight this call.
pub fn set_retention(
    conn: &Connection,
    table: &str,
    retain: Option<&str>,
) -> Result<(), InitError> {
    let bad = InitError::BadSchema;
    let mut policy = catalog::get_policy(conn, table)?
        .ok_or_else(|| bad(format!("no policy for '{table}' — init/create it first")))?;
    policy.retain_us = match retain {
        None => None,
        Some(dur) => {
            let us = silodb_schema::parse_duration_micros(dur)
                .ok_or_else(|| bad(format!("bad retention duration '{dur}'")))?;
            let largest = *policy.tiers_us.last().expect("validated at create");
            if us < largest {
                return Err(bad(format!(
                    "retention '{dur}' is shorter than the largest tier window — \
                     files merge into windows bigger than the retention period \
                     and could never be evicted whole"
                )));
            }
            Some(us)
        }
    };
    catalog::set_policy(conn, &policy)?;
    Ok(())
}

/// ADD COLUMN schema evolution: widen a silodb table in place. The only
/// supported evolution — DROP COLUMN, RENAME and type changes would
/// rewrite immutable history and are refused by omission.
///
/// `coldef` is one `name TYPE` pair (`"humidity REAL"`). The hot table is
/// ALTERed; the view/trigger/cold-vtab (init-style) or the managed vtab
/// are regenerated around the wider schema; registered rollups, their
/// views, and the stats table gain the new column's slots. Existing cold
/// files stay untouched: rows written before the ALTER read back as NULL
/// in the new column, exactly like plain SQLite's ADD COLUMN, and merges
/// NULL-pad old files up to the newest schema as tiers converge.
///
/// If the bucket axis was being discovered by type, it is frozen into the
/// policy first — adding a second TIMESTAMP column must not change which
/// column buckets.
pub fn alter_table_add_column(
    conn: &Connection,
    table: &str,
    coldef: &str,
) -> Result<(), InitError> {
    let bad = InitError::BadSchema;
    if coldef.contains(',') {
        return Err(bad(format!(
            "one column at a time: '{coldef}' — call again for the next"
        )));
    }
    let (new_decl, verbatim) = parse_schema(coldef)?.into_iter().next().expect("non-empty");
    if verbatim.is_empty() {
        return Err(bad(format!("column '{}' needs a declared type", new_decl.name)));
    }

    let mut policy = catalog::get_policy(conn, table)?.ok_or_else(|| {
        bad(format!("no policy for '{table}' — not a silodb table"))
    })?;
    let hot = resolve_hot_table(conn, table)?
        .ok_or_else(|| bad(format!("no hot table for '{table}'")))?;
    let existing = hot_decls(conn, &hot)?;
    if existing.iter().any(|c| c.name == new_decl.name) {
        return Err(bad(format!(
            "column '{}' already exists on '{table}'",
            new_decl.name
        )));
    }
    // Validate everything that could refuse BEFORE the first ALTER — a
    // half-widened table is worse than an error.
    let rollup_specs = catalog::rollups_for_table(conn, table)?;
    for spec in &rollup_specs {
        let kind: Option<String> = conn
            .query_row(
                "SELECT type FROM sqlite_master WHERE name = ?1",
                [&spec.rollup_table],
                |r| r.get(0),
            )
            .optional()?;
        let is_vtab: bool = conn
            .query_row(
                "SELECT sql LIKE 'CREATE VIRTUAL%' OR type = 'view' \
                 FROM sqlite_master WHERE name = ?1",
                [&spec.rollup_table],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(false);
        if kind.is_none() || is_vtab {
            return Err(bad(format!(
                "rollup target '{}' is not a plain table — altering a table \
                 with a tiered rollup target isn't supported yet",
                spec.rollup_table
            )));
        }
    }

    // Freeze the bucket axis: discovery-by-type must keep resolving to the
    // same column after the schema widens.
    if policy.ts_column.is_none() {
        let idx = silodb_schema::resolve_ts_index(&existing, None)
            .map_err(|e| bad(format!("hot table: {e}")))?;
        policy.ts_column = Some(existing[idx].name.clone());
        catalog::set_policy(conn, &policy)?;
    }

    conn.execute_batch(&format!(
        "ALTER TABLE {} ADD COLUMN {} {verbatim}",
        quote_ident(&hot),
        quote_ident(&new_decl.name),
    ))?;

    // Reconstruct the widened schema string from the hot table's verbatim
    // decls (same move as convert_table).
    let new_schema = conn
        .prepare(&format!("PRAGMA table_info({})", quote_ident(&hot)))?
        .query_map([], |r| {
            Ok(format!(
                "{} {}",
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");

    if hot == format!("{table}_hot") {
        // init-style surface: view + trigger + cold vtab carry the column
        // list; drop and rebuild them around the wider hot table. The
        // vtab's destroy() deletes nothing.
        conn.execute_batch(&format!(
            "DROP TRIGGER IF EXISTS {trig};
             DROP VIEW IF EXISTS {t};
             DROP TABLE IF EXISTS {cold};",
            trig = quote_ident(&format!("{table}_insert")),
            t = quote_ident(table),
            cold = quote_ident(&format!("{table}_cold")),
        ))?;
        init_table_inner(conn, table, &new_schema, &policy.base_dir, policy.ts_column.as_deref())?;
    } else {
        // Managed vtab: DROP detaches the name (shadow already ALTERed
        // above survives untouched), CREATE reattaches it with the wider
        // schema. Policy string reconstructed from what's stored; retain
        // survives via create()'s preserve-on-absent rule.
        let mut tiers = policy
            .tiers_us
            .iter()
            .map(|&t| silodb_schema::format_duration_micros(t))
            .collect::<Vec<_>>()
            .join(",");
        if policy.origin_us != 0 {
            tiers.push_str(&format!(",origin={}", policy.origin_us));
        }
        conn.execute_batch(&format!("DROP TABLE {}", quote_ident(table)))?;
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE {t} USING silodb('{base}', schema='{schema_esc}', \
             tiers='{tiers}'{ts_arg})",
            t = quote_ident(table),
            base = policy.base_dir,
            schema_esc = new_schema.replace('\'', "''"),
            ts_arg = policy
                .ts_column
                .as_deref()
                .map(|t| format!(", ts_column={t}"))
                .unwrap_or_default(),
        ))?;
    }

    // Rollup tables + stats table gain the new column's slots; existing
    // rows keep NULL there (no history to attribute). Their INSERTs are
    // by name, so append-at-end ordering is fine.
    let stat_adds: Vec<String> = if new_decl.ty == silodb_schema::SqliteType::Real {
        [
            ("count", "INTEGER"),
            ("sum", "REAL"),
            ("sumsq", "REAL"),
            ("min", "REAL"),
            ("max", "REAL"),
        ]
        .iter()
        .map(|(suffix, ty)| {
            format!(
                "{} {ty}",
                quote_ident(&format!("{}_{suffix}", new_decl.name))
            )
        })
        .collect()
    } else {
        vec![format!(
            "{} {}",
            quote_ident(&new_decl.name),
            new_decl.ty.decl()
        )]
    };
    for spec in &rollup_specs {
        for add in &stat_adds {
            conn.execute_batch(&format!(
                "ALTER TABLE {} ADD COLUMN {add}",
                quote_ident(&spec.rollup_table)
            ))?;
        }
        // The convenience view names every column — regenerate it if it
        // exists (create_rollup_view is the single source of its shape).
        let grain = silodb_schema::format_duration_micros(spec.grain_us);
        let view = format!("{table}_{grain}");
        let has_view: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='view' AND name=?1",
                [&view],
                |r| r.get(0),
            )
            .optional()?;
        if has_view.is_some() {
            conn.execute_batch(&format!("DROP VIEW {}", quote_ident(&view)))?;
            create_rollup_view(conn, table, &grain)?;
        }
    }
    let stats_table = silodb_compact::stats_table_name(table);
    let has_stats: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            [&stats_table],
            |r| r.get(0),
        )
        .optional()?;
    if has_stats.is_some() {
        for add in &stat_adds {
            conn.execute_batch(&format!(
                "ALTER TABLE {} ADD COLUMN {add}",
                quote_ident(&stats_table)
            ))?;
        }
    }
    Ok(())
}
