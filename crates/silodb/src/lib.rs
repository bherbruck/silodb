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
    init_table_tiered(conn, table, schema, base_dir, "1d")
}

/// [`init_table`] plus an explicit compaction-tier policy, e.g.
/// `"1d, 7d, 28d"`: hot rows compact into 1-day bucket files; once a
/// 7-day window is fully in the past its daily files merge into one
/// weekly file; likewise weekly → 28-day. Units: `s m h d w`. Each tier
/// must be an exact multiple of the previous one (windows are
/// epoch-aligned; `30d` after `7d` would strand straddling files — use
/// `28d`). The policy persists in `_silodb_policy`; [`maintain`] executes
/// it.
pub fn init_table_tiered(
    conn: &Connection,
    table: &str,
    schema: &str,
    base_dir: impl AsRef<Path>,
    tiers: &str,
) -> Result<(), InitError> {
    let tiers_us = parse_tiers(tiers)?;
    catalog::set_policy(
        conn,
        &catalog::TablePolicy {
            logical_table: table.to_owned(),
            tiers_us,
            safety_margin_us: 2 * 3600 * 1_000_000, // 2h, per spec contract
        },
    )?;
    init_table_inner(conn, table, schema, base_dir)
}

/// Parse `"1d, 7d, 28d"` into ascending microsecond windows, validating
/// that each tier is a multiple of the previous.
fn parse_tiers(tiers: &str) -> Result<Vec<i64>, InitError> {
    let bad = |m: String| InitError::BadSchema(m);
    let mut out = Vec::new();
    for part in tiers.split(',') {
        let part = part.trim();
        let (num, unit) = part.split_at(part.len().saturating_sub(1));
        let n: i64 = num
            .trim()
            .parse()
            .map_err(|_| bad(format!("bad tier '{part}'")))?;
        let secs = match unit {
            "s" => 1,
            "m" => 60,
            "h" => 3600,
            "d" => 86_400,
            "w" => 7 * 86_400,
            _ => return Err(bad(format!("bad tier unit in '{part}' (use s/m/h/d/w)"))),
        };
        let us = n
            .checked_mul(secs)
            .and_then(|s| s.checked_mul(1_000_000))
            .filter(|&us| us > 0)
            .ok_or_else(|| bad(format!("tier '{part}' out of range")))?;
        if let Some(&prev) = out.last()
            && (us <= prev || us % prev != 0)
        {
            return Err(bad(format!(
                "tier '{part}' must be an ascending exact multiple of the \
                 previous tier (epoch-aligned windows can't merge \
                 straddling files — e.g. use 28d after 7d, not 30d)"
            )));
        }
        out.push(us);
    }
    if out.is_empty() {
        return Err(bad("no tiers".into()));
    }
    Ok(out)
}

fn init_table_inner(
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
    /// A superseded file unlinked and its catalog row removed.
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
    base_dir: impl AsRef<Path>,
    now_us: i64,
) -> Result<Vec<MaintainAction>, MaintainError> {
    let base = base_dir.as_ref();
    let policy = catalog::get_policy(conn, table)?
        .ok_or_else(|| MaintainError::NoPolicy(table.to_owned()))?;
    let cutoff = now_us.saturating_sub(policy.safety_margin_us);
    let t0 = policy.tiers_us[0];
    let mut actions = Vec::new();

    // --- tier 0: age closed buckets out of the hot table -------------
    let hot = format!("{table}_hot");
    let hot_exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            [&hot],
            |r| r.get(0),
        )
        .optional()?;
    if hot_exists.is_some() {
        // The ts column: same discovery compact_bucket uses.
        let cols: Vec<silodb_schema::ColumnDecl> = conn
            .prepare(&format!("PRAGMA table_info({})", quote_ident(&hot)))?
            .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
            .filter_map(|r| r.ok().and_then(|(n, d)| silodb_schema::ColumnDecl::parse(&n, &d)))
            .collect();
        if let Ok(ts_idx) = silodb_schema::resolve_ts_index(&cols, None) {
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
                let first = lo.div_euclid(t0);
                let last = hi.div_euclid(t0);
                for b in first..=last {
                    let (start, end) = (b * t0, (b + 1) * t0);
                    if end > cutoff {
                        break; // bucket still open (or inside the margin)
                    }
                    match compact_table(conn, table, start, end, base)? {
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

    // --- higher tiers: promote finer files into closed windows -------
    for &w in &policy.tiers_us[1..] {
        // Candidate windows = distinct epoch-aligned windows containing
        // active files strictly finer than w, where the window itself is
        // fully behind the cutoff.
        let entries = catalog::entries_for_table(conn, table)?;
        let mut windows: Vec<i64> = entries
            .iter()
            .filter(|e| (e.range_end - e.range_start) < w)
            .map(|e| e.range_start.div_euclid(w))
            .collect();
        windows.sort_unstable();
        windows.dedup();
        for win in windows {
            let (start, end) = (win * w, (win + 1) * w);
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

    // --- GC superseded files ------------------------------------------
    for entry in catalog::superseded_entries(conn, table)? {
        match std::fs::remove_file(&entry.path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(MaintainError::Compact(e.into())),
        }
        catalog::delete_entry(conn, table, &entry.path)?;
        actions.push(MaintainAction::Gc { path: entry.path });
    }

    Ok(actions)
}
