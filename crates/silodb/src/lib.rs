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
    base_dir: impl AsRef<Path>,
) -> Result<(), InitError> {
    init_table_tiered(conn, table, schema, base_dir, "1d")
}

/// [`init_table`] plus an explicit compaction-tier policy, e.g.
/// `"1d, 7d, 28d"`: hot rows compact into 1-day bucket files; once a
/// 7-day window is fully in the past its daily files merge into one
/// weekly file; likewise weekly → 28-day. Units: `s m h d w y` (y = 365d).
/// Each tier must be an exact multiple of the previous one (windows are
/// epoch-aligned; `30d` after `7d` would strand straddling files — use
/// `28d`).
///
/// An optional trailing `retain=<duration>` element sets the retention
/// policy: cold files entirely older than `now - retain` are evicted
/// (deleted) by [`maintain`], at whole-file granularity — a file
/// straddling the cutoff stays until all of it has expired. Example:
/// `"1d, 7d, 28d, retain=2y"`. Without it, history is kept forever.
/// The policy persists in `_silodb_policy`; [`maintain`] executes it.
pub fn init_table_tiered(
    conn: &Connection,
    table: &str,
    schema: &str,
    base_dir: impl AsRef<Path>,
    tiers: &str,
) -> Result<(), InitError> {
    let mut policy = catalog::parse_policy_string(table, tiers)
        .map_err(InitError::BadSchema)?;
    // Origin is immutable once set: every written file's windows are
    // anchored to it, and moving it would misalign all of them. Detect a
    // changed origin loudly instead of silently re-anchoring.
    if let Some(existing) = catalog::get_policy(conn, table)?
        && existing.origin_us != policy.origin_us
    {
        return Err(InitError::BadSchema(format!(
            "origin changed for '{table}' ({} -> {}); the window grid is \
             immutable once files exist — re-aligning is a migration, not a \
             knob",
            existing.origin_us, policy.origin_us
        )));
    }
    policy.logical_table = table.to_owned();
    catalog::set_policy(conn, &policy)?;
    init_table_inner(conn, table, schema, base_dir)
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
    let hot = resolve_hot_table(conn, table)
        .map_err(CompactError::Sqlite)?
        .unwrap_or_else(|| format!("{table}_hot"));
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
    base_dir: impl AsRef<Path>,
    now_us: i64,
) -> Result<Vec<MaintainAction>, MaintainError> {
    let base = base_dir.as_ref();
    let policy = catalog::get_policy(conn, table)?
        .ok_or_else(|| MaintainError::NoPolicy(table.to_owned()))?;
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
                let first = (lo - policy.origin_us).div_euclid(t0);
                let last = (hi - policy.origin_us).div_euclid(t0);
                for b in first..=last {
                    let (start, end) =
                        (b * t0 + policy.origin_us, (b + 1) * t0 + policy.origin_us);
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
    let ts_idx = silodb_schema::resolve_ts_index(&decls, None)
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
    let ts_idx = silodb_schema::resolve_ts_index(&columns, None)
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
