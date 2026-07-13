//! Benchmark: silodb (hot+cold view over hourly parquet buckets) vs a
//! plain fully-hot indexed SQLite table vs DuckDB reading the very same
//! parquet files silodb wrote.
//!
//! Run: `cargo run -p silodb-bench --release [-- <out_dir> [rows]]`
//! DuckDB numbers require a `duckdb` CLI on PATH (skipped otherwise).
//!
//! Methodology: deterministic synthetic time series, 1 row/second, 16
//! sensor names round-robin. Each query gets 3 warmups + 10 timed runs;
//! median and min reported. Sizes measured after VACUUM so SQLite freelist
//! pages don't flatter or punish anyone.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rusqlite::Connection;

const DEFAULT_ROWS: i64 = 2_000_000;
const SENSORS: i64 = 16;
const US_PER_SEC: i64 = 1_000_000;
const BUCKET_US: i64 = 3600 * US_PER_SEC; // hourly buckets

struct Lcg(u64);
impl Lcg {
    fn next_f64(&mut self) -> f64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn median_min(mut times: Vec<f64>) -> (f64, f64) {
    times.sort_by(f64::total_cmp);
    (times[times.len() / 2], times[0])
}

/// 3 warmups + 10 timed runs of `f`; returns (median_ms, min_ms).
fn time_query(mut f: impl FnMut() -> i64) -> (f64, f64) {
    for _ in 0..3 {
        std::hint::black_box(f());
    }
    let times: Vec<f64> = (0..10)
        .map(|_| {
            let t = Instant::now();
            std::hint::black_box(f());
            t.elapsed().as_secs_f64() * 1e3
        })
        .collect();
    median_min(times)
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for e in entries.flatten() {
            let p = e.path();
            total += if p.is_dir() {
                dir_size(&p)
            } else {
                e.metadata().map(|m| m.len()).unwrap_or(0)
            };
        }
    }
    total
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / 1_048_576.0
}

struct QuerySet {
    /// (label, silodb/sqlite SQL, duckdb SQL)
    queries: Vec<(&'static str, String, String)>,
}

fn build_queries(rows: i64, table: &str, duck_src: &str) -> QuerySet {
    // Range anchors: an hour and a day in the middle of the data.
    let mid_hour = (rows / 2 / 3600) * BUCKET_US;
    let day_start = mid_hour;
    let day_end = day_start + 24 * BUCKET_US;
    let hour_end = mid_hour + BUCKET_US;

    let q = |sqlite: String, duck: String| (sqlite, duck);
    let mut queries = Vec::new();

    let (s, d) = q(
        format!("SELECT count(*), avg(value) FROM {table} WHERE ts >= {mid_hour} AND ts < {hour_end}"),
        format!("SELECT count(*), avg(value) FROM {duck_src} WHERE ts >= make_timestamp({mid_hour}) AND ts < make_timestamp({hour_end})"),
    );
    queries.push(("1h range agg (~0.2%)", s, d));

    let (s, d) = q(
        format!("SELECT count(*), avg(value) FROM {table} WHERE ts >= {day_start} AND ts < {day_end}"),
        format!("SELECT count(*), avg(value) FROM {duck_src} WHERE ts >= make_timestamp({day_start}) AND ts < make_timestamp({day_end})"),
    );
    queries.push(("24h range agg (~4%)", s, d));

    let (s, d) = q(
        format!("SELECT count(*), avg(value) FROM {table}"),
        format!("SELECT count(*), avg(value) FROM {duck_src}"),
    );
    queries.push(("full-history agg (100%)", s, d));

    let (s, d) = q(
        format!(
            "SELECT count(*) FROM {table} WHERE ts >= {day_start} AND ts < {day_end} AND name = 'sensor-7'"
        ),
        format!(
            "SELECT count(*) FROM {duck_src} WHERE ts >= make_timestamp({day_start}) AND ts < make_timestamp({day_end}) AND name = 'sensor-7'"
        ),
    );
    queries.push(("24h range + name filter", s, d));

    let (s, d) = q(
        format!("SELECT max(value) FROM (SELECT value FROM {table} WHERE ts >= {mid_hour} AND ts < {hour_end})"),
        format!("SELECT max(value) FROM (SELECT value FROM {duck_src} WHERE ts >= make_timestamp({mid_hour}) AND ts < make_timestamp({hour_end})) t"),
    );
    queries.push(("1h raw rows materialized", s, d));

    QuerySet { queries }
}

fn count_query(conn: &Connection, sql: &str) -> i64 {
    // Every benchmark query returns at least one aggregate row; sum the
    // first column as an i64-ish sink so nothing is optimized away.
    conn.query_row(sql, [], |r| r.get::<_, f64>(0).or(Ok(0.0)))
        .unwrap() as i64
}

fn insert_rows(conn: &Connection, table: &str, rows: i64) -> f64 {
    let mut rng = Lcg(42);
    let t = Instant::now();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare(&format!("INSERT INTO {table} VALUES (?1, ?2, ?3, ?4)"))
            .unwrap();
        for i in 0..rows {
            stmt.execute(rusqlite::params![
                i * US_PER_SEC,
                i,
                rng.next_f64() * 100.0,
                format!("sensor-{}", i % SENSORS),
            ])
            .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    t.elapsed().as_secs_f64()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let out: PathBuf = args
        .next()
        .map(Into::into)
        .unwrap_or_else(|| PathBuf::from("target/bench"));
    let rows: i64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_ROWS);
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).unwrap();
    let base = out.join("cold");

    println!("# silodb bench — {rows} rows, hourly buckets, 1 row/s\n");

    // ---------- silodb ----------
    let silo_db = out.join("silo.db");
    let conn = Connection::open(&silo_db).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
    silodb::load_module(&conn).unwrap();
    silodb::init_table(
        &conn,
        "readings",
        "ts TIMESTAMP, seq INTEGER, value REAL, name TEXT",
        &base,
    )
    .unwrap();

    let insert_s = insert_rows(&conn, "readings", rows);
    println!("hot insert: {:.2}s ({:.0} rows/s)", insert_s, rows as f64 / insert_s);

    let t = Instant::now();
    let buckets = rows * US_PER_SEC / BUCKET_US + 1;
    let mut files = 0;
    for b in 0..buckets {
        if let silodb::CompactOutcome::Compacted { .. } =
            silodb::compact_table(&conn, "readings", b * BUCKET_US, (b + 1) * BUCKET_US, &base)
                .unwrap()
        {
            files += 1;
        }
    }
    let compact_s = t.elapsed().as_secs_f64();
    println!(
        "compaction: {:.2}s ({:.0} rows/s) into {files} hourly files",
        compact_s,
        rows as f64 / compact_s
    );
    conn.execute_batch("VACUUM").unwrap();

    // ---------- plain fully-hot indexed SQLite ----------
    let plain_db = out.join("plain.db");
    let plain = Connection::open(&plain_db).unwrap();
    plain.pragma_update(None, "journal_mode", "WAL").unwrap();
    plain.pragma_update(None, "synchronous", "NORMAL").unwrap();
    plain
        .execute_batch(
            "CREATE TABLE readings (ts INTEGER, seq INTEGER, value REAL, name TEXT);",
        )
        .unwrap();
    let plain_insert_s = insert_rows(&plain, "readings", rows);
    let t = Instant::now();
    plain
        .execute_batch("CREATE INDEX idx_readings_ts ON readings(ts)")
        .unwrap();
    let index_s = t.elapsed().as_secs_f64();
    println!(
        "plain sqlite insert: {:.2}s, ts index build: {index_s:.2}s",
        plain_insert_s
    );
    plain.execute_batch("VACUUM").unwrap();

    // ---------- sizes ----------
    plain.pragma_update(None, "wal_checkpoint", "TRUNCATE").ok();
    conn.pragma_update(None, "wal_checkpoint", "TRUNCATE").ok();
    let silo_total = std::fs::metadata(&silo_db).map(|m| m.len()).unwrap_or(0) + dir_size(&base);
    let plain_total = std::fs::metadata(&plain_db).map(|m| m.len()).unwrap_or(0);
    println!("\n## on-disk size");
    println!(
        "- silodb: {:.1} MB  (hot.db {:.1} MB + parquet {:.1} MB)",
        mb(silo_total),
        mb(std::fs::metadata(&silo_db).map(|m| m.len()).unwrap_or(0)),
        mb(dir_size(&base)),
    );
    println!("- plain sqlite (+ts index): {:.1} MB", mb(plain_total));

    // ---------- queries ----------
    let duck_src = format!("read_parquet('{}/readings/*.parquet')", base.display());
    let qs = build_queries(rows, "readings", &duck_src);

    let mut table_md = String::new();
    writeln!(table_md, "\n## query latency, median ms (min ms)\n").unwrap();
    writeln!(
        table_md,
        "| query | silodb view | plain sqlite | duckdb on same parquet |"
    )
    .unwrap();
    writeln!(table_md, "|---|---|---|---|").unwrap();

    let duck_times = run_duckdb(&qs, &out);

    for (i, (label, sqlite_sql, _)) in qs.queries.iter().enumerate() {
        let (silo_med, silo_min) = time_query(|| count_query(&conn, sqlite_sql));
        let (plain_med, plain_min) = time_query(|| count_query(&plain, sqlite_sql));
        let duck = duck_times
            .as_ref()
            .map(|t| format!("{:.1} ({:.1})", t[i].0, t[i].1))
            .unwrap_or_else(|| "n/a".into());
        writeln!(
            table_md,
            "| {label} | {silo_med:.1} ({silo_min:.1}) | {plain_med:.1} ({plain_min:.1}) | {duck} |"
        )
        .unwrap();
    }
    println!("{table_md}");

    if let Some(stats) = silodb::last_scan_stats() {
        println!("(last silodb scan: {stats:?})");
    }
}

/// Run each duckdb query 13 times (3 warmups) in one CLI session with
/// `.timer on`; parse the "real" seconds. Returns per-query (median, min)
/// in ms, or None if no duckdb CLI.
fn run_duckdb(qs: &QuerySet, out: &Path) -> Option<Vec<(f64, f64)>> {
    let mut script = String::from(".timer on\n");
    for (_, _, duck_sql) in &qs.queries {
        for _ in 0..13 {
            script.push_str(duck_sql);
            script.push_str(";\n");
        }
    }
    let script_path = out.join("duck.sql");
    std::fs::write(&script_path, script).unwrap();

    let output = std::process::Command::new("duckdb")
        .arg("-batch")
        .stdin(std::fs::File::open(&script_path).unwrap())
        .output()
        .ok()?;
    if !output.status.success() {
        eprintln!(
            "duckdb failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut reals: Vec<f64> = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.trim().strip_prefix("Run Time (s): real") {
            let secs: f64 = rest.split_whitespace().next()?.parse().ok()?;
            reals.push(secs * 1e3);
        }
    }
    if reals.len() != qs.queries.len() * 13 {
        eprintln!(
            "duckdb: expected {} timings, got {}",
            qs.queries.len() * 13,
            reals.len()
        );
        return None;
    }
    Some(
        reals
            .chunks(13)
            .map(|chunk| median_min(chunk[3..].to_vec()))
            .collect(),
    )
}
