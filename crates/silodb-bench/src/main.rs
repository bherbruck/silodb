//! Benchmark: silodb (hot+cold view over daily parquet buckets) vs a
//! plain fully-hot indexed SQLite table vs DuckDB reading the very same
//! parquet files silodb wrote.
//!
//! Scenario: one year of telemetry at 1-minute interval from 10 devices x
//! 10 sensors = 100 series -> 52.56M rows, daily buckets (365 files,
//! 144k rows each).
//!
//! Run: `cargo run -p silodb-bench --release [-- <out_dir> [days]]`
//! DuckDB numbers require a `duckdb` CLI on PATH (skipped otherwise).
//!
//! Methodology: deterministic synthetic data; each query gets 3 warmups +
//! 10 timed runs; median and min reported. Sizes measured after VACUUM +
//! WAL truncation.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rusqlite::Connection;

const DEFAULT_DAYS: i64 = 365;
const DEVICES: i64 = 10;
const SENSORS: i64 = 10;
const SERIES: i64 = DEVICES * SENSORS;
const INTERVAL_US: i64 = 60 * 1_000_000; // 1 minute
const BUCKET_US: i64 = 86_400 * 1_000_000; // daily buckets
const ROWS_PER_DAY: i64 = 1440 * SERIES;

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

fn build_queries(days: i64, table: &str, duck_src: &str) -> QuerySet {
    // Anchors in the middle of the year: a day boundary, noon of that day.
    let day = (days / 2) * BUCKET_US;
    let hour = day + 12 * 3600 * 1_000_000;
    let hour_end = hour + 3600 * 1_000_000;
    let day_end = day + BUCKET_US;
    let week_end = day + 7 * BUCKET_US;

    let mut queries = Vec::new();
    let mut push = |label, sqlite: String, duck: String| queries.push((label, sqlite, duck));

    push(
        "1h, one series (~0.0001%)",
        format!(
            "SELECT count(*), avg(value) FROM {table} WHERE ts >= {hour} AND ts < {hour_end} \
             AND device = 'device-03' AND sensor = 'sensor-07'"
        ),
        format!(
            "SELECT count(*), avg(value) FROM {duck_src} WHERE ts >= make_timestamp({hour}) \
             AND ts < make_timestamp({hour_end}) AND device = 'device-03' AND sensor = 'sensor-07'"
        ),
    );
    push(
        "1 day, all series (~0.27%)",
        format!("SELECT count(*), avg(value) FROM {table} WHERE ts >= {day} AND ts < {day_end}"),
        format!(
            "SELECT count(*), avg(value) FROM {duck_src} WHERE ts >= make_timestamp({day}) \
             AND ts < make_timestamp({day_end})"
        ),
    );
    push(
        "1 week, one series (~0.02%)",
        format!(
            "SELECT count(*), avg(value), min(value), max(value) FROM {table} \
             WHERE ts >= {day} AND ts < {week_end} \
             AND device = 'device-03' AND sensor = 'sensor-07'"
        ),
        format!(
            "SELECT count(*), avg(value), min(value), max(value) FROM {duck_src} \
             WHERE ts >= make_timestamp({day}) AND ts < make_timestamp({week_end}) \
             AND device = 'device-03' AND sensor = 'sensor-07'"
        ),
    );
    push(
        "1 week, all series (~2%)",
        format!(
            "SELECT count(*), avg(value) FROM {table} WHERE ts >= {day} AND ts < {week_end}"
        ),
        format!(
            "SELECT count(*), avg(value) FROM {duck_src} WHERE ts >= make_timestamp({day}) \
             AND ts < make_timestamp({week_end})"
        ),
    );
    push(
        "full year agg (100%)",
        format!("SELECT count(*), avg(value) FROM {table}"),
        format!("SELECT count(*), avg(value) FROM {duck_src}"),
    );

    QuerySet { queries }
}

fn count_query(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get::<_, f64>(0).or(Ok(0.0)))
        .unwrap() as i64
}

/// Insert `days` worth of telemetry in day-sized transactions.
///
/// Values are realistic, not adversarial: each series is a baseline + a
/// daily sinusoid + a small bounded random walk, quantized to 0.1 like a
/// real sensor would report. (Full-precision random doubles are
/// incompressible by definition and benchmark nothing but entropy.)
fn insert_rows(conn: &Connection, table: &str, days: i64) -> f64 {
    let devices: Vec<String> = (0..DEVICES).map(|d| format!("device-{d:02}")).collect();
    let sensors: Vec<String> = (0..SENSORS).map(|s| format!("sensor-{s:02}")).collect();
    let mut rng = Lcg(42);
    let mut walks = vec![0.0f64; SERIES as usize];
    let t = Instant::now();
    for day in 0..days {
        conn.execute_batch("BEGIN").unwrap();
        {
            let mut stmt = conn
                .prepare_cached(&format!("INSERT INTO {table} VALUES (?1, ?2, ?3, ?4)"))
                .unwrap();
            for minute in 0..1440 {
                let ts = day * BUCKET_US + minute * INTERVAL_US;
                let phase = (minute as f64 / 1440.0) * std::f64::consts::TAU;
                for (di, device) in devices.iter().enumerate() {
                    for (si, sensor) in sensors.iter().enumerate() {
                        let series = di * SENSORS as usize + si;
                        let walk = &mut walks[series];
                        *walk = (*walk + (rng.next_f64() - 0.5) * 0.2).clamp(-3.0, 3.0);
                        let v = 20.0 + series as f64 + 5.0 * phase.sin() + *walk;
                        let quantized = (v * 10.0).round() / 10.0;
                        stmt.execute(rusqlite::params![ts, device, sensor, quantized])
                            .unwrap();
                    }
                }
            }
        }
        conn.execute_batch("COMMIT").unwrap();
    }
    t.elapsed().as_secs_f64()
}

/// Bump when the generator or schema changes — invalidates cached datasets.
const DATASET_VERSION: u32 = 2;

/// Build the (silodb daily-compacted + plain indexed) dataset into `cache`
/// unless a completed one is already there. This is the expensive part —
/// two 52M-row inserts and 365 compactions at year scale — and it's
/// identical every time, so it happens once per (version, days).
fn ensure_dataset(cache: &Path, days: i64) {
    let done = cache.join(".complete");
    if done.is_file() && std::env::var_os("SILODB_BENCH_REBUILD").is_none() {
        println!("(dataset cache hit: {})", cache.display());
        return;
    }
    println!("(building dataset cache: {} — one-time cost)", cache.display());
    let _ = std::fs::remove_dir_all(cache);
    std::fs::create_dir_all(cache).unwrap();
    let base = cache.join("cold");
    let rows = days * ROWS_PER_DAY;

    // silodb side: hot insert then daily compaction.
    let conn = Connection::open(cache.join("silo.db")).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
    silodb::load_module(&conn).unwrap();
    silodb::init_table_tiered(
        &conn,
        "readings",
        "ts TIMESTAMP, device TEXT, sensor TEXT, value REAL",
        &base,
        "1d,7d,28d",
    )
    .unwrap();
    let insert_s = insert_rows(&conn, "readings", days);
    println!("  hot insert: {:.1}s ({:.0} rows/s)", insert_s, rows as f64 / insert_s);
    let t = Instant::now();
    let mut files = 0;
    for b in 0..days {
        if let silodb::CompactOutcome::Compacted { .. } =
            silodb::compact_table(&conn, "readings", b * BUCKET_US, (b + 1) * BUCKET_US, &base)
                .unwrap()
        {
            files += 1;
        }
    }
    println!(
        "  compaction: {:.1}s ({:.0} rows/s) into {files} daily files",
        t.elapsed().as_secs_f64(),
        rows as f64 / t.elapsed().as_secs_f64()
    );
    conn.execute_batch("VACUUM").unwrap();
    conn.pragma_update(None, "wal_checkpoint", "TRUNCATE").ok();
    drop(conn);

    // plain side: one big indexed table.
    let plain = Connection::open(cache.join("plain.db")).unwrap();
    plain.pragma_update(None, "journal_mode", "WAL").unwrap();
    plain.pragma_update(None, "synchronous", "NORMAL").unwrap();
    plain
        .execute_batch(
            "CREATE TABLE readings (ts INTEGER, device TEXT, sensor TEXT, value REAL);",
        )
        .unwrap();
    let plain_insert_s = insert_rows(&plain, "readings", days);
    let t = Instant::now();
    plain
        .execute_batch("CREATE INDEX idx_readings_ts ON readings(ts)")
        .unwrap();
    println!(
        "  plain sqlite insert: {plain_insert_s:.1}s, ts index build: {:.1}s",
        t.elapsed().as_secs_f64()
    );
    plain.pragma_update(None, "wal_checkpoint", "TRUNCATE").ok();
    drop(plain);

    std::fs::write(done, b"ok").unwrap();
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let dest = to.join(e.file_name());
        if e.path().is_dir() {
            copy_dir(&e.path(), &dest);
        } else {
            std::fs::copy(e.path(), &dest).unwrap();
        }
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let out: PathBuf = args
        .next()
        .map(Into::into)
        .unwrap_or_else(|| PathBuf::from("target/bench"));
    let days: i64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_DAYS);
    let rows = days * ROWS_PER_DAY;

    println!(
        "# silodb bench — {days} days x {DEVICES} devices x {SENSORS} sensors @ 1min = {rows} rows, daily buckets\n"
    );

    let cache = PathBuf::from(format!("target/bench-cache/v{DATASET_VERSION}-{days}d"));
    ensure_dataset(&cache, days);

    // The silodb side gets mutated by tiered maintenance → work on a copy
    // (small: hot.db is ~empty + the parquet dir). The plain 3 GB db is
    // only ever queried → used straight from the cache.
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).unwrap();
    let t = Instant::now();
    let silo_db = out.join("silo.db");
    let base = out.join("cold");
    std::fs::copy(cache.join("silo.db"), &silo_db).unwrap();
    copy_dir(&cache.join("cold"), &base);
    println!("(dataset staged from cache in {:.1}s)", t.elapsed().as_secs_f64());

    let conn = Connection::open(&silo_db).unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
    silodb::load_module(&conn).unwrap();
    // Catalog paths are stored verbatim — re-point them at the copy, or
    // this run's GC would delete the cache's files.
    conn.execute(
        "UPDATE _silodb_catalog SET path = replace(path, ?1, ?2)",
        rusqlite::params![
            cache.join("cold").display().to_string(),
            base.display().to_string()
        ],
    )
    .unwrap();

    let plain = Connection::open(cache.join("plain.db")).unwrap();

    // ---------- sizes ----------
    let files = silodb::catalog::entries_for_table(&conn, "readings")
        .unwrap()
        .len();
    let silo_hot = std::fs::metadata(&silo_db).map(|m| m.len()).unwrap_or(0);
    let silo_cold = dir_size(&base);
    let plain_total = std::fs::metadata(cache.join("plain.db"))
        .map(|m| m.len())
        .unwrap_or(0);
    println!("\n## on-disk size");
    println!(
        "- silodb: {:.1} MB  (hot.db {:.1} MB + parquet {:.1} MB, {files} files)",
        mb(silo_hot + silo_cold),
        mb(silo_hot),
        mb(silo_cold),
    );
    println!("- plain sqlite (+ts index): {:.1} MB", mb(plain_total));

    // ---------- queries ----------
    let duck_src = format!("read_parquet('{}/readings/*.parquet')", base.display());
    let qs = build_queries(days, "readings", &duck_src);

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
        let stats = silodb::last_scan_stats();
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
        if let Some(s) = stats {
            eprintln!(
                "  [{label}] files {}/{} rg {}/{}",
                s.scanned_files, s.total_files, s.scanned_row_groups, s.total_row_groups
            );
        }
    }
    println!("{table_md}");

    // ---------- tiered maintenance (1d -> 7d -> 28d), then re-measure ----
    let t = Instant::now();
    // Clock just past the end of the data + safety margin: everything due.
    let now = days * BUCKET_US + 3 * 3600 * 1_000_000;
    let actions = silodb::maintain(&conn, "readings", &base, now).unwrap();
    let (mut merges, mut gcs) = (0, 0);
    for a in &actions {
        match a {
            silodb::MaintainAction::Merged { .. } => merges += 1,
            silodb::MaintainAction::Gc { .. } => gcs += 1,
            silodb::MaintainAction::Compacted { .. } => {}
        }
    }
    let active = silodb::catalog::entries_for_table(&conn, "readings")
        .unwrap()
        .len();
    println!(
        "\n## tiered maintenance (1d,7d,28d): {merges} merges, {gcs} files GC'd in {:.1}s -> {active} active files, parquet {:.1} MB",
        t.elapsed().as_secs_f64(),
        mb(dir_size(&base)),
    );

    let duck_times = run_duckdb(&qs, &out);
    let mut tiered_md = String::new();
    writeln!(
        tiered_md,
        "\n| query | silodb tiered | duckdb tiered |\n|---|---|---|"
    )
    .unwrap();
    for (i, (label, sqlite_sql, _)) in qs.queries.iter().enumerate() {
        let (med, min) = time_query(|| count_query(&conn, sqlite_sql));
        let duck = duck_times
            .as_ref()
            .map(|t| format!("{:.1} ({:.1})", t[i].0, t[i].1))
            .unwrap_or_else(|| "n/a".into());
        writeln!(tiered_md, "| {label} | {med:.1} ({min:.1}) | {duck} |").unwrap();
    }
    println!("{tiered_md}");

    duckdb_native(&qs, &out, &base, days);
}

/// DuckDB as a storage engine, not just a parquet reader: bulk ingest into
/// a native table (its best case), query it, and sample row-at-a-time SQL
/// ingest (its documented weak spot — measured, not asserted).
fn duckdb_native(qs: &QuerySet, out: &Path, base: &Path, days: i64) {
    let db = out.join("duck.db");
    let _ = std::fs::remove_file(&db);

    // Bulk load from the (tiered) parquet + timed queries on the native table.
    let mut script = String::from(".timer on\n");
    script.push_str(&format!(
        "CREATE TABLE readings AS SELECT ts, device, sensor, value \
         FROM read_parquet('{}/readings/*.parquet');\n",
        base.display()
    ));
    for (_, _, duck_sql) in &qs.queries {
        let native = duck_sql.replace(
            &format!("read_parquet('{}/readings/*.parquet')", base.display()),
            "readings",
        );
        for _ in 0..13 {
            script.push_str(&native);
            script.push_str(";\n");
        }
    }
    script.push_str("CHECKPOINT;\n");
    let script_path = out.join("duck_native.sql");
    std::fs::write(&script_path, &script).unwrap();
    let output = std::process::Command::new("duckdb")
        .arg("-batch")
        .arg(&db)
        .stdin(std::fs::File::open(&script_path).unwrap())
        .output()
        .ok();
    let Some(output) = output.filter(|o| o.status.success()) else {
        println!("\n(duckdb native phase skipped: CLI failed)");
        return;
    };
    let text = String::from_utf8_lossy(&output.stdout);
    let reals: Vec<f64> = text
        .lines()
        .filter_map(|l| l.trim().strip_prefix("Run Time (s): real"))
        .filter_map(|rest| rest.split_whitespace().next()?.parse::<f64>().ok())
        .map(|s| s * 1e3)
        .collect();
    let expected = 1 + qs.queries.len() * 13 + 1; // CTAS + queries + CHECKPOINT
    if reals.len() != expected {
        println!("\n(duckdb native phase skipped: expected {expected} timings, got {})", reals.len());
        return;
    }
    let bulk_s = reals[0] / 1e3;
    let rows = days * ROWS_PER_DAY;
    let native_size = std::fs::metadata(&db).map(|m| m.len()).unwrap_or(0);
    println!(
        "\n## duckdb native table\nbulk ingest (parquet -> native, its best case): {bulk_s:.1}s ({:.0} rows/s); db size {:.1} MB",
        rows as f64 / bulk_s,
        mb(native_size),
    );

    let mut md = String::new();
    writeln!(md, "\n| query | duckdb native table |\n|---|---|").unwrap();
    for (i, (label, _, _)) in qs.queries.iter().enumerate() {
        let (med, min) = median_min(reals[1 + i * 13 + 3..1 + (i + 1) * 13].to_vec());
        writeln!(md, "| {label} | {med:.1} ({min:.1}) |").unwrap();
    }
    println!("{md}");

    // Row-at-a-time SQL ingest sample: one day of rows (144k) as single-row
    // INSERTs inside day-sized transactions, mirroring the sqlite loop.
    let sample_rows = ROWS_PER_DAY;
    let mut rng = Lcg(7);
    let mut ins = String::with_capacity(20 << 20);
    ins.push_str("CREATE TABLE rw (ts TIMESTAMP, device VARCHAR, sensor VARCHAR, value DOUBLE);\nBEGIN;\n");
    for minute in 0..1440 {
        for d in 0..DEVICES {
            for s in 0..SENSORS {
                let v = (rng.next_f64() * 1000.0).round() / 10.0;
                ins.push_str(&format!(
                    "INSERT INTO rw VALUES (make_timestamp({}), 'device-{d:02}', 'sensor-{s:02}', {v});\n",
                    minute * INTERVAL_US
                ));
            }
        }
    }
    ins.push_str("COMMIT;\n");
    let ins_path = out.join("duck_rowwise.sql");
    std::fs::write(&ins_path, &ins).unwrap();
    let t = Instant::now();
    let ok = std::process::Command::new("duckdb")
        .arg("-batch")
        .arg(out.join("duck_rw.db"))
        .stdin(std::fs::File::open(&ins_path).unwrap())
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if ok {
        let el = t.elapsed().as_secs_f64();
        println!(
            "row-at-a-time SQL ingest sample ({sample_rows} rows, one txn): {el:.1}s ({:.0} rows/s)",
            sample_rows as f64 / el
        );
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
