//! A narrated tour of silodb: ten days of sensor data through the whole
//! lifecycle — ingest, tiered compaction, pruned queries, continuous
//! aggregates, late data, retention.
//!
//! Run: `cargo run -p silodb-examples --bin quickstart`

use rusqlite::{params, Connection};

const HOUR: i64 = 3600 * 1_000_000;
const DAY: i64 = 24 * HOUR;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let demo = std::path::PathBuf::from("target/silodb-demo");
    let _ = std::fs::remove_dir_all(&demo);
    std::fs::create_dir_all(&demo)?;
    let db_path = demo.join("hot.db");

    println!("== boot ==============================================");
    let conn = Connection::open(&db_path)?;
    silodb::load_module(&conn)?; // per-connection, every open
    // One call defines the table. Cold files default to hot.db.silodb/
    // (next to the database); tiers: daily files merge into weekly.
    silodb::init_table_tiered(
        &conn,
        "readings",
        "ts TIMESTAMP, device TEXT, value REAL",
        "1d,7d",
    )?;
    let policy = silodb::catalog::get_policy(&conn, "readings")?.unwrap();
    println!("table 'readings' ready; cold dir = {}", policy.base_dir);

    println!("\n== ingest: 10 days, 3 devices, every minute ==========");
    let t = std::time::Instant::now();
    conn.execute_batch("BEGIN")?;
    for d in 0..10i64 {
        for m in 0..1440 {
            let ts = d * DAY + m * 60 * 1_000_000;
            for dev in ["boiler", "chiller", "pump"] {
                let v = 20.0 + (m as f64 / 1440.0 * std::f64::consts::TAU).sin() * 5.0;
                conn.execute(
                    "INSERT INTO readings VALUES (?1, ?2, ?3)",
                    params![ts, dev, (v * 10.0).round() / 10.0],
                )?;
            }
        }
    }
    conn.execute_batch("COMMIT")?;
    let rows: i64 = conn.query_row("SELECT count(*) FROM readings", [], |r| r.get(0))?;
    println!("{rows} rows in {:.1}s — all hot (plain SQLite table)", t.elapsed().as_secs_f64());

    println!("\n== maintain: one call, clock at day 10 ===============");
    // In production this runs on a dumb timer with the real clock; here the
    // clock is simulated. It compacts every closed day AND merges the
    // first full week into one file, then GCs the daily children.
    let now = 10 * DAY + 3 * HOUR;
    let actions = silodb::maintain(&conn, "readings", now)?;
    let mut compacted = 0;
    let mut merged = 0;
    let mut gcd = 0;
    for a in &actions {
        match a {
            silodb::MaintainAction::Compacted { .. } => compacted += 1,
            silodb::MaintainAction::Merged { .. } => merged += 1,
            silodb::MaintainAction::Gc { .. } => gcd += 1,
            _ => {}
        }
    }
    println!("{compacted} buckets compacted, {merged} weekly merge, {gcd} files GC'd");
    let hot: i64 = conn.query_row("SELECT count(*) FROM readings_hot", [], |r| r.get(0))?;
    println!("hot tier now holds {hot} rows; the view still shows all {rows}");

    println!("\ncold files on disk:");
    for e in silodb::catalog::entries_for_table(&conn, "readings")? {
        let size = std::fs::metadata(&e.path).map(|m| m.len()).unwrap_or(0);
        println!(
            "  days {:>2}..{:<2}  {:>7.1} KB  {}",
            e.range_start / DAY,
            e.range_end / DAY,
            size as f64 / 1024.0,
            std::path::Path::new(&e.path).file_name().unwrap().to_string_lossy()
        );
    }

    println!("\n== queries prune, invisibly ==========================");
    let avg: f64 = conn.query_row(
        "SELECT avg(value) FROM readings
         WHERE ts >= ?1 AND ts < ?2 AND device = 'boiler'",
        params![2 * DAY, 3 * DAY],
        |r| r.get(0),
    )?;
    let s = silodb::last_scan_stats().unwrap();
    println!(
        "day 2 boiler avg = {avg:.2} — touched {}/{} files, {}/{} row groups",
        s.scanned_files, s.total_files, s.scanned_row_groups, s.total_row_groups
    );

    println!("\n== continuous aggregate (declare-anytime) ============");
    silodb::create_rollup(&conn, "readings", "1h")?; // backfills from cold files
    silodb::create_rollup_view(&conn, "readings", "1h")?;
    let (n, avg): (i64, f64) = conn.query_row(
        "SELECT value_count, value_avg FROM readings_1h
         WHERE ts = silodb_bucket('1h', ?1) AND device = 'pump'",
        [5 * DAY + 12 * HOUR],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    println!("day 5, noon hour, pump: {n} samples, avg {avg:.2} (one indexed row)");

    println!("\n== late data self-heals ==============================");
    conn.execute(
        "INSERT INTO readings VALUES (?1, 'boiler', 99.9)",
        params![3 * DAY + HOUR + 17],
    )?;
    let actions = silodb::maintain(&conn, "readings", now)?;
    println!(
        "late row for day 3 → {} actions (compact follow-up, re-merge week, GC)",
        actions.len()
    );
    let total: i64 = conn.query_row("SELECT count(*) FROM readings", [], |r| r.get(0))?;
    println!("view total = {total} (the late row, exactly once)");

    println!("\n== the files are just parquet ========================");
    println!(
        "open them with anything:\n  duckdb -c \"SELECT device, avg(value) FROM read_parquet('{}/readings/*.parquet') GROUP BY 1\"",
        policy.base_dir
    );
    Ok(())
}
