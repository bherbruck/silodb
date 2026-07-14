//! It's still just SQLite: silodb tables join with ordinary tables, the
//! internals are inspectable with vanilla SQL, and nothing stops you from
//! keeping plain tables right beside tiered ones in the same database.
//!
//! Run: `cargo run -p silodb-examples --bin plain_sqlite`

use rusqlite::{params, Connection};

const HOUR: i64 = 3600 * 1_000_000;
const DAY: i64 = 24 * HOUR;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let demo = std::path::PathBuf::from("target/silodb-plain-demo");
    let _ = std::fs::remove_dir_all(&demo);
    std::fs::create_dir_all(&demo)?;
    let conn = Connection::open(demo.join("app.db"))?;
    silodb::load_module(&conn)?;

    println!("== one database, mixed tables ========================");
    // An ordinary application table — no silodb involvement at all.
    conn.execute_batch(
        "CREATE TABLE devices (
            device   TEXT PRIMARY KEY,
            location TEXT,
            alarm_threshold REAL
         );
         INSERT INTO devices VALUES
            ('boiler',  'basement',  28.0),
            ('chiller', 'roof',      24.0),
            ('pump',    'well house', 26.0);",
    )?;
    // A tiered time-series table next to it.
    silodb::init_table_tiered(
        &conn,
        "readings",
        "ts TIMESTAMP, device TEXT, value REAL",
        "1d,7d",
    )?;

    // Three days of data, then age it out so the join below spans tiers.
    for d in 0..3i64 {
        for h in 0..24 {
            for (dev, base) in [("boiler", 26.0), ("chiller", 21.0), ("pump", 24.0)] {
                conn.execute(
                    "INSERT INTO readings VALUES (?1, ?2, ?3)",
                    params![d * DAY + h * HOUR, dev, base + (h % 6) as f64],
                )?;
            }
        }
    }
    silodb::maintain(&conn, "readings", 3 * DAY + 3 * HOUR)?;

    println!("\n== JOIN across a tiered table and a plain table ======");
    // This is the thing TSDB query languages can't do: readings is
    // (mostly) parquet on disk, devices is a normal SQLite table, and
    // it's just SQL.
    let mut stmt = conn.prepare(
        "SELECT r.device, d.location, count(*) AS over_threshold
         FROM readings r
         JOIN devices d USING (device)
         WHERE r.value > d.alarm_threshold
         GROUP BY 1, 2 ORDER BY 3 DESC",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let (dev, loc, n): (String, String, i64) =
            (row.get(0)?, row.get(1)?, row.get(2)?);
        println!("  {dev:<8} ({loc:<10}) exceeded its alarm threshold {n} times");
    }
    let s = silodb::last_scan_stats().unwrap();
    println!(
        "  (the tiered side of that join read {}/{} cold files)",
        s.scanned_files, s.total_files
    );

    println!("\n== the internals are plain SQL too ===================");
    // The hot tier is a real table; the catalog and policy are real tables.
    let hot: i64 = conn.query_row("SELECT count(*) FROM readings_hot", [], |r| r.get(0))?;
    println!("  readings_hot (ordinary table): {hot} rows after maintenance");
    let mut stmt = conn.prepare(
        "SELECT status, count(*), sum(row_count) FROM _silodb_catalog GROUP BY 1",
    )?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let (status, files, rc): (String, i64, i64) =
            (row.get(0)?, row.get(1)?, row.get(2)?);
        println!("  catalog: {files} '{status}' entr{} covering {rc} rows",
            if files == 1 { "y" } else { "ies" });
    }
    let tiers: String = conn.query_row(
        "SELECT tiers_us FROM _silodb_policy WHERE logical_table = 'readings'",
        [],
        |r| r.get(0),
    )?;
    println!("  policy: tiers_us = {tiers} (µs windows, plain row)");

    println!("\n== and plain tables stay plain ========================");
    // Nothing about the database is silodb-flavored unless you ask:
    // devices above never touches parquet, triggers, or policies.
    let n: i64 = conn.query_row("SELECT count(*) FROM devices", [], |r| r.get(0))?;
    println!("  devices: {n} rows, zero silodb machinery — just SQLite");
    Ok(())
}
