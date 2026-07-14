//! The SQL-only surface: after `load_module`, everything happens in SQL —
//! the exact experience the loadable extension will give Python/Node/CLI
//! users. TimescaleDB-style: plain CREATE TABLE, then convert in place.
//!
//! Run: `cargo run -p silodb --example sql_only`

use rusqlite::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let demo = std::path::PathBuf::from("target/silodb-sql-demo");
    let _ = std::fs::remove_dir_all(&demo);
    std::fs::create_dir_all(&demo)?;
    let conn = Connection::open(demo.join("hot.db"))?;
    silodb::load_module(&conn)?; // the only Rust line

    let script = String::from(
        r#"
        -- plain DDL — the TIMESTAMP column is the bucket axis
        CREATE TABLE readings (ts TIMESTAMP, device TEXT, value REAL);

        -- convert in place, create_hypertable-style (rows would survive)
        SELECT silodb_create_table('readings', NULL, '1d,7d');

        -- a week of hourly data via the timestamp helpers
        WITH RECURSIVE hours(h) AS (SELECT 0 UNION ALL SELECT h+1 FROM hours WHERE h < 8*24-1)
        INSERT INTO readings
        SELECT silodb_ts('2026-07-01') + h*3600000000, 'boiler', 20.0 + (h % 24)
        FROM hours;

        -- run the declared policy: compacts closed days, merges the week
        SELECT 'maintain actions: ' || silodb_maintain('readings', silodb_ts('2026-07-09T03:00:00Z'));

        -- everything still one name, with real-date helpers both ways
        SELECT 'rows total:      ' || count(*) FROM readings;
        SELECT 'day 3 avg:       ' || round(avg(value), 2) FROM readings
          WHERE ts >= silodb_bucket('1d', silodb_ts('2026-07-04'))
            AND ts <  silodb_bucket('1d', silodb_ts('2026-07-05'));
        SELECT 'first sample:    ' || silodb_datetime(min(ts)) FROM readings;

        -- the catalog is honest SQL too
        SELECT 'cold file:       ' || path || '  [' || silodb_datetime(range_start)
               || ' .. ' || silodb_datetime(range_end) || ')'
        FROM _silodb_catalog WHERE status = 'active' ORDER BY range_start;
        "#,
    );

    // Run the script, printing every row-returning statement's first column.
    for stmt_sql in script.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        let mut stmt = conn
            .prepare(stmt_sql)
            .map_err(|e| format!("prepare failed: {e}\n--- statement ---\n{stmt_sql}"))?;
        if stmt.column_count() > 0 {
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                if let Ok(text) = row.get::<_, String>(0) {
                    println!("{text}");
                }
            }
        } else {
            stmt.execute([])
                .map_err(|e| format!("execute failed: {e}\n--- statement ---\n{stmt_sql}"))?;
        }
    }
    Ok(())
}
