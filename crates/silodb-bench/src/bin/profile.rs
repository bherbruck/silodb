//! Profiling harness: loop hot queries against a dataset previously
//! produced by silodb-bench, so `perf record` sees mostly query work.
//!
//! Usage: cargo run -p silodb-bench --release --bin profile -- <bench_dir> [iters] [query]
//! where query is one of: full (default), narrow, week

use std::time::Instant;

use rusqlite::Connection;

fn main() {
    let mut args = std::env::args().skip(1);
    let dir: std::path::PathBuf = args
        .next()
        .map(Into::into)
        .unwrap_or_else(|| "target/bench".into());
    let iters: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(20);
    let which = args.next().unwrap_or_else(|| "full".into());

    let conn = Connection::open(dir.join("silo.db")).unwrap();
    silodb::load_module(&conn).unwrap();

    let sql = match which.as_str() {
        "narrow" => {
            "SELECT count(*), avg(value) FROM readings \
             WHERE ts >= 604800000000 AND ts < 604803600000000 \
             AND device = 'device-03' AND sensor = 'sensor-07'"
        }
        "week" => {
            "SELECT count(*), avg(value) FROM readings \
             WHERE ts >= 0 AND ts < 604800000000"
        }
        _ => "SELECT count(*), avg(value) FROM readings",
    };

    let t = Instant::now();
    let mut sink = 0.0f64;
    for _ in 0..iters {
        let (n, avg): (i64, f64) = conn
            .query_row(sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap();
        sink += n as f64 + avg;
    }
    let el = t.elapsed().as_secs_f64();
    println!(
        "{which}: {iters} iters in {el:.2}s ({:.1} ms/iter) sink={sink:.1}",
        el / iters as f64 * 1e3
    );
}
