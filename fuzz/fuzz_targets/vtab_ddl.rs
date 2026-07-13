//! Fuzz the vtab argument surface: arbitrary text spliced into
//! CREATE VIRTUAL TABLE ... USING silodb(...) must error or succeed, never
//! panic. Exercises the arg parser, schema= parser, ts resolution, and the
//! hot-table borrow path (a `readings` table exists in the connection).

#![no_main]

use libfuzzer_sys::fuzz_target;
use rusqlite::Connection;

thread_local! {
    static CONN: Connection = {
        let conn = Connection::open_in_memory().unwrap();
        silodb::load_module(&conn).unwrap();
        conn.execute_batch(
            "CREATE TABLE readings (ts TIMESTAMP, value REAL, name TEXT)",
        )
        .unwrap();
        conn
    };
}

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    // Interior NULs would terminate the C string early; skip those inputs.
    if s.contains('\0') {
        return;
    }
    CONN.with(|conn| {
        let ddl = format!("CREATE VIRTUAL TABLE fuzzed USING silodb({s})");
        if conn.execute_batch(&ddl).is_ok() {
            // Creation succeeded: a scan must also not panic.
            let _ = conn
                .prepare("SELECT count(*) FROM fuzzed")
                .and_then(|mut st| st.query_row([], |r| r.get::<_, i64>(0)));
            let _ = conn.execute_batch("DROP TABLE fuzzed");
        }
    });
});
