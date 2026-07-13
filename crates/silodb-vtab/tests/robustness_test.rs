//! Hostile-input robustness: a catalog row can point at anything on disk.
//! Every case must surface as a clean SQLite error — never a panic, which
//! would unwind across the vtab FFI boundary.

mod common;

use common::{cold_env, write_id_ts_file, ColdEnv};

fn query_err(env: &ColdEnv, sql: &str) -> String {
    env.conn
        .prepare(sql)
        .unwrap()
        .query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err()
        .to_string()
}

/// One good file plus one hostile file registered in the catalog.
fn env_with_bad_file(bytes: &[u8]) -> ColdEnv {
    let env = cold_env();
    let good = env.table_dir.join("bucket-0.parquet");
    write_id_ts_file(&good, &[(1, 0), (2, 100)], 2);
    env.register(&good, 0, 1000, 2);

    let bad = env.table_dir.join("bucket-1000.parquet");
    std::fs::write(&bad, bytes).unwrap();
    env.register(&bad, 1000, 2000, 2);

    env.create_vtab(ColdEnv::ID_TS_SCHEMA);
    env
}

#[test]
fn empty_file_errors_cleanly() {
    let env = env_with_bad_file(b"");
    let err = query_err(&env, "SELECT id FROM cold");
    assert!(err.contains("silodb"), "{err}");
}

#[test]
fn random_garbage_errors_cleanly() {
    // Deterministic pseudo-garbage, no parquet magic anywhere.
    let junk: Vec<u8> = (0..4096u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
        .collect();
    let env = env_with_bad_file(&junk);
    let err = query_err(&env, "SELECT id FROM cold");
    assert!(err.contains("silodb"), "{err}");
}

#[test]
fn fake_magic_errors_cleanly() {
    // Correct magic bytes framing garbage: exercises footer parsing deeper.
    let mut bytes = b"PAR1".to_vec();
    bytes.extend(std::iter::repeat_n(0xABu8, 512));
    bytes.extend((64u32).to_le_bytes()); // bogus footer length
    bytes.extend(b"PAR1");
    let env = env_with_bad_file(&bytes);
    let err = query_err(&env, "SELECT id FROM cold");
    assert!(err.contains("silodb"), "{err}");
}

#[test]
fn truncated_real_file_errors_cleanly() {
    // A genuine parquet file cut off mid-way: valid prefix, missing footer.
    let env = cold_env();
    let good = env.table_dir.join("bucket-0.parquet");
    write_id_ts_file(&good, &[(1, 0), (2, 100), (3, 200), (4, 300)], 2);
    let full = std::fs::read(&good).unwrap();

    let bad = env.table_dir.join("bucket-1000.parquet");
    std::fs::write(&bad, &full[..full.len() / 2]).unwrap();
    env.register(&bad, 1000, 2000, 4);
    env.create_vtab(ColdEnv::ID_TS_SCHEMA);

    let err = query_err(&env, "SELECT id FROM cold");
    assert!(err.contains("silodb"), "{err}");
}

#[test]
fn directory_registered_as_file_errors_cleanly() {
    let env = cold_env();
    let sub = env.table_dir.join("not-a-file.parquet");
    std::fs::create_dir(&sub).unwrap();
    env.register(&sub, 0, 1000, 1);
    env.create_vtab(ColdEnv::ID_TS_SCHEMA);

    let err = query_err(&env, "SELECT id FROM cold");
    assert!(err.contains("silodb"), "{err}");
}

#[test]
fn hostile_vtab_arguments_error_cleanly() {
    let env = cold_env();
    for ddl in [
        // Unknown parameter.
        "CREATE VIRTUAL TABLE c1 USING silodb('d/', nonsense=1)",
        // Bare non-k=v argument.
        "CREATE VIRTUAL TABLE c2 USING silodb('d/', whatever)",
        // Empty path.
        "CREATE VIRTUAL TABLE c3 USING silodb('')",
        // No arguments at all.
        "CREATE VIRTUAL TABLE c4 USING silodb",
        // Garbage schema strings.
        "CREATE VIRTUAL TABLE c5 USING silodb('d/', schema='')",
        "CREATE VIRTUAL TABLE c6 USING silodb('d/', schema=',,,')",
        "CREATE VIRTUAL TABLE c7 USING silodb('d/', schema='x NUMERIC')",
        "CREATE VIRTUAL TABLE c8 USING silodb('d/', schema='value REAL')", // no ts
    ] {
        let err = env.conn.execute_batch(ddl).unwrap_err().to_string();
        assert!(err.contains("silodb"), "{ddl} → {err}");
    }
}
