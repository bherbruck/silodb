//! Provisioned API keys: server-owned tables in the same database (the
//! engine never touches them), inspectable with plain SQL like everything
//! else. Secrets are random, stored only as SHA-256, and shown exactly
//! once at creation. Scopes are a proper many-to-many: no scope rows =
//! unscoped key (same reach as the env tokens of the matching role).

use crate::auth::Role;
use rusqlite::{Connection, OptionalExtension};
use sha2::{Digest, Sha256};

/// A resolved credential: what the caller may do (role) and where
/// (scope). `scope: None` = every table.
#[derive(Clone, Debug, PartialEq)]
pub struct Auth {
    pub role: Role,
    pub scope: Option<Vec<String>>,
    /// Key name for provisioned keys; None for env tokens.
    pub key_name: Option<String>,
}

impl Auth {
    pub fn unscoped(role: Role) -> Auth {
        Auth {
            role,
            scope: None,
            key_name: None,
        }
    }

    pub fn allows_table(&self, table: &str) -> bool {
        match &self.scope {
            None => true,
            Some(tables) => tables.iter().any(|t| t == table),
        }
    }
}

pub fn ensure_tables(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _silodb_server_keys (
            id           INTEGER PRIMARY KEY,
            name         TEXT NOT NULL UNIQUE,
            secret_hash  TEXT NOT NULL UNIQUE,
            role         TEXT NOT NULL CHECK (role IN ('read','write','ddl')),
            created_at   INTEGER NOT NULL,
            revoked      INTEGER NOT NULL DEFAULT 0
         );
         CREATE TABLE IF NOT EXISTS _silodb_server_key_scopes (
            key_id        INTEGER NOT NULL REFERENCES _silodb_server_keys(id),
            logical_table TEXT NOT NULL,
            PRIMARY KEY (key_id, logical_table)
         );",
    )
}

fn hash_secret(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

fn role_str(role: Role) -> &'static str {
    match role {
        Role::ReadOnly => "read",
        Role::ReadWrite => "write",
        Role::Ddl => "ddl",
    }
}

fn parse_role(s: &str) -> Option<Role> {
    match s {
        "read" | "readonly" => Some(Role::ReadOnly),
        "write" | "readwrite" => Some(Role::ReadWrite),
        "ddl" => Some(Role::Ddl),
        _ => None,
    }
}

/// Create a key; returns the secret — the only time it exists in the
/// clear. Scope tables are recorded as given; they don't have to exist
/// yet (provision the key, create the table later, either order).
pub fn create(
    conn: &Connection,
    name: &str,
    role: &str,
    scope: &[String],
    created_at: i64,
) -> Result<String, String> {
    let role = parse_role(role).ok_or_else(|| format!("bad role '{role}' (read|write|ddl)"))?;
    ensure_tables(conn).map_err(|e| e.to_string())?;
    let mut raw = [0u8; 24];
    getrandom::fill(&mut raw).map_err(|e| e.to_string())?;
    let secret = format!(
        "sk_{}",
        raw.iter().map(|b| format!("{b:02x}")).collect::<String>()
    );
    conn.execute(
        "INSERT INTO _silodb_server_keys (name, secret_hash, role, created_at)
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![name, hash_secret(&secret), role_str(role), created_at],
    )
    .map_err(|e| match e {
        rusqlite::Error::SqliteFailure(f, _)
            if f.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            format!("key '{name}' already exists")
        }
        e => e.to_string(),
    })?;
    let key_id = conn.last_insert_rowid();
    for table in scope {
        conn.execute(
            "INSERT OR IGNORE INTO _silodb_server_key_scopes (key_id, logical_table)
             VALUES (?1, ?2)",
            rusqlite::params![key_id, table],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(secret)
}

/// Revoke by name. The row stays (audit trail); the hash stops matching
/// anything at lookup time.
pub fn revoke(conn: &Connection, name: &str) -> Result<bool, String> {
    ensure_tables(conn).map_err(|e| e.to_string())?;
    let n = conn
        .execute(
            "UPDATE _silodb_server_keys SET revoked = 1 WHERE name = ?1",
            [name],
        )
        .map_err(|e| e.to_string())?;
    Ok(n > 0)
}

/// Resolve a presented secret to its Auth. Lookup is by hash, so timing
/// reveals nothing about the secret itself.
pub fn lookup(conn: &Connection, secret: &str) -> rusqlite::Result<Option<Auth>> {
    let has: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_silodb_server_keys'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if has.is_none() {
        return Ok(None);
    }
    let row: Option<(i64, String, String)> = conn
        .query_row(
            "SELECT id, name, role FROM _silodb_server_keys
             WHERE secret_hash = ?1 AND revoked = 0",
            [hash_secret(secret)],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((id, name, role)) = row else {
        return Ok(None);
    };
    let Some(role) = parse_role(&role) else {
        return Ok(None);
    };
    let tables: Vec<String> = conn
        .prepare(
            "SELECT logical_table FROM _silodb_server_key_scopes
             WHERE key_id = ?1 ORDER BY 1",
        )?
        .query_map([id], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(Some(Auth {
        role,
        scope: if tables.is_empty() { None } else { Some(tables) },
        key_name: Some(name),
    }))
}

/// List keys for the admin API — names, roles, scopes; never secrets.
/// Runs on read-only connections: probe rather than ensure (a fresh
/// database simply has no keys yet).
pub fn list(conn: &Connection) -> rusqlite::Result<Vec<serde_json::Value>> {
    let has: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_silodb_server_keys'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if has.is_none() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT k.id, k.name, k.role, k.created_at, k.revoked
         FROM _silodb_server_keys k ORDER BY k.name",
    )?;
    let rows: Vec<(i64, String, String, i64, i64)> = stmt
        .query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })?
        .collect::<Result<_, _>>()?;
    for (id, name, role, created_at, revoked) in rows {
        let scope: Vec<String> = conn
            .prepare(
                "SELECT logical_table FROM _silodb_server_key_scopes
                 WHERE key_id = ?1 ORDER BY 1",
            )?
            .query_map([id], |r| r.get(0))?
            .collect::<Result<_, _>>()?;
        out.push(serde_json::json!({
            "name": name,
            "role": role,
            "scope": if scope.is_empty() { serde_json::Value::Null } else { serde_json::json!(scope) },
            "created_at": created_at,
            "revoked": revoked != 0,
        }));
    }
    Ok(out)
}

/// The tables a scoped key may touch through SQL, given its logical
/// tables: the single-name surface plus the engine's per-table artifacts.
/// Engine internals (`_silodb_*`, `sqlite_*`) are readable — the catalog
/// is metadata, and the vtab reads it on the caller's connection.
pub fn sql_read_allowed(scope: &[String], table: &str) -> bool {
    // The server's own tables hold credential material (key hashes,
    // scopes) — never readable through SQL except by unscoped ddl.
    if table.starts_with("_silodb_server_") {
        return false;
    }
    if table.starts_with("_silodb_") || table.starts_with("sqlite_") {
        return true;
    }
    scope.iter().any(|t| {
        table == t
            || table
                .strip_prefix(t.as_str())
                .and_then(|rest| rest.strip_prefix('_'))
                .is_some_and(|suffix| {
                    matches!(suffix, "hot" | "data" | "cold" | "stats" | "insert")
                        || suffix.starts_with("rollup_")
                        // grain views: <table>_1h, <table>_30m, …
                        || (suffix.starts_with(|c: char| c.is_ascii_digit())
                            && suffix.chars().all(|c| c.is_ascii_alphanumeric()))
                })
    })
}

/// DML (INSERT/UPDATE/DELETE) targets for a scoped key: the logical
/// table and its hot tier only — never engine internals or artifacts.
pub fn sql_write_allowed(scope: &[String], table: &str) -> bool {
    scope.iter().any(|t| {
        table == t
            || table == format!("{t}_hot")
            || table == format!("{t}_data")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_expansion() {
        let scope = vec!["weather".to_string()];
        for ok in [
            "weather", "weather_hot", "weather_data", "weather_cold", "weather_stats",
            "weather_rollup_1h", "weather_1h", "weather_30m", "_silodb_catalog",
            "sqlite_master",
        ] {
            assert!(sql_read_allowed(&scope, ok), "{ok}");
        }
        for bad in ["intruder", "weather2", "weatherx_hot", "weather_secret"] {
            assert!(!sql_read_allowed(&scope, bad), "{bad}");
        }
        assert!(sql_write_allowed(&scope, "weather"));
        assert!(sql_write_allowed(&scope, "weather_hot"));
        assert!(!sql_write_allowed(&scope, "weather_stats"));
        assert!(!sql_write_allowed(&scope, "_silodb_catalog"));
    }

    #[test]
    fn key_lifecycle() {
        let conn = Connection::open_in_memory().unwrap();
        let secret = create(&conn, "site-a", "write", &["weather".into()], 42).unwrap();
        assert!(secret.starts_with("sk_"));
        let auth = lookup(&conn, &secret).unwrap().unwrap();
        assert_eq!(auth.role, Role::ReadWrite);
        assert_eq!(auth.scope, Some(vec!["weather".to_string()]));
        assert!(auth.allows_table("weather"));
        assert!(!auth.allows_table("other"));

        // Duplicate name refused; wrong secret unknown.
        assert!(create(&conn, "site-a", "read", &[], 43).unwrap_err().contains("exists"));
        assert!(lookup(&conn, "sk_nope").unwrap().is_none());

        // Revoke kills lookup but keeps the row.
        assert!(revoke(&conn, "site-a").unwrap());
        assert!(lookup(&conn, &secret).unwrap().is_none());
        assert_eq!(list(&conn).unwrap()[0]["revoked"], true);
        assert!(!revoke(&conn, "ghost").unwrap());
    }

    #[test]
    fn unscoped_key_and_bad_role() {
        let conn = Connection::open_in_memory().unwrap();
        let secret = create(&conn, "root2", "ddl", &[], 0).unwrap();
        let auth = lookup(&conn, &secret).unwrap().unwrap();
        assert_eq!(auth.scope, None);
        assert!(auth.allows_table("anything"));
        assert!(create(&conn, "x", "admin", &[], 0).unwrap_err().contains("bad role"));
    }
}
