//! Admin JSON API: key provisioning and table management. This is the
//! DDL front door for scoped keys — the SQL surface denies them DDL
//! outright, and these endpoints check scope against the named table.
//!
//! Auth: ddl role. Key management additionally requires an **unscoped**
//! ddl credential (a scoped key must not mint itself wider access).

use crate::auth::Role;
use crate::keys::{self, Auth};
use crate::{bad_request, forbidden, now_micros, resolve_auth, unauthorized, ApiError, AppState};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use rusqlite::{Connection, OptionalExtension};
use serde::Deserialize;
use serde_json::json;

async fn require_ddl(state: &AppState, headers: &HeaderMap) -> Result<Auth, ApiError> {
    let auth = resolve_auth(state, headers, None).await.ok_or_else(unauthorized)?;
    if auth.role != Role::Ddl {
        return Err(forbidden("admin API needs a ddl credential"));
    }
    Ok(auth)
}

fn require_table_in_scope(auth: &Auth, table: &str) -> Result<(), ApiError> {
    if !auth.allows_table(table) {
        return Err(forbidden(format!(
            "table '{table}' is outside this key's scope"
        )));
    }
    Ok(())
}

fn check_ident(name: &str) -> Result<(), ApiError> {
    let mut chars = name.chars();
    let ok = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(bad_request(format!(
            "'{name}' is not a valid identifier ([A-Za-z_][A-Za-z0-9_]*)"
        )))
    }
}

// --- keys ---------------------------------------------------------------

#[derive(Deserialize)]
pub struct CreateKey {
    name: String,
    role: String,
    #[serde(default)]
    scope: Vec<String>,
}

pub async fn create_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateKey>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = require_ddl(&state, &headers).await?;
    if auth.scope.is_some() {
        return Err(forbidden("key management needs an unscoped ddl credential"));
    }
    if req.name.trim().is_empty() {
        return Err(bad_request("key needs a name"));
    }
    for t in &req.scope {
        check_ident(t)?;
    }
    let now = now_micros();
    let secret = state
        .writer
        .run(move |conn| keys::create(conn, &req.name, &req.role, &req.scope, now))
        .await
        .map_err(bad_request)?;
    Ok(Json(json!({
        "secret": secret,
        "note": "store this now — it is shown exactly once and only its hash is kept",
    })))
}

pub async fn list_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = require_ddl(&state, &headers).await?;
    if auth.scope.is_some() {
        return Err(forbidden("key management needs an unscoped ddl credential"));
    }
    let keys = state
        .readers
        .get()
        .run(|conn| keys::list(conn))
        .await
        .map_err(|e| bad_request(e.to_string()))?;
    Ok(Json(json!({ "keys": keys })))
}

pub async fn revoke_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = require_ddl(&state, &headers).await?;
    if auth.scope.is_some() {
        return Err(forbidden("key management needs an unscoped ddl credential"));
    }
    let revoked = state
        .writer
        .run(move |conn| keys::revoke(conn, &name))
        .await
        .map_err(bad_request)?;
    if !revoked {
        return Err(ApiError(
            axum::http::StatusCode::NOT_FOUND,
            "no such key".into(),
        ));
    }
    Ok(Json(json!({ "revoked": true })))
}

// --- tables --------------------------------------------------------------

#[derive(Deserialize)]
pub struct CreateTable {
    name: String,
    /// Column defs, `init_table` style: `"ts TIMESTAMP, device TEXT, value REAL"`.
    schema: String,
    tiers: Option<String>,
    retention: Option<String>,
}

pub async fn create_table(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateTable>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = require_ddl(&state, &headers).await?;
    check_ident(&req.name)?;
    require_table_in_scope(&auth, &req.name)?;
    let tiers = req.tiers.unwrap_or_else(|| state.default_tiers.clone());
    state
        .writer
        .run(move |conn| {
            silodb::init_table_tiered(conn, &req.name, &req.schema, &tiers)
                .map_err(|e| bad_request(e.to_string()))?;
            if let Some(r) = &req.retention {
                silodb::set_retention(conn, &req.name, Some(r))
                    .map_err(|e| bad_request(e.to_string()))?;
            }
            Ok(Json(json!({ "created": req.name })))
        })
        .await
}

#[derive(Deserialize)]
pub struct AddColumn {
    coldef: String,
}

pub async fn add_column(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(table): Path<String>,
    Json(req): Json<AddColumn>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = require_ddl(&state, &headers).await?;
    require_table_in_scope(&auth, &table)?;
    state
        .writer
        .run(move |conn| {
            silodb::alter_table_add_column(conn, &table, &req.coldef)
                .map_err(|e| bad_request(e.to_string()))?;
            Ok(Json(json!({ "added": req.coldef })))
        })
        .await
}

#[derive(Deserialize)]
pub struct SetRetention {
    /// Duration string, or null to keep forever.
    retain: Option<String>,
}

pub async fn set_retention(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(table): Path<String>,
    Json(req): Json<SetRetention>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = require_ddl(&state, &headers).await?;
    require_table_in_scope(&auth, &table)?;
    state
        .writer
        .run(move |conn| {
            silodb::set_retention(conn, &table, req.retain.as_deref())
                .map_err(|e| bad_request(e.to_string()))?;
            Ok(Json(json!({ "retention": req.retain })))
        })
        .await
}

pub async fn list_tables(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Any credential may look — a scoped one sees only its own tables.
    let auth = resolve_auth(&state, &headers, None)
        .await
        .ok_or_else(unauthorized)?;
    state
        .readers
        .get()
        .run(move |conn| {
            let mut tables = Vec::new();
            for t in table_names(conn).map_err(bad_request)? {
                if !auth.allows_table(&t) {
                    continue;
                }
                tables.push(table_info(conn, &t).map_err(bad_request)?);
            }
            Ok(Json(json!({ "tables": tables })))
        })
        .await
}

fn table_names(conn: &Connection) -> Result<Vec<String>, String> {
    let has: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_silodb_policy'",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    if has.is_none() {
        return Ok(Vec::new());
    }
    conn.prepare("SELECT logical_table FROM _silodb_policy ORDER BY 1")
        .and_then(|mut s| s.query_map([], |r| r.get(0))?.collect())
        .map_err(|e| e.to_string())
}

fn table_info(conn: &Connection, table: &str) -> Result<serde_json::Value, String> {
    let policy = silodb::catalog::get_policy(conn, table)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no policy for '{table}'"))?;
    let hot = silodb::resolve_hot_table(conn, table).map_err(|e| e.to_string())?;
    let hot_rows: i64 = match &hot {
        Some(h) => conn
            .query_row(&format!("SELECT count(*) FROM \"{h}\""), [], |r| r.get(0))
            .unwrap_or(0),
        None => 0,
    };
    let has_catalog: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_silodb_catalog'",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    let (files, cold_rows, range): (i64, i64, Option<(i64, i64)>) = if has_catalog.is_some() {
        conn.query_row(
            "SELECT count(*), coalesce(sum(row_count), 0),
                    min(range_start), max(range_end)
             FROM _silodb_catalog WHERE logical_table = ?1 AND status = 'active'",
            [table],
            |r| {
                let lo: Option<i64> = r.get(2)?;
                let hi: Option<i64> = r.get(3)?;
                Ok((r.get(0)?, r.get(1)?, lo.zip(hi)))
            },
        )
        .map_err(|e| e.to_string())?
    } else {
        (0, 0, None)
    };
    let columns: Vec<serde_json::Value> = match &hot {
        Some(h) => conn
            .prepare(&format!("PRAGMA table_info(\"{h}\")"))
            .and_then(|mut s| {
                s.query_map([], |r| {
                    Ok(json!({
                        "name": r.get::<_, String>(1)?,
                        "type": r.get::<_, String>(2)?,
                    }))
                })?
                .collect()
            })
            .map_err(|e| e.to_string())?,
        None => Vec::new(),
    };
    Ok(json!({
        "table": table,
        "tiers": policy.tiers_us.iter().map(|&us| human_duration(us)).collect::<Vec<_>>(),
        "retention": policy.retain_us.map(human_duration),
        "ts_column": policy.ts_column.clone().unwrap_or_else(|| "ts".into()),
        "base_dir": policy.base_dir,
        "columns": columns,
        "hot_rows": hot_rows,
        "active_files": files,
        "cold_rows": cold_rows,
        "cold_range": range.map(|(lo, hi)| json!([lo, hi])),
    }))
}

/// µs → compact human duration ("7d", "90m") — largest unit that divides
/// exactly, µs as the last resort. (The layer can't import the engine's
/// schema crate — facade only — so this tiny formatter lives here.)
fn human_duration(us: i64) -> String {
    for (unit, size) in [
        ("y", 365 * 86_400 * 1_000_000i64),
        ("w", 7 * 86_400 * 1_000_000),
        ("d", 86_400 * 1_000_000),
        ("h", 3_600 * 1_000_000),
        ("m", 60 * 1_000_000),
        ("s", 1_000_000),
    ] {
        if us != 0 && us % size == 0 {
            return format!("{}{unit}", us / size);
        }
    }
    format!("{us}us")
}
