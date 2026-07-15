//! silodb-server: a standalone HTTP layer over a silodb database. Not
//! part of the core — the engine stays an embeddable library; this crate
//! is one way to run it as a service.
//!
//! - `POST /sql`    — full SQL (role-gated), JSON in/out
//! - `POST /write`  — InfluxDB line protocol with autoschema
//! - `GET  /health` — liveness + per-table stats
//!
//! Three bearer tokens (env: `SILODB_READONLY_TOKEN`, `SILODB_READWRITE_TOKEN`,
//! `SILODB_DDL_TOKEN`) map to three roles, enforced at the database, not
//! just the route: readonly runs on read-only connections, readwrite runs
//! under a SQLite authorizer that denies DDL and silodb admin functions.

pub mod admin;
pub mod auth;
pub mod db;
pub mod influx;
pub mod influxql;
pub mod keys;
pub mod lineproto;

use auth::{Role, Tokens};
use keys::Auth;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use db::{Actor, ReaderPool};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::types::{Value, ValueRef};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct Config {
    pub db_path: std::path::PathBuf,
    pub addr: String,
    pub tokens: Tokens,
    pub default_tiers: String,
    pub cold_dir: Option<String>,
    pub maintain_secs: u64,
    pub readers: usize,
    pub max_rows: usize,
}

impl Config {
    pub fn from_env() -> Config {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        Config {
            db_path: var("SILODB_DB").unwrap_or_else(|| "silodb.db".into()).into(),
            addr: var("SILODB_ADDR").unwrap_or_else(|| "0.0.0.0:8080".into()),
            tokens: Tokens::from_env(),
            default_tiers: var("SILODB_DEFAULT_TIERS").unwrap_or_else(|| "1d".into()),
            cold_dir: var("SILODB_COLD_DIR"),
            maintain_secs: var("SILODB_MAINTAIN_SECS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
            readers: var("SILODB_READERS").and_then(|v| v.parse().ok()).unwrap_or(4),
            max_rows: var("SILODB_MAX_ROWS").and_then(|v| v.parse().ok()).unwrap_or(10_000),
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub writer: Actor,
    pub readers: Arc<ReaderPool>,
    pub tokens: Tokens,
    pub default_tiers: String,
    pub max_rows: usize,
}

pub struct ApiError(pub StatusCode, pub String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

pub(crate) fn bad_request(msg: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, msg.into())
}
pub(crate) fn forbidden(msg: impl Into<String>) -> ApiError {
    ApiError(StatusCode::FORBIDDEN, msg.into())
}

/// Open connections, spawn actors, build the router. The caller serves it.
pub fn boot(config: &Config) -> Result<AppState, Box<dyn std::error::Error>> {
    if !config.tokens.any_configured() {
        return Err("no tokens configured — set SILODB_READONLY_TOKEN / \
                    SILODB_READWRITE_TOKEN / SILODB_DDL_TOKEN (an unset role \
                    is disabled; with none set, nothing could ever connect)"
            .into());
    }
    let writer = Connection::open(&config.db_path)?;
    // journal_mode returns a row; query it rather than pragma_update.
    writer.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))?;
    // Materialize the -wal/-shm before read-only connections open: a
    // READ_ONLY conn can use WAL's shared memory but can't create it.
    writer.execute_batch("BEGIN IMMEDIATE; COMMIT;")?;
    silodb::load_module(&writer)?;
    if let Some(dir) = &config.cold_dir {
        silodb::set_default_dir(&writer, dir)?;
    }
    let writer = Actor::spawn(writer);

    let mut readers = Vec::with_capacity(config.readers.max(1));
    for _ in 0..config.readers.max(1) {
        let conn = Connection::open_with_flags(
            &config.db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        silodb::load_module(&conn)?;
        readers.push(Actor::spawn(conn));
    }

    Ok(AppState {
        writer,
        readers: Arc::new(ReaderPool::new(readers)),
        tokens: config.tokens.clone(),
        default_tiers: config.default_tiers.clone(),
        max_rows: config.max_rows,
    })
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/sql", post(sql_handler))
        .route("/write", post(write_handler))
        .route("/health", get(health_handler))
        // InfluxDB 1.x lookalike — stock Grafana's core InfluxDB
        // datasource (InfluxQL) points here, builder autocomplete and all.
        .route("/ping", get(influx_ping).head(influx_ping))
        .route("/query", get(influx_query).post(influx_query))
        // Admin API: key provisioning + table management (the DDL front
        // door for scoped keys — their SQL surface denies DDL outright).
        .route(
            "/admin/api/keys",
            post(admin::create_key).get(admin::list_keys),
        )
        .route("/admin/api/keys/{name}", axum::routing::delete(admin::revoke_key))
        .route(
            "/admin/api/tables",
            get(admin::list_tables).post(admin::create_table),
        )
        .route("/admin/api/tables/{table}/columns", post(admin::add_column))
        .route(
            "/admin/api/tables/{table}/retention",
            axum::routing::put(admin::set_retention),
        )
        // The admin UI: a Dioxus WASM SPA, compiled ahead of time
        // (admin-ui/build.sh) and embedded into the binary — cargo build
        // needs no toolchain beyond the committed ui-dist/.
        .route("/admin", get(admin_ui))
        .route("/admin/", get(admin_ui))
        .route("/admin/{*path}", get(admin_ui))
        // A human hitting the root in a browser wants the panel, not a 404.
        .route(
            "/",
            get(|| async { axum::response::Redirect::temporary("/admin") }),
        )
        .with_state(state)
}

#[derive(rust_embed::Embed)]
#[folder = "ui-dist/"]
struct AdminAssets;

async fn admin_ui(req: axum::extract::Request) -> Response {
    let path = req
        .uri()
        .path()
        .trim_start_matches("/admin")
        .trim_start_matches('/');
    let file = AdminAssets::get(path)
        // SPA fallback: any non-asset path serves the app shell.
        .or_else(|| AdminAssets::get("index.html"));
    match file {
        Some(f) => {
            let mime = mime_guess::from_path(if path.is_empty() { "index.html" } else { path })
                .first_or_else(|| mime_guess::mime::TEXT_HTML);
            (
                [
                    ("content-type", mime.to_string()),
                    // Hashed asset names make long caching safe; the
                    // shell itself must revalidate.
                    (
                        "cache-control",
                        if path.starts_with("assets/") {
                            "public, max-age=31536000, immutable".into()
                        } else {
                            "no-cache".into()
                        },
                    ),
                ],
                f.data,
            )
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "admin UI not embedded").into_response(),
    }
}

async fn influx_ping() -> impl IntoResponse {
    (
        StatusCode::NO_CONTENT,
        [("X-Influxdb-Version", "1.8.10-silodb")],
    )
}

/// InfluxDB 1.x `/query`: `q=` holds semicolon-separated InfluxQL,
/// `epoch=` the output time unit. Read-only by construction — every
/// statement runs on the reader pool regardless of role.
async fn influx_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: String,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut params: BTreeMap<String, String> = query
        .as_deref()
        .and_then(|q| serde_urlencoded::from_str(q).ok())
        .unwrap_or_default();
    // POST form body (Grafana uses it for long queries) — query-string
    // params win on conflict.
    if !body.is_empty()
        && let Ok(form) = serde_urlencoded::from_str::<BTreeMap<String, String>>(&body)
    {
        for (k, v) in form {
            params.entry(k).or_insert(v);
        }
    }
    // Header auth (Bearer / Basic password), else influx-classic u/p.
    let auth = resolve_auth(&state, &headers, params.get("p").cloned())
        .await
        .ok_or_else(unauthorized)?;
    let scope = auth.scope;
    let q = params
        .get("q")
        .cloned()
        .ok_or_else(|| bad_request("missing q parameter"))?;
    let epoch = influx::Epoch::parse(params.get("epoch").map(String::as_str))
        .ok_or_else(|| bad_request("bad epoch (ns|us|ms|s|rfc3339)"))?;
    let now = now_micros();
    let max_rows = state.max_rows;

    state
        .readers
        .get()
        .run(move |conn| {
            let mut results = Vec::new();
            for (i, stmt_text) in influxql::split_statements(&q).into_iter().enumerate() {
                let outcome = influxql::parse(&stmt_text, now)
                    .map_err(|e| e.to_string())
                    .and_then(|stmt| {
                        influx::execute(conn, &stmt, epoch, max_rows, scope.as_deref())
                    });
                results.push(match outcome {
                    Ok(series) if series.as_array().is_some_and(|s| s.is_empty()) => {
                        json!({ "statement_id": i })
                    }
                    Ok(series) => json!({ "statement_id": i, "series": series }),
                    Err(e) => json!({ "statement_id": i, "error": e }),
                });
            }
            Ok(Json(json!({ "results": results })))
        })
        .await
}

/// Background maintenance: every `secs`, run `maintain(now)` on every
/// table registered in `_silodb_policy`. The server owns the
/// one-maintainer contract.
pub async fn maintenance_loop(writer: Actor, secs: u64) {
    if secs == 0 {
        return;
    }
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        writer
            .run(|conn| {
                let tables: Vec<String> = conn
                    .prepare("SELECT logical_table FROM _silodb_policy")
                    .and_then(|mut s| {
                        s.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()
                    })
                    .unwrap_or_default();
                let now = now_micros();
                for t in tables {
                    if let Err(e) = silodb::maintain(conn, &t, now) {
                        eprintln!("maintain('{t}') failed: {e}");
                    }
                }
            })
            .await;
    }
}

pub fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before 1970")
        .as_micros() as i64
}

// --- auth resolution -----------------------------------------------------

/// Resolve a request to an [`Auth`]: env tokens first (unscoped root
/// credentials), then provisioned keys from `_silodb_server_keys`.
/// `extra_secret` carries query-param credentials (influx `p=`).
pub(crate) async fn resolve_auth(
    state: &AppState,
    headers: &HeaderMap,
    extra_secret: Option<String>,
) -> Option<Auth> {
    let secret = auth::extract_secret(headers).or(extra_secret)?;
    if let Some(role) = state.tokens.role_for_secret(&secret) {
        return Some(Auth::unscoped(role));
    }
    state
        .readers
        .get()
        .run(move |conn| keys::lookup(conn, &secret).ok().flatten())
        .await
}

pub(crate) fn unauthorized() -> ApiError {
    ApiError(StatusCode::UNAUTHORIZED, "bad or missing token".into())
}

/// The database-level fence for a **scoped** key, any role: reads only
/// within the scope's table family (plus engine internals), DML only for
/// write/ddl roles and only on the scope's own tables, and no DDL or
/// silodb admin functions at all — scoped schema changes go through the
/// admin API, where scope is checked against the named table.
fn scoped_authorizer(
    role: Role,
    scope: Vec<String>,
) -> impl for<'a> FnMut(AuthContext<'a>) -> Authorization + Send + 'static {
    move |ctx| match ctx.action {
        AuthAction::Select
        | AuthAction::Transaction { .. }
        | AuthAction::Savepoint { .. }
        | AuthAction::Recursive => Authorization::Allow,
        // Reading a view emits NO event for the view's own name — only
        // underlying-table reads tagged with the view as accessor. So an
        // accessor is a grant exactly when the accessor itself is in
        // scope ("this is the weather view doing the reading"), never a
        // blanket pass — otherwise every view is readable by everyone.
        AuthAction::Read { table_name, .. } => {
            if keys::sql_read_allowed(&scope, table_name)
                || ctx.accessor.is_some_and(|a| keys::sql_read_allowed(&scope, a))
            {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        AuthAction::Insert { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::Update { table_name, .. } => {
            if role == Role::ReadOnly {
                return Authorization::Deny;
            }
            // Same accessor rule: the scope's own INSTEAD OF trigger
            // ("weather_insert") may route into the hot table; a foreign
            // trigger may not.
            if keys::sql_write_allowed(&scope, table_name)
                || ctx.accessor.is_some_and(|a| keys::sql_read_allowed(&scope, a))
            {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }
        AuthAction::Function { function_name } => {
            if function_name.to_ascii_lowercase().starts_with("silodb_")
                && ADMIN_FUNCTIONS
                    .iter()
                    .any(|a| function_name.eq_ignore_ascii_case(a))
            {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        _ => Authorization::Deny,
    }
}

const ADMIN_FUNCTIONS: [&str; 5] = [
    "silodb_create_table",
    "silodb_add_column",
    "silodb_set_retention",
    "silodb_set_default_dir",
    "silodb_maintain",
];

// --- /sql --------------------------------------------------------------

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
    #[serde(default)]
    params: Vec<serde_json::Value>,
}

async fn sql_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: String,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Parse the body ourselves instead of the Json extractor: `curl -d`
    // sends form-urlencoded content-type and shouldn't 415 for it.
    let req: SqlRequest = serde_json::from_str(&body).map_err(|e| {
        bad_request(format!("body must be JSON {{\"sql\": \"...\", \"params\": [...]}}: {e}"))
    })?;
    let auth = resolve_auth(&state, &headers, None)
        .await
        .ok_or_else(unauthorized)?;
    let max_rows = state.max_rows;
    let actor = match auth.role {
        Role::ReadOnly => state.readers.get().clone(),
        Role::ReadWrite | Role::Ddl => state.writer.clone(),
    };
    actor
        .run(move |conn| {
            let guarded = match (&auth.scope, auth.role) {
                (Some(scope), role) => {
                    conn.authorizer(Some(scoped_authorizer(role, scope.clone())))
                        .map_err(|e| bad_request(e.to_string()))?;
                    true
                }
                (None, Role::ReadWrite) => {
                    conn.authorizer(Some(readwrite_authorizer))
                        .map_err(|e| bad_request(e.to_string()))?;
                    true
                }
                // Unscoped readonly runs on a read-only connection —
                // writes/DDL already impossible; the authorizer only
                // hides the credential tables.
                (None, Role::ReadOnly) => {
                    conn.authorizer(Some(readonly_authorizer))
                        .map_err(|e| bad_request(e.to_string()))?;
                    true
                }
                // unscoped ddl is the root credential — unrestricted
                (None, Role::Ddl) => false,
            };
            let result = run_sql(conn, &req, max_rows);
            if guarded {
                let _ = conn.authorizer(None::<fn(AuthContext) -> Authorization>);
            }
            result
        })
        .await
        .map(Json)
}

fn run_sql(
    conn: &Connection,
    req: &SqlRequest,
    max_rows: usize,
) -> Result<serde_json::Value, ApiError> {
    let params: Vec<Value> = req.params.iter().map(json_to_sql).collect::<Result<_, _>>()?;
    let mut stmt = conn.prepare(&req.sql).map_err(sql_err)?;
    if stmt.column_count() == 0 {
        let n = stmt
            .execute(rusqlite::params_from_iter(params))
            .map_err(sql_err)?;
        return Ok(json!({ "rows_affected": n }));
    }
    let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let mut rows_out = Vec::new();
    let mut truncated = false;
    let mut rows = stmt
        .query(rusqlite::params_from_iter(params))
        .map_err(sql_err)?;
    while let Some(row) = rows.next().map_err(sql_err)? {
        if rows_out.len() >= max_rows {
            truncated = true;
            break;
        }
        let mut out = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            out.push(sql_to_json(row.get_ref(i).map_err(sql_err)?));
        }
        rows_out.push(serde_json::Value::Array(out));
    }
    Ok(json!({ "columns": columns, "rows": rows_out, "truncated": truncated }))
}

fn sql_err(e: rusqlite::Error) -> ApiError {
    match e {
        rusqlite::Error::MultipleStatement => bad_request(
            "one statement per request (multi-statement scripts aren't supported)",
        ),
        // SQLite authorizer denials: "not authorized" (statement-level)
        // and "access to <t>.<c> is prohibited" (column-level).
        e if e.to_string().contains("not authorized")
            || e.to_string().contains("prohibited") =>
        {
            forbidden(format!(
                "{e} — this credential's role or scope doesn't allow that \
                 (DDL and silodb admin functions need an unscoped ddl token; \
                 scoped keys reach only their own tables)"
            ))
        }
        e if e.to_string().contains("readonly database") => {
            forbidden(format!("{e} — this token is read-only"))
        }
        e => bad_request(e.to_string()),
    }
}

fn json_to_sql(v: &serde_json::Value) -> Result<Value, ApiError> {
    Ok(match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Integer(*b as i64),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Real(n.as_f64().ok_or_else(|| bad_request("bad number"))?)
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        other => return Err(bad_request(format!("unsupported param: {other}"))),
    })
}

fn sql_to_json(v: ValueRef<'_>) -> serde_json::Value {
    match v {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(i) => json!(i),
        ValueRef::Real(f) => json!(f),
        ValueRef::Text(t) => json!(String::from_utf8_lossy(t)),
        ValueRef::Blob(b) => {
            json!({ "$hex": b.iter().map(|x| format!("{x:02x}")).collect::<String>() })
        }
    }
}

/// Unscoped readonly's only fence: the read-only connection already
/// refuses writes and DDL; this hides the credential tables.
fn readonly_authorizer(ctx: AuthContext<'_>) -> Authorization {
    match ctx.action {
        AuthAction::Read { table_name, .. } if table_name.starts_with("_silodb_server_") => {
            Authorization::Deny
        }
        _ => Authorization::Allow,
    }
}

/// The readwrite role's database-level fence: DML on user tables is fine,
/// everything schema-shaped or engine-internal is not.
fn readwrite_authorizer(ctx: AuthContext<'_>) -> Authorization {
    let internal =
        |t: &str| t.starts_with("_silodb_") || t.starts_with("sqlite_");
    match ctx.action {
        AuthAction::Select
        | AuthAction::Transaction { .. }
        | AuthAction::Savepoint { .. }
        | AuthAction::Recursive => Authorization::Allow,
        AuthAction::Read { table_name, .. } => {
            // Credential tables are unscoped-ddl-only, even for reads.
            if table_name.starts_with("_silodb_server_") {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        AuthAction::Insert { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::Update { table_name, .. } => {
            // Trigger-driven writes (the view's INSTEAD OF INSERT) come
            // through with an accessor; they're the engine's own routing.
            if internal(table_name) && ctx.accessor.is_none() {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        AuthAction::Function { function_name } => {
            let admin = [
                "silodb_create_table",
                "silodb_add_column",
                "silodb_set_retention",
                "silodb_set_default_dir",
                "silodb_maintain",
            ];
            if admin.iter().any(|a| function_name.eq_ignore_ascii_case(a)) {
                Authorization::Deny
            } else {
                Authorization::Allow
            }
        }
        _ => Authorization::Deny, // Create*/Drop*/Alter/Pragma/Attach/…
    }
}

// --- /write (line protocol) ---------------------------------------------

async fn write_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: String,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = resolve_auth(&state, &headers, None)
        .await
        .ok_or_else(unauthorized)?;
    if auth.role == Role::ReadOnly {
        return Err(forbidden("readonly token can't write"));
    }
    let precision = query
        .as_deref()
        .and_then(|q| {
            q.split('&')
                .find_map(|kv| kv.strip_prefix("precision="))
        })
        .unwrap_or("ns");
    let (div, mul) = lineproto::precision_to_us(precision)
        .ok_or_else(|| bad_request(format!("bad precision '{precision}' (ns|us|ms|s)")))?;

    let lines = lineproto::parse(&body).map_err(|e| bad_request(e.to_string()))?;
    if lines.is_empty() {
        return Ok(Json(json!({ "written": 0 })));
    }
    // Scope is exact here: line protocol names its measurement. A scoped
    // ddl key may autoschema-create, but only tables its scope names.
    for line in &lines {
        if !auth.allows_table(&line.measurement) {
            return Err(forbidden(format!(
                "measurement '{}' is outside this key's scope",
                line.measurement
            )));
        }
    }
    let allow_ddl = auth.role == Role::Ddl;
    let default_tiers = state.default_tiers.clone();
    let now = now_micros();

    state
        .writer
        .run(move |conn| {
            write_lines(conn, lines, div, mul, now, allow_ddl, &default_tiers)
        })
        .await
        .map(|written| Json(json!({ "written": written })))
}

fn write_lines(
    conn: &mut Connection,
    lines: Vec<lineproto::Line>,
    div: i64,
    mul: i64,
    now_us: i64,
    allow_ddl: bool,
    default_tiers: &str,
) -> Result<usize, ApiError> {
    // Autoschema per measurement first (DDL can't run inside the batch
    // transaction — table conversion commits on its own), then all row
    // inserts in one transaction: influx all-or-nothing semantics.
    let mut ts_names: BTreeMap<String, String> = BTreeMap::new();
    for line in &lines {
        if !ts_names.contains_key(&line.measurement) {
            let ts = ensure_schema(conn, line, allow_ddl, default_tiers)?;
            ts_names.insert(line.measurement.clone(), ts);
        } else {
            ensure_columns(conn, line, allow_ddl)?;
        }
    }
    let tx = conn.transaction().map_err(|e| bad_request(e.to_string()))?;
    let mut written = 0usize;
    for line in &lines {
        let ts_name = &ts_names[&line.measurement];
        let ts_us = match line.timestamp {
            Some(t) => t / div * mul,
            None => now_us,
        };
        let mut cols = vec![format!("\"{ts_name}\"")];
        let mut vals: Vec<Value> = vec![Value::Integer(ts_us)];
        for (k, v) in &line.tags {
            cols.push(format!("\"{k}\""));
            vals.push(Value::Text(v.clone()));
        }
        for (k, v, _) in &line.fields {
            cols.push(format!("\"{k}\""));
            vals.push(v.clone());
        }
        let sql = format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            line.measurement,
            cols.join(", "),
            (1..=vals.len()).map(|i| format!("?{i}")).collect::<Vec<_>>().join(", ")
        );
        let mut stmt = tx.prepare_cached(&sql).map_err(|e| bad_request(e.to_string()))?;
        stmt.execute(rusqlite::params_from_iter(vals))
            .map_err(|e| bad_request(e.to_string()))?;
        written += 1;
    }
    tx.commit().map_err(|e| bad_request(e.to_string()))?;
    Ok(written)
}

/// Table exists → check/evolve columns; missing → create it (ddl only).
/// Returns the table's timestamp column name.
fn ensure_schema(
    conn: &Connection,
    line: &lineproto::Line,
    allow_ddl: bool,
    default_tiers: &str,
) -> Result<String, ApiError> {
    let table = &line.measurement;
    let hot = silodb::resolve_hot_table(conn, table).map_err(|e| bad_request(e.to_string()))?;
    if hot.is_none() {
        if !allow_ddl {
            return Err(forbidden(format!(
                "measurement '{table}' doesn't exist and this token can't create \
                 tables (autoschema needs the ddl token)"
            )));
        }
        let mut cols = vec!["ts TIMESTAMP".to_owned()];
        for (k, _) in &line.tags {
            cols.push(format!("{k} TEXT"));
        }
        for (k, _, fv) in &line.fields {
            cols.push(format!("{k} {}", lineproto::field_decl(fv)));
        }
        silodb::init_table_tiered(conn, table, &cols.join(", "), default_tiers)
            .map_err(|e| bad_request(format!("autoschema create '{table}': {e}")))?;
        return Ok("ts".into());
    }
    ensure_columns(conn, line, allow_ddl)?;
    let ts = silodb::catalog::get_policy(conn, table)
        .map_err(|e| bad_request(e.to_string()))?
        .and_then(|p| p.ts_column)
        .unwrap_or_else(|| "ts".into());
    Ok(ts)
}

/// Existing-table path: every tag/field must exist with a compatible
/// class, or (ddl only) get added via ADD COLUMN evolution.
fn ensure_columns(
    conn: &Connection,
    line: &lineproto::Line,
    allow_ddl: bool,
) -> Result<(), ApiError> {
    let table = &line.measurement;
    let hot = silodb::resolve_hot_table(conn, table)
        .map_err(|e| bad_request(e.to_string()))?
        .ok_or_else(|| bad_request(format!("no such measurement '{table}'")))?;
    let existing: BTreeMap<String, String> = conn
        .prepare(&format!("PRAGMA table_info(\"{hot}\")"))
        .and_then(|mut s| {
            s.query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
                .collect()
        })
        .map_err(|e| bad_request(e.to_string()))?;

    let mut wanted: Vec<(String, &'static str)> = Vec::new();
    for (k, _) in &line.tags {
        wanted.push((k.clone(), "TEXT"));
    }
    for (k, _, fv) in &line.fields {
        wanted.push((k.clone(), lineproto::field_decl(fv)));
    }
    for (name, decl) in wanted {
        match existing.get(&name) {
            Some(have) => {
                if !class_compatible(have, decl) {
                    return Err(bad_request(format!(
                        "column '{name}' on '{table}' is {have}, line protocol \
                         sent {decl} — types never change; write compatible \
                         values or use a new column"
                    )));
                }
            }
            None => {
                if !allow_ddl {
                    return Err(forbidden(format!(
                        "column '{name}' doesn't exist on '{table}' and this \
                         token can't evolve schemas (needs the ddl token)"
                    )));
                }
                silodb::alter_table_add_column(conn, table, &format!("{name} {decl}"))
                    .map_err(|e| bad_request(format!("add column '{name}': {e}")))?;
            }
        }
    }
    Ok(())
}

/// Loose affinity compatibility: what the line protocol may write into an
/// existing declared type. Integers fit REAL columns; nothing else crosses.
fn class_compatible(declared: &str, incoming: &'static str) -> bool {
    let d = declared.to_ascii_uppercase();
    let is_int = d.contains("INT");
    let is_real = d.contains("REAL") || d.contains("FLOA") || d.contains("DOUB");
    let is_text = d.contains("CHAR") || d.contains("TEXT") || d.contains("CLOB");
    match incoming {
        "INTEGER" => is_int || is_real,
        "REAL" => is_real,
        "TEXT" => is_text,
        _ => false,
    }
}

// --- /health ------------------------------------------------------------

async fn health_handler(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state
        .readers
        .get()
        .run(|conn| {
            // Both engine tables are created lazily (policy at first
            // init, catalog at first compaction) — probe before querying
            // rather than swallowing "no such table".
            let exists = |t: &str| -> bool {
                conn.query_row(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                    [t],
                    |_| Ok(()),
                )
                .optional()
                .unwrap_or(None)
                .is_some()
            };
            let mut tables: Vec<serde_json::Value> = Vec::new();
            if exists("_silodb_policy") {
                let names: Vec<String> = conn
                    .prepare("SELECT logical_table FROM _silodb_policy ORDER BY 1")
                    .and_then(|mut s| s.query_map([], |r| r.get(0))?.collect())
                    .map_err(|e| bad_request(e.to_string()))?;
                let has_catalog = exists("_silodb_catalog");
                for t in names {
                    let files: i64 = if has_catalog {
                        conn.query_row(
                            "SELECT count(*) FROM _silodb_catalog \
                             WHERE logical_table = ?1 AND status = 'active'",
                            [&t],
                            |r| r.get(0),
                        )
                        .unwrap_or(0)
                    } else {
                        0
                    };
                    tables.push(json!({ "table": t, "active_files": files }));
                }
            }
            Ok(Json(json!({ "status": "ok", "tables": tables })))
        })
        .await
}
