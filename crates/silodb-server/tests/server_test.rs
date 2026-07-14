//! End-to-end HTTP tests: real database in a tempdir, real router, no
//! network — requests go through tower's oneshot. Covers the three-role
//! auth fence (route level AND database level), line-protocol autoschema
//! with ADD COLUMN evolution, and full SQL across tiers.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use silodb_server::auth::Tokens;
use silodb_server::{app, boot, Config};
use tower::util::ServiceExt;

const HOUR: i64 = 3600 * 1_000_000;
const DAY: i64 = 24 * HOUR;

struct TestServer {
    router: axum::Router,
    _dir: tempfile::TempDir,
}

fn server() -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        db_path: dir.path().join("hot.db"),
        addr: String::new(),
        tokens: Tokens {
            readonly: Some("r-token".into()),
            readwrite: Some("w-token".into()),
            ddl: Some("d-token".into()),
        },
        default_tiers: "1d".into(),
        cold_dir: None,
        maintain_secs: 0,
        readers: 2,
        max_rows: 10_000,
    };
    let state = boot(&config).unwrap();
    TestServer {
        router: app(state),
        _dir: dir,
    }
}

async fn call(
    ts: &TestServer,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: &str,
    content_type: &str,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", content_type);
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let resp = ts
        .router
        .clone()
        .oneshot(req.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::String(
            String::from_utf8_lossy(&bytes).into_owned(),
        ))
    };
    (status, v)
}

async fn sql(ts: &TestServer, token: &str, sql: &str, params: Value) -> (StatusCode, Value) {
    let body = json!({ "sql": sql, "params": params }).to_string();
    call(ts, "POST", "/sql", Some(token), &body, "application/json").await
}

async fn write(ts: &TestServer, token: &str, uri: &str, body: &str) -> (StatusCode, Value) {
    call(ts, "POST", uri, Some(token), body, "text/plain").await
}

#[tokio::test]
async fn line_protocol_autoschema_and_sql_roundtrip() {
    let ts = server();
    // ddl token: first sight of 'weather' creates the table.
    let (status, v) = write(
        &ts,
        "d-token",
        "/write",
        "weather,city=SF temp=21.5,humidity=40i 3600000000000\n\
         weather,city=LA temp=25.0,humidity=30i 7200000000000\n",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["written"], 2);

    // Data readable over /sql with the readonly token (reader pool).
    let (status, v) = sql(
        &ts,
        "r-token",
        "SELECT city, temp, humidity, ts FROM weather ORDER BY ts",
        json!([]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["columns"], json!(["city", "temp", "humidity", "ts"]));
    assert_eq!(v["rows"][0], json!(["SF", 21.5, 40, 3_600_000_000i64]));
    assert_eq!(v["rows"][1][3], json!(7_200_000_000i64), "ns → µs");

    // Params work.
    let (status, v) = sql(
        &ts,
        "r-token",
        "SELECT count(*) FROM weather WHERE city = ?1",
        json!(["SF"]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["rows"][0][0], 1);
}

#[tokio::test]
async fn autoschema_evolves_with_add_column() {
    let ts = server();
    write(&ts, "d-token", "/write", "m,site=a v=1.0 1000000000000").await;
    // New field + new tag mid-stream → ADD COLUMN evolution (ddl token).
    let (status, v) = write(
        &ts,
        "d-token",
        "/write",
        "m,site=b,rack=r1 v=2.0,pressure=9.5 2000000000000",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (_, v) = sql(
        &ts,
        "r-token",
        "SELECT site, rack, pressure FROM m ORDER BY ts",
        json!([]),
    )
    .await;
    assert_eq!(v["rows"][0], json!(["a", null, null]), "history reads NULL");
    assert_eq!(v["rows"][1], json!(["b", "r1", 9.5]));

    // Type conflict is a 400, not a silent coercion.
    let (status, v) = write(&ts, "d-token", "/write", "m site=1.5,v=1.0").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
    assert!(v["error"].as_str().unwrap().contains("types never change"), "{v}");
}

#[tokio::test]
async fn precision_parameter() {
    let ts = server();
    let (status, v) = write(&ts, "d-token", "/write?precision=s", "m v=1.0 3600").await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (_, v) = sql(&ts, "r-token", "SELECT ts FROM m", json!([])).await;
    assert_eq!(v["rows"][0][0], json!(HOUR));
    let (status, v) = write(&ts, "d-token", "/write?precision=fortnights", "m v=1.0").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
}

#[tokio::test]
async fn auth_fences() {
    let ts = server();
    write(&ts, "d-token", "/write", "m,site=a v=1.0 1000000000000").await;

    // No/бad token → 401.
    let (status, _) = call(&ts, "POST", "/sql", None, r#"{"sql":"SELECT 1"}"#, "application/json").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = sql(&ts, "wrong", "SELECT 1", json!([])).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // readonly: reads fine, writes refused BY THE DATABASE (read-only conn).
    let (status, _) = sql(&ts, "r-token", "SELECT count(*) FROM m", json!([])).await;
    assert_eq!(status, StatusCode::OK);
    let (status, v) = sql(&ts, "r-token", "INSERT INTO m (ts, v) VALUES (1, 2.0)", json!([])).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{v}");
    let (status, _) = write(&ts, "r-token", "/write", "m v=9.9").await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // readwrite: DML on user tables fine…
    let (status, v) = sql(
        &ts,
        "w-token",
        "INSERT INTO m (ts, site, v) VALUES (2000000000000, 'b', 2.0)",
        json!([]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (status, _) = write(&ts, "w-token", "/write", "m,site=a v=3.0").await;
    assert_eq!(status, StatusCode::OK);
    // …but DDL, admin functions, internal tables, and schema growth are not.
    for (bad_sql, why) in [
        ("CREATE TABLE evil (x)", "create"),
        ("DROP TABLE m", "drop"),
        ("SELECT silodb_create_table('evil2')", "admin fn"),
        ("SELECT silodb_set_retention('m', '30d')", "admin fn"),
        ("INSERT INTO _silodb_policy (logical_table, tiers_us, safety_margin_us, base_dir) VALUES ('x','1',1,'y')", "internal table"),
        ("PRAGMA journal_mode=DELETE", "pragma"),
    ] {
        let (status, v) = sql(&ts, "w-token", bad_sql, json!([])).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{why}: {v}");
    }
    let (status, v) = write(&ts, "w-token", "/write", "new_measurement v=1.0").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{v}");
    let (status, v) = write(&ts, "w-token", "/write", "m,site=a v=1.0,newfield=2.0").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{v}");

    // ddl: everything.
    let (status, v) = sql(&ts, "d-token", "SELECT silodb_set_retention('m', '30d')", json!([])).await;
    assert_eq!(status, StatusCode::OK, "{v}");
}

#[tokio::test]
async fn maintenance_compacts_and_queries_span_tiers() {
    let ts = server();
    // Two days of hourly data via line protocol (µs precision).
    let mut body = String::new();
    for h in 0..48 {
        body.push_str(&format!("readings,device=a value={} {}\n", h, h * HOUR));
    }
    let (status, v) = write(&ts, "d-token", "/write?precision=us", &body).await;
    assert_eq!(status, StatusCode::OK, "{v}");

    // Maintain via SQL (ddl token) — closed day compacts to parquet.
    let now = 2 * DAY + 3 * HOUR;
    let (status, v) = sql(&ts, "d-token", &format!("SELECT silodb_maintain('readings', {now})"), json!([])).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert!(v["rows"][0][0].as_i64().unwrap() >= 1, "{v}");

    // Reader connections see hot ∪ cold through the one name.
    let (status, v) = sql(&ts, "r-token", "SELECT count(*), sum(value) FROM readings", json!([])).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["rows"][0][0], 48);
    assert_eq!(v["rows"][0][1], (0..48).sum::<i64>() as f64);

    // Health reports the table with active files.
    let (status, v) = call(&ts, "GET", "/health", None, "", "text/plain").await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["tables"][0]["table"], "readings");
    assert!(v["tables"][0]["active_files"].as_i64().unwrap() >= 1, "{v}");
}

#[tokio::test]
async fn sql_shape_and_errors() {
    let ts = server();
    write(&ts, "d-token", "/write", "m v=1.0 1000000000000").await;
    // Multi-statement refused.
    let (status, v) = sql(&ts, "d-token", "SELECT 1; SELECT 2", json!([])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
    assert!(v["error"].as_str().unwrap().contains("one statement"), "{v}");
    // Bad SQL is a clean 400.
    let (status, _) = sql(&ts, "d-token", "SELEKT 1", json!([])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Writes land (rows_affected is SQLite changes(), which reports 0 for
    // INSTEAD OF trigger inserts through the view — count() is the truth).
    let (status, v) = sql(
        &ts,
        "w-token",
        "INSERT INTO m (ts, v) VALUES (?1, ?2)",
        json!([2_000_000_000_000i64, 2.5]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (_, v) = sql(&ts, "r-token", "SELECT count(*) FROM m", json!([])).await;
    assert_eq!(v["rows"][0][0], 2);
    // Line-protocol parse errors carry line numbers.
    let (status, v) = write(&ts, "d-token", "/write", "m v=1.0\nbroken-line").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{v}");
    assert!(v["error"].as_str().unwrap().contains("line 2"), "{v}");
}
