//! Scoped keys + admin API, end to end: provision keys over HTTP, then
//! prove the fences hold on every surface a key can reach — /write
//! (measurement scope), /sql (authorizer scope), /query (influx scope),
//! and the admin endpoints themselves (scoped ddl = its tables only,
//! key management = unscoped only).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use silodb_server::auth::Tokens;
use silodb_server::{app, boot, Config};
use tower::util::ServiceExt;

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
            readonly: None,
            readwrite: None,
            ddl: Some("root".into()),
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
    token: &str,
    body: &str,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap();
    let resp = ts.router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, v)
}

async fn sql(ts: &TestServer, token: &str, q: &str) -> (StatusCode, Value) {
    call(ts, "POST", "/sql", token, &json!({ "sql": q }).to_string()).await
}

/// Create a key via the admin API; return its secret.
async fn mint(ts: &TestServer, name: &str, role: &str, scope: &[&str]) -> String {
    let (status, v) = call(
        ts,
        "POST",
        "/admin/api/keys",
        "root",
        &json!({ "name": name, "role": role, "scope": scope }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    v["secret"].as_str().unwrap().to_owned()
}

async fn seed_tables(ts: &TestServer) {
    for t in ["weather", "secrets"] {
        let (status, v) = call(
            ts,
            "POST",
            "/admin/api/tables",
            "root",
            &json!({
                "name": t,
                "schema": "ts TIMESTAMP, city TEXT, temp REAL",
                "tiers": "1d,7d",
                "retention": "4w",
            })
            .to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
    }
    for (t, city, temp) in [("weather", "SF", 21.5), ("secrets", "X", 9.9)] {
        let (status, v) = call(
            ts,
            "POST",
            "/write?precision=us",
            "root",
            &format!("{t},city={city} temp={temp} 3600000000"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
    }
}

#[tokio::test]
async fn key_lifecycle_and_management_fences() {
    let ts = server();
    let secret = mint(&ts, "site-a", "write", &["weather"]).await;
    assert!(secret.starts_with("sk_"));

    // Listed without the secret; scope round-trips.
    let (_, v) = call(&ts, "GET", "/admin/api/keys", "root", "").await;
    assert_eq!(v["keys"][0]["name"], "site-a");
    assert_eq!(v["keys"][0]["scope"], json!(["weather"]));
    assert!(v["keys"][0].get("secret").is_none());

    // A scoped ddl key cannot manage keys (no self-widening).
    let scoped_ddl = mint(&ts, "site-admin", "ddl", &["weather"]).await;
    let (status, _) = call(
        &ts,
        "POST",
        "/admin/api/keys",
        &scoped_ddl,
        &json!({ "name": "evil", "role": "ddl" }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Revoke kills auth immediately; unknown key 404s.
    seed_tables(&ts).await;
    let (status, _) = call(&ts, "POST", "/write", &secret, "weather temp=1.0").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&ts, "DELETE", "/admin/api/keys/site-a", "root", "").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&ts, "POST", "/write", &secret, "weather temp=1.0").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&ts, "DELETE", "/admin/api/keys/ghost", "root", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn write_scope_is_exact() {
    let ts = server();
    seed_tables(&ts).await;
    let key = mint(&ts, "site-a", "write", &["weather"]).await;

    // In scope: writes flow.
    let (status, v) = call(&ts, "POST", "/write?precision=us", &key, "weather,city=LA temp=25.0 7200000000").await;
    assert_eq!(status, StatusCode::OK, "{v}");
    // Out of scope: named refusal, nothing written.
    let (status, v) = call(&ts, "POST", "/write", &key, "secrets temp=0.0").await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{v}");
    assert!(v["error"].as_str().unwrap().contains("scope"), "{v}");
    // Write role can't grow schema even in scope.
    let (status, _) = call(&ts, "POST", "/write", &key, "weather,city=SF temp=1.0,newfield=2.0").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn sql_scope_is_enforced_by_the_database() {
    let ts = server();
    seed_tables(&ts).await;
    let key = mint(&ts, "site-a", "write", &["weather"]).await;

    // Own table: read and DML both work, artifacts included.
    let (status, v) = sql(&ts, &key, "SELECT count(*) FROM weather").await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["rows"][0][0], 1);
    let (status, _) = sql(&ts, &key, "SELECT count(*) FROM weather_hot").await;
    assert_eq!(status, StatusCode::OK);
    let (status, v) = sql(
        &ts,
        &key,
        "INSERT INTO weather (ts, city, temp) VALUES (9000000000, 'SD', 19.0)",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");

    // The other table is invisible: SELECT, join sneak, DML all refused
    // by SQLite itself.
    for q in [
        "SELECT count(*) FROM secrets",
        "SELECT count(*) FROM secrets_hot",
        "SELECT w.temp, s.temp FROM weather w, secrets s",
        "INSERT INTO secrets (ts, temp) VALUES (1, 1.0)",
        "DELETE FROM secrets_hot",
    ] {
        let (status, v) = sql(&ts, &key, q).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{q}: {v}");
    }
    // DDL and admin functions are denied for scoped keys, every role.
    let ddl_key = mint(&ts, "site-admin", "ddl", &["weather"]).await;
    for q in [
        "CREATE TABLE evil (x)",
        "ALTER TABLE weather_hot ADD COLUMN sneaky REAL",
        "SELECT silodb_add_column('secrets', 'x REAL')",
        "SELECT silodb_maintain('secrets', 0)",
        "DROP TABLE secrets",
    ] {
        let (status, v) = sql(&ts, &ddl_key, q).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{q}: {v}");
    }

    // Read-only scoped key on the reader pool: read in scope, nothing else.
    let ro = mint(&ts, "viewer", "read", &["weather"]).await;
    let (status, _) = sql(&ts, &ro, "SELECT count(*) FROM weather").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = sql(&ts, &ro, "SELECT count(*) FROM secrets").await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Credential tables are unscoped-ddl-only — every other credential,
    // scoped or not, is refused (they hold key hashes and scopes).
    for tok in [key.as_str(), ddl_key.as_str(), ro.as_str()] {
        let (status, v) = sql(&ts, tok, "SELECT name FROM _silodb_server_keys").await;
        assert_eq!(status, StatusCode::FORBIDDEN, "credential leak: {v}");
        let (status, _) = sql(&ts, tok, "SELECT * FROM _silodb_server_key_scopes").await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
    let (status, _) = sql(&ts, "root", "SELECT name FROM _silodb_server_keys").await;
    assert_eq!(status, StatusCode::OK, "root still administers the key tables");
    // Engine internals stay readable — catalog/policy are metadata.
    let (status, _) = sql(&ts, &ro, "SELECT count(*) FROM _silodb_policy").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn scoped_ddl_autoschema_and_admin_surface() {
    let ts = server();
    let key = mint(&ts, "site-admin", "ddl", &["sensors"]).await;

    // Autoschema-creates its scoped measurement over /write…
    let (status, v) = call(&ts, "POST", "/write?precision=us", &key, "sensors,unit=a v=1.0 3600000000").await;
    assert_eq!(status, StatusCode::OK, "{v}");
    // …and evolves it…
    let (status, v) = call(&ts, "POST", "/write?precision=us", &key, "sensors,unit=a v=1.0,rpm=900i 7200000000").await;
    assert_eq!(status, StatusCode::OK, "{v}");
    // …but cannot create anything else.
    let (status, _) = call(&ts, "POST", "/write", &key, "other v=1.0").await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Admin table endpoints obey the same scope.
    let (status, v) = call(
        &ts,
        "POST",
        "/admin/api/tables/sensors/columns",
        &key,
        &json!({ "coldef": "humidity REAL" }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (status, _) = call(
        &ts,
        "POST",
        "/admin/api/tables",
        &key,
        &json!({ "name": "other", "schema": "ts TIMESTAMP, v REAL" }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, v) = call(
        &ts,
        "PUT",
        "/admin/api/tables/sensors/retention",
        &key,
        &json!({ "retain": "8w" }).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
}

#[tokio::test]
async fn table_listing_reflects_scope_and_policy() {
    let ts = server();
    seed_tables(&ts).await;
    // Root sees everything with full policy detail.
    let (status, v) = call(&ts, "GET", "/admin/api/tables", "root", "").await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let tables = v["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 2);
    let w = tables.iter().find(|t| t["table"] == "weather").unwrap();
    assert_eq!(w["tiers"], json!(["1d", "1w"]), "largest exact unit formatting");
    assert_eq!(w["retention"], "4w");
    assert_eq!(w["hot_rows"], 1);
    assert!(w["columns"].as_array().unwrap().len() == 3, "{w}");

    // A scoped key sees only its slice.
    let key = mint(&ts, "viewer", "read", &["weather"]).await;
    let (_, v) = call(&ts, "GET", "/admin/api/tables", &key, "").await;
    let tables = v["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0]["table"], "weather");
}

#[tokio::test]
async fn influx_surface_respects_scope() {
    let ts = server();
    seed_tables(&ts).await;
    let key = mint(&ts, "viewer", "read", &["weather"]).await;

    let q = |q: &str| {
        let uri = format!(
            "/query?epoch=ms&q={}",
            q.bytes()
                .map(|b| if b.is_ascii_alphanumeric() { (b as char).to_string() } else { format!("%{b:02X}") })
                .collect::<String>()
        );
        uri
    };
    let (status, v) = call(&ts, "GET", &q("SHOW MEASUREMENTS"), &key, "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        v["results"][0]["series"][0]["values"],
        json!([["weather"]]),
        "scoped SHOW hides other measurements: {v}"
    );
    let (_, v) = call(&ts, "GET", &q("SELECT mean(temp) FROM secrets"), &key, "").await;
    assert!(v["results"][0]["error"].as_str().unwrap().contains("scope"), "{v}");
    let (_, v) = call(&ts, "GET", &q("SELECT mean(temp) FROM weather"), &key, "").await;
    assert_eq!(v["results"][0]["series"][0]["values"][0][1], 21.5, "{v}");
}
