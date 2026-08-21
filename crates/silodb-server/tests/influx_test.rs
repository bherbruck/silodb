//! The InfluxDB lookalike, exercised with byte-for-byte Grafana-shaped
//! traffic: the builder's meta-queries, the panel query shape
//! (aggregate + time bucket + tag group + fill), template-variable
//! regexes, epoch conversion, and influx auth conventions.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use silodb_server::auth::Tokens;
use silodb_server::{app, boot, Config};
use tower::util::ServiceExt;

const HOUR_US: i64 = 3600 * 1_000_000;

struct TestServer {
    router: axum::Router,
    _dir: tempfile::TempDir,
}

fn server() -> TestServer {
    server_with_cap(10_000)
}

fn server_with_cap(max_rows: usize) -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        db_path: dir.path().join("hot.db"),
        addr: String::new(),
        tokens: Tokens {
            readonly: Some("r-token".into()),
            readwrite: None,
            ddl: Some("d-token".into()),
        },
        default_tiers: "1d".into(),
        cold_dir: None,
        maintain_secs: 0,
        readers: 2,
        max_rows,
    };
    let state = boot(&config).unwrap();
    TestServer {
        router: app(state),
        _dir: dir,
    }
}

async fn raw(ts: &TestServer, req: Request<Body>) -> (StatusCode, Value) {
    let resp = ts.router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let v = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, v)
}

/// GET /query the way Grafana proxies it: URL-encoded q, epoch=ms, basic
/// auth with the token in the password slot.
async fn query(ts: &TestServer, q: &str) -> Value {
    let uri = format!(
        "/query?db=silodb&epoch=ms&q={}",
        urlencode(q)
    );
    // base64("grafana:r-token")
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Basic {}", b64("grafana:r-token")))
        .body(Body::empty())
        .unwrap();
    let (status, v) = raw(ts, req).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    v
}

async fn query_as(ts: &TestServer, q: &str, token: &str) -> Value {
    let uri = format!("/query?db=silodb&epoch=ms&q={}", urlencode(q));
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, v) = raw(ts, req).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    v
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn b64(s: &str) -> String {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let b = s.as_bytes();
    let mut out = String::new();
    for chunk in b.chunks(3) {
        let n = ((chunk[0] as u32) << 16)
            | ((chunk.get(1).copied().unwrap_or(0) as u32) << 8)
            | chunk.get(2).copied().unwrap_or(0) as u32;
        out.push(A[(n >> 18) as usize & 63] as char);
        out.push(A[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { A[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { A[n as usize & 63] as char } else { '=' });
    }
    out
}

async fn seed(ts: &TestServer) {
    // Two cities, hourly temps for 6 hours (µs precision timestamps).
    let mut body = String::new();
    for h in 0..6i64 {
        body.push_str(&format!(
            "weather,city=SF temp={},humidity={}i {}\n",
            20.0 + h as f64,
            40 + h,
            h * HOUR_US
        ));
        body.push_str(&format!(
            "weather,city=LA temp={} {}\n",
            30.0 + h as f64,
            h * HOUR_US
        ));
    }
    let req = Request::builder()
        .method("POST")
        .uri("/write?precision=us")
        .header("authorization", "Bearer d-token")
        .body(Body::from(body))
        .unwrap();
    let (status, v) = raw(ts, req).await;
    assert_eq!(status, StatusCode::OK, "{v}");
}

#[tokio::test]
async fn ping_and_auth_conventions() {
    let ts = server();
    let (status, _) = raw(
        &ts,
        Request::builder().uri("/ping").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    seed(&ts).await;
    // u/p query params (influx classic).
    let uri = format!("/query?u=x&p=r-token&q={}", urlencode("SHOW MEASUREMENTS"));
    let (status, v) = raw(
        &ts,
        Request::builder().uri(uri).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    // No credentials → 401.
    let uri = format!("/query?q={}", urlencode("SHOW MEASUREMENTS"));
    let (status, _) = raw(
        &ts,
        Request::builder().uri(uri).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn builder_meta_queries() {
    let ts = server();
    seed(&ts).await;

    let v = query(&ts, "SHOW MEASUREMENTS LIMIT 100").await;
    assert_eq!(v["results"][0]["series"][0]["values"], serde_json::json!([["weather"]]));

    let v = query(&ts, r#"SHOW TAG KEYS FROM "weather""#).await;
    assert_eq!(v["results"][0]["series"][0]["values"], serde_json::json!([["city"]]));

    let v = query(&ts, r#"SHOW FIELD KEYS FROM "weather""#).await;
    let fields = &v["results"][0]["series"][0]["values"];
    assert_eq!(
        *fields,
        serde_json::json!([["humidity", "integer"], ["temp", "float"]])
    );

    let v = query(
        &ts,
        r#"SHOW TAG VALUES FROM "weather" WITH KEY = "city" WHERE time > now() - 100w"#,
    )
    .await;
    assert_eq!(
        v["results"][0]["series"][0]["values"],
        serde_json::json!([["city", "LA"], ["city", "SF"]])
    );

    // Health-check queries Grafana variants send on save & test.
    let v = query(&ts, "SHOW RETENTION POLICIES on \"silodb\"").await;
    assert_eq!(v["results"][0]["series"][0]["values"][0][0], "autogen");
    let v = query(&ts, "SHOW DATABASES").await;
    assert_eq!(v["results"][0]["series"][0]["values"][0][0], "silodb");
}

#[tokio::test]
async fn panel_query_buckets_and_tags() {
    let ts = server();
    seed(&ts).await;
    // The exact shape Grafana's builder emits (time literals in ms).
    let v = query(
        &ts,
        r#"SELECT mean("temp") FROM "weather" WHERE time >= 0ms and time <= 21600000ms GROUP BY time(2h), "city" fill(null)"#,
    )
    .await;
    let series = v["results"][0]["series"].as_array().unwrap();
    assert_eq!(series.len(), 2, "{v}");
    let la = series.iter().find(|s| s["tags"]["city"] == "LA").unwrap();
    let sf = series.iter().find(|s| s["tags"]["city"] == "SF").unwrap();
    assert_eq!(la["name"], "weather");
    assert_eq!(la["columns"], serde_json::json!(["time", "mean"]));
    // 2h buckets of hourly 30,31,32,33,34,35 → 30.5, 32.5, 34.5; ms epoch.
    assert_eq!(
        la["values"],
        serde_json::json!([[0, 30.5], [7_200_000, 32.5], [14_400_000, 34.5]])
    );
    assert_eq!(sf["values"][0][1], 20.5);
}

#[tokio::test]
async fn template_variable_regex_and_selectors() {
    let ts = server();
    seed(&ts).await;
    // Multi-value variable: =~ /^(SF|LA)$/ ; single: last() for stat panels.
    let v = query(
        &ts,
        r#"SELECT last("temp") FROM "weather" WHERE "city" =~ /^(SF)$/ AND time >= 0 GROUP BY "city""#,
    )
    .await;
    let s = &v["results"][0]["series"][0];
    assert_eq!(s["tags"]["city"], "SF");
    assert_eq!(s["values"][0][1], 25.0, "{v}");
    assert_eq!(s["values"][0][0], 5 * 3_600_000, "last() stamps the winning row's time");

    // first() and whole-range mean.
    let v = query(&ts, r#"SELECT first("temp") FROM "weather" GROUP BY "city""#).await;
    let series = v["results"][0]["series"].as_array().unwrap();
    assert!(series.iter().any(|s| s["values"][0][1] == 30.0));
    let v = query(&ts, r#"SELECT mean("temp") FROM "weather" WHERE "city" = 'SF'"#).await;
    assert_eq!(v["results"][0]["series"][0]["values"][0][1], 22.5);

    // count + sum + min + max together, bucketed.
    let v = query(
        &ts,
        r#"SELECT count("temp"), sum("temp"), min("temp"), max("temp") FROM "weather" WHERE "city" = 'LA' GROUP BY time(6h)"#,
    )
    .await;
    assert_eq!(
        v["results"][0]["series"][0]["values"],
        serde_json::json!([[0, 6, 195.0, 30.0, 35.0]])
    );
}

#[tokio::test]
async fn raw_mode_desc_limit_and_multi_statement() {
    let ts = server();
    seed(&ts).await;
    let v = query(
        &ts,
        r#"SELECT "temp" FROM "weather" WHERE "city" = 'SF' ORDER BY time DESC LIMIT 2; SHOW MEASUREMENTS"#,
    )
    .await;
    let results = v["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["statement_id"], 0);
    assert_eq!(
        results[0]["series"][0]["values"],
        serde_json::json!([[5 * 3_600_000, 25.0], [4 * 3_600_000, 24.0]])
    );
    assert_eq!(results[1]["series"][0]["values"][0][0], "weather");
}

#[tokio::test]
async fn errors_are_inline_and_unknowns_are_empty() {
    let ts = server();
    seed(&ts).await;
    // Unsupported InfluxQL → per-statement error, HTTP 200 (influx shape).
    let v = query(&ts, "SELECT derivative(temp) FROM weather").await;
    assert!(v["results"][0]["error"].as_str().unwrap().contains("derivative"), "{v}");
    // Unknown measurement → empty result, no error (Grafana-friendly).
    let v = query(&ts, "SELECT mean(x) FROM nothere GROUP BY time(1m)").await;
    assert!(v["results"][0].get("series").is_none(), "{v}");
    assert!(v["results"][0].get("error").is_none(), "{v}");
    // Unknown field on a real measurement → loud.
    let v = query(&ts, r#"SELECT mean("nope") FROM "weather""#).await;
    assert!(v["results"][0]["error"].as_str().unwrap().contains("nope"), "{v}");
}

#[tokio::test]
async fn epoch_formats() {
    let ts = server();
    seed(&ts).await;
    for (epoch, expected_h1) in [("s", 3600i64), ("ms", 3_600_000), ("us", HOUR_US), ("ns", HOUR_US * 1000)] {
        let uri = format!(
            "/query?epoch={}&u=x&p=r-token&q={}",
            epoch,
            // Builder-shaped (GROUP BY time) so it stays on the InfluxQL
            // path, which is the one that formats epochs.
            urlencode(
                r#"SELECT mean("temp") FROM "weather" WHERE "city" = 'SF' GROUP BY time(1h)"#
            )
        );
        let (status, v) = raw(
            &ts,
            Request::builder().uri(uri).body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["results"][0]["series"][0]["values"][1][0], expected_h1, "epoch={epoch}");
    }
    // Default (no epoch=) is RFC3339 strings.
    let uri = format!(
        "/query?u=x&p=r-token&q={}",
        urlencode(r#"SELECT mean("temp") FROM "weather" GROUP BY time(1h)"#)
    );
    let (_, v) = raw(
        &ts,
        Request::builder().uri(uri).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(v["results"][0]["series"][0]["values"][0][0], "1970-01-01T00:00:00Z");
}

/// The query box is a SQLite console for anything InfluxQL can't say.
/// Grafana passes raw text through untouched, so these arrive verbatim.
#[tokio::test]
async fn plain_sqlite_through_the_query_box() {
    let ts = server();
    seed(&ts).await;

    // Valid InfluxQL that answers nothing — the case an error-only
    // fallback would miss, and the simplest thing anyone types.
    let v = query(&ts, "SELECT * FROM weather").await;
    let series = &v["results"][0]["series"][0];
    assert_eq!(series["values"].as_array().unwrap().len(), 12, "{v}");

    // Things the InfluxQL subset cannot express at all.
    for q in [
        "SELECT city, sum(temp) FROM weather GROUP BY city ORDER BY 2 DESC",
        "SELECT count(*) FROM weather WHERE city LIKE 'S%'",
        "WITH x AS (SELECT * FROM weather) SELECT count(*) FROM x",
        "SELECT ts, temp FROM weather WHERE temp > 22.0 ORDER BY ts LIMIT 3",
    ] {
        let v = query(&ts, q).await;
        assert!(v["results"][0].get("error").is_none(), "{q} -> {v}");
        assert!(v["results"][0]["series"][0]["values"][0].is_array(), "{q} -> {v}");
    }

    // A leading ts/time column becomes the series axis, in the requested
    // epoch (ms here), so time-series panels render it.
    let v = query(&ts, "SELECT ts, temp FROM weather ORDER BY ts LIMIT 1").await;
    let series = &v["results"][0]["series"][0];
    assert_eq!(series["columns"][0], "time", "{v}");
    assert_eq!(series["values"][0][0], 0, "{v}");
    let v = query(&ts, "SELECT ts, temp FROM weather ORDER BY ts DESC LIMIT 1").await;
    assert_eq!(v["results"][0]["series"][0]["values"][0][0], 5 * HOUR_US / 1000, "{v}");

    // A non-time first column is left alone rather than coerced.
    let v = query(&ts, "SELECT city, temp FROM weather LIMIT 1").await;
    assert_eq!(v["results"][0]["series"][0]["columns"][0], "city", "{v}");
}

/// A query legal in both dialects is read as SQL — silodb is a SQL
/// store. Grafana's builder loses nothing by this, because everything it
/// emits is a SQLite syntax error and stays on the InfluxQL path.
#[tokio::test]
async fn ambiguous_queries_are_sql_and_the_builder_is_unaffected() {
    let ts = server();
    seed(&ts).await;

    // Ambiguous → SQL: exactly the columns asked for, nothing prepended.
    let v = query(&ts, r#"SELECT "temp" FROM "weather" WHERE "city" = 'SF' LIMIT 2"#).await;
    let series = &v["results"][0]["series"][0];
    assert_eq!(series["name"], "result", "{v}");
    assert_eq!(series["columns"].as_array().unwrap(), &vec!["temp"], "{v}");

    // Every builder shape is still InfluxQL: named series, time axis.
    for q in [
        r#"SELECT mean("temp") FROM "weather" GROUP BY time(1h)"#,
        r#"SELECT mean("temp") FROM "weather" WHERE time >= 0ms GROUP BY time(1h), "city" fill(null)"#,
        r#"SELECT last("temp") FROM "weather""#,
    ] {
        let v = query(&ts, q).await;
        let series = &v["results"][0]["series"][0];
        assert_eq!(series["name"], "weather", "{q} -> {v}");
        assert_eq!(series["columns"][0], "time", "{q} -> {v}");
    }

    // SHOW verbs have no SQLite reading at all.
    let v = query(&ts, "SHOW MEASUREMENTS").await;
    assert_eq!(v["results"][0]["series"][0]["values"][0][0], "weather", "{v}");
}

/// SQL on /query runs behind the same authorizer as POST /sql: the
/// credential tables stay invisible, and denial is an error rather than
/// an empty result that reads like "no data".
#[tokio::test]
async fn sql_on_query_is_fenced_like_sql_endpoint() {
    let ts = server();
    seed(&ts).await;
    // Mint a key so the credential table actually exists to be denied.
    let req = Request::builder()
        .method("POST")
        .uri("/admin/api/keys")
        .header("authorization", "Bearer d-token")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"name":"k1","role":"readonly","scope":["weather"]}"#))
        .unwrap();
    let (status, v) = raw(&ts, req).await;
    assert_eq!(status, StatusCode::OK, "{v}");

    for token in ["r-token", "d-token"] {
        let v = query_as(&ts, "SELECT * FROM _silodb_server_keys", token).await;
        let err = v["results"][0]["error"].as_str().unwrap_or("");
        assert!(err.contains("prohibited"), "{token} got {v}");
    }
}

/// Same rule as POST /sql: a chart drawn from a truncated result is a
/// wrong chart, and Grafana has no way to send a limit.
#[tokio::test]
async fn sql_on_query_refuses_to_truncate() {
    let ts = server_with_cap(5);
    seed(&ts).await;
    let v = query(&ts, "SELECT * FROM weather").await;
    let err = v["results"][0]["error"].as_str().unwrap_or("");
    assert!(err.contains("exceeds 5 rows"), "{v}");
}
