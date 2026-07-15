//! Typed client for silodb-server's JSON API — RPC ergonomics
//! (`api::tables().await?`), plain fetch underneath, same endpoints curl
//! uses. The bearer token rides in localStorage.

use gloo_net::http::{Method, RequestBuilder};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct TableInfo {
    pub table: String,
    pub tiers: Vec<String>,
    pub retention: Option<String>,
    pub ts_column: String,
    pub base_dir: String,
    pub columns: Vec<ColumnInfo>,
    pub hot_rows: i64,
    pub active_files: i64,
    pub cold_rows: i64,
    pub cold_range: Option<(i64, i64)>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct KeyInfo {
    pub name: String,
    pub role: String,
    pub scope: Option<Vec<String>>,
    pub created_at: i64,
    pub revoked: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Default)]
pub struct SqlResult {
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub rows: Vec<Vec<serde_json::Value>>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub rows_affected: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CreateTable {
    pub name: String,
    pub schema: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tiers: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CreateKey {
    pub name: String,
    pub role: String,
    pub scope: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApiError {
    pub status: u16,
    pub message: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

const TOKEN_KEY: &str = "silodb-admin-token";

fn storage() -> web_sys::Storage {
    web_sys::window().unwrap().local_storage().unwrap().unwrap()
}

pub fn token() -> String {
    storage().get_item(TOKEN_KEY).unwrap().unwrap_or_default()
}

pub fn set_token(t: &str) {
    storage().set_item(TOKEN_KEY, t).unwrap();
}

pub fn clear_token() {
    storage().remove_item(TOKEN_KEY).unwrap();
}

async fn call<T: DeserializeOwned>(
    method: Method,
    path: &str,
    body: Option<serde_json::Value>,
) -> Result<T, ApiError> {
    let err = |message: String| ApiError { status: 0, message };
    let req = RequestBuilder::new(path)
        .method(method)
        .header("authorization", &format!("Bearer {}", token()))
        .header("content-type", "application/json");
    let resp = match body {
        Some(b) => req.json(&b).map_err(|e| err(e.to_string()))?.send().await,
        None => req.send().await,
    }
    .map_err(|e| err(e.to_string()))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status >= 400 {
        let message = serde_json::from_str::<serde_json::Value>(&text)
            .ok()
            .and_then(|v| v["error"].as_str().map(str::to_owned))
            .unwrap_or_else(|| format!("HTTP {status}"));
        return Err(ApiError { status, message });
    }
    serde_json::from_str(&text).map_err(|e| err(format!("bad response: {e}")))
}

#[derive(Deserialize)]
struct Tables {
    tables: Vec<TableInfo>,
}
#[derive(Deserialize)]
struct Keys {
    keys: Vec<KeyInfo>,
}
#[derive(Deserialize)]
struct Secret {
    secret: String,
}

pub async fn tables() -> Result<Vec<TableInfo>, ApiError> {
    call::<Tables>(Method::GET, "/admin/api/tables", None).await.map(|t| t.tables)
}

pub async fn create_table(req: &CreateTable) -> Result<(), ApiError> {
    call::<serde_json::Value>(
        Method::POST,
        "/admin/api/tables",
        Some(serde_json::to_value(req).unwrap()),
    )
    .await
    .map(|_| ())
}

pub async fn add_column(table: &str, coldef: &str) -> Result<(), ApiError> {
    call::<serde_json::Value>(
        Method::POST,
        &format!("/admin/api/tables/{table}/columns"),
        Some(serde_json::json!({ "coldef": coldef })),
    )
    .await
    .map(|_| ())
}

pub async fn set_retention(table: &str, retain: Option<&str>) -> Result<(), ApiError> {
    call::<serde_json::Value>(
        Method::PUT,
        &format!("/admin/api/tables/{table}/retention"),
        Some(serde_json::json!({ "retain": retain })),
    )
    .await
    .map(|_| ())
}

pub async fn keys() -> Result<Vec<KeyInfo>, ApiError> {
    call::<Keys>(Method::GET, "/admin/api/keys", None).await.map(|k| k.keys)
}

pub async fn create_key(req: &CreateKey) -> Result<String, ApiError> {
    call::<Secret>(
        Method::POST,
        "/admin/api/keys",
        Some(serde_json::to_value(req).unwrap()),
    )
    .await
    .map(|s| s.secret)
}

pub async fn revoke_key(name: &str) -> Result<(), ApiError> {
    call::<serde_json::Value>(Method::DELETE, &format!("/admin/api/keys/{name}"), None)
        .await
        .map(|_| ())
}

pub async fn sql(query: &str) -> Result<SqlResult, ApiError> {
    call(
        Method::POST,
        "/sql",
        Some(serde_json::json!({ "sql": query })),
    )
    .await
}
