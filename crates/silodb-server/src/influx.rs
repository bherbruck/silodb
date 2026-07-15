//! Execution half of the InfluxQL emulation: translate the parsed subset
//! to the engine's SQL (`GROUP BY time(...)` → `silodb_bucket(...)`), run
//! it on a read-only connection, and shape rows into InfluxDB 1.x's
//! response JSON (`results` → `series` → `columns`/`values`, one series
//! per GROUP BY tag combination).

use crate::influxql::{Agg, Cond, Fill, ScalarValue, Select, Statement};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value as Json};
use std::collections::BTreeMap;

/// Output timestamp format, from the `epoch=` query parameter.
#[derive(Clone, Copy)]
pub enum Epoch {
    Ns,
    Us,
    Ms,
    S,
    Rfc3339,
}

impl Epoch {
    pub fn parse(s: Option<&str>) -> Option<Epoch> {
        match s {
            None => Some(Epoch::Rfc3339),
            Some("ns") => Some(Epoch::Ns),
            Some("u" | "us" | "µ" | "µs") => Some(Epoch::Us),
            Some("ms") => Some(Epoch::Ms),
            Some("s") => Some(Epoch::S),
            Some("rfc3339") => Some(Epoch::Rfc3339),
            _ => None,
        }
    }

    fn time_json(&self, us: i64) -> Json {
        match self {
            Epoch::Ns => json!(us.saturating_mul(1000)),
            Epoch::Us => json!(us),
            Epoch::Ms => json!(us / 1000),
            Epoch::S => json!(us / 1_000_000),
            Epoch::Rfc3339 => json!(rfc3339(us)),
        }
    }
}

/// µs → RFC 3339 UTC (Howard Hinnant's civil-from-days).
fn rfc3339(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let sub_us = us.rem_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let (h, min, s) = (tod / 3600, (tod / 60) % 60, tod % 60);
    if sub_us == 0 {
        format!("{y:04}-{m:02}-{d:02}T{h:02}:{min:02}:{s:02}Z")
    } else {
        format!("{y:04}-{m:02}-{d:02}T{h:02}:{min:02}:{s:02}.{sub_us:06}Z")
    }
}

fn qid(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// One measurement's shape, derived from its hot table exactly the way
/// line-protocol autoschema writes it: TEXT → tag, REAL/INTEGER → field.
struct Shape {
    ts: String,
    tags: Vec<String>,
    /// (name, influx field type)
    fields: Vec<(String, &'static str)>,
}

fn shape(conn: &Connection, table: &str) -> Result<Option<Shape>, String> {
    let hot = silodb::resolve_hot_table(conn, table).map_err(|e| e.to_string())?;
    let Some(hot) = hot else { return Ok(None) };
    let ts = silodb::catalog::get_policy(conn, table)
        .map_err(|e| e.to_string())?
        .and_then(|p| p.ts_column)
        .unwrap_or_else(|| "ts".into());
    let mut tags = Vec::new();
    let mut fields = Vec::new();
    let cols: Vec<(String, String)> = conn
        .prepare(&format!("PRAGMA table_info({})", qid(&hot)))
        .and_then(|mut s| {
            s.query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
                .collect()
        })
        .map_err(|e| e.to_string())?;
    for (name, decl) in cols {
        if name == ts {
            continue;
        }
        let d = decl.to_ascii_uppercase();
        if d.contains("CHAR") || d.contains("TEXT") || d.contains("CLOB") {
            tags.push(name);
        } else if d.contains("REAL") || d.contains("FLOA") || d.contains("DOUB") {
            fields.push((name, "float"));
        } else if d.contains("INT") || d.contains("TIMESTAMP") || d.contains("DATETIME") {
            fields.push((name, "integer"));
        }
    }
    // Influx lists keys alphabetically; SELECT only does membership
    // lookups here, so declaration order carries nothing.
    tags.sort();
    fields.sort();
    Ok(Some(Shape { ts, tags, fields }))
}

fn measurements(conn: &Connection) -> Result<Vec<String>, String> {
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

/// Execute one statement; returns the influx `series` array. `scope`
/// (a provisioned key's table list) filters SHOW results and fences
/// SELECT to the scope's measurements.
pub fn execute(
    conn: &Connection,
    stmt: &Statement,
    epoch: Epoch,
    max_rows: usize,
    scope: Option<&[String]>,
) -> Result<Json, String> {
    let in_scope = |m: &str| scope.is_none_or(|s| s.iter().any(|t| t == m));
    match stmt {
        Statement::ShowDatabases => Ok(json!([{
            "name": "databases", "columns": ["name"], "values": [["silodb"]]
        }])),
        Statement::ShowRetentionPolicies => Ok(json!([{
            "columns": ["name", "duration", "shardGroupDuration", "replicaN", "default"],
            "values": [["autogen", "0s", "168h0m0s", 1, true]]
        }])),
        Statement::ShowMeasurements { limit } => {
            let mut names = measurements(conn)?;
            names.retain(|n| in_scope(n));
            if let Some(l) = limit {
                names.truncate(*l as usize);
            }
            if names.is_empty() {
                return Ok(json!([]));
            }
            Ok(json!([{
                "name": "measurements",
                "columns": ["name"],
                "values": names.iter().map(|n| json!([n])).collect::<Vec<_>>(),
            }]))
        }
        Statement::ShowTagKeys { from } => {
            let mut out = Vec::new();
            for m in from_or_all(conn, from)?.into_iter().filter(|m| in_scope(m)) {
                if let Some(sh) = shape(conn, &m)? {
                    if sh.tags.is_empty() {
                        continue;
                    }
                    out.push(json!({
                        "name": m,
                        "columns": ["tagKey"],
                        "values": sh.tags.iter().map(|t| json!([t])).collect::<Vec<_>>(),
                    }));
                }
            }
            Ok(Json::Array(out))
        }
        Statement::ShowFieldKeys { from } => {
            let mut out = Vec::new();
            for m in from_or_all(conn, from)?.into_iter().filter(|m| in_scope(m)) {
                if let Some(sh) = shape(conn, &m)? {
                    if sh.fields.is_empty() {
                        continue;
                    }
                    out.push(json!({
                        "name": m,
                        "columns": ["fieldKey", "fieldType"],
                        "values": sh.fields.iter().map(|(n, t)| json!([n, t])).collect::<Vec<_>>(),
                    }));
                }
            }
            Ok(Json::Array(out))
        }
        Statement::ShowTagValues { from, key } => {
            let mut out = Vec::new();
            for m in from_or_all(conn, from)?.into_iter().filter(|m| in_scope(m)) {
                let Some(sh) = shape(conn, &m)? else { continue };
                if !sh.tags.contains(key) {
                    continue;
                }
                let vals: Vec<String> = conn
                    .prepare(&format!(
                        "SELECT DISTINCT {k} FROM {m} WHERE {k} IS NOT NULL ORDER BY 1",
                        k = qid(key),
                        m = qid(&m)
                    ))
                    .and_then(|mut s| s.query_map([], |r| r.get(0))?.collect())
                    .map_err(|e| e.to_string())?;
                if vals.is_empty() {
                    continue;
                }
                out.push(json!({
                    "name": m,
                    "columns": ["key", "value"],
                    "values": vals.iter().map(|v| json!([key, v])).collect::<Vec<_>>(),
                }));
            }
            Ok(Json::Array(out))
        }
        Statement::Select(sel) => {
            if !in_scope(&sel.measurement) {
                return Err(format!(
                    "measurement '{}' is outside this key's scope",
                    sel.measurement
                ));
            }
            select(conn, sel, epoch, max_rows)
        }
    }
}

fn from_or_all(conn: &Connection, from: &Option<String>) -> Result<Vec<String>, String> {
    match from {
        Some(m) => Ok(vec![m.clone()]),
        None => measurements(conn),
    }
}

fn select(conn: &Connection, sel: &Select, epoch: Epoch, max_rows: usize) -> Result<Json, String> {
    let Some(sh) = shape(conn, &sel.measurement)? else {
        return Ok(json!([])); // unknown measurement = empty result (influx-like)
    };

    // Resolve GROUP BY * and validate tag names.
    let mut group_tags: Vec<String> = Vec::new();
    for t in &sel.group_tags {
        if t == "*" {
            group_tags.extend(sh.tags.iter().cloned());
        } else if sh.tags.contains(t) {
            group_tags.push(t.clone());
        }
        // unknown grouping tag: influx just yields it empty — skip it
    }
    group_tags.dedup();

    // Resolve fields; `*` expands to every field column.
    let mut fields: Vec<(Agg, String, String)> = Vec::new(); // (agg, col, out-name)
    for f in &sel.fields {
        let cols: Vec<String> = if f.column == "*" {
            sh.fields.iter().map(|(n, _)| n.clone()).collect()
        } else {
            vec![f.column.clone()]
        };
        for c in cols {
            if !sh.fields.iter().any(|(n, _)| *n == c) && !sh.tags.contains(&c) {
                return Err(format!(
                    "unknown field '{c}' on '{}' (fields: {})",
                    sel.measurement,
                    sh.fields.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>().join(", ")
                ));
            }
            let name = f.alias.clone().unwrap_or_else(|| match f.agg {
                Agg::None => c.clone(),
                Agg::Mean => "mean".into(),
                Agg::Sum => "sum".into(),
                Agg::Min => "min".into(),
                Agg::Max => "max".into(),
                Agg::Count => "count".into(),
                Agg::Last => "last".into(),
                Agg::First => "first".into(),
                Agg::Spread => "spread".into(),
            });
            fields.push((f.agg, c, name));
        }
    }
    if fields.is_empty() {
        return Ok(json!([]));
    }
    let has_agg = fields.iter().any(|(a, ..)| *a != Agg::None);
    let has_selector = fields
        .iter()
        .any(|(a, ..)| matches!(a, Agg::Last | Agg::First));
    if has_selector && fields.len() > 1 {
        return Err("last()/first() must be the only selected field".into());
    }
    if !has_agg && sel.group_time.is_some() {
        return Err("GROUP BY time() needs an aggregate (mean, sum, …)".into());
    }

    // WHERE
    let mut where_parts: Vec<String> = Vec::new();
    let mut params: Vec<Value> = Vec::new();
    let ts = qid(&sh.ts);
    if let Some(lo) = sel.time_lo {
        params.push(Value::Integer(lo));
        where_parts.push(format!("{ts} >= ?{}", params.len()));
    }
    if let Some(hi) = sel.time_hi {
        params.push(Value::Integer(hi));
        where_parts.push(format!("{ts} <= ?{}", params.len()));
    }
    for c in &sel.conds {
        match c {
            Cond::True => {}
            Cond::Eq(col, v) | Cond::Ne(col, v) => {
                params.push(match v {
                    ScalarValue::Text(s) => Value::Text(s.clone()),
                    ScalarValue::Num(n) => Value::Real(*n),
                });
                let op = if matches!(c, Cond::Eq(..)) { "=" } else { "!=" };
                where_parts.push(format!("{} {op} ?{}", qid(col), params.len()));
            }
            Cond::Gt(col, n) | Cond::Ge(col, n) | Cond::Lt(col, n) | Cond::Le(col, n) => {
                params.push(Value::Real(*n));
                let op = match c {
                    Cond::Gt(..) => ">",
                    Cond::Ge(..) => ">=",
                    Cond::Lt(..) => "<",
                    _ => "<=",
                };
                where_parts.push(format!("{} {op} ?{}", qid(col), params.len()));
            }
            Cond::In(col, vs) | Cond::NotIn(col, vs) => {
                let mut ph = Vec::new();
                for v in vs {
                    params.push(Value::Text(v.clone()));
                    ph.push(format!("?{}", params.len()));
                }
                let not = if matches!(c, Cond::NotIn(..)) { "NOT " } else { "" };
                where_parts.push(format!("{} {not}IN ({})", qid(col), ph.join(", ")));
            }
        }
    }
    let where_sql = if where_parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_parts.join(" AND "))
    };

    // SELECT list: time expr, group tags, then values.
    let time_expr = match sel.group_time {
        Some(iv) => format!("silodb_bucket({iv}, {ts})"),
        None if has_selector => {
            let (agg, ..) = fields[0];
            // SQLite's documented bare-column rule: with a single min()/
            // max() aggregate, other selected columns come from the
            // winning row — exactly last()/first().
            if agg == Agg::Last {
                format!("max({ts})")
            } else {
                format!("min({ts})")
            }
        }
        // Whole-range aggregate (no buckets): a bare ts next to avg()
        // would be an arbitrary row — influx stamps the range start.
        None if has_agg => format!("{}", sel.time_lo.unwrap_or(0)),
        None => ts.clone(),
    };
    let mut select_parts = vec![format!("{time_expr} AS __time")];
    for t in &group_tags {
        select_parts.push(qid(t));
    }
    for (agg, col, _) in &fields {
        let c = qid(col);
        select_parts.push(match agg {
            Agg::None => c,
            Agg::Mean => format!("avg({c})"),
            Agg::Sum => format!("sum({c})"),
            Agg::Min => format!("min({c})"),
            Agg::Max => format!("max({c})"),
            Agg::Count => format!("count({c})"),
            Agg::Spread => format!("max({c}) - min({c})"),
            // bare column beside the single min/max(ts) in time_expr
            Agg::Last | Agg::First => {
                if sel.group_time.is_some() {
                    // inside buckets we still need the selector aggregate
                    // to be the one min/max in the row
                    c
                } else {
                    c
                }
            }
        });
    }

    // last()/first() inside time buckets: the bucket expr isn't min/max,
    // so add the selector as the single min/max aggregate.
    let mut group_parts: Vec<String> = Vec::new();
    if sel.group_time.is_some() || has_agg {
        if sel.group_time.is_some() {
            group_parts.push("__time".into());
        }
        for t in &group_tags {
            group_parts.push(qid(t));
        }
    }
    let mut sql = format!(
        "SELECT {} FROM {}{}",
        select_parts.join(", "),
        qid(&sel.measurement),
        where_sql
    );
    if has_selector && sel.group_time.is_some() {
        // rewrite: wrap the selector column with the min/max-of-ts trick
        // inside each bucket by adding max/min(ts) as an extra aggregate.
        let (agg, ..) = fields[0];
        let sel_ts = if agg == Agg::Last {
            format!("max({ts})")
        } else {
            format!("min({ts})")
        };
        sql = format!(
            "SELECT {} , {sel_ts} AS __selts FROM {}{}",
            select_parts.join(", "),
            qid(&sel.measurement),
            where_sql
        );
    }
    if !group_parts.is_empty() {
        sql.push_str(&format!(" GROUP BY {}", group_parts.join(", ")));
    }
    sql.push_str(&format!(
        " ORDER BY __time {}",
        if sel.order_desc { "DESC" } else { "ASC" }
    ));
    let limit = sel.limit.map(|l| l as usize).unwrap_or(max_rows).min(max_rows);
    sql.push_str(&format!(" LIMIT {limit}"));

    // Run + assemble series (one per distinct group-tag combination).
    let mut stmt = conn.prepare(&sql).map_err(|e| format!("{e} (sql: {sql})"))?;
    let n_tags = group_tags.len();
    let mut rows = stmt
        .query(rusqlite::params_from_iter(params))
        .map_err(|e| e.to_string())?;
    let mut series: BTreeMap<Vec<String>, Vec<Json>> = BTreeMap::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let t_us: i64 = row.get::<_, Option<i64>>(0).map_err(|e| e.to_string())?.unwrap_or(0);
        let mut key = Vec::with_capacity(n_tags);
        for i in 0..n_tags {
            key.push(
                row.get::<_, Option<String>>(1 + i)
                    .map_err(|e| e.to_string())?
                    .unwrap_or_default(),
            );
        }
        let mut vals = Vec::with_capacity(1 + fields.len());
        vals.push(epoch.time_json(t_us));
        for i in 0..fields.len() {
            let v = row.get_ref(1 + n_tags + i).map_err(|e| e.to_string())?;
            vals.push(match v {
                rusqlite::types::ValueRef::Null => {
                    if sel.fill == Fill::Zero {
                        json!(0)
                    } else {
                        Json::Null
                    }
                }
                rusqlite::types::ValueRef::Integer(i) => json!(i),
                rusqlite::types::ValueRef::Real(f) => json!(f),
                rusqlite::types::ValueRef::Text(t) => json!(String::from_utf8_lossy(t)),
                rusqlite::types::ValueRef::Blob(_) => Json::Null,
            });
        }
        series.entry(key).or_default().push(Json::Array(vals));
    }

    let columns: Vec<Json> = std::iter::once(json!("time"))
        .chain(fields.iter().map(|(_, _, n)| json!(n)))
        .collect();
    let out: Vec<Json> = series
        .into_iter()
        .map(|(key, values)| {
            let mut s = json!({
                "name": sel.measurement,
                "columns": columns,
                "values": values,
            });
            if n_tags > 0 {
                let tags: serde_json::Map<String, Json> = group_tags
                    .iter()
                    .zip(&key)
                    .map(|(k, v)| (k.clone(), json!(v)))
                    .collect();
                s["tags"] = Json::Object(tags);
            }
            s
        })
        .collect();
    Ok(Json::Array(out))
}
