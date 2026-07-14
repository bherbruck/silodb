//! InfluxDB line protocol: parse, autoschema, insert.
//!
//! `measurement[,tag=v...] field=v[,field=v...] [timestamp]`
//! - measurement → silodb table (created on first sight, ddl role only)
//! - tags → TEXT columns; fields → REAL (bare float), INTEGER (`i`
//!   suffix / booleans), TEXT (quoted strings)
//! - new tag/field on an existing table → ADD COLUMN evolution (ddl only)
//! - timestamp defaults to nanoseconds (influx convention), converted to
//!   the engine's µs; missing timestamp = server now

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    Boolean(bool),
    Text,
}

#[derive(Debug)]
pub struct Line {
    pub measurement: String,
    pub tags: Vec<(String, String)>,
    /// (name, value-as-sqlite, class) — class drives autoschema typing.
    pub fields: Vec<(String, rusqlite::types::Value, FieldValue)>,
    pub timestamp: Option<i64>,
}

#[derive(Debug)]
pub struct ParseError {
    pub line_no: usize,
    pub msg: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line_no, self.msg)
    }
}

/// Timestamp multiplier to µs for a `?precision=` value.
pub fn precision_to_us(precision: &str) -> Option<(i64, i64)> {
    // (divide, multiply): ns divides by 1000, s multiplies by 1e6.
    match precision {
        "ns" => Some((1000, 1)),
        "us" | "u" => Some((1, 1)),
        "ms" => Some((1, 1000)),
        "s" => Some((1, 1_000_000)),
        _ => None,
    }
}

pub fn parse(body: &str) -> Result<Vec<Line>, ParseError> {
    let mut out = Vec::new();
    for (i, raw) in body.lines().enumerate() {
        let line_no = i + 1;
        let raw = raw.trim();
        if raw.is_empty() || raw.starts_with('#') {
            continue;
        }
        out.push(parse_line(raw).map_err(|msg| ParseError { line_no, msg })?);
    }
    Ok(out)
}

/// Split `raw` on unescaped/unquoted `sep`, at most once, returning
/// (head, rest). Backslash escapes the next char; double quotes group.
fn split_once_unescaped(raw: &str, sep: char) -> (&str, Option<&str>) {
    let mut in_quotes = false;
    let mut escaped = false;
    for (i, c) in raw.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match c {
            '\\' => escaped = true,
            '"' => in_quotes = !in_quotes,
            c if c == sep && !in_quotes => return (&raw[..i], Some(&raw[i + c.len_utf8()..])),
            _ => {}
        }
    }
    (raw, None)
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut escaped = false;
    for c in s.chars() {
        if escaped {
            out.push(c);
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else {
            out.push(c);
        }
    }
    out
}

/// Column/table names become SQL identifiers — hold them to a strict
/// charset instead of trusting quoting alone.
fn check_ident(name: &str, what: &str) -> Result<(), String> {
    let mut chars = name.chars();
    let ok = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !ok {
        return Err(format!(
            "{what} '{name}' is not a valid identifier ([A-Za-z_][A-Za-z0-9_]*)"
        ));
    }
    Ok(())
}

/// Tags and fields become columns beside the timestamp — `ts` is taken.
fn check_column(name: &str, what: &str) -> Result<(), String> {
    check_ident(name, what)?;
    if name.eq_ignore_ascii_case("ts") {
        return Err(format!("{what} 'ts' collides with the timestamp column"));
    }
    Ok(())
}

fn parse_line(raw: &str) -> Result<Line, String> {
    // measurement+tags | fields | optional timestamp — space-separated at
    // the top level (spaces inside quoted field values don't count).
    let (head, rest) = split_once_unescaped(raw, ' ');
    let rest = rest.ok_or("missing fields section")?;
    let (fields_part, ts_part) = split_once_unescaped(rest.trim_start(), ' ');

    // head: measurement[,tag=v,...]
    let (measurement_raw, mut tags_rest) = split_once_unescaped(head, ',');
    let measurement = unescape(measurement_raw);
    check_ident(&measurement, "measurement")?;
    let mut tags = Vec::new();
    while let Some(part) = tags_rest {
        let (pair, next) = split_once_unescaped(part, ',');
        tags_rest = next;
        let (k, v) = split_once_unescaped(pair, '=');
        let v = v.ok_or_else(|| format!("tag '{pair}' has no value"))?;
        let k = unescape(k);
        check_column(&k, "tag")?;
        tags.push((k, unescape(v)));
    }

    // fields: k=v[,k=v...]
    let mut fields = Vec::new();
    let mut fields_rest = Some(fields_part);
    while let Some(part) = fields_rest {
        let (pair, next) = split_once_unescaped(part, ',');
        fields_rest = next;
        if pair.is_empty() {
            continue;
        }
        let (k, v) = split_once_unescaped(pair, '=');
        let v = v.ok_or_else(|| format!("field '{pair}' has no value"))?;
        let k = unescape(k);
        check_column(&k, "field")?;
        fields.push(parse_field(k, v)?);
    }
    if fields.is_empty() {
        return Err("no fields".into());
    }

    let timestamp = match ts_part.map(str::trim) {
        None | Some("") => None,
        Some(t) => Some(
            t.parse::<i64>()
                .map_err(|_| format!("bad timestamp '{t}'"))?,
        ),
    };

    Ok(Line {
        measurement,
        tags,
        fields,
        timestamp,
    })
}

fn parse_field(
    key: String,
    v: &str,
) -> Result<(String, rusqlite::types::Value, FieldValue), String> {
    use rusqlite::types::Value;
    if let Some(stripped) = v.strip_prefix('"') {
        let inner = stripped
            .strip_suffix('"')
            .ok_or_else(|| format!("field '{key}': unterminated string"))?;
        return Ok((key, Value::Text(unescape(inner)), FieldValue::Text));
    }
    if let Some(int) = v.strip_suffix(['i', 'u']) {
        let n = int
            .parse::<i64>()
            .map_err(|_| format!("field '{key}': bad integer '{v}'"))?;
        return Ok((key, Value::Integer(n), FieldValue::Integer(n)));
    }
    match v {
        "t" | "T" | "true" | "True" | "TRUE" => {
            return Ok((key, Value::Integer(1), FieldValue::Boolean(true)))
        }
        "f" | "F" | "false" | "False" | "FALSE" => {
            return Ok((key, Value::Integer(0), FieldValue::Boolean(false)))
        }
        _ => {}
    }
    let f = v
        .parse::<f64>()
        .map_err(|_| format!("field '{key}': bad value '{v}'"))?;
    Ok((key, Value::Real(f), FieldValue::Float(f)))
}

/// SQLite declared type for a field's autoschema column.
pub fn field_decl(v: &FieldValue) -> &'static str {
    match v {
        FieldValue::Float(_) => "REAL",
        FieldValue::Integer(_) | FieldValue::Boolean(_) => "INTEGER",
        FieldValue::Text => "TEXT",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::types::Value;

    #[test]
    fn basic_line() {
        let lines =
            parse("weather,city=SF,station=a1 temp=21.5,humidity=40i,ok=true 1700000000000000000\n")
                .unwrap();
        assert_eq!(lines.len(), 1);
        let l = &lines[0];
        assert_eq!(l.measurement, "weather");
        assert_eq!(l.tags, vec![("city".into(), "SF".into()), ("station".into(), "a1".into())]);
        assert_eq!(l.fields[0].1, Value::Real(21.5));
        assert_eq!(l.fields[1].1, Value::Integer(40));
        assert_eq!(l.fields[2].1, Value::Integer(1));
        assert_eq!(l.timestamp, Some(1_700_000_000_000_000_000));
    }

    #[test]
    fn escapes_and_quotes() {
        let lines = parse(r#"m,tag=a\ b msg="hello, \"world\"",n=1i"#).unwrap();
        assert_eq!(lines[0].tags[0].1, "a b");
        assert_eq!(lines[0].fields[0].1, Value::Text(r#"hello, "world""#.into()));
        assert_eq!(lines[0].timestamp, None);
    }

    #[test]
    fn empty_and_comment_lines_skipped() {
        let lines = parse("# comment\n\nm v=1\n").unwrap();
        assert_eq!(lines.len(), 1);
    }

    #[test]
    fn errors_carry_line_numbers() {
        let err = parse("m v=1\nbroken").unwrap_err();
        assert_eq!(err.line_no, 2);
        // Hostile identifiers rejected, not quoted-through.
        assert!(parse("m,ba\\\"d=1 v=1").is_err());
        assert!(parse("drop table x v=1").is_err());
        assert!(parse("m ts=1i").unwrap_err().msg.contains("collides"));
    }

    #[test]
    fn negative_and_scientific_floats() {
        let lines = parse("m a=-1.5,b=2e3,c=-7i").unwrap();
        assert_eq!(lines[0].fields[0].1, Value::Real(-1.5));
        assert_eq!(lines[0].fields[1].1, Value::Real(2000.0));
        assert_eq!(lines[0].fields[2].1, Value::Integer(-7));
    }
}
