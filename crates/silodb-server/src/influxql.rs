//! InfluxQL emulation — enough of InfluxDB 1.x's query language for stock
//! Grafana's core InfluxDB datasource to work against silodb: the visual
//! builder's meta-queries (SHOW MEASUREMENTS / TAG KEYS / FIELD KEYS /
//! TAG VALUES) and the panel-query shape it emits:
//!
//! ```text
//! SELECT mean("temp") FROM "weather" WHERE ("city" = 'SF') AND time >= now() - 6h
//! GROUP BY time(30s), "city" fill(null) ORDER BY time DESC LIMIT 100
//! ```
//!
//! This is deliberately a subset, not an InfluxQL implementation —
//! anything outside the shape Grafana generates gets a clear error naming
//! what is supported. Translation targets the engine's own SQL:
//! `GROUP BY time(...)` becomes `silodb_bucket(...)`.

use std::fmt;

// --- AST -----------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub enum Statement {
    ShowDatabases,
    ShowRetentionPolicies,
    ShowMeasurements { limit: Option<u64> },
    ShowTagKeys { from: Option<String> },
    ShowFieldKeys { from: Option<String> },
    ShowTagValues { from: Option<String>, key: String },
    Select(Select),
}

#[derive(Debug, PartialEq)]
pub struct Select {
    pub fields: Vec<Field>,
    pub measurement: String,
    /// Tag/field conditions (ANDed; that's all the builder emits).
    pub conds: Vec<Cond>,
    /// Time range in engine µs, half-open-ish (lo inclusive, hi inclusive
    /// like influx).
    pub time_lo: Option<i64>,
    pub time_hi: Option<i64>,
    /// GROUP BY time(interval) in µs.
    pub group_time: Option<i64>,
    pub group_tags: Vec<String>,
    pub fill: Fill,
    pub order_desc: bool,
    pub limit: Option<u64>,
}

#[derive(Debug, PartialEq)]
pub struct Field {
    pub agg: Agg,
    pub column: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Agg {
    None,
    Mean,
    Sum,
    Min,
    Max,
    Count,
    Last,
    First,
    Spread,
}

#[derive(Debug, PartialEq)]
pub enum Cond {
    Eq(String, ScalarValue),
    Ne(String, ScalarValue),
    Gt(String, f64),
    Ge(String, f64),
    Lt(String, f64),
    Le(String, f64),
    /// `=~ /^(a|b)$/` — Grafana's multi-value template variables.
    In(String, Vec<String>),
    NotIn(String, Vec<String>),
    /// `=~ /.*/` or `=~ /^$/` style match-alls — dropped.
    True,
}

#[derive(Debug, PartialEq)]
pub enum ScalarValue {
    Text(String),
    Num(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Fill {
    Null,
    None,
    Zero,
}

#[derive(Debug)]
pub struct QlError(pub String);

impl fmt::Display for QlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

fn err<T>(msg: impl Into<String>) -> Result<T, QlError> {
    Err(QlError(msg.into()))
}

// --- tokenizer -----------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),  // bare or "quoted"
    Str(String),    // 'single quoted'
    Num(f64),
    /// integer literal with optional duration suffix, resolved to µs when
    /// used as a duration/timestamp
    Dur { raw: i64, unit: Option<String> },
    Regex(String),
    LParen,
    RParen,
    Comma,
    Op(String), // = != <> =~ !~ < <= > >= + -
    Star,
}

fn tokenize(q: &str) -> Result<Vec<Tok>, QlError> {
    let mut toks = Vec::new();
    let b = q.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let c = b[i] as char;
        match c {
            ' ' | '\t' | '\r' | '\n' => i += 1,
            '(' => {
                toks.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                toks.push(Tok::RParen);
                i += 1;
            }
            ',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            '*' => {
                toks.push(Tok::Star);
                i += 1;
            }
            '"' | '`' => {
                let quote = c;
                let start = i + 1;
                let mut j = start;
                while j < b.len() && b[j] as char != quote {
                    j += 1;
                }
                if j >= b.len() {
                    return err("unterminated quoted identifier");
                }
                toks.push(Tok::Ident(q[start..j].to_owned()));
                i = j + 1;
            }
            '\'' => {
                let start = i + 1;
                let mut j = start;
                let mut s = String::new();
                while j < b.len() {
                    let cj = b[j] as char;
                    if cj == '\\' && j + 1 < b.len() {
                        s.push(b[j + 1] as char);
                        j += 2;
                    } else if cj == '\'' {
                        break;
                    } else {
                        s.push(cj);
                        j += 1;
                    }
                }
                if j >= b.len() {
                    return err("unterminated string");
                }
                toks.push(Tok::Str(s));
                i = j + 1;
            }
            '/' => {
                // regex literal (only appears after =~ / !~ in the subset)
                let start = i + 1;
                let mut j = start;
                while j < b.len() && b[j] as char != '/' {
                    if b[j] as char == '\\' {
                        j += 1;
                    }
                    j += 1;
                }
                if j >= b.len() {
                    return err("unterminated regex");
                }
                toks.push(Tok::Regex(q[start..j].to_owned()));
                i = j + 1;
            }
            '=' | '!' | '<' | '>' => {
                let two = &q[i..(i + 2).min(q.len())];
                if matches!(two, "=~" | "!~" | "!=" | "<>" | "<=" | ">=") {
                    toks.push(Tok::Op(two.to_owned()));
                    i += 2;
                } else {
                    toks.push(Tok::Op(c.to_string()));
                    i += 1;
                }
            }
            '+' | '-' => {
                toks.push(Tok::Op(c.to_string()));
                i += 1;
            }
            // A lone '.' between quoted identifiers ("db"."rp"."m").
            '.' if b.get(i + 1).is_none_or(|n| !n.is_ascii_digit()) => {
                toks.push(Tok::Op(".".into()));
                i += 1;
            }
            '0'..='9' | '.' => {
                let start = i;
                let mut j = i;
                let mut is_float = false;
                while j < b.len() && (b[j].is_ascii_digit() || b[j] as char == '.') {
                    if b[j] as char == '.' {
                        is_float = true;
                    }
                    j += 1;
                }
                // exponent
                if j < b.len() && (b[j] as char == 'e' || b[j] as char == 'E') {
                    is_float = true;
                    j += 1;
                    if j < b.len() && (b[j] as char == '+' || b[j] as char == '-') {
                        j += 1;
                    }
                    while j < b.len() && b[j].is_ascii_digit() {
                        j += 1;
                    }
                }
                let num = &q[start..j];
                // optional duration suffix: ns, u/µs, ms, s, m, h, d, w
                let sufstart = j;
                while j < b.len() && (b[j].is_ascii_alphabetic() || b[j] as char == 'µ') {
                    j += 1;
                }
                let suffix = &q[sufstart..j];
                if !suffix.is_empty() && !is_float {
                    toks.push(Tok::Dur {
                        raw: num.parse::<i64>().map_err(|_| QlError(format!("bad number '{num}'")))?,
                        unit: Some(suffix.to_owned()),
                    });
                } else if suffix.is_empty() && !is_float {
                    toks.push(Tok::Dur {
                        raw: num.parse::<i64>().map_err(|_| QlError(format!("bad number '{num}'")))?,
                        unit: None,
                    });
                } else if suffix.is_empty() {
                    toks.push(Tok::Num(
                        num.parse::<f64>().map_err(|_| QlError(format!("bad number '{num}'")))?,
                    ));
                } else {
                    return err(format!("bad numeric literal '{num}{suffix}'"));
                }
                i = j;
            }
            c if c.is_ascii_alphabetic() || c == '_' || c == '$' => {
                let start = i;
                let mut j = i;
                while j < b.len()
                    && ((b[j] as char).is_ascii_alphanumeric()
                        || b[j] as char == '_'
                        || b[j] as char == '.'
                        || b[j] as char == '$')
                {
                    j += 1;
                }
                toks.push(Tok::Ident(q[start..j].to_owned()));
                i = j;
            }
            _ => return err(format!("unexpected character '{c}'")),
        }
    }
    Ok(toks)
}

fn duration_us(raw: i64, unit: &str) -> Result<i64, QlError> {
    let mul: i64 = match unit {
        "ns" => return Ok(raw / 1000),
        "u" | "us" | "µ" | "µs" => 1,
        "ms" => 1_000,
        "s" => 1_000_000,
        "m" => 60 * 1_000_000,
        "h" => 3_600 * 1_000_000,
        "d" => 86_400 * 1_000_000,
        "w" => 7 * 86_400 * 1_000_000,
        _ => return err(format!("bad duration unit '{unit}'")),
    };
    Ok(raw.saturating_mul(mul))
}

// --- parser --------------------------------------------------------------

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
    now_us: i64,
}

/// Split a raw `q=` payload into statements on unquoted semicolons.
pub fn split_statements(q: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth_quote: Option<char> = None;
    let mut cur = String::new();
    for c in q.chars() {
        match depth_quote {
            Some(qc) => {
                cur.push(c);
                if c == qc {
                    depth_quote = None;
                }
            }
            None => match c {
                '\'' | '"' => {
                    depth_quote = Some(c);
                    cur.push(c);
                }
                ';' => {
                    if !cur.trim().is_empty() {
                        out.push(std::mem::take(&mut cur));
                    }
                }
                _ => cur.push(c),
            },
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

pub fn parse(q: &str, now_us: i64) -> Result<Statement, QlError> {
    let toks = tokenize(q)?;
    let mut p = Parser { toks, pos: 0, now_us };
    let stmt = p.statement()?;
    if p.pos < p.toks.len() {
        return err(format!(
            "unsupported trailing syntax after statement (from token {:?})",
            p.toks[p.pos]
        ));
    }
    Ok(stmt)
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if let Some(Tok::Ident(w)) = self.peek()
            && w.eq_ignore_ascii_case(kw)
        {
            self.pos += 1;
            return true;
        }
        false
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), QlError> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            err(format!("expected '{kw}' (at {:?})", self.peek()))
        }
    }

    fn ident(&mut self, what: &str) -> Result<String, QlError> {
        match self.next() {
            Some(Tok::Ident(s)) => Ok(s),
            other => err(format!("expected {what}, got {other:?}")),
        }
    }

    /// Measurement possibly written as db.rp.name (bare or quoted
    /// segments) — keep the last segment.
    fn measurement(&mut self) -> Result<String, QlError> {
        let raw = self.ident("measurement")?;
        let mut last = raw.rsplit('.').next().unwrap_or(&raw).to_owned();
        while let Some(Tok::Op(op)) = self.peek()
            && op == "."
        {
            self.pos += 1;
            let seg = self.ident("measurement segment")?;
            last = seg.rsplit('.').next().unwrap_or(&seg).to_owned();
        }
        Ok(last)
    }

    fn statement(&mut self) -> Result<Statement, QlError> {
        if self.eat_kw("SHOW") {
            return self.show();
        }
        if self.eat_kw("SELECT") {
            return self.select().map(Statement::Select);
        }
        err(
            "only SELECT and SHOW (MEASUREMENTS / TAG KEYS / FIELD KEYS / \
             TAG VALUES / DATABASES / RETENTION POLICIES) are supported",
        )
    }

    fn show(&mut self) -> Result<Statement, QlError> {
        if self.eat_kw("DATABASES") {
            return Ok(Statement::ShowDatabases);
        }
        if self.eat_kw("RETENTION") {
            self.expect_kw("POLICIES")?;
            // optional: ON "db"
            if self.eat_kw("ON") {
                let _ = self.next();
            }
            return Ok(Statement::ShowRetentionPolicies);
        }
        if self.eat_kw("MEASUREMENTS") {
            let mut limit = None;
            if self.eat_kw("LIMIT")
                && let Some(Tok::Dur { raw, unit: None }) = self.next()
            {
                limit = Some(raw as u64);
            }
            // tolerate WHERE/regex filters by ignoring the rest
            self.pos = self.toks.len();
            return Ok(Statement::ShowMeasurements { limit });
        }
        if self.eat_kw("TAG") {
            if self.eat_kw("KEYS") {
                let from = self.opt_from()?;
                self.pos = self.toks.len(); // ignore WHERE/LIMIT tail
                return Ok(Statement::ShowTagKeys { from });
            }
            self.expect_kw("VALUES")?;
            let from = self.opt_from()?;
            self.expect_kw("WITH")?;
            self.expect_kw("KEY")?;
            match self.next() {
                Some(Tok::Op(op)) if op == "=" => {}
                other => return err(format!("expected '=' after WITH KEY, got {other:?}")),
            }
            let key = self.ident("tag key")?;
            self.pos = self.toks.len(); // ignore WHERE time filter tail
            return Ok(Statement::ShowTagValues { from, key });
        }
        if self.eat_kw("FIELD") {
            self.expect_kw("KEYS")?;
            let from = self.opt_from()?;
            self.pos = self.toks.len();
            return Ok(Statement::ShowFieldKeys { from });
        }
        err("unsupported SHOW — try MEASUREMENTS / TAG KEYS / FIELD KEYS / TAG VALUES")
    }

    fn opt_from(&mut self) -> Result<Option<String>, QlError> {
        if self.eat_kw("FROM") {
            Ok(Some(self.measurement()?))
        } else {
            Ok(None)
        }
    }

    fn select(&mut self) -> Result<Select, QlError> {
        let mut fields = Vec::new();
        loop {
            fields.push(self.field()?);
            if let Some(Tok::Comma) = self.peek() {
                self.pos += 1;
                continue;
            }
            break;
        }
        self.expect_kw("FROM")?;
        let measurement = self.measurement()?;

        let mut sel = Select {
            fields,
            measurement,
            conds: Vec::new(),
            time_lo: None,
            time_hi: None,
            group_time: None,
            group_tags: Vec::new(),
            fill: Fill::Null,
            order_desc: false,
            limit: None,
        };

        if self.eat_kw("WHERE") {
            self.where_clause(&mut sel)?;
        }
        if self.eat_kw("GROUP") {
            self.expect_kw("BY")?;
            self.group_by(&mut sel)?;
        }
        // fill(...) may trail GROUP BY in influx syntax
        if let Some(Tok::Ident(w)) = self.peek()
            && w.eq_ignore_ascii_case("fill")
        {
            self.pos += 1;
            self.fill(&mut sel)?;
        }
        if self.eat_kw("ORDER") {
            self.expect_kw("BY")?;
            let col = self.ident("time")?;
            if !col.eq_ignore_ascii_case("time") {
                return err("only ORDER BY time [DESC] is supported");
            }
            if self.eat_kw("DESC") {
                sel.order_desc = true;
            } else {
                let _ = self.eat_kw("ASC");
            }
        }
        if self.eat_kw("LIMIT") {
            match self.next() {
                Some(Tok::Dur { raw, unit: None }) if raw >= 0 => sel.limit = Some(raw as u64),
                other => return err(format!("bad LIMIT {other:?}")),
            }
        }
        // tolerate SLIMIT/SOFFSET/TZ by ignoring? No — error clearly.
        Ok(sel)
    }

    fn field(&mut self) -> Result<Field, QlError> {
        let mut f = match self.next() {
            Some(Tok::Star) => Field {
                agg: Agg::None,
                column: "*".into(),
                alias: None,
            },
            Some(Tok::Ident(name)) => {
                if let Some(Tok::LParen) = self.peek() {
                    let agg = match name.to_ascii_lowercase().as_str() {
                        "mean" => Agg::Mean,
                        "sum" => Agg::Sum,
                        "min" => Agg::Min,
                        "max" => Agg::Max,
                        "count" => Agg::Count,
                        "last" => Agg::Last,
                        "first" => Agg::First,
                        "spread" => Agg::Spread,
                        other => {
                            return err(format!(
                                "aggregate '{other}' isn't supported (mean, sum, min, max, \
                                 count, last, first, spread are)"
                            ))
                        }
                    };
                    self.pos += 1; // (
                    let column = match self.next() {
                        Some(Tok::Ident(c)) => c,
                        Some(Tok::Star) => "*".into(),
                        other => return err(format!("expected column in {name}(), got {other:?}")),
                    };
                    match self.next() {
                        Some(Tok::RParen) => {}
                        other => return err(format!("expected ')' , got {other:?}")),
                    }
                    Field {
                        agg,
                        column,
                        alias: None,
                    }
                } else {
                    Field {
                        agg: Agg::None,
                        column: name,
                        alias: None,
                    }
                }
            }
            other => return err(format!("bad select field {other:?}")),
        };
        if self.eat_kw("AS") {
            f.alias = Some(self.ident("alias")?);
        }
        Ok(f)
    }

    /// WHERE: ANDed conditions, optional parens around each; time bounds
    /// fold into the range, everything else becomes a Cond.
    fn where_clause(&mut self, sel: &mut Select) -> Result<(), QlError> {
        loop {
            let mut parens = 0usize;
            while let Some(Tok::LParen) = self.peek() {
                self.pos += 1;
                parens += 1;
            }
            self.condition(sel)?;
            // conditions inside one paren group may be ANDed/ORed —
            // OR only supported between conditions on the same tag via
            // regex; plain OR is out of subset.
            loop {
                if self.eat_kw("AND") {
                    self.condition(sel)?;
                } else if let Some(Tok::RParen) = self.peek() {
                    if parens == 0 {
                        break;
                    }
                    self.pos += 1;
                    parens -= 1;
                } else {
                    break;
                }
            }
            if parens > 0 {
                return err("unbalanced parentheses in WHERE");
            }
            if self.eat_kw("AND") {
                continue;
            }
            if self.eat_kw("OR") {
                return err(
                    "OR isn't supported — Grafana multi-value variables use \
                     =~ /^(a|b)$/, which is",
                );
            }
            break;
        }
        Ok(())
    }

    fn condition(&mut self, sel: &mut Select) -> Result<(), QlError> {
        let col = match self.next() {
            Some(Tok::Ident(c)) => c,
            other => return err(format!("expected column in WHERE, got {other:?}")),
        };
        let op = match self.next() {
            Some(Tok::Op(o)) => o,
            other => return err(format!("expected operator after '{col}', got {other:?}")),
        };
        if col.eq_ignore_ascii_case("time") {
            let t = self.time_value()?;
            match op.as_str() {
                ">" | ">=" => sel.time_lo = Some(t),
                "<" | "<=" => sel.time_hi = Some(t),
                "=" => {
                    sel.time_lo = Some(t);
                    sel.time_hi = Some(t);
                }
                _ => return err(format!("unsupported time operator '{op}'")),
            }
            return Ok(());
        }
        let cond = match op.as_str() {
            "=" => match self.next() {
                Some(Tok::Str(s)) => Cond::Eq(col, ScalarValue::Text(s)),
                Some(Tok::Num(n)) => Cond::Eq(col, ScalarValue::Num(n)),
                Some(Tok::Dur { raw, unit: None }) => Cond::Eq(col, ScalarValue::Num(raw as f64)),
                Some(Tok::Ident(b)) if b.eq_ignore_ascii_case("true") => {
                    Cond::Eq(col, ScalarValue::Num(1.0))
                }
                Some(Tok::Ident(b)) if b.eq_ignore_ascii_case("false") => {
                    Cond::Eq(col, ScalarValue::Num(0.0))
                }
                other => return err(format!("bad value for '{col}': {other:?}")),
            },
            "!=" | "<>" => match self.next() {
                Some(Tok::Str(s)) => Cond::Ne(col, ScalarValue::Text(s)),
                Some(Tok::Num(n)) => Cond::Ne(col, ScalarValue::Num(n)),
                Some(Tok::Dur { raw, unit: None }) => Cond::Ne(col, ScalarValue::Num(raw as f64)),
                other => return err(format!("bad value for '{col}': {other:?}")),
            },
            ">" | ">=" | "<" | "<=" => {
                let n = match self.next() {
                    Some(Tok::Num(n)) => n,
                    Some(Tok::Dur { raw, unit: None }) => raw as f64,
                    other => return err(format!("bad numeric value for '{col}': {other:?}")),
                };
                match op.as_str() {
                    ">" => Cond::Gt(col, n),
                    ">=" => Cond::Ge(col, n),
                    "<" => Cond::Lt(col, n),
                    _ => Cond::Le(col, n),
                }
            }
            "=~" | "!~" => {
                let re = match self.next() {
                    Some(Tok::Regex(r)) => r,
                    other => return err(format!("expected /regex/ after {op}, got {other:?}")),
                };
                let vals = regex_to_values(&re);
                match (vals, op.as_str()) {
                    (Some(vs), "=~") if vs.is_empty() => Cond::True,
                    (Some(vs), "=~") => Cond::In(col, vs),
                    (Some(vs), _) if vs.is_empty() => Cond::True,
                    (Some(vs), _) => Cond::NotIn(col, vs),
                    (None, _) => {
                        return err(format!(
                            "regex /{re}/ is too general — only Grafana's \
                             ^(a|b)$ multi-value form and match-alls are supported"
                        ))
                    }
                }
            }
            _ => return err(format!("unsupported operator '{op}'")),
        };
        sel.conds.push(cond);
        Ok(())
    }

    /// A time expression: `now()`, `now() - 6h`, `1720000000000000000`
    /// (ns), or `1720000000000ms`.
    fn time_value(&mut self) -> Result<i64, QlError> {
        match self.next() {
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("now") => {
                match (self.next(), self.next()) {
                    (Some(Tok::LParen), Some(Tok::RParen)) => {}
                    _ => return err("expected now()"),
                }
                let mut t = self.now_us;
                while let Some(Tok::Op(op)) = self.peek().cloned() {
                    if op != "+" && op != "-" {
                        break;
                    }
                    self.pos += 1;
                    let d = match self.next() {
                        Some(Tok::Dur { raw, unit: Some(u) }) => duration_us(raw, &u)?,
                        other => return err(format!("expected duration after now(){op}, got {other:?}")),
                    };
                    t = if op == "-" { t - d } else { t + d };
                }
                Ok(t)
            }
            Some(Tok::Dur { raw, unit }) => match unit.as_deref() {
                // bare integer time literal = nanoseconds (influx wire rule)
                None => Ok(raw / 1000),
                Some(u) => duration_us(raw, u),
            },
            Some(Tok::Str(s)) => err(format!(
                "string time literal '{s}' isn't supported — use epoch \
                 nanoseconds or now() arithmetic (what Grafana sends)"
            )),
            other => err(format!("bad time value {other:?}")),
        }
    }

    fn group_by(&mut self, sel: &mut Select) -> Result<(), QlError> {
        loop {
            match self.next() {
                Some(Tok::Star) => { /* GROUP BY * — all tags; resolved later */
                    sel.group_tags.push("*".into());
                }
                Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("time") => {
                    match self.next() {
                        Some(Tok::LParen) => {}
                        other => return err(format!("expected time(interval), got {other:?}")),
                    }
                    let iv = match self.next() {
                        Some(Tok::Dur { raw, unit: Some(u) }) => duration_us(raw, &u)?,
                        other => return err(format!("bad time() interval {other:?}")),
                    };
                    if iv <= 0 {
                        return err("time() interval must be positive");
                    }
                    // optional offset arg — ignored (rare, epoch-aligned)
                    if let Some(Tok::Comma) = self.peek() {
                        self.pos += 1;
                        let _ = self.next();
                    }
                    match self.next() {
                        Some(Tok::RParen) => {}
                        other => return err(format!("expected ')', got {other:?}")),
                    }
                    sel.group_time = Some(iv);
                }
                Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("fill") => {
                    self.fill(sel)?;
                }
                Some(Tok::Ident(tag)) => sel.group_tags.push(tag),
                other => return err(format!("bad GROUP BY element {other:?}")),
            }
            if let Some(Tok::Comma) = self.peek() {
                self.pos += 1;
                continue;
            }
            break;
        }
        Ok(())
    }

    fn fill(&mut self, sel: &mut Select) -> Result<(), QlError> {
        match self.next() {
            Some(Tok::LParen) => {}
            other => return err(format!("expected fill(...), got {other:?}")),
        }
        sel.fill = match self.next() {
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("null") => Fill::Null,
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("none") => Fill::None,
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("previous") => Fill::None,
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("linear") => Fill::None,
            Some(Tok::Dur { raw: 0, unit: None }) => Fill::Zero,
            Some(Tok::Num(0.0)) => Fill::Zero,
            other => return err(format!("unsupported fill({other:?})")),
        };
        match self.next() {
            Some(Tok::RParen) => Ok(()),
            other => err(format!("expected ')', got {other:?}")),
        }
    }
}

/// Grafana's template-variable regexes, and nothing more general:
/// `^(a|b|c)$` / `^a$` → value list; `.*`, `^$`, `` → match-all (empty
/// list). Anything else → None (unsupported).
fn regex_to_values(re: &str) -> Option<Vec<String>> {
    let trimmed = re.trim();
    if trimmed.is_empty() || trimmed == ".*" || trimmed == "^$" || trimmed == "^.*$" {
        return Some(Vec::new());
    }
    let inner = trimmed.strip_prefix('^').unwrap_or(trimmed);
    let inner = inner.strip_suffix('$').unwrap_or(inner);
    let inner = inner
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(inner);
    // Reject anything still containing regex metacharacters (escaped
    // alternatives from Grafana come through as literal-safe values).
    if inner
        .chars()
        .any(|c| "[](){}.*+?^$".contains(c))
    {
        return None;
    }
    Some(
        inner
            .split('|')
            .map(|s| s.replace('\\', ""))
            .filter(|s| !s.is_empty())
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_752_500_000_000_000;

    #[test]
    fn grafana_panel_query_parses() {
        let q = r#"SELECT mean("temp") FROM "weather" WHERE ("city" = 'SF') AND time >= now() - 6h and time <= now() GROUP BY time(30s), "city" fill(null) ORDER BY time DESC LIMIT 100"#;
        let Statement::Select(s) = parse(q, NOW).unwrap() else {
            panic!()
        };
        assert_eq!(s.measurement, "weather");
        assert_eq!(s.fields, vec![Field { agg: Agg::Mean, column: "temp".into(), alias: None }]);
        assert_eq!(s.conds, vec![Cond::Eq("city".into(), ScalarValue::Text("SF".into()))]);
        assert_eq!(s.time_lo, Some(NOW - 6 * 3_600 * 1_000_000));
        assert_eq!(s.time_hi, Some(NOW));
        assert_eq!(s.group_time, Some(30_000_000));
        assert_eq!(s.group_tags, vec!["city".to_string()]);
        assert_eq!(s.fill, Fill::Null);
        assert!(s.order_desc);
        assert_eq!(s.limit, Some(100));
    }

    #[test]
    fn time_literal_forms() {
        // ns bare literal (influx wire), ms suffix (Grafana $timeFilter)
        let q = "SELECT last(v) FROM m WHERE time >= 1700000000000000000 AND time <= 1700000001000ms";
        let Statement::Select(s) = parse(q, NOW).unwrap() else {
            panic!()
        };
        assert_eq!(s.time_lo, Some(1_700_000_000_000_000));
        assert_eq!(s.time_hi, Some(1_700_000_001_000_000));
    }

    #[test]
    fn multivalue_regex_becomes_in() {
        let q = r#"SELECT sum("v") FROM m WHERE "host" =~ /^(a|b)$/ AND "dc" !~ /^x$/ GROUP BY time(1m)"#;
        let Statement::Select(s) = parse(q, NOW).unwrap() else {
            panic!()
        };
        assert_eq!(s.conds[0], Cond::In("host".into(), vec!["a".into(), "b".into()]));
        assert_eq!(s.conds[1], Cond::NotIn("dc".into(), vec!["x".into()]));
        // match-all collapses to no-op
        let q = r#"SELECT sum("v") FROM m WHERE "host" =~ /.*/"#;
        let Statement::Select(s) = parse(q, NOW).unwrap() else {
            panic!()
        };
        assert_eq!(s.conds[0], Cond::True);
        // general regex is a clear error, not silently wrong
        assert!(parse(r#"SELECT sum(v) FROM m WHERE h =~ /a.+b/"#, NOW).is_err());
    }

    #[test]
    fn show_statements() {
        assert_eq!(parse("SHOW DATABASES", NOW).unwrap(), Statement::ShowDatabases);
        assert_eq!(
            parse("SHOW MEASUREMENTS LIMIT 100", NOW).unwrap(),
            Statement::ShowMeasurements { limit: Some(100) }
        );
        assert_eq!(
            parse(r#"SHOW TAG KEYS FROM "weather""#, NOW).unwrap(),
            Statement::ShowTagKeys { from: Some("weather".into()) }
        );
        assert_eq!(
            parse(r#"SHOW FIELD KEYS FROM "telegraf"."autogen"."weather""#, NOW).unwrap(),
            Statement::ShowFieldKeys { from: Some("weather".into()) }
        );
        assert_eq!(
            parse(r#"SHOW TAG VALUES FROM "weather" WITH KEY = "city" WHERE time > now() - 1h"#, NOW)
                .unwrap(),
            Statement::ShowTagValues { from: Some("weather".into()), key: "city".into() }
        );
        assert_eq!(
            parse(r#"SHOW RETENTION POLICIES on "silodb""#, NOW).unwrap(),
            Statement::ShowRetentionPolicies
        );
    }

    #[test]
    fn statement_splitting() {
        let parts = split_statements("SELECT 1; SHOW MEASUREMENTS;; SELECT 'a;b' FROM m");
        assert_eq!(parts.len(), 3);
        assert!(parts[2].contains("a;b"));
    }

    #[test]
    fn unsupported_is_loud() {
        assert!(parse("DROP MEASUREMENT m", NOW).is_err());
        assert!(parse("SELECT derivative(v) FROM m", NOW).is_err());
        assert!(parse("SELECT v FROM m WHERE a = 1 OR b = 2", NOW).is_err());
    }
}
