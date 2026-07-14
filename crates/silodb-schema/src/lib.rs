//! Parquet/Arrow ↔ SQLite type mapping.
//!
//! Single source of truth for how a Parquet (Arrow) schema surfaces as a
//! SQLite table (read path, `silodb-vtab`) and how SQLite storage classes
//! become Arrow types when compacting hot rows into Parquet (write path,
//! `silodb-compact`). Must never depend on `rusqlite`.

use arrow_schema::{DataType, Field, Schema, TimeUnit};

/// SQLite storage classes silodb maps onto. NULL is a value, not a column
/// type, so it isn't listed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteType {
    Integer,
    Real,
    Text,
    Blob,
}

impl SqliteType {
    /// The declared type name to use in `CREATE TABLE` column definitions.
    pub fn decl(self) -> &'static str {
        match self {
            SqliteType::Integer => "INTEGER",
            SqliteType::Real => "REAL",
            SqliteType::Text => "TEXT",
            SqliteType::Blob => "BLOB",
        }
    }

    /// Storage class for a column's declared type, per SQLite's affinity
    /// rules (https://sqlite.org/datatype3.html#determination_of_column_affinity),
    /// minus NUMERIC: a declared type we'd have to guess a storage class for
    /// returns `None` and the caller should refuse, not guess.
    ///
    /// One narrow, named exception to the NUMERIC refusal: declared types
    /// containing `TIMESTAMP` or `DATETIME` (which SQLite's own algorithm
    /// files under NUMERIC) map to INTEGER. Nothing is guessed — those two
    /// substrings get a deliberate rule (epoch-microseconds columns, see
    /// [`is_timestamp_decl`]); every other NUMERIC-affinity decl stays
    /// refused exactly as before.
    pub fn from_decl(decl: &str) -> Option<Self> {
        let d = decl.to_ascii_uppercase();
        // is_timestamp_decl first: it's the NUMERIC carve-out and must not
        // be shadowed by any later bucket.
        if is_timestamp_decl(decl) || d.contains("INT") {
            Some(SqliteType::Integer)
        } else if d.contains("CHAR") || d.contains("CLOB") || d.contains("TEXT") {
            Some(SqliteType::Text)
        } else if d.contains("BLOB") || d.is_empty() {
            Some(SqliteType::Blob)
        } else if d.contains("REAL") || d.contains("FLOA") || d.contains("DOUB") {
            Some(SqliteType::Real)
        } else {
            None
        }
    }
}

/// True if a declared column type marks a silodb timestamp (epoch
/// microseconds stored as INTEGER, surfaced as a real Parquet
/// TIMESTAMP(µs, UTC)). Case-insensitive substring match, the same
/// convention SQLite's affinity scanner uses for INT/CHAR/REAL.
pub fn is_timestamp_decl(decl: &str) -> bool {
    let d = decl.to_ascii_uppercase();
    d.contains("TIMESTAMP") || d.contains("DATETIME")
}

/// One parsed hot-table / schema-argument column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDecl {
    pub name: String,
    pub ty: SqliteType,
    /// Declared as TIMESTAMP/DATETIME — written to Parquet as a real
    /// timestamp even when it isn't the bucket axis.
    pub declared_timestamp: bool,
}

impl ColumnDecl {
    /// Parse one `(name, declared type)` pair; `None` = unsupported decl.
    pub fn parse(name: &str, decl: &str) -> Option<Self> {
        Some(ColumnDecl {
            name: name.to_owned(),
            ty: SqliteType::from_decl(decl)?,
            declared_timestamp: is_timestamp_decl(decl),
        })
    }
}

/// Why [`resolve_ts_index`] couldn't pick a timestamp column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TsResolveError {
    /// The explicit `ts_column=` name is missing or not INTEGER-class.
    ExplicitNotUsable(String),
    /// More than one TIMESTAMP/DATETIME column and no explicit choice —
    /// refusing to guess which is the bucket axis.
    MultipleTimestamps(Vec<String>),
    /// No TIMESTAMP/DATETIME column and no INTEGER column named `ts`.
    NoneFound,
}

impl std::fmt::Display for TsResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExplicitNotUsable(name) => {
                write!(f, "ts column '{name}' is missing or not INTEGER-class")
            }
            Self::MultipleTimestamps(names) => write!(
                f,
                "multiple TIMESTAMP/DATETIME columns ({}); pass ts_column= to pick the bucket axis",
                names.join(", ")
            ),
            Self::NoneFound => write!(
                f,
                "no TIMESTAMP/DATETIME column and no INTEGER column named 'ts'"
            ),
        }
    }
}

impl std::error::Error for TsResolveError {}

/// Which column is the bucket/timestamp axis. Total precedence order:
///
/// 1. An explicit name (`ts_column=`) always wins — type-driven discovery
///    never runs when it's given.
/// 2. Otherwise exactly one TIMESTAMP/DATETIME-declared column; zero or
///    several is not guessed at.
/// 3. Otherwise the legacy name convention: an INTEGER column named `ts`.
pub fn resolve_ts_index(
    cols: &[ColumnDecl],
    explicit: Option<&str>,
) -> Result<usize, TsResolveError> {
    if let Some(name) = explicit {
        return cols
            .iter()
            .position(|c| c.name == name && c.ty == SqliteType::Integer)
            .ok_or_else(|| TsResolveError::ExplicitNotUsable(name.to_owned()));
    }
    let stamped: Vec<usize> = (0..cols.len())
        .filter(|&i| cols[i].declared_timestamp)
        .collect();
    match stamped.as_slice() {
        [one] => Ok(*one),
        [] => cols
            .iter()
            .position(|c| c.name == "ts" && c.ty == SqliteType::Integer)
            .ok_or(TsResolveError::NoneFound),
        many => Err(TsResolveError::MultipleTimestamps(
            many.iter().map(|&i| cols[i].name.clone()).collect(),
        )),
    }
}

/// Error for Arrow types silodb deliberately does not support.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedType {
    pub column: String,
    pub data_type: String,
}

impl std::fmt::Display for UnsupportedType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unsupported Parquet/Arrow type {} on column '{}'",
            self.data_type, self.column
        )
    }
}

impl std::error::Error for UnsupportedType {}

/// Map an Arrow `DataType` (as read from a Parquet file) to the SQLite
/// storage class it surfaces as through the vtab.
///
/// Deliberately explicit and narrow: only the types this project needs.
/// Timestamps surface as INTEGER holding the raw value in the file's own
/// time unit; Booleans as INTEGER 0/1.
pub fn sqlite_type_for(dt: &DataType) -> Option<SqliteType> {
    match dt {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64 => Some(SqliteType::Integer),
        // UInt64 is excluded: it doesn't fit SQLite's signed 64-bit INTEGER.
        DataType::Float32 | DataType::Float64 => Some(SqliteType::Real),
        DataType::Utf8 | DataType::LargeUtf8 => Some(SqliteType::Text),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Some(SqliteType::Blob)
        }
        _ => None,
    }
}

/// Build the `CREATE TABLE` statement `xCreate`/`xConnect` must declare for
/// a Parquet file with the given Arrow schema. The table name is a dummy —
/// SQLite ignores it in `sqlite3_declare_vtab`.
pub fn create_table_sql(schema: &Schema) -> Result<String, UnsupportedType> {
    let mut sql = String::from("CREATE TABLE x(");
    for (i, field) in schema.fields().iter().enumerate() {
        let ty = sqlite_type_for(field.data_type()).ok_or_else(|| UnsupportedType {
            column: field.name().clone(),
            data_type: field.data_type().to_string(),
        })?;
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push('"');
        // Double-quote escaping for identifiers.
        sql.push_str(&field.name().replace('"', "\"\""));
        sql.push_str("\" ");
        sql.push_str(ty.decl());
    }
    sql.push(')');
    Ok(sql)
}

/// Map a SQLite storage class to the Arrow type `compact_bucket` writes it
/// as. The write path is narrower than the read path on purpose: the hot
/// table only ever hands us these four classes, except the designated
/// timestamp column, which callers write as `timestamp_arrow_type()` so the
/// read path (and row-group pruning) can treat it as a timestamp.
pub fn arrow_type_for(ty: SqliteType) -> DataType {
    match ty {
        SqliteType::Integer => DataType::Int64,
        SqliteType::Real => DataType::Float64,
        SqliteType::Text => DataType::Utf8,
        SqliteType::Blob => DataType::Binary,
    }
}

/// Arrow type used for the compaction bucket's timestamp column. The hot
/// table stores epoch **microseconds** as INTEGER; this is the one place
/// that convention is encoded.
///
/// UTC-tagged on purpose: parquet files then carry a real, unambiguous
/// TIMESTAMP logical type, so external tools (pandas, DuckDB, parquet
/// viewers) render actual datetimes instead of bare integers — the cold
/// files are directly exportable with no decoding step. Through the vtab
/// the value still surfaces as the raw INTEGER microseconds, matching the
/// hot table.
pub fn timestamp_arrow_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// Convenience: build an Arrow field, always nullable — SQLite columns are
/// nullable unless constrained, and the vtab surfaces NULLs as NULLs.
pub fn arrow_field(name: &str, dt: DataType) -> Field {
    Field::new(name, dt, true)
}

/// The Arrow schema a compacted bucket file has, derived from the hot
/// table's columns. This is the one definition both paths share:
/// `silodb-compact` writes files with it, and `silodb-vtab` uses it to
/// declare columns when no file exists yet (empty catalog) — so the two
/// can never drift.
///
/// The bucket axis (`ts_idx`) becomes a non-nullable
/// [`timestamp_arrow_type`] — bucket range predicates exclude NULLs.
/// Any *other* TIMESTAMP/DATETIME-declared column also becomes a real
/// Parquet timestamp (nullable), so secondary datetime columns export as
/// dates too. Everything else maps through [`arrow_type_for`], nullable.
pub fn bucket_arrow_schema(columns: &[ColumnDecl], ts_idx: usize) -> Schema {
    Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if i == ts_idx {
                    Field::new(&col.name, timestamp_arrow_type(), false)
                } else if col.declared_timestamp {
                    arrow_field(&col.name, timestamp_arrow_type())
                } else {
                    arrow_field(&col.name, arrow_type_for(col.ty))
                }
            })
            .collect::<Vec<_>>(),
    )
}

// --- durations & bucketing, pure logic (no deps) ------------------------

/// Parse a duration like `"1h"`, `"7d"`, `"2y"` into microseconds.
/// Units: s m h d w y (y = 365d). `None` on anything else — no guessing.
/// The one shared definition for policy strings, `silodb_bucket()`, and
/// rollup grains, so they can never disagree.
pub fn parse_duration_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    // Last *char*, not last byte — split_at on a byte index panics inside
    // multibyte characters (found by the never-panics proptest).
    let (last_idx, _) = s.char_indices().last()?;
    let (num, unit) = s.split_at(last_idx);
    let n: i64 = num.trim().parse().ok()?;
    let secs: i64 = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        "w" => 7 * 86_400,
        "y" => 365 * 86_400,
        _ => return None,
    };
    n.checked_mul(secs)
        .and_then(|x| x.checked_mul(1_000_000))
        .filter(|&us| us > 0)
}

/// Floor `ts` to the start of its `width`-sized window, in the grid
/// anchored at `origin` (0 = epoch). The single bucketing definition used
/// by the `silodb_bucket()` SQL function, tier/compaction windows, and
/// rollup grains — query-side and write-side bucketing cannot disagree.
///
/// Euclidean: correct for pre-origin timestamps too. `None` only on
/// arithmetic overflow at the i64 edges.
pub fn bucket_floor(width_us: i64, ts_us: i64, origin_us: i64) -> Option<i64> {
    if width_us <= 0 {
        return None;
    }
    let rel = ts_us.checked_sub(origin_us)?;
    rel.checked_sub(rel.rem_euclid(width_us))?
        .checked_add(origin_us)
}

// --- timestamp text ↔ epoch-microseconds, pure logic (no deps) ---------
//
// Backs the `silodb_ts()` / `silodb_datetime()` SQL helpers the facade
// registers. Civil-date math is Howard Hinnant's days_from_civil /
// civil_from_days; all times are UTC.

const MICROS_PER_SEC: i64 = 1_000_000;

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m as i64 + 9) % 12; // Mar=0..Feb=11
    let doy = (153 * mp + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { yoe + era * 400 + 1 } else { yoe + era * 400 }, m, d)
}

/// Format epoch microseconds as ISO 8601 UTC:
/// `YYYY-MM-DDTHH:MM:SSZ`, or `...SS.ffffffZ` when sub-second.
pub fn format_timestamp_micros(us: i64) -> String {
    let (secs, sub) = (us.div_euclid(MICROS_PER_SEC), us.rem_euclid(MICROS_PER_SEC));
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (y, mo, d) = civil_from_days(days);
    let (h, mi, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    if sub == 0 {
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
    } else {
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{sub:06}Z")
    }
}

/// Parse an ISO-8601-ish UTC datetime to epoch microseconds. Accepted:
/// `YYYY-MM-DD`, plus optional `[T or space]HH:MM[:SS[.fraction]]`, plus
/// optional trailing `Z`. Naive inputs are taken as UTC. Fractions beyond
/// microseconds truncate. Returns `None` on anything else — no guessing.
pub fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let s = s.trim().strip_suffix('Z').unwrap_or_else(|| s.trim());
    let (date, time) = match s.split_once(['T', ' ']) {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };

    // A leading '-' is a negative (BCE) year, not a field separator —
    // format_timestamp_micros emits them for very negative µs values.
    let (neg_year, date) = match date.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, date),
    };
    let mut dp = date.split('-');
    let y: i64 = dp.next()?.parse().ok()?;
    let y = if neg_year { -y } else { y };
    let mo: u32 = dp.next()?.parse().ok()?;
    let d: u32 = dp.next()?.parse().ok()?;
    // i64 microseconds spans roughly ±292,000 years around 1970; anything
    // beyond can't be represented (and would overflow the math below).
    if dp.next().is_some()
        || !(-300_000..=300_000).contains(&y)
        || !(1..=12).contains(&mo)
        || !(1..=31).contains(&d)
    {
        return None;
    }
    // Reject day numbers the month doesn't have (round-trip check).
    let days = days_from_civil(y, mo, d);
    if civil_from_days(days) != (y, mo, d) {
        return None;
    }

    let mut us: i64 = 0;
    if let Some(t) = time {
        let (hms, frac) = match t.split_once('.') {
            Some((hms, frac)) => (hms, Some(frac)),
            None => (t, None),
        };
        let mut tp = hms.split(':');
        let h: i64 = tp.next()?.parse().ok()?;
        let mi: i64 = tp.next()?.parse().ok()?;
        let sec: i64 = match tp.next() {
            Some(x) => x.parse().ok()?,
            None => 0,
        };
        if tp.next().is_some() || !(0..24).contains(&h) || !(0..60).contains(&mi) || !(0..60).contains(&sec)
        {
            return None;
        }
        us = (h * 3600 + mi * 60 + sec) * MICROS_PER_SEC;
        if let Some(frac) = frac {
            if frac.is_empty() || !frac.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let digits: String = frac.chars().take(6).collect();
            let mut sub: i64 = digits.parse().ok()?;
            sub *= 10_i64.pow(6 - digits.len() as u32);
            us += sub;
        }
    }
    // Checked: valid-looking dates near the ±292,000-year edges can still
    // land just outside i64 microseconds.
    days.checked_mul(86_400 * MICROS_PER_SEC)?.checked_add(us)
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 2048, ..ProptestConfig::default() })]

        /// Any i64 formats and parses back exactly.
        #[test]
        fn format_parse_round_trips_any_micros(us in any::<i64>()) {
            let text = format_timestamp_micros(us);
            prop_assert_eq!(parse_timestamp_micros(&text), Some(us), "{}", text);
        }

        /// Arbitrary strings never panic the parser (and Some(_) implies
        /// re-formatting also doesn't panic).
        #[test]
        fn parser_never_panics(s in "\\PC*") {
            if let Some(us) = parse_timestamp_micros(&s) {
                let _ = format_timestamp_micros(us);
            }
        }

        /// ASCII-ish date-shaped garbage specifically (denser than \\PC*).
        #[test]
        fn parser_never_panics_on_date_shaped_input(
            s in "[0-9TZ:. +-]{0,40}",
        ) {
            let _ = parse_timestamp_micros(&s);
        }

        /// from_decl / ColumnDecl::parse never panic on arbitrary decls.
        #[test]
        fn decl_parsing_never_panics(name in "\\PC{0,16}", decl in "\\PC{0,24}") {
            let _ = SqliteType::from_decl(&decl);
            let _ = ColumnDecl::parse(&name, &decl);
        }

        /// bucket_floor lands in [bucket, bucket + width) with the bucket on
        /// the origin grid, for any inputs that don't overflow.
        #[test]
        fn bucket_floor_is_a_floor(
            width in 1i64..10_000_000_000,
            ts in -2_000_000_000_000_000i64..2_000_000_000_000_000,
            origin in -1_000_000_000_000i64..1_000_000_000_000,
        ) {
            let b = bucket_floor(width, ts, origin).unwrap();
            prop_assert!(b <= ts && ts < b + width);
            prop_assert_eq!((b - origin).rem_euclid(width), 0);
        }

        /// Duration parsing never panics on arbitrary strings.
        #[test]
        fn duration_parsing_never_panics(s in "\\PC{0,12}") {
            let _ = parse_duration_micros(&s);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;

    #[test]
    fn integers_and_timestamps_map_to_integer() {
        for dt in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Date32,
            DataType::Date64,
        ] {
            assert_eq!(sqlite_type_for(&dt), Some(SqliteType::Integer), "{dt}");
        }
    }

    #[test]
    fn floats_map_to_real() {
        assert_eq!(sqlite_type_for(&DataType::Float32), Some(SqliteType::Real));
        assert_eq!(sqlite_type_for(&DataType::Float64), Some(SqliteType::Real));
    }

    #[test]
    fn strings_and_binary() {
        assert_eq!(sqlite_type_for(&DataType::Utf8), Some(SqliteType::Text));
        assert_eq!(
            sqlite_type_for(&DataType::LargeUtf8),
            Some(SqliteType::Text)
        );
        assert_eq!(sqlite_type_for(&DataType::Binary), Some(SqliteType::Blob));
        assert_eq!(
            sqlite_type_for(&DataType::FixedSizeBinary(16)),
            Some(SqliteType::Blob)
        );
    }

    #[test]
    fn uint64_and_nested_are_rejected() {
        assert_eq!(sqlite_type_for(&DataType::UInt64), None);
        assert_eq!(
            sqlite_type_for(&DataType::List(
                Field::new("item", DataType::Int64, true).into()
            )),
            None
        );
        assert_eq!(sqlite_type_for(&DataType::Struct(Default::default())), None);
    }

    #[test]
    fn create_table_sql_declares_all_columns() {
        let schema = Schema::new(vec![
            arrow_field("ts", DataType::Timestamp(TimeUnit::Microsecond, None)),
            arrow_field("value", DataType::Float64),
            arrow_field("name", DataType::Utf8),
            arrow_field("payload", DataType::Binary),
        ]);
        assert_eq!(
            create_table_sql(&schema).unwrap(),
            r#"CREATE TABLE x("ts" INTEGER, "value" REAL, "name" TEXT, "payload" BLOB)"#
        );
    }

    #[test]
    fn create_table_sql_escapes_quotes_in_names() {
        let schema = Schema::new(vec![arrow_field("we\"ird", DataType::Int64)]);
        assert_eq!(
            create_table_sql(&schema).unwrap(),
            r#"CREATE TABLE x("we""ird" INTEGER)"#
        );
    }

    #[test]
    fn create_table_sql_rejects_unsupported() {
        let schema = Schema::new(vec![arrow_field("u", DataType::UInt64)]);
        let err = create_table_sql(&schema).unwrap_err();
        assert_eq!(err.column, "u");
    }

    #[test]
    fn timestamp_decls_are_a_narrow_numeric_exception() {
        for decl in ["TIMESTAMP", "timestamp", "DATETIME", "SMALLDATETIME", "TIMESTAMPTZ"] {
            assert!(is_timestamp_decl(decl), "{decl}");
            assert_eq!(SqliteType::from_decl(decl), Some(SqliteType::Integer), "{decl}");
        }
        // Everything else in NUMERIC affinity stays refused.
        for decl in ["NUMERIC", "DECIMAL(10,5)", "DATE", "BOOLEAN"] {
            assert!(!is_timestamp_decl(decl), "{decl}");
            assert_eq!(SqliteType::from_decl(decl), None, "{decl}");
        }
        assert!(!is_timestamp_decl("INTEGER"));
    }

    fn col(name: &str, decl: &str) -> ColumnDecl {
        ColumnDecl::parse(name, decl).unwrap()
    }

    #[test]
    fn ts_resolution_precedence_is_total() {
        let two_stamps = [col("a", "TIMESTAMP"), col("b", "DATETIME"), col("ts", "INTEGER")];
        // 1. explicit always wins, even over TIMESTAMP-typed columns.
        assert_eq!(resolve_ts_index(&two_stamps, Some("ts")), Ok(2));
        assert_eq!(resolve_ts_index(&two_stamps, Some("a")), Ok(0));
        assert!(matches!(
            resolve_ts_index(&two_stamps, Some("nope")),
            Err(TsResolveError::ExplicitNotUsable(_))
        ));
        // 2. exactly one TIMESTAMP column → discovered by type, any name.
        let one = [col("stamped_at", "TIMESTAMP"), col("value", "REAL")];
        assert_eq!(resolve_ts_index(&one, None), Ok(0));
        // ...but two without an explicit pick is refused, not guessed.
        assert!(matches!(
            resolve_ts_index(&two_stamps, None),
            Err(TsResolveError::MultipleTimestamps(_))
        ));
        // 3. zero TIMESTAMP columns → legacy 'ts INTEGER' name convention.
        let legacy = [col("ts", "INTEGER"), col("value", "REAL")];
        assert_eq!(resolve_ts_index(&legacy, None), Ok(0));
        assert!(matches!(
            resolve_ts_index(&[col("value", "REAL")], None),
            Err(TsResolveError::NoneFound)
        ));
    }

    #[test]
    fn secondary_timestamp_columns_export_as_parquet_timestamps() {
        let cols = [
            col("ts", "TIMESTAMP"),
            col("created_at", "DATETIME"),
            col("value", "REAL"),
        ];
        let schema = bucket_arrow_schema(&cols, 0);
        assert_eq!(*schema.field(0).data_type(), timestamp_arrow_type());
        assert!(!schema.field(0).is_nullable(), "bucket axis non-null");
        assert_eq!(*schema.field(1).data_type(), timestamp_arrow_type());
        assert!(schema.field(1).is_nullable(), "secondary stamp nullable");
        assert_eq!(*schema.field(2).data_type(), DataType::Float64);
    }

    #[test]
    fn timestamp_text_round_trips() {
        for (text, us) in [
            ("1970-01-01", 0i64),
            ("1970-01-01T00:00:00Z", 0),
            ("2026-07-13 10:42:00", 1_783_939_320_000_000),
            ("2026-07-13T10:42:00.5Z", 1_783_939_320_500_000),
            ("2026-07-13T10:42:00.123456789", 1_783_939_320_123_456), // truncates
            ("1969-12-31T23:59:59Z", -1_000_000),
            ("2000-02-29", 951_782_400_000_000), // leap day
        ] {
            assert_eq!(parse_timestamp_micros(text), Some(us), "{text}");
        }
        // format → parse is exact for any µs value.
        for us in [0i64, 1, -1, 1_783_939_320_500_000, -62_167_219_200_000_000] {
            let text = format_timestamp_micros(us);
            assert_eq!(parse_timestamp_micros(&text), Some(us), "{text}");
        }
        assert_eq!(
            format_timestamp_micros(1_783_939_320_000_000),
            "2026-07-13T10:42:00Z"
        );
        // Garbage and impossible dates are refused, not guessed.
        for bad in ["", "not a date", "2026-13-01", "2026-02-30", "2026-07-13T25:00:00", "2026-07-13T10:42:00.abc"] {
            assert_eq!(parse_timestamp_micros(bad), None, "{bad}");
        }
    }

    #[test]
    fn write_path_mapping_round_trips_through_read_path() {
        for ty in [
            SqliteType::Integer,
            SqliteType::Real,
            SqliteType::Text,
            SqliteType::Blob,
        ] {
            assert_eq!(sqlite_type_for(&arrow_type_for(ty)), Some(ty));
        }
        assert_eq!(
            sqlite_type_for(&timestamp_arrow_type()),
            Some(SqliteType::Integer)
        );
    }
}
