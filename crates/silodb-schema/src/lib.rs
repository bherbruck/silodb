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
    pub fn from_decl(decl: &str) -> Option<Self> {
        let d = decl.to_ascii_uppercase();
        if d.contains("INT") {
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
pub fn timestamp_arrow_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
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
/// The timestamp column (`ts_idx`) becomes a non-nullable
/// [`timestamp_arrow_type`] — bucket range predicates exclude NULLs — and
/// every other column maps through [`arrow_type_for`], nullable.
pub fn bucket_arrow_schema(columns: &[(String, SqliteType)], ts_idx: usize) -> Schema {
    Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| {
                if i == ts_idx {
                    Field::new(name, timestamp_arrow_type(), false)
                } else {
                    arrow_field(name, arrow_type_for(*ty))
                }
            })
            .collect::<Vec<_>>(),
    )
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
