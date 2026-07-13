//! Facade crate: the only crate the supervisory binary should depend on.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let conn = rusqlite::Connection::open("hot.db")?;
//! silodb::catalog::ensure_catalog(&conn)?;
//! silodb::load_module(&conn)?;
//!
//! // Age a closed bucket out of the hot table...
//! silodb::compact_bucket(
//!     &conn,
//!     &silodb::BucketSpec {
//!         hot_table: "readings",
//!         logical_table: "readings",
//!         ts_column: "ts",
//!         bucket_start: 0,
//!         bucket_end: 3_600_000_000,
//!     },
//!     std::path::Path::new("buckets/readings/bucket-0.parquet"),
//! )?;
//!
//! // ...and query hot + cold through one connection.
//! conn.execute_batch(
//!     "CREATE VIRTUAL TABLE cold USING silodb('buckets/readings/');
//!      CREATE VIEW all_readings AS
//!        SELECT * FROM readings UNION ALL SELECT * FROM cold;",
//! )?;
//! # Ok(()) }
//! ```

pub use silodb_compact::{compact_bucket, BucketSpec, CompactError, CompactOutcome};
pub use silodb_vtab::{last_scan_stats, load_module, ScanStats};

/// Catalog schema and operations (`_silodb_catalog` in the hot database).
pub use silodb_catalog as catalog;
