//! Facade crate: the only crate the supervisory binary should depend on.
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let conn = rusqlite::Connection::open("hot.db")?;
//! silodb::load_module(&conn)?;
//!
//! // One base directory serves every cold table; the vtab's own name picks
//! // the logical table. Works on day zero, before anything was compacted.
//! conn.execute_batch(
//!     "CREATE VIRTUAL TABLE readings_cold USING silodb('buckets/', table=readings);
//!      CREATE VIEW all_readings AS
//!        SELECT * FROM readings UNION ALL SELECT * FROM readings_cold;",
//! )?;
//!
//! // Age a closed bucket out of the hot table. The file is named and
//! // cataloged internally; re-runs, crash recovery, and late rows are all
//! // handled — every call pattern is idempotent.
//! silodb::compact_bucket(
//!     &conn,
//!     &silodb::BucketSpec {
//!         hot_table: "readings",
//!         logical_table: "readings",
//!         ts_column: "ts",
//!         bucket_start: 0,
//!         bucket_end: 3_600_000_000,
//!     },
//!     std::path::Path::new("buckets/readings/"),
//! )?;
//! # Ok(()) }
//! ```

pub use silodb_compact::{compact_bucket, BucketSpec, CompactError, CompactOutcome};
pub use silodb_vtab::{last_scan_stats, load_module, ScanStats};

/// Catalog schema and operations (`_silodb_catalog` in the hot database).
pub use silodb_catalog as catalog;
