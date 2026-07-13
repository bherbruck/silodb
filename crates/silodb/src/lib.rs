//! Facade crate: the only crate the supervisory binary should depend on.
//!
//! Re-exports grow as phases land — vtab registration now, `compact_bucket`
//! in Phase 3.

pub use silodb_vtab::load_module;
