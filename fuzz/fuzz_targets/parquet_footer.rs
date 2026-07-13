//! Fuzz the parquet footer/metadata parsing path the vtab hits when a
//! catalog row points at a hostile file. Errors are fine; panics are not —
//! in production a panic here would unwind across the SQLite FFI boundary.
//!
//! Uses `bytes::Bytes` as the ChunkReader so each iteration is pure
//! in-memory, no disk I/O.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};

fuzz_target!(|data: &[u8]| {
    let bytes = Bytes::copy_from_slice(data);
    if let Ok(meta) = ArrowReaderMetadata::load(&bytes, Default::default()) {
        // Footer parsed: push on into reader construction and one batch,
        // like a real scan would.
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, meta);
        if let Ok(reader) = builder.build() {
            for batch in reader.take(4) {
                let _ = batch;
            }
        }
    }
});
