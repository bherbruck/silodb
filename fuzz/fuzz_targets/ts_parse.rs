//! Fuzz the timestamp text parser: must never panic, and anything it
//! accepts must survive a format→parse round trip.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    if let Some(us) = silodb_schema::parse_timestamp_micros(s) {
        let text = silodb_schema::format_timestamp_micros(us);
        assert_eq!(
            silodb_schema::parse_timestamp_micros(&text),
            Some(us),
            "round trip broke for accepted input {s:?} -> {text:?}"
        );
    }
});
