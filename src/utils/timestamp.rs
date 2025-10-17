//! Timestamp conversion utilities for PostgreSQL replication
//!
//! Provides functions for converting between different timestamp formats
//! used by PostgreSQL and standard Unix timestamps.

use chrono::DateTime;
use std::time::{SystemTime, UNIX_EPOCH};

// PostgreSQL epoch constants
const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800; // Seconds from Unix epoch (1970) to PostgreSQL epoch (2000)

/// Convert SystemTime to PostgreSQL timestamp format.
///
/// This function converts a standard Unix SystemTime to a PostgreSQL-compatible
/// timestamp by shifting the epoch from Unix (1970-01-01) to PostgreSQL (2000-01-01).
/// The result is in microseconds since the PostgreSQL epoch.
///
/// # Arguments
/// * `time` - The SystemTime to convert
///
/// # Returns
/// A TimestampTz value representing the time in PostgreSQL format
pub fn system_time_to_postgres_timestamp(time: SystemTime) -> crate::utils::binary::TimestampTz {
    let duration_since_unix = time
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime is before Unix epoch");

    let unix_secs = duration_since_unix.as_secs() as i64;
    let unix_micros = unix_secs * 1_000_000 + (duration_since_unix.subsec_micros() as i64);

    // Shift Unix epoch to PostgreSQL epoch
    unix_micros - PG_EPOCH_OFFSET_SECS * 1_000_000
}

/// Convert a microsecond or nanosecond timestamp to a formatted UTC date string.
///
/// This function converts a PostgreSQL timestamp (in microseconds since epoch)
/// into a human-readable string format.
///
/// # Arguments
/// * `ts` - The timestamp value in microseconds since the PostgreSQL epoch
///
/// # Returns
/// A `String` in "YYYY-MM-DD HH:MM:SS.sss UTC" format
pub fn format_timestamp_from_pg(ts: i64) -> String {
    let secs = ts / 1_000_000 + PG_EPOCH_OFFSET_SECS;
    let nsecs = (ts % 1_000_000) * 1_000;

    let datetime = DateTime::from_timestamp(secs, nsecs as u32).expect("Invalid timestamp");

    datetime.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string()
}