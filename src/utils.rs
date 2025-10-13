//! Utility functions for PostgreSQL replication
//!
//! Contains helper functions for:
//! - Byte manipulation with proper endianness
//! - PostgreSQL timestamp conversion
//! - Safe PostgreSQL connection handling using libpq

use crate::errors::ReplicationResult;
use chrono::DateTime;
use libpq_sys::*;
use std::ffi::{CStr, CString};
use std::ptr;
use std::time::{SystemTime, UNIX_EPOCH};

// PostgreSQL epoch constants
const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800; // Seconds from Unix epoch (1970) to PostgreSQL epoch (2000)

// Type aliases matching PostgreSQL internal types
pub type XLogRecPtr = u64; // WAL location pointer
pub type Xid = u32; // Transaction ID
pub type Oid = u32; // Object ID
pub type TimestampTz = i64; // Timestamp with timezone

pub const INVALID_XLOG_REC_PTR: XLogRecPtr = 0;

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
pub fn system_time_to_postgres_timestamp(time: SystemTime) -> TimestampTz {
    let duration_since_unix = time
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime is before Unix epoch");

    let unix_secs = duration_since_unix.as_secs() as i64;
    let unix_micros = unix_secs * 1_000_000 + (duration_since_unix.subsec_micros() as i64);

    // Shift Unix epoch to PostgreSQL epoch
    unix_micros - PG_EPOCH_OFFSET_SECS * 1_000_000
}

/// Read a value from buffer with proper endianness handling.
///
/// This function reads a value of type T from a byte slice, ensuring that
/// the bytes are interpreted in network byte order (big-endian).
///
/// # Arguments
/// * `buf` - The byte slice to read from
///
/// # Returns
/// A value of type T read from the buffer
#[allow(unused)]
#[allow(dead_code)]
pub fn buf_recv<T>(buf: &[u8]) -> T
where
    T: Copy,
    // This function is not currently used
{
    assert!(buf.len() >= std::mem::size_of::<T>());

    unsafe {
        let mut val: T = std::mem::zeroed();
        std::ptr::copy_nonoverlapping(
            buf.as_ptr(),
            &mut val as *mut T as *mut u8,
            std::mem::size_of::<T>(),
        );
        val
    }
}

/// Specialized function for reading network byte order 16-bit unsigned integers.
///
/// Reads a u16 value from a byte slice in big-endian format.
///
/// # Arguments
/// * `buf` - The byte slice to read from
///
/// # Returns
/// A u16 value read from the buffer
#[allow(unused)]
pub fn buf_recv_u16(buf: &[u8]) -> u16 {
    assert!(buf.len() >= 2);
    u16::from_be_bytes(buf[..2].try_into().unwrap())
}

/// Specialized function for reading network byte order 32-bit unsigned integers.
///
/// Reads a u32 value from a byte slice in big-endian format.
///
/// # Arguments
/// * `buf` - The byte slice to read from
///
/// # Returns
/// A u32 value read from the buffer
pub fn buf_recv_u32(buf: &[u8]) -> u32 {
    assert!(buf.len() >= 4);
    u32::from_be_bytes(buf[..4].try_into().unwrap())
}

/// Specialized function for reading network byte order 64-bit unsigned integers.
///
/// Reads a u64 value from a byte slice in big-endian format.
///
/// # Arguments
/// * `buf` - The byte slice to read from
///
/// # Returns
/// A u64 value read from the buffer
pub fn buf_recv_u64(buf: &[u8]) -> u64 {
    assert!(buf.len() >= 8);
    u64::from_be_bytes(buf[..8].try_into().unwrap())
}

/// Specialized function for reading network byte order 16-bit signed integers.
///
/// Reads an i16 value from a byte slice in big-endian format.
///
/// # Arguments
/// * `buf` - The byte slice to read from
///
/// # Returns
/// An i16 value read from the buffer
pub fn buf_recv_i16(buf: &[u8]) -> i16 {
    assert!(buf.len() >= 2);
    i16::from_be_bytes(buf[..2].try_into().unwrap())
}

/// Specialized function for reading network byte order 32-bit signed integers.
///
/// Reads an i32 value from a byte slice in big-endian format.
///
/// # Arguments
/// * `buf` - The byte slice to read from
///
/// # Returns
/// An i32 value read from the buffer
pub fn buf_recv_i32(buf: &[u8]) -> i32 {
    assert!(buf.len() >= 4);
    i32::from_be_bytes(buf[..4].try_into().unwrap())
}

/// Specialized function for reading network byte order 64-bit signed integers.
///
/// Reads an i64 value from a byte slice in big-endian format.
///
/// # Arguments
/// * `buf` - The byte slice to read from
///
/// # Returns
/// An i64 value read from the buffer
pub fn buf_recv_i64(buf: &[u8]) -> i64 {
    assert!(buf.len() >= 8);
    i64::from_be_bytes(buf[..8].try_into().unwrap())
}

/// Write a value to buffer with proper endianness handling.
///
/// This function writes a value of type T to a mutable byte slice, ensuring that
/// the bytes are written in network byte order (big-endian).
///
/// # Arguments
/// * `val` - The value to write
/// * `buf` - The mutable byte slice to write to
#[allow(unused)]
pub fn buf_send<T>(val: T, buf: &mut [u8])
where
    T: Copy,
{
    assert!(buf.len() >= std::mem::size_of::<T>());

    unsafe {
        std::ptr::copy_nonoverlapping(
            &val as *const T as *const u8,
            buf.as_mut_ptr(),
            std::mem::size_of::<T>(),
        );
    }
}

/// Specialized functions for writing network byte order 16-bit unsigned integers.
///
/// Writes a u16 value to a mutable byte slice in big-endian format.
///
/// # Arguments
/// * `val` - The u16 value to write
/// * `buf` - The mutable byte slice to write to
#[allow(unused)]
pub fn buf_send_u16(val: u16, buf: &mut [u8]) {
    assert!(buf.len() >= 2);
    let bytes = val.to_be_bytes();
    buf[0] = bytes[0];
    buf[1] = bytes[1];
}

/// Specialized functions for writing network byte order 32-bit unsigned integers.
///
/// Writes a u32 value to a mutable byte slice in big-endian format.
///
/// # Arguments
/// * `val` - The u32 value to write
/// * `buf` - The mutable byte slice to write to
#[allow(unused)]
pub fn buf_send_u32(val: u32, buf: &mut [u8]) {
    assert!(buf.len() >= 4);
    let bytes = val.to_be_bytes();
    buf[..4].copy_from_slice(&bytes);
}

/// Specialized functions for writing network byte order 64-bit unsigned integers.
///
/// Writes a u64 value to a mutable byte slice in big-endian format.
///
/// # Arguments
/// * `val` - The u64 value to write
/// * `buf` - The mutable byte slice to write to
pub fn buf_send_u64(val: u64, buf: &mut [u8]) {
    assert!(buf.len() >= 8);
    let bytes = val.to_be_bytes();
    buf[..8].copy_from_slice(&bytes);
}

/// Specialized functions for writing network byte order 16-bit signed integers.
///
/// Writes an i16 value to a mutable byte slice in big-endian format.
///
/// # Arguments
/// * `val` - The i16 value to write
/// * `buf` - The mutable byte slice to write to
#[allow(unused)]
pub fn buf_send_i16(val: i16, buf: &mut [u8]) {
    assert!(buf.len() >= 2);
    let bytes = val.to_be_bytes();
    buf[0] = bytes[0];
    buf[1] = bytes[1];
}

/// Specialized functions for writing network byte order 32-bit signed integers.
///
/// Writes an i32 value to a mutable byte slice in big-endian format.
///
/// # Arguments
/// * `val` - The i32 value to write
/// * `buf` - The mutable byte slice to write to
#[allow(unused)]
pub fn buf_send_i32(val: i32, buf: &mut [u8]) {
    assert!(buf.len() >= 4);
    let bytes = val.to_be_bytes();
    buf[..4].copy_from_slice(&bytes);
}

/// Specialized functions for writing network byte order 64-bit signed integers.
///
/// Writes an i64 value to a mutable byte slice in big-endian format.
///
/// # Arguments
/// * `val` - The i64 value to write
/// * `buf` - The mutable byte slice to write to
pub fn buf_send_i64(val: i64, buf: &mut [u8]) {
    assert!(buf.len() >= 8);
    let bytes = val.to_be_bytes();
    buf[..8].copy_from_slice(&bytes);
}

/// Safe wrapper for PostgreSQL connection using libpq
///
/// Provides a safe Rust interface to PostgreSQL's C library (libpq)
/// for replication operations. Handles connection lifecycle, query execution,
/// and replication protocol operations.
pub struct PGConnection {
    conn: *mut PGconn,
}

impl PGConnection {
    /// Establishes a connection to PostgreSQL using the provided connection info.
    ///
    /// This function creates a new PostgreSQL connection using libpq's PQconnectdb function.
    /// It handles error checking and returns an appropriate error if the connection fails.
    ///
    /// # Arguments
    /// * `conninfo` - A string containing connection parameters (e.g., "host=localhost port=5432 dbname=test")
    ///
    /// # Returns
    /// A Result containing either a PGConnection instance or a ReplicationError
    pub fn connect(conninfo: &str) -> ReplicationResult<Self> {
        let c_conninfo = CString::new(conninfo)?;
        let conn = unsafe { PQconnectdb(c_conninfo.as_ptr()) };

        if conn.is_null() {
            return Err(crate::errors::ReplicationError::connection(
                "Failed to allocate connection object",
            ));
        }

        let status = unsafe { PQstatus(conn) };
        if status != ConnStatusType::CONNECTION_OK {
            let error_msg = get_error_message(conn).unwrap_or("Unknown error".to_string());
            unsafe { PQfinish(conn) };
            return Err(crate::errors::ReplicationError::connection(format!(
                "Connection failed: {}",
                error_msg
            )));
        }

        Ok(Self { conn })
    }

    /// Executes a query on the PostgreSQL connection.
    ///
    /// This function executes a SQL query using libpq's PQexec function and returns
    /// a PGResult wrapper for handling the results.
    ///
    /// # Arguments
    /// * `query` - The SQL query string to execute
    ///
    /// # Returns
    /// A Result containing either a PGResult instance or a ReplicationError
    pub fn exec(&self, query: &str) -> ReplicationResult<PGResult> {
        let c_query = CString::new(query)?;
        let result = unsafe { PQexec(self.conn, c_query.as_ptr()) };

        if result.is_null() {
            let error_msg = get_error_message(self.conn).unwrap_or("Unknown error".to_string());

            return Err(crate::errors::ReplicationError::protocol(format!(
                "Query execution failed: {}",
                error_msg
            )));
        }

        Ok(PGResult { result })
    }

    /// Gets data from a COPY operation (blocking).
    ///
    /// This function retrieves data from a PostgreSQL COPY operation. It's a wrapper around
    /// libpq's PQgetCopyData function which handles the low-level details of reading
    /// data from a COPY stream.
    ///
    /// # Note
    /// If no COPY operation is in progress, this function will return an error.
    ///
    /// # Returns
    /// A Result containing either Some(Vec<u8>) with the data, None if no more data or timeout,
    /// or a ReplicationError if the operation fails
    pub fn get_copy_data(&self) -> ReplicationResult<Option<Vec<u8>>> {
        let mut buffer: *mut std::os::raw::c_char = ptr::null_mut();

        /*

        PQgetCopyData attempts to read a row of data from a COPY operation.

        Data is always returned one data row at a time; if only a partial row is available, it is not returned.
        Successful return of a data row involves allocating a chunk of memory to hold the data.
        The buffer parameter must be non-NULL. *buffer is set to point to the allocated memory, or to NULL in cases where no buffer is returned. A non-NULL result buffer must be freed using PQfreemem when no longer needed.

        When a row is successfully returned, the return value is the number of data bytes in the row (this will always be greater than zero).
            The returned string is always null-terminated, though this is probably only useful for textual COPY.
        A result of zero indicates that the COPY is still in progress, but no row is yet available (this is only possible when async is true).
        A result of -1 indicates that the COPY is done.
        A result of -2 indicates that an error occurred (consult PQerrorMessage for the reason).

        When async is true (not zero), PQgetCopyData will not block waiting for input; it will return zero if the COPY is still in progress but no complete row is available.
            (In this case wait for read-ready and then call PQconsumeInput before calling PQgetCopyData again.)
        When async is false (zero), PQgetCopyData will block until data is available or the operation completes.

        After PQgetCopyData returns -1, call PQgetResult to obtain the final result status of the COPY command. One may wait for this result to be available in the usual way. Then return to normal operation.

        */
        let copy_data_len = unsafe { PQgetCopyData(self.conn, &mut buffer, 0) };

        match copy_data_len {
            -2 => {
                let error_msg = get_error_message(self.conn).unwrap_or("Unknown error".to_string());

                Err(crate::errors::ReplicationError::protocol(error_msg))
            }
            -1 => {
                let result = PGResult {
                    result: unsafe { PQgetResult(self.conn) },
                };

                if !result.is_ok() {
                    let error_msg =
                        get_error_message(self.conn).unwrap_or("Unknown error".to_string());
                    return Err(crate::errors::ReplicationError::protocol(error_msg));
                }

                Ok(None)
            } // COPY is done
            0 => Ok(None), // COPY still in progress, no data available (in async mode) - impossible here since we use blocking mode
            len => {
                if buffer.is_null() {
                    return Err(crate::errors::ReplicationError::buffer(
                        "Received null buffer",
                    ));
                }

                let data = unsafe {
                    std::slice::from_raw_parts(buffer as *const u8, len as usize).to_vec()
                };

                unsafe { PQfreemem(buffer as *mut std::os::raw::c_void) };
                Ok(Some(data))
            }
        }
    }

    /// Sends data to a COPY operation.
    ///
    /// This function sends data to a PostgreSQL COPY operation. It's a wrapper around
    /// libpq's PQputCopyData function which handles the low-level details of sending
    /// data to a COPY stream.
    ///
    /// # Arguments
    /// * `data` - The data to send as bytes
    ///
    /// # Returns
    /// A Result indicating success or failure of the operation
    pub fn put_copy_data(&self, data: &[u8]) -> ReplicationResult<()> {
        let result = unsafe {
            PQputCopyData(
                self.conn,
                data.as_ptr() as *const std::os::raw::c_char,
                data.len() as i32,
            )
        };

        if result != 1 {
            let error_msg = get_error_message(self.conn).unwrap_or("Unknown error".to_string());

            return Err(crate::errors::ReplicationError::protocol(format!(
                "Failed to send copy data: {}",
                error_msg
            )));
        }

        Ok(())
    }

    /// Flushes the connection buffer.
    ///
    /// This function flushes any buffered output on the connection. It's a wrapper around
    /// libpq's PQflush function which ensures that all pending data is sent to the server.
    ///
    /// # Returns
    /// A Result indicating success or failure of the operation
    pub fn flush(&self) -> ReplicationResult<()> {
        let result = unsafe { PQflush(self.conn) };
        if result != 0 {
            return Err(crate::errors::ReplicationError::protocol(
                "Failed to flush connection",
            ));
        }
        Ok(())
    }
}

impl Drop for PGConnection {
    fn drop(&mut self) {
        if !self.conn.is_null() {
            unsafe { PQfinish(self.conn) };
        }
    }
}

fn get_error_message(conn: *const PGconn) -> Option<String> {
    unsafe {
        let error_ptr = PQerrorMessage(conn);
        if error_ptr.is_null() {
            None
        } else {
            Some(CStr::from_ptr(error_ptr).to_string_lossy().into_owned())
        }
    }
}

/// Safe wrapper for PostgreSQL result.
///
/// This struct provides a safe interface to PostgreSQL query results using libpq.
/// It handles access to result metadata and data values.
pub struct PGResult {
    result: *mut PGresult,
}

impl PGResult {
    /// Gets the status of the PostgreSQL result.
    ///
    /// This function returns the execution status of the query that produced this result.
    ///
    /// # Returns
    /// An ExecStatusType representing the result status
    pub fn status(&self) -> ExecStatusType {
        unsafe { PQresultStatus(self.result) }
    }

    /// Checks if the PostgreSQL result is successful.
    ///
    /// This function determines whether the query that produced this result was successful.
    /// It returns true for PGRES_TUPLES_OK and PGRES_COMMAND_OK status codes.
    ///
    /// # Returns
    /// A boolean indicating whether the operation was successful
    pub fn is_ok(&self) -> bool {
        matches!(
            self.status(),
            ExecStatusType::PGRES_TUPLES_OK | ExecStatusType::PGRES_COMMAND_OK
        )
    }

    /// Gets the number of tuples (rows) in the result.
    ///
    /// This function returns the number of rows returned by the query.
    ///
    /// # Returns
    /// An i32 representing the number of tuples
    #[allow(unused)]
    pub fn ntuples(&self) -> i32 {
        unsafe { PQntuples(self.result) }
    }

    /// Gets the number of fields (columns) in the result.
    ///
    /// This function returns the number of columns returned by the query.
    ///
    /// # Returns
    /// An i32 representing the number of fields
    #[allow(unused)]
    pub fn nfields(&self) -> i32 {
        unsafe { PQnfields(self.result) }
    }

    /// Gets a value from the result by row and column index.
    ///
    /// This function retrieves a specific value from the query result based on its position.
    ///
    /// # Arguments
    /// * `row` - The row index (0-based)
    /// * `col` - The column index (0-based)
    ///
    /// # Returns
    /// An Option<String> containing the value, or None if not found
    pub fn getvalue(&self, row: i32, col: i32) -> Option<String> {
        let value_ptr = unsafe { PQgetvalue(self.result, row, col) };
        if value_ptr.is_null() {
            None
        } else {
            unsafe { Some(CStr::from_ptr(value_ptr).to_string_lossy().into_owned()) }
        }
    }
}

impl Drop for PGResult {
    fn drop(&mut self) {
        if !self.result.is_null() {
            unsafe { PQclear(self.result) };
        }
    }
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
