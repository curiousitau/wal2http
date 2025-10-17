//! PostgreSQL connection handling utilities
//!
//! Provides a safe wrapper around PostgreSQL's C library (libpq)
//! for replication operations. Handles connection lifecycle, query execution,
//! and replication protocol operations.

use crate::core::errors::ReplicationResult;
use libpq_sys::*;
use std::ffi::{CStr, CString};
use std::ptr;

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
            return Err(crate::core::errors::ReplicationError::connection(
                "Failed to allocate connection object",
            ));
        }

        let status = unsafe { PQstatus(conn) };
        if status != ConnStatusType::CONNECTION_OK {
            let error_msg = get_error_message(conn).unwrap_or("Unknown error".to_string());
            unsafe { PQfinish(conn) };
            return Err(crate::core::errors::ReplicationError::connection(format!(
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

            return Err(crate::core::errors::ReplicationError::protocol(format!(
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

                Err(crate::core::errors::ReplicationError::protocol(error_msg))
            }
            -1 => {
                let result = PGResult {
                    result: unsafe { PQgetResult(self.conn) },
                };

                if !result.is_ok() {
                    let error_msg =
                        get_error_message(self.conn).unwrap_or("Unknown error".to_string());
                    return Err(crate::core::errors::ReplicationError::protocol(error_msg));
                }

                Ok(None)
            } // COPY is done
            0 => Ok(None), // COPY still in progress, no data available (in async mode) - impossible here since we use blocking mode
            len => {
                if buffer.is_null() {
                    return Err(crate::core::errors::ReplicationError::buffer(
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

            return Err(crate::core::errors::ReplicationError::protocol(format!(
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
            return Err(crate::core::errors::ReplicationError::protocol(
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