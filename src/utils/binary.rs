//! Binary data manipulation utilities for PostgreSQL protocol handling
//!
//! Provides functions for reading and writing binary data with proper endianness
//! handling for network byte order communication with PostgreSQL.

// Type aliases matching PostgreSQL internal types
pub type XLogRecPtr = u64;     // WAL location pointer
pub type Xid = u32;            // Transaction ID
pub type Oid = u32;            // Object ID
pub type TimestampTz = i64;    // Timestamp with timezone

pub const INVALID_XLOG_REC_PTR: XLogRecPtr = 0;

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