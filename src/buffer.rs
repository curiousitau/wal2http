use crate::errors::{ReplicationError, ReplicationResult};
use crate::utils::{buf_recv_i16, buf_recv_i32, buf_recv_i64, buf_recv_u32, buf_recv_u64};

/// A buffer reader that manages position and provides meaningful parsing methods
#[derive(Debug)]
pub struct BufferReader<'a> {
    buffer: &'a [u8],
    position: usize,
}

impl<'a> BufferReader<'a> {
    /// Create a new buffer reader from a byte slice
    pub fn new(buffer: &'a [u8]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }

    /// Get current position in the buffer
    pub fn position(&self) -> usize {
        self.position
    }

    /// Get remaining bytes in the buffer
    pub fn remaining(&self) -> usize {
        if self.position < self.buffer.len() {
            self.buffer.len() - self.position
        } else {
            0
        }
    }

    /// Check if we have at least `count` bytes remaining
    pub fn has_bytes(&self, count: usize) -> bool {
        self.remaining() >= count
    }

    /// Skip the message type byte (typically the first byte)
    pub fn skip_message_type(&mut self) -> ReplicationResult<char> {
        if !self.has_bytes(1) {
            return Err(ReplicationError::parse(
                "Empty buffer or not enough bytes for message type",
            ));
        }
        let msg_type = self.buffer[self.position] as char;
        self.position += 1;
        Ok(msg_type)
    }

    /// Read a single byte at current position
    pub fn read_u8(&mut self) -> ReplicationResult<u8> {
        if !self.has_bytes(1) {
            return Err(ReplicationError::parse("Not enough bytes for u8"));
        }
        let value = self.buffer[self.position];
        self.position += 1;
        Ok(value)
    }

    /// Read a 16-bit integer at current position
    pub fn read_i16(&mut self) -> ReplicationResult<i16> {
        if !self.has_bytes(2) {
            return Err(ReplicationError::parse("Not enough bytes for i16"));
        }
        let value = buf_recv_i16(&self.buffer[self.position..]);
        self.position += 2;
        Ok(value)
    }

    /// Read a 32-bit unsigned integer at current position
    pub fn read_u32(&mut self) -> ReplicationResult<u32> {
        if !self.has_bytes(4) {
            return Err(ReplicationError::parse("Not enough bytes for u32"));
        }
        let value = buf_recv_u32(&self.buffer[self.position..]);
        self.position += 4;
        Ok(value)
    }

    /// Read a 32-bit signed integer at current position
    pub fn read_i32(&mut self) -> ReplicationResult<i32> {
        if !self.has_bytes(4) {
            return Err(ReplicationError::parse("Not enough bytes for i32"));
        }
        let value = buf_recv_i32(&self.buffer[self.position..]);
        self.position += 4;
        Ok(value)
    }

    /// Read a 64-bit unsigned integer at current position
    pub fn read_u64(&mut self) -> ReplicationResult<u64> {
        if !self.has_bytes(8) {
            return Err(ReplicationError::parse("Not enough bytes for u64"));
        }
        let value = buf_recv_u64(&self.buffer[self.position..]);
        self.position += 8;
        Ok(value)
    }

    /// Read a 64-bit signed integer at current position
    pub fn read_i64(&mut self) -> ReplicationResult<i64> {
        if !self.has_bytes(8) {
            return Err(ReplicationError::parse("Not enough bytes for i64"));
        }
        let value = buf_recv_i64(&self.buffer[self.position..]);
        self.position += 8;
        Ok(value)
    }

    /// Read a null-terminated string at current position
    pub fn read_null_terminated_string(&mut self) -> ReplicationResult<String> {
        let start_pos = self.position;

        // Find the null terminator
        while self.position < self.buffer.len() && self.buffer[self.position] != 0 {
            self.position += 1;
        }

        if self.position >= self.buffer.len() {
            return Err(ReplicationError::parse("String not null-terminated"));
        }

        // Extract the string
        let string_bytes = &self.buffer[start_pos..self.position];
        let string_value = String::from_utf8_lossy(string_bytes).into_owned();

        // Skip the null terminator
        self.position += 1;

        Ok(string_value)
    }

    /// Read a length-prefixed string (32-bit length followed by data)
    pub fn read_length_prefixed_string(&mut self) -> ReplicationResult<String> {
        let length = self.read_i32()?;

        if length < 0 {
            return Err(ReplicationError::parse("Negative string length"));
        }

        // Check for potential integer overflow when converting i32 to usize
        // and ensure the length is reasonable (prevent DoS attacks)
        const MAX_STRING_LENGTH: usize = 1024 * 1024; // 1MB limit
        let length = length as usize;
        if length > MAX_STRING_LENGTH {
            return Err(ReplicationError::parse(
                "String length exceeds maximum allowed size",
            ));
        }

        if !self.has_bytes(length) {
            return Err(ReplicationError::parse("String data truncated"));
        }

        let string_bytes = &self.buffer[self.position..self.position + length];
        let string_value = String::from_utf8_lossy(string_bytes).into_owned();

        self.position += length;
        Ok(string_value)
    }

    /// Peek at the next byte without advancing position
    pub fn peek_u8(&self) -> ReplicationResult<u8> {
        if !self.has_bytes(1) {
            return Err(ReplicationError::parse("No bytes to peek"));
        }
        Ok(self.buffer[self.position])
    }

    /// Set position directly (use with caution)
    pub fn set_position(&mut self, position: usize) -> ReplicationResult<()> {
        if position > self.buffer.len() {
            return Err(ReplicationError::parse("Position out of bounds"));
        }
        self.position = position;
        Ok(())
    }

    pub(crate) fn read_char(&mut self) -> ReplicationResult<char> {
        if !self.has_bytes(1) {
            return Err(ReplicationError::parse("Not enough bytes for char"));
        }
        let val = self.buffer[self.position] as char;
        self.position += 1;
        Ok(val)
    }

    pub(crate) fn read_bytes(&mut self, remaining: usize) -> ReplicationResult<Vec<u8>> {
        if !self.has_bytes(remaining) {
            return Err(ReplicationError::parse("Not enough bytes"));
        }
        let bytes = &self.buffer[self.position..self.position + remaining];
        self.position += remaining;
        Ok(bytes.to_vec())
    }
}

/// A buffer writer that manages position and provides meaningful writing methods
/// for constructing binary messages with automatic position tracking
#[derive(Debug)]
pub struct BufferWriter<'a> {
    buffer: &'a mut [u8],
    position: usize,
}

impl<'a> BufferWriter<'a> {
    /// Create a new buffer writer from a mutable byte slice
    pub fn new(buffer: &'a mut [u8]) -> Self {
        Self {
            buffer,
            position: 0,
        }
    }

    /// Get remaining space in the buffer
    pub fn remaining(&self) -> usize {
        if self.position < self.buffer.len() {
            self.buffer.len() - self.position
        } else {
            0
        }
    }

    /// Check if we have at least `count` bytes remaining
    pub fn has_space(&self, count: usize) -> bool {
        self.remaining() >= count
    }

    /// Write a single byte at current position
    pub fn write_u8(&mut self, value: u8) -> ReplicationResult<()> {
        if !self.has_space(1) {
            return Err(ReplicationError::parse("Not enough space for u8"));
        }
        self.buffer[self.position] = value;
        self.position += 1;
        Ok(())
    }

    /// Write a 64-bit unsigned integer at current position
    pub fn write_u64(&mut self, value: u64) -> ReplicationResult<()> {
        if !self.has_space(8) {
            return Err(ReplicationError::parse("Not enough space for u64"));
        }
        crate::utils::buf_send_u64(value, &mut self.buffer[self.position..]);
        self.position += 8;
        Ok(())
    }

    /// Write a 64-bit signed integer at current position
    pub fn write_i64(&mut self, value: i64) -> ReplicationResult<()> {
        if !self.has_space(8) {
            return Err(ReplicationError::parse("Not enough space for i64"));
        }
        crate::utils::buf_send_i64(value, &mut self.buffer[self.position..]);
        self.position += 8;
        Ok(())
    }

    /// Get the total bytes written so far
    pub fn bytes_written(&self) -> usize {
        self.position
    }

    pub(crate) fn write_char(&mut self, message_type: char) -> ReplicationResult<()> {
        if !self.has_space(1) {
            return Err(ReplicationError::parse("Not enough space for char"));
        }
        self.buffer[self.position] = message_type as u8;
        self.position += 1;
        Ok(())
    }

    pub(crate) fn write_u32(&mut self, xmin: u32) -> ReplicationResult<()> {
        if !self.has_space(4) {
            return Err(ReplicationError::parse("Not enough space for u32"));
        }
        crate::utils::buf_send_u32(xmin, &mut self.buffer[self.position..]);
        self.position += 4;
        Ok(())
    }
}
