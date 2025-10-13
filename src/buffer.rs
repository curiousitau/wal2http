use crate::errors::{ReplicationError, Result};
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
    pub fn skip_message_type(&mut self) -> Result<char> {
        if self.buffer.is_empty() {
            return Err(ReplicationError::parse("Empty buffer"));
        }
        let msg_type = self.buffer[0] as char;
        self.position = 1;
        Ok(msg_type)
    }

    /// Read a single byte at current position
    pub fn read_u8(&mut self) -> Result<u8> {
        if !self.has_bytes(1) {
            return Err(ReplicationError::parse("Not enough bytes for u8"));
        }
        let value = self.buffer[self.position];
        self.position += 1;
        Ok(value)
    }

    /// Read a 16-bit integer at current position
    pub fn read_i16(&mut self) -> Result<i16> {
        if !self.has_bytes(2) {
            return Err(ReplicationError::parse("Not enough bytes for i16"));
        }
        let value = buf_recv_i16(&self.buffer[self.position..]);
        self.position += 2;
        Ok(value)
    }

    /// Read a 32-bit unsigned integer at current position
    pub fn read_u32(&mut self) -> Result<u32> {
        if !self.has_bytes(4) {
            return Err(ReplicationError::parse("Not enough bytes for u32"));
        }
        let value = buf_recv_u32(&self.buffer[self.position..]);
        self.position += 4;
        Ok(value)
    }

    /// Read a 32-bit signed integer at current position
    pub fn read_i32(&mut self) -> Result<i32> {
        if !self.has_bytes(4) {
            return Err(ReplicationError::parse("Not enough bytes for i32"));
        }
        let value = buf_recv_i32(&self.buffer[self.position..]);
        self.position += 4;
        Ok(value)
    }

    /// Read a 64-bit unsigned integer at current position
    pub fn read_u64(&mut self) -> Result<u64> {
        if !self.has_bytes(8) {
            return Err(ReplicationError::parse("Not enough bytes for u64"));
        }
        let value = buf_recv_u64(&self.buffer[self.position..]);
        self.position += 8;
        Ok(value)
    }

    /// Read a 64-bit signed integer at current position
    pub fn read_i64(&mut self) -> Result<i64> {
        if !self.has_bytes(8) {
            return Err(ReplicationError::parse("Not enough bytes for i64"));
        }
        let value = buf_recv_i64(&self.buffer[self.position..]);
        self.position += 8;
        Ok(value)
    }

    /// Read a null-terminated string at current position
    pub fn read_null_terminated_string(&mut self) -> Result<String> {
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
    pub fn read_length_prefixed_string(&mut self) -> Result<String> {
        let length = self.read_i32()?;

        if length < 0 {
            return Err(ReplicationError::parse("Negative string length"));
        }

        let length = length as usize;
        if !self.has_bytes(length) {
            return Err(ReplicationError::parse("String data truncated"));
        }

        let string_bytes = &self.buffer[self.position..self.position + length];
        let string_value = String::from_utf8_lossy(string_bytes).into_owned();

        self.position += length;
        Ok(string_value)
    }

    /// Read a length-prefixed byte vector (32-bit length followed by data)
    pub fn read_length_prefixed_bytes(&mut self) -> Result<Vec<u8>> {
        let length = self.read_i32()?;

        if length < 0 {
            return Err(ReplicationError::parse("Negative byte array length"));
        }

        let length = length as usize;
        if !self.has_bytes(length) {
            return Err(ReplicationError::parse("Byte array data truncated"));
        }

        let bytes = self.buffer[self.position..self.position + length].to_vec();
        self.position += length;
        Ok(bytes)
    }

    /// Read all remaining bytes as a new buffer
    pub fn read_remaining_bytes(&mut self) -> Result<Vec<u8>> {
        let remaining = self.remaining();
        if remaining == 0 {
            return Ok(Vec::new());
        }

        let bytes = self.buffer[self.position..].to_vec();
        self.position = self.buffer.len();
        Ok(bytes)
    }

    /// Validate that the current position aligns with expected boundaries
    pub fn validate_position(&self, context: &str) -> Result<()> {
        if self.position > self.buffer.len() {
            return Err(ReplicationError::parse_with_context(
                "Buffer position exceeds bounds",
                format!("Context: {}, Position: {}, Buffer size: {}",
                    context, self.position, self.buffer.len())
            ));
        }
        Ok(())
    }

    /// Peek at the next byte without advancing position
    pub fn peek_u8(&self) -> Result<u8> {
        if !self.has_bytes(1) {
            return Err(ReplicationError::parse("No bytes to peek"));
        }
        Ok(self.buffer[self.position])
    }

    /// Set position directly (use with caution)
    pub fn set_position(&mut self, position: usize) -> Result<()> {
        if position > self.buffer.len() {
            return Err(ReplicationError::parse("Position out of bounds"));
        }
        self.position = position;
        Ok(())
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
    pub fn write_u8(&mut self, value: u8) -> Result<()> {
        if !self.has_space(1) {
            return Err(ReplicationError::parse("Not enough space for u8"));
        }
        self.buffer[self.position] = value;
        self.position += 1;
        Ok(())
    }

    /// Write a 64-bit unsigned integer at current position
    pub fn write_u64(&mut self, value: u64) -> Result<()> {
        if !self.has_space(8) {
            return Err(ReplicationError::parse("Not enough space for u64"));
        }
        crate::utils::buf_send_u64(value, &mut self.buffer[self.position..]);
        self.position += 8;
        Ok(())
    }

    /// Write a 64-bit signed integer at current position
    pub fn write_i64(&mut self, value: i64) -> Result<()> {
        if !self.has_space(8) {
            return Err(ReplicationError::parse("Not enough space for i64"));
        }
        crate::utils::buf_send_i64(value, &mut self.buffer[self.position..]);
        self.position += 8;
        Ok(())
    }

    /// Write a 32-bit unsigned integer at current position
    pub fn write_u32(&mut self, value: u32) -> Result<()> {
        if !self.has_space(4) {
            return Err(ReplicationError::parse("Not enough space for u32"));
        }
        crate::utils::buf_send_u32(value, &mut self.buffer[self.position..]);
        self.position += 4;
        Ok(())
    }

    /// Write a 32-bit signed integer at current position
    pub fn write_i32(&mut self, value: i32) -> Result<()> {
        if !self.has_space(4) {
            return Err(ReplicationError::parse("Not enough space for i32"));
        }
        crate::utils::buf_send_i32(value, &mut self.buffer[self.position..]);
        self.position += 4;
        Ok(())
    }

    /// Write a length-prefixed string (32-bit length followed by data)
    pub fn write_length_prefixed_string(&mut self, value: &str) -> Result<()> {
        let bytes = value.as_bytes();
        let length = bytes.len() as i32;

        if !self.has_space(4 + length as usize) {
            return Err(ReplicationError::parse("Not enough space for length-prefixed string"));
        }

        self.write_i32(length)?;
        self.buffer[self.position..self.position + bytes.len()].copy_from_slice(bytes);
        self.position += bytes.len();
        Ok(())
    }

    /// Write raw bytes
    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if !self.has_space(bytes.len()) {
            return Err(ReplicationError::parse("Not enough space for bytes"));
        }

        self.buffer[self.position..self.position + bytes.len()].copy_from_slice(bytes);
        self.position += bytes.len();
        Ok(())
    }

    /// Get the total bytes written so far
    pub fn bytes_written(&self) -> usize {
        self.position
    }
}
