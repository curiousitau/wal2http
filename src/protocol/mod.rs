//! Protocol module for PostgreSQL logical replication protocol handling
//!
//! This module contains all the components needed to parse and handle
//! PostgreSQL's logical replication protocol, including message parsing,
//! buffer management, and protocol message definitions.

pub mod buffer;
pub mod messages;
pub mod parser;

// Re-export for convenience
pub use buffer::{BufferReader, BufferWriter};
pub use messages::*;
pub use parser::MessageParser;