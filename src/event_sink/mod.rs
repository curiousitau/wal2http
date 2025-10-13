//! Event sink foundation for PostgreSQL logical replication
//! Provides pluggable architecture for sending replication events to various destinations

pub mod event_formatter;
pub mod sink;
pub mod stdout;

pub use event_formatter::*;
pub use sink::*;
pub use stdout::*;