//! Event sink foundation for PostgreSQL logical replication
//! Provides pluggable architecture for sending replication events to various destinations

use crate::errors::Result;
use crate::types::ReplicationMessage;
use async_trait::async_trait;

pub mod event_formatter;
pub mod http;
pub mod sink;
pub mod stdout;

/// EventSink trait for common event sending functionality
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Send a replication event
    async fn send_event(&self, event: &ReplicationMessage) -> Result<()>;
}

pub use event_formatter::*;
pub use http::*;
pub use sink::*;
pub use stdout::*;