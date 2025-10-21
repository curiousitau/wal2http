//! Event processing module for PostgreSQL logical replication
//!
//! This module contains all components for handling and processing replication events,
//! including different event sinks (HTTP, Hook0, STDOUT) and event formatting.

use async_trait::async_trait;
use crate::core::errors::ReplicationResult;
use crate::protocol::messages::ReplicationMessage;

pub mod sink;
pub mod processors;

// Re-export for convenience
pub use sink::EventSinkRegistry;

/// EventSink trait for common event sending functionality
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Send a replication event
    async fn send_event(&self, event: &ReplicationMessage) -> ReplicationResult<()>;
}