//! Hook0 event sink implementation
//!
//! Provides an event sink for sending replication events to Hook0 service.

use crate::protocol::messages::ReplicationMessage;
use super::super::EventSink;
use async_trait::async_trait;
use crate::core::errors::ReplicationResult;

/// Configuration for Hook0 event sink
pub struct Hook0EventSinkConfig {
    pub api_url: String,
    pub application_id: uuid::Uuid,
    pub api_token: String,
}

/// Hook0 event sink for sending events to Hook0 service
pub struct Hook0EventSink {
    config: Hook0EventSinkConfig,
}

impl Hook0EventSink {
    /// Create a new Hook0 event sink
    pub fn new(config: Hook0EventSinkConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl EventSink for Hook0EventSink {
    async fn send_event(&self, message: &ReplicationMessage) -> ReplicationResult<()> {
        // TODO: Implement Hook0 event sending
        tracing::warn!("Hook0 event sink not yet implemented");
        Ok(())
    }
}