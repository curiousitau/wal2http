//! HTTP event sink implementation
//!
//! Provides an event sink for sending replication events to HTTP endpoints.

use crate::protocol::messages::ReplicationMessage;
use super::super::EventSink;
use async_trait::async_trait;
use crate::core::errors::ReplicationResult;

/// Configuration for HTTP event sink
pub struct HttpEventSinkConfig {
    pub endpoint_url: String,
}

/// HTTP event sink for sending events to HTTP endpoints
pub struct HttpEventSink {
    config: HttpEventSinkConfig,
}

impl HttpEventSink {
    /// Create a new HTTP event sink
    pub fn new(config: HttpEventSinkConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl EventSink for HttpEventSink {
    async fn send_event(&self, message: &ReplicationMessage) -> ReplicationResult<()> {
        // TODO: Implement HTTP event sending
        tracing::warn!("HTTP event sink not yet implemented");
        Ok(())
    }
}