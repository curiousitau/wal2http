//! STDOUT event sink implementation
//!
//! Provides a simple event sink that writes replication events to standard output.
//! This is useful for debugging, testing, or when no other sink is configured.

use async_trait::async_trait;
use crate::core::errors::ReplicationResult;
use crate::protocol::messages::ReplicationMessage;

/// Event sink that writes events to standard output
pub struct StdoutEventSink {}

impl StdoutEventSink {
    /// Create a new STDOUT event sink
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StdoutEventSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl super::EventSink for StdoutEventSink {
    async fn send_event(&self, event: &ReplicationMessage) -> ReplicationResult<()> {
        println!("{:?}", event);
        Ok(())
    }
}