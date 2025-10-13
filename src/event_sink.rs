use crate::errors::ReplicationResult;
use crate::types::ReplicationMessage;
use async_trait::async_trait;
mod event_formatter;
pub(crate) mod hook0;
pub(crate) mod hook0_error;
pub(crate) mod http;
mod pg_type_conversion;
pub(crate) mod stdout;

/// EventSink trait for common event sending functionality
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Send a replication event
    async fn send_event(&self, event: &ReplicationMessage) -> ReplicationResult<()>;
}
