use crate::errors::ReplicationResult;
use crate::tracing_context::CorrelationId;
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
    /// Send a replication event with correlation ID
    async fn send_event(&self, event: &ReplicationMessage, correlation_id: Option<&CorrelationId>) -> ReplicationResult<()>;

    /// Send a replication event (backward compatibility)
    async fn send_event_legacy(&self, event: &ReplicationMessage) -> ReplicationResult<()> {
        self.send_event(event, None).await
    }
}
