use async_trait::async_trait;

use crate::{errors::ReplicationResult, event_sink::EventSink, tracing_context::CorrelationId};

pub(crate) struct StdoutEventSink {}

#[async_trait]
impl EventSink for StdoutEventSink {
    async fn send_event(&self, event: &crate::types::ReplicationMessage, correlation_id: Option<&CorrelationId>) -> ReplicationResult<()> {
        if let Some(cid) = correlation_id {
            println!("[{}] {:?}", cid, event);
        } else {
            println!("{:?}", event);
        }
        Ok(())
    }
}
