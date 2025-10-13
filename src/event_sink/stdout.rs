use async_trait::async_trait;

use crate::{errors::ReplicationResult, event_sink::EventSink};

pub(crate) struct StdoutEventSink {}

#[async_trait]
impl EventSink for StdoutEventSink {
    async fn send_event(&self, event: &crate::types::ReplicationMessage) -> ReplicationResult<()> {
        println!("{:?}", event);
        Ok(())
    }
}
