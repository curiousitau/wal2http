//! Standard output event sink for testing and debugging

use crate::errors::Result;
use crate::event_sink::EventSink;
use crate::types::ReplicationMessage;
use async_trait::async_trait;
use std::io::{self, Write};
use tracing::debug;

/// Standard output event sink implementation
pub struct StdoutSink {
    enabled: bool,
}

impl StdoutSink {
    /// Create a new stdout sink
    pub fn new() -> Self {
        Self { enabled: true }
    }

    /// Create a new disabled stdout sink
    pub fn new_disabled() -> Self {
        Self { enabled: false }
    }
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventSink for StdoutSink {
    async fn send_event(&self, event: &ReplicationMessage) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        debug!("Sending event to stdout: {:?}", event);

        // Convert the event to debug string for printing
        let debug_str = format!("{:#?}", event);

        let mut stdout = io::stdout();
        writeln!(stdout, "{}", debug_str)?;
        stdout.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stdout_sink_basic() {
        let sink = StdoutSink::new();
        // Just test that it doesn't panic
        assert!(sink.enabled);
    }

    #[tokio::test]
    async fn test_stdout_sink_disabled() {
        let sink = StdoutSink::new_disabled();
        assert!(!sink.enabled);
    }
}