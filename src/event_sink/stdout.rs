//! Standard output event sink for testing and debugging

use crate::errors::Result;
use crate::event_sink::{BaseEventSink, EventSink, FormattedEvent, SinkConfig, SinkMetrics};
use async_trait::async_trait;
use std::io::{self, Write};
use tracing::{debug, error, info, warn};

/// Standard output event sink implementation
pub struct StdoutSink {
    base: BaseEventSink,
}

impl StdoutSink {
    /// Create a new stdout sink
    pub fn new() -> Self {
        Self {
            base: BaseEventSink::new(SinkConfig {
                sink_type: "stdout".to_string(),
                parameters: std::collections::HashMap::new(),
                enabled: true,
                retry_config: Default::default(),
            }),
        }
    }
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventSink for StdoutSink {
    fn sink_type(&self) -> &'static str {
        "stdout"
    }

    async fn initialize(&mut self, config: &SinkConfig) -> Result<()> {
        info!("Initializing stdout sink");
        self.base = BaseEventSink::new(config.clone());
        self.base.set_initialized(true);
        Ok(())
    }

    async fn send_event(&mut self, event: &FormattedEvent) -> Result<()> {
        if !self.base.is_initialized() {
            return Err(crate::errors::ReplicationError::config("Sink not initialized"));
        }

        debug!("Sending event to stdout: {}", event.event_type);

        // Try to send the event
        match self.try_send_event(event).await {
            Ok(bytes_sent) => {
                self.base.record_success(bytes_sent);
                Ok(())
            }
            Err(e) => {
                self.base.record_failure();
                warn!("Failed to send event to stdout: {}", e);

                // Implement retry logic
                if self.base.should_retry() {
                    self.base.increment_retry();
                    let delay = self.base.get_retry_delay();
                    info!("Retrying in {:?} (attempt {}/{})",
                          delay,
                          self.base.get_metrics().current_retry_count,
                          self.base.config().retry_config.max_attempts);

                    tokio::time::sleep(delay).await;
                    return self.send_event(event).await;
                } else {
                    error!("Max retry attempts reached for stdout sink");
                    Err(e)
                }
            }
        }
    }

    async fn health_check(&self) -> Result<bool> {
        // For stdout, we just check if stdout is available
        Ok(true)
    }

    fn get_metrics(&self) -> &SinkMetrics {
        self.base.get_metrics()
    }

    async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down stdout sink");
        io::stdout().flush()?;
        self.base.set_initialized(false);
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        io::stdout().flush()?;
        Ok(())
    }
}

impl StdoutSink {
    /// Try to send an event to stdout
    async fn try_send_event(&self, event: &FormattedEvent) -> Result<usize> {
        let json_str = serde_json::to_string_pretty(event)
            .map_err(|e| crate::errors::ReplicationError::protocol(format!("Failed to serialize event: {}", e)))?;

        let mut stdout = io::stdout();
        writeln!(stdout, "{}", json_str)?;
        stdout.flush()?;

        Ok(json_str.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_sink::SinkConfig;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_stdout_sink_basic() {
        let mut sink = StdoutSink::new();
        let config = SinkConfig {
            sink_type: "stdout".to_string(),
            parameters: HashMap::new(),
            enabled: true,
            retry_config: Default::default(),
        };

        assert!(sink.initialize(&config).await.is_ok());
        assert_eq!(sink.sink_type(), "stdout");
        assert!(sink.health_check().await.unwrap());
    }

    #[tokio::test]
    async fn test_stdout_sink_send_event() {
        let mut sink = StdoutSink::new();
        let config = SinkConfig {
            sink_type: "stdout".to_string(),
            parameters: HashMap::new(),
            enabled: true,
            retry_config: Default::default(),
        };

        sink.initialize(&config).await.unwrap();

        let event = FormattedEvent {
            event_type: "test".to_string(),
            transaction_id: Some(123),
            lsn: Some(456),
            timestamp: Some(789),
            schema: Some("public".to_string()),
            table: Some("test_table".to_string()),
            data: serde_json::json!({"test": "data"}),
            metadata: HashMap::new(),
        };

        // This should print the event to stdout
        assert!(sink.send_event(&event).await.is_ok());
    }
}