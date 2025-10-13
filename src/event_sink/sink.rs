//! Event sink trait and basic implementations for PostgreSQL logical replication

use crate::errors::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use tracing::{debug, info};

/// Configuration for event sinks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    /// Type of sink (http, hook0, stdout, etc.)
    pub sink_type: String,
    /// Additional configuration parameters specific to the sink type
    pub parameters: HashMap<String, String>,
    /// Enable/disable the sink
    pub enabled: bool,
    /// Retry configuration
    pub retry_config: RetryConfig,
}

/// Retry configuration for event sinks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial delay between retries (in seconds)
    pub initial_delay_secs: u64,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Maximum delay between retries (in seconds)
    pub max_delay_secs: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_secs: 1,
            backoff_multiplier: 2.0,
            max_delay_secs: 30,
        }
    }
}

/// Metrics for sink performance monitoring
#[derive(Debug, Default, Clone)]
pub struct SinkMetrics {
    /// Total events sent
    pub events_sent: u64,
    /// Total events failed
    pub events_failed: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Last successful send time
    pub last_success_time: Option<SystemTime>,
    /// Last error time
    pub last_error_time: Option<SystemTime>,
    /// Current retry count
    pub current_retry_count: u32,
}

/// Formatted event ready for sending to sinks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormattedEvent {
    /// Event type (begin, commit, insert, update, delete, truncate)
    pub event_type: String,
    /// Transaction ID if available
    pub transaction_id: Option<u32>,
    /// LSN (Log Sequence Number) for this event
    pub lsn: Option<u64>,
    /// Timestamp when the event occurred
    pub timestamp: Option<i64>,
    /// Schema name
    pub schema: Option<String>,
    /// Table name
    pub table: Option<String>,
    /// Operation data (varies by event type)
    pub data: serde_json::Value,
    /// Event metadata
    pub metadata: HashMap<String, String>,
}

/// Trait for event sinks - implementations handle sending events to various destinations
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Get the sink type identifier
    fn sink_type(&self) -> &'static str;

    /// Initialize the sink with configuration
    async fn initialize(&mut self, config: &SinkConfig) -> Result<()>;

    /// Send a formatted event to the sink
    async fn send_event(&mut self, event: &FormattedEvent) -> Result<()>;

    /// Check if the sink is healthy and ready to accept events
    async fn health_check(&self) -> Result<bool>;

    /// Get current sink metrics
    fn get_metrics(&self) -> &SinkMetrics;

    /// Shutdown the sink gracefully
    async fn shutdown(&mut self) -> Result<()>;

    /// Flush any pending events
    async fn flush(&mut self) -> Result<()>;
}

/// Base event sink with common functionality
pub struct BaseEventSink {
    config: SinkConfig,
    metrics: SinkMetrics,
    is_initialized: bool,
}

impl BaseEventSink {
    /// Create a new base event sink
    pub fn new(config: SinkConfig) -> Self {
        Self {
            config,
            metrics: SinkMetrics::default(),
            is_initialized: false,
        }
    }

    /// Get configuration reference
    pub fn config(&self) -> &SinkConfig {
        &self.config
    }

    /// Update metrics after successful send
    pub fn record_success(&mut self, bytes_sent: usize) {
        self.metrics.events_sent += 1;
        self.metrics.bytes_sent += bytes_sent as u64;
        self.metrics.last_success_time = Some(SystemTime::now());
        self.metrics.current_retry_count = 0;
    }

    /// Update metrics after failed send
    pub fn record_failure(&mut self) {
        self.metrics.events_failed += 1;
        self.metrics.last_error_time = Some(SystemTime::now());
    }

    /// Increment retry count
    pub fn increment_retry(&mut self) {
        self.metrics.current_retry_count += 1;
    }

    /// Check if we should retry based on retry configuration
    pub fn should_retry(&self) -> bool {
        self.metrics.current_retry_count < self.config.retry_config.max_attempts
    }

    /// Get delay for next retry attempt
    pub fn get_retry_delay(&self) -> std::time::Duration {
        let delay = self.config.retry_config.initial_delay_secs as f64
            * self.config.retry_config.backoff_multiplier.powi(self.metrics.current_retry_count as i32);

        let delay_secs = delay.min(self.config.retry_config.max_delay_secs as f64) as u64;
        std::time::Duration::from_secs(delay_secs)
    }

    /// Mark as initialized
    pub fn set_initialized(&mut self, initialized: bool) {
        self.is_initialized = initialized;
    }

    /// Check if initialized
    pub fn is_initialized(&self) -> bool {
        self.is_initialized
    }

    /// Get metrics reference
    pub fn get_metrics(&self) -> &SinkMetrics {
        &self.metrics
    }
}

/// Registry for available event sinks
pub struct SinkRegistry {
    sinks: HashMap<String, Box<dyn EventSink>>,
}

impl SinkRegistry {
    /// Create a new sink registry
    pub fn new() -> Self {
        Self {
            sinks: HashMap::new(),
        }
    }

    /// Register a sink with a name
    pub fn register_sink(&mut self, name: String, sink: Box<dyn EventSink>) {
        info!("Registering sink: {}", name);
        self.sinks.insert(name, sink);
    }

    /// Get a sink by name
    pub fn get_sink(&self, name: &str) -> Option<&(dyn EventSink + '_)> {
        self.sinks.get(name).map(|sink| sink.as_ref())
    }

    
    /// Get all sink names
    pub fn sink_names(&self) -> Vec<String> {
        self.sinks.keys().cloned().collect()
    }

    /// Send event to all registered sinks
    pub async fn send_to_all(&mut self, event: &FormattedEvent) -> Vec<(String, Result<()>)> {
        let mut results = Vec::new();

        for (name, sink) in &mut self.sinks {
            debug!("Sending event to sink: {}", name);
            let result = sink.send_event(event).await;
            results.push((name.clone(), result));
        }

        results
    }

    /// Health check all sinks
    pub async fn health_check_all(&self) -> HashMap<String, bool> {
        let mut health_status = HashMap::new();

        for (name, sink) in &self.sinks {
            let healthy = sink.health_check().await.unwrap_or(false);
            health_status.insert(name.clone(), healthy);
        }

        health_status
    }

    /// Get metrics from all sinks
    pub fn get_all_metrics(&self) -> HashMap<String, &SinkMetrics> {
        let mut all_metrics = HashMap::new();

        for (name, sink) in &self.sinks {
            all_metrics.insert(name.clone(), sink.get_metrics());
        }

        all_metrics
    }

    /// Shutdown all sinks
    pub async fn shutdown_all(&mut self) -> Vec<(String, Result<()>)> {
        let mut results = Vec::new();

        for (name, sink) in &mut self.sinks {
            info!("Shutting down sink: {}", name);
            let result = sink.shutdown().await;
            results.push((name.clone(), result));
        }

        results
    }
}

impl Default for SinkRegistry {
    fn default() -> Self {
        Self::new()
    }
}