# HTTP Event Sink Implementation Plan

## 1. Introduction

This document provides a detailed implementation plan for adding HTTP event sink functionality to the PostgreSQL replication checker. The goal is to create a modular, configurable HTTP event sink that can send replication events to an HTTP endpoint without affecting the main replication processing.

## 2. Implementation Steps

### 2.1 Phase 1: Foundation

**Task 1: Create HTTP Event Sink Module**
- Create `http_sink.rs` file
- Define `EventSink` trait
- Implement `HttpEventSink` struct
- Create `HttpSinkConfig` struct
- Implement `EventProcessor` struct
- Create `HttpClient` struct
- Define `HttpEvent` struct

**Task 2: Add EventSink Trait to ReplicationServer**
- Add `EventSink` trait to `ReplicationServer`
- Add method to register event sinks
- Modify `process_replication_message` to notify event sinks

**Task 3: Update ReplicationConfig**
- Add HTTP sink configuration options to `ReplicationConfig`
- Implement validation for HTTP sink configuration

### 2.2 Phase 2: Event Processing

**Task 4: Implement Event Processor**
- Implement `process_event` method for different message types
- Create mapping from `ReplicationMessage` to `HttpEvent`
- Implement `format_payload` method for HTTP transmission

**Task 5: Implement HTTP Client**
- Choose HTTP client library (reqwest, hyper, etc.)
- Implement `send_request` method with retry logic
- Add support for different HTTP methods
- Implement authentication header support

### 2.3 Phase 3: Integration

**Task 6: Integrate with ReplicationServer**
- Register `HttpEventSink` with `ReplicationServer`
- Pass configuration from `ReplicationConfig` to `HttpEventSink`
- Ensure event processing is non-blocking

**Task 7: Add Configuration Parsing**
- Add HTTP sink configuration to environment variable parsing
- Implement fallback values for missing configuration
- Add validation for HTTP endpoint URL

### 2.4 Phase 4: Error Handling

**Task 8: Implement Error Handling**
- Add error handling for event processing
- Implement retry logic for HTTP requests
- Add logging for successful and failed events
- Implement graceful degradation for HTTP sink failures

### 2.5 Phase 5: Testing

**Task 9: Unit Tests**
- Write unit tests for `EventProcessor`
- Write unit tests for `HttpClient`
- Write unit tests for `HttpEventSink`

**Task 10: Integration Tests**
- Write integration tests for full event processing flow
- Test with different HTTP endpoints
- Test error handling and retry logic

**Task 11: Performance Testing**
- Test performance impact on main replication processing
- Measure latency of HTTP event processing
- Test with high event volume

## 3. Detailed Implementation Plan

### 3.1 HTTP Event Sink Module

```rust
// http_sink.rs

use crate::types::*;
use crate::errors::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub trait EventSink: Send + Sync {
    fn on_event(&self, message: ReplicationMessage) -> Result<()>;
}

pub struct HttpEventSink {
    config: HttpSinkConfig,
    event_processor: EventProcessor,
    http_client: HttpClient,
}

impl HttpEventSink {
    pub fn new(config: HttpSinkConfig) -> Self {
        HttpEventSink {
            config,
            event_processor: EventProcessor::new(),
            http_client: HttpClient::new(config.clone()),
        }
    }
}

impl EventSink for HttpEventSink {
    fn on_event(&self, message: ReplicationMessage) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let event = self.event_processor.process_event(message)?;
        let payload = self.event_processor.format_payload(&event);

        let headers = if let Some(auth_header) = &self.config.auth_header {
            let mut headers = HashMap::new();
            headers.insert("Authorization", auth_header.clone());
            Some(headers)
        } else {
            None
        };

        self.http_client.send_request(
            &self.config.url,
            &self.config.method,
            payload,
            headers,
        )?;

        Ok(())
    }
}

pub struct HttpSinkConfig {
    pub enabled: bool,
    pub url: String,
    pub method: String,
    pub timeout: u64,
    pub retries: u32,
    pub auth_header: Option<String>,
    pub event_format: String,
}

pub struct EventProcessor {
    // Implementation details
}

impl EventProcessor {
    pub fn new() -> Self {
        EventProcessor {
            // Initialize
        }
    }

    pub fn process_event(&self, message: ReplicationMessage) -> Result<HttpEvent> {
        // Implementation
    }

    pub fn format_payload(&self, event: &HttpEvent) -> String {
        // Implementation
    }
}

pub struct HttpClient {
    config: HttpSinkConfig,
}

impl HttpClient {
    pub fn new(config: HttpSinkConfig) -> Self {
        HttpClient { config }
    }

    pub async fn send_request(
        &self,
        url: &str,
        method: &str,
        payload: String,
        headers: Option<HashMap<String, String>>,
    ) -> Result<String> {
        // Implementation with retry logic
    }
}

pub struct HttpEvent {
    pub event_type: String,
    pub relation_id: Oid,
    pub table_name: String,
    pub data: String,
    pub timestamp: i64,
    pub xid: Option<Xid>,
}
```

### 3.2 ReplicationServer Integration

```rust
// server.rs

use crate::http_sink::{EventSink, HttpEventSink};
use crate::types::ReplicationConfig;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct ReplicationServer {
    connection: PGConnection,
    config: ReplicationConfig,
    state: ReplicationState,
    event_sinks: Arc<Mutex<Vec<Box<dyn EventSink>>>>,
}

impl ReplicationServer {
    pub fn new(config: ReplicationConfig) -> Result<Self> {
        // Existing implementation

        let event_sinks = Arc::new(Mutex::new(Vec::new()));

        // Initialize HTTP event sink if enabled
        if config.http_sink_enabled {
            let http_config = HttpSinkConfig {
                enabled: config.http_sink_enabled,
                url: config.http_sink_url,
                method: config.http_sink_method,
                timeout: config.http_sink_timeout,
                retries: config.http_sink_retries,
                auth_header: config.http_sink_auth_header,
                event_format: config.http_sink_event_format,
            };
            let http_sink = HttpEventSink::new(http_config);
            event_sinks.lock().await.push(Box::new(http_sink));
        }

        Ok(Self {
            connection,
            config,
            state,
            event_sinks,
        })
    }

    pub fn process_replication_message(&mut self, message: ReplicationMessage) -> Result<()> {
        // Existing implementation

        // Notify event sinks
        for sink in self.event_sinks.lock().await.iter() {
            let _ = sink.on_event(message.clone());
        }

        Ok(())
    }
}
```

### 3.3 Configuration Updates

```rust
// types.rs

pub struct ReplicationConfig {
    pub connection_string: String,
    pub publication_name: String,
    pub slot_name: String,
    pub feedback_interval_secs: u64,

    // HTTP sink configuration
    pub http_sink_enabled: bool,
    pub http_sink_url: String,
    pub http_sink_method: String,
    pub http_sink_timeout: u64,
    pub http_sink_retries: u32,
    pub http_sink_auth_header: Option<String>,
    pub http_sink_event_format: String,
}

impl ReplicationConfig {
    pub fn new(
        connection_string: String,
        publication_name: String,
        slot_name: String,
    ) -> crate::errors::Result<Self> {
        // Existing validation

        // HTTP sink configuration with defaults
        let http_sink_enabled = env::var("HTTP_SINK_ENABLED")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        let http_sink_url = env::var("HTTP_SINK_URL")
            .unwrap_or_else(|_| "http://localhost:8080/events".to_string());

        let http_sink_method = env::var("HTTP_SINK_METHOD")
            .unwrap_or_else(|_| "POST".to_string());

        let http_sink_timeout = env::var("HTTP_SINK_TIMEOUT")
            .map(|v| v.parse().unwrap_or(5))
            .unwrap_or(5);

        let http_sink_retries = env::var("HTTP_SINK_RETRIES")
            .map(|v| v.parse().unwrap_or(3))
            .unwrap_or(3);

        let http_sink_auth_header = env::var("HTTP_SINK_AUTH_HEADER").ok();

        let http_sink_event_format = env::var("HTTP_SINK_EVENT_FORMAT")
            .unwrap_or_else(|_| "json".to_string());

        Ok(Self {
            connection_string,
            publication_name,
            slot_name,
            feedback_interval_secs: 1,
            http_sink_enabled,
            http_sink_url,
            http_sink_method,
            http_sink_timeout,
            http_sink_retries,
            http_sink_auth_header,
            http_sink_event_format,
        })
    }
}
```

## 4. Testing Strategy

### 4.1 Unit Tests

- Test `EventProcessor` with different `ReplicationMessage` types
- Test `HttpClient` with different HTTP methods and status codes
- Test `HttpEventSink` with enabled/disabled configuration
- Test error handling and retry logic

### 4.2 Integration Tests

- Test full event processing flow from replication message to HTTP request
- Test with different HTTP endpoints and authentication
- Test performance impact on main replication processing
- Test error handling and graceful degradation

### 4.3 Performance Testing

- Measure latency of HTTP event processing
- Test with high event volume
- Test impact on main replication processing
- Test retry logic under network failures

## 5. Deployment Considerations

- HTTP event sink should be disabled by default
- Provide clear documentation for configuration options
- Include examples of HTTP endpoint implementations
- Provide monitoring and logging guidance
- Include performance considerations in documentation

## 6. Security Considerations

- Validate HTTP endpoint URL to prevent injection attacks
- Handle authentication securely (avoid hardcoding credentials)
- Implement rate limiting to prevent abuse
- Validate event data before sending to HTTP endpoint
- Consider HTTPS for secure communication

## 7. Backward Compatibility

- HTTP event sink will be optional and disabled by default
- No changes to existing replication functionality
- Configuration-driven activation of the feature
- Graceful degradation if HTTP sink fails

This implementation plan provides a comprehensive roadmap for adding HTTP event sink functionality to the PostgreSQL replication checker while maintaining the system's reliability and performance.