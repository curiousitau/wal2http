# HTTP Event Sink Architecture for PostgreSQL Replication Checker

## 1. Introduction

This document outlines the architecture for adding HTTP event sink functionality to the PostgreSQL replication checker. The goal is to create a modular, configurable HTTP event sink that can send replication events to an HTTP endpoint without affecting the main replication processing.

## 2. Architecture Overview

The HTTP event sink will be implemented as a separate module that can be optionally enabled via configuration. It will listen for replication events and send them to a configurable HTTP endpoint.

```
[Replication Server]
    |
    +-- [ReplicationState]
    |
    +-- [HTTP Event Sink] (optional)
         |
         +-- [Event Processor]
         |
         +-- [HTTP Client]
```

## 3. Key Components

### 3.1 HTTP Event Sink Module

The core module that handles event processing and HTTP communication.

**Responsibilities:**
- Receive replication events from the main server
- Transform events into HTTP-friendly format
- Send events to configured HTTP endpoint
- Handle errors gracefully
- Provide configuration options

**Integration Points:**
- Registers with `ReplicationServer` to receive events
- Uses `ReplicationState` to access relation information

### 3.2 Event Processor

Handles the transformation of replication messages into HTTP event payloads.

**Responsibilities:**
- Map `ReplicationMessage` variants to HTTP event formats
- Handle different message types (INSERT, UPDATE, DELETE, etc.)
- Format data for HTTP transmission
- Maintain event sequence and ordering

**Key Methods:**
- `process_event(message: ReplicationMessage) -> HttpEvent`
- `format_payload(event: HttpEvent) -> String`

### 3.3 HTTP Client

Handles the actual HTTP communication.

**Responsibilities:**
- Send HTTP requests to configured endpoint
- Handle timeouts and retries
- Support different HTTP methods (POST, PUT)
- Handle authentication if needed
- Provide connection pooling

**Dependencies:**
- `reqwest` or similar HTTP client library
- Configurable timeout settings
- Retry logic with exponential backoff

## 4. Event Processing Flow

1. **Event Reception**: The HTTP event sink receives replication events from the main server
2. **Event Processing**: The event processor transforms the event into an HTTP-friendly format
3. **HTTP Transmission**: The HTTP client sends the event to the configured endpoint
4. **Error Handling**: Any errors during processing or transmission are logged and handled gracefully
5. **Feedback**: Success/failure status is returned to the main server

## 5. Configuration Options

The HTTP event sink will be configurable via environment variables or configuration file:

- `HTTP_SINK_ENABLED`: Boolean to enable/disable the sink
- `HTTP_SINK_URL`: URL endpoint to send events to
- `HTTP_SINK_METHOD`: HTTP method (POST, PUT)
- `HTTP_SINK_TIMEOUT`: Request timeout in seconds
- `HTTP_SINK_RETRIES`: Number of retry attempts
- `HTTP_SINK_AUTH_HEADER`: Authentication header for the HTTP request
- `HTTP_SINK_EVENT_FORMAT`: Format of the event payload (JSON, XML)

## 6. Error Handling Strategy

The HTTP event sink will implement a robust error handling strategy:

1. **Transient Errors**: Network timeouts, temporary server unavailability
   - Implement retry logic with exponential backoff
   - Use a dedicated error queue for transient failures

2. **Permanent Errors**: Invalid configuration, authentication failures
   - Log errors and continue processing
   - Provide detailed error messages for debugging

3. **Graceful Degradation**: If HTTP sink fails, main replication processing continues
   - Failures in HTTP sink don't affect the main replication stream
   - Events can be buffered and retried later

## 7. Integration Points with Existing Codebase

The HTTP event sink will integrate with the existing codebase at specific points:

1. **ReplicationServer**: Register the event sink to receive events
   - Add method to register event listeners
   - Modify `process_replication_message` to notify event sink

2. **ReplicationState**: Access relation information for event formatting
   - Use existing methods to get relation details

3. **Configuration**: Add HTTP sink configuration to `ReplicationConfig`
   - Parse HTTP-specific configuration parameters

## 8. Implementation Plan

1. Create `http_sink.rs` module with event processor and HTTP client
2. Add event sink registration to `ReplicationServer`
3. Implement event processing and HTTP transmission
4. Add configuration parsing for HTTP sink
5. Implement error handling and retry logic
6. Add tests for HTTP event sink functionality

## 9. Security Considerations

- Validate HTTP endpoint URL to prevent injection attacks
- Handle authentication securely (avoid hardcoding credentials)
- Implement rate limiting to prevent abuse
- Validate event data before sending to HTTP endpoint

## 10. Performance Considerations

- Non-blocking HTTP requests to avoid impacting main replication processing
- Connection pooling for efficient HTTP communication
- Configurable batching of events for reduced network overhead
- Asynchronous processing to prevent bottlenecks

## 11. Monitoring and Logging

- Log successful event transmissions
- Log failed transmissions with retry attempts
- Provide metrics for event processing (success rate, latency)
- Configurable log level for HTTP sink operations

## 12. Backward Compatibility

- HTTP event sink will be optional and disabled by default
- No changes to existing replication functionality
- Configuration-driven activation of the feature
- Graceful degradation if HTTP sink fails

This architecture provides a solid foundation for implementing HTTP event sink functionality in the PostgreSQL replication checker while maintaining the system's reliability and performance.