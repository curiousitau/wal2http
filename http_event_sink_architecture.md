# HTTP Event Sink Architecture for PostgreSQL Replication Checker

## 1. Introduction

This document outlines the architecture for adding HTTP event sink functionality to the PostgreSQL replication checker. The goal is to create a modular, configurable HTTP event sink that can send replication events to an HTTP endpoint without affecting the main replication processing.

## 2. Architecture Overview

The event sink system is implemented as a modular, trait-based architecture that supports multiple sink types. The system includes HTTP, Hook0, STDOUT, and email notification capabilities.

```
[Replication Server]
    |
    +-- [ReplicationState]
    |
    +-- [Event Sink] (trait-based, configurable)
         |
         +-- [HTTP Event Sink] (optional)
         |    |
         |    +-- [Event Formatter]
         |    |
         |    +-- [HTTP Client]
         |    |
         |    +-- [Email Notifications]
         |
         +-- [Hook0 Event Sink] (optional)
         |    |
         |    +-- [Hook0 Client]
         |    |
         |    +-- [Error Handler]
         |
         +-- [STDOUT Event Sink] (default/fallback)
```

## 3. Key Components

### 3.1 Event Sink Trait

The core trait that defines the interface for all event sink implementations.

**Responsibilities:**
- Define common interface for event processing
- Support async event sending
- Handle different sink implementations uniformly

**Key Method:**
- `send_event(&self, event: &ReplicationMessage) -> ReplicationResult<()>`

### 3.2 HTTP Event Sink

Handles HTTP endpoint communication with email notifications for failures.

**Responsibilities:**
- Transform events into JSON format using EventFormatter
- Send events to configured HTTP endpoint via POST requests
- Implement retry logic with exponential backoff (5 retries, 1s to 30s delays)
- Send email notifications when HTTP requests fail after retries
- Handle authentication and timeouts

**Dependencies:**
- `reqwest` HTTP client library
- `lettre` email library for notifications
- Configurable email settings via environment variables

### 3.3 Hook0 Event Sink

Specialized sink for Hook0 webhook service integration.

**Responsibilities:**
- Transform events for Hook0 API format
- Handle Hook0-specific authentication and metadata
- Process custom event table schema decoding
- Simplify metadata and labels processing
- Handle Hook0 API errors gracefully

**Dependencies:**
- `hook0-client` library
- Hook0 API configuration (URL, application ID, token)

### 3.4 STDOUT Event Sink

Default fallback sink for debugging and development.

**Responsibilities:**
- Print formatted events to console
- Provide immediate feedback during development
- Serve as fallback when other sinks are not configured

### 3.5 Event Formatter

Transforms replication messages into standardized JSON format.

**Responsibilities:**
- Convert all ReplicationMessage variants to JSON
- Handle PostgreSQL type conversions
- Maintain event sequence and metadata
- Support different message types (BEGIN, COMMIT, INSERT, UPDATE, DELETE, TRUNCATE, etc.)

### 3.6 Email Configuration

Provides email notification capabilities for sink failures.

**Responsibilities:**
- Load SMTP configuration from environment variables
- Validate email settings
- Send failure notifications when sinks fail

**Environment Variables:**
- `EMAIL_SMTP_HOST`, `EMAIL_SMTP_PORT`
- `EMAIL_SMTP_USERNAME`, `EMAIL_SMTP_PASSWORD`
- `EMAIL_FROM`, `EMAIL_TO`

## 4. Event Processing Flow

1. **Sink Selection**: Based on configuration, the appropriate event sink is selected:
   - Hook0 sink if Hook0 configuration is provided
   - HTTP sink if HTTP_ENDPOINT_URL is configured
   - STDOUT sink as fallback/default

2. **Event Reception**: The selected event sink receives replication events from the main server
3. **Event Processing**: Events are processed sequentially and asynchronously:
   - Events are formatted to JSON using EventFormatter
   - PostgreSQL types are converted to JSON-compatible formats
   - Event ordering is maintained

4. **Event Transmission**: The sink sends the processed event:
   - HTTP Sink: POST JSON to configured endpoint with retries
   - Hook0 Sink: Send to Hook0 API with proper authentication
   - STDOUT Sink: Print formatted event to console

5. **Error Handling & Notifications**:
   - HTTP/HTTP0 failures trigger retry logic with exponential backoff
   - After max retries, email notifications are sent (if configured)
   - Errors are logged but don't stop replication processing
   - Applied LSN is only updated after successful event delivery

6. **Feedback**: Success/failure status affects LSN tracking:
   - Successful delivery updates applied_lsn in ReplicationState
   - Failures prevent LSN advancement until resolved

## 5. Configuration Options

The event sink system is configurable via environment variables:

**Required Configuration:**
- `DATABASE_URL`: PostgreSQL connection string
- `SLOT_NAME`: Replication slot name (default: "sub")
- `PUB_NAME`: Publication name (default: "pub")

**HTTP Sink Configuration:**
- `HTTP_ENDPOINT_URL`: URL endpoint to send events to (enables HTTP sink)

**Hook0 Sink Configuration:**
- `HOOK0_API_URL`: Hook0 API endpoint URL
- `HOOK0_APPLICATION_ID`: Hook0 application ID (UUID format)
- `HOOK0_API_TOKEN`: Hook0 API authentication token

**Email Notification Configuration:**
- `EMAIL_SMTP_HOST`: SMTP server hostname
- `EMAIL_SMTP_PORT`: SMTP server port
- `EMAIL_SMTP_USERNAME`: SMTP authentication username
- `EMAIL_SMTP_PASSWORD`: SMTP authentication password
- `EMAIL_FROM`: From email address for notifications
- `EMAIL_TO`: To email address for notifications

**Sink Selection Priority:**
1. Hook0 sink (if all Hook0 vars are configured)
2. HTTP sink (if HTTP_ENDPOINT_URL is configured)
3. STDOUT sink (default fallback)

## 6. Error Handling Strategy

The event sink system implements a robust error handling strategy:

1. **Retry Logic with Exponential Backoff**:
   - Maximum 5 retry attempts
   - Base delay: 1 second, maximum delay: 30 seconds
   - Exponential backoff: delay doubles after each attempt (up to max)
   - Applied to both HTTP and Hook0 sinks

2. **Email Notifications**:
   - Triggered after maximum retry attempts are exhausted
   - Provides detailed error information including failure reason
   - Only sent if email configuration is properly set up
   - Gracefully handled if email configuration is missing

3. **Graceful Degradation**:
   - Sink failures don't stop the main replication processing
   - Applied LSN is only updated after successful event delivery
   - Replication continues even if sinks are temporarily unavailable
   - STDOUT sink always available as ultimate fallback

4. **Error Types**:
   - **Transient Errors**: Network timeouts, temporary server unavailability (retried)
   - **Permanent Errors**: Invalid configuration, authentication failures (logged, not retried)
   - **Sink-Specific Errors**: HTTP status errors, Hook0 API errors (retried with limits)

5. **Logging**:
   - Comprehensive logging at debug and error levels
   - Detailed retry attempt information
   - Success/failure tracking with sink identification

## 7. Integration Points with Existing Codebase

The event sink system integrates with the existing codebase at specific points:

1. **ReplicationServer**: Manages sink selection and event delivery
   - Sink selection based on configuration in constructor
   - Sequential async event processing in `process_replication_message`
   - Applied LSN tracking based on sink success/failure

2. **ReplicationState**: Enhanced with LSN tracking
   - Added `applied_lsn` field for tracking successful event delivery
   - Only updated after successful sink delivery
   - Prevents data loss during sink failures

3. **Configuration**: Integrated into `ReplicationConfig`
   - HTTP endpoint URL: `http_endpoint_url`
   - Hook0 configuration: `hook0_api_url`, `hook0_application_id`, `hook0_api_token`
   - Configuration validation in `ReplicationConfig::new()`

4. **Main Entry Point**: Environment variable loading
   - Configuration loaded from environment variables in `main()`
   - Graceful handling of missing optional configurations
   - Informative logging of configuration status

## 8. Implementation Status

**Completed Components:**

1. ✅ **Event Sink Trait** (`src/event_sink.rs`)
   - Async trait definition with `send_event` method
   - Support for multiple sink implementations

2. ✅ **HTTP Event Sink** (`src/event_sink/http.rs`)
   - JSON event formatting and HTTP POST requests
   - Retry logic with exponential backoff (5 retries, 1s-30s)
   - Email notifications for failed deliveries
   - Integration with email configuration

3. ✅ **Hook0 Event Sink** (`src/event_sink/hook0.rs`)
   - Hook0 API integration with authentication
   - Custom event schema handling
   - Simplified metadata and labels processing
   - Hook0-specific error handling

4. ✅ **STDOUT Event Sink** (`src/event_sink/stdout.rs`)
   - Console output for debugging and development
   - Fallback sink when others are not configured

5. ✅ **Event Formatter** (`src/event_sink/event_formatter.rs`)
   - JSON serialization of all replication message types
   - PostgreSQL type conversions
   - Comprehensive event metadata

6. ✅ **Email Configuration** (`src/email_config.rs`)
   - SMTP configuration from environment variables
   - Validation and error handling

7. ✅ **Server Integration** (`src/server.rs`)
   - Sink selection and initialization
   - Sequential async event processing
   - Applied LSN tracking

8. ✅ **Main Configuration** (`src/main.rs`, `src/types.rs`)
   - Environment variable loading
   - Configuration validation
   - Sink selection logic

## 9. Security Considerations

- **URL Validation**: HTTP/Hook0 endpoint URLs are validated to ensure they start with http:// or https://
- **Configuration Security**: All sensitive data (tokens, passwords) loaded from environment variables, not hardcoded
- **Input Validation**: Configuration values validated for format and constraints (UUIDs, URLs, port numbers)
- **Error Information**: Sensitive information filtered from error logs and notifications

## 10. Performance Considerations

- **Sequential Processing**: Events processed sequentially to maintain ordering and prevent overwhelming sinks
- **Async Operations**: All sink operations are async to prevent blocking replication processing
- **Retry Limits**: Configurable retry limits prevent infinite loops and resource exhaustion
- **Connection Management**: HTTP client reused across requests for efficiency

## 11. Monitoring and Logging

- **Comprehensive Logging**: Debug, info, and error level logging for all sink operations
- **Success Tracking**: Successful event transmissions logged with LSN information
- **Failure Details**: Failed transmissions logged with retry attempts and error details
- **Configuration Logging**: Sink selection and configuration status logged at startup

## 12. Testing Infrastructure

- **Test Servers**: JavaScript test servers for HTTP and Hook0 endpoint testing
- **Test Scripts**: Shell scripts for automated sink testing
- **Email Test Server**: Node.js server for testing email notifications
- **Integration Tests**: Test files for validating end-to-end functionality

## 13. Backward Compatibility

- **Optional Sinks**: All event sinks are optional and disabled by default
- **Configuration-Driven**: No changes to existing replication functionality when sinks not configured
- **Graceful Degradation**: Replication continues even if sinks fail or are misconfigured
- **STDOUT Fallback**: Always available fallback sink for debugging

This architecture provides a robust, modular foundation for event sink functionality in the PostgreSQL replication checker while maintaining the system's reliability, security, and performance.