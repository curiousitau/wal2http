# Event Sink Refactoring Plan

## 1. Current Architecture

The current architecture has a single HTTP event sink that's tightly coupled with the replication server:

```mermaid
sequenceDiagram
participant PG as PostgreSQL Server
participant RC as ReplicationChecker
participant HS as HTTP Event Sink

Note right of RC: Main replication processing

PG->>RC: Send replication data
RC->>RC: Parse replication message
RC->>HS: Notify event (if enabled)
HS->>HS: Process event
HS->>HS: Send HTTP request
RC->>RC: Continue processing
```

## 2. Proposed Architecture

The refactored architecture will introduce a modular event sink system:

```mermaid
classDiagram
class ReplicationServer {
  +config: ReplicationConfig
  +state: ReplicationState
  +event_sinks: Vec<Box<dyn EventSink>>
  +process_replication_message(message: ReplicationMessage) -> Result<()>
  +register_event_sink(sink: Box<dyn EventSink>) -> Result<()>
}

class ReplicationConfig {
  +connection_string: String
  +publication_name: String
  +slot_name: String
  +feedback_interval_secs: u64
  +event_sinks: Vec<EventSinkConfig>
}

class ReplicationState {
  +relations: HashMap<Oid, RelationInfo>
  +received_lsn: u64
  +flushed_lsn: u64
  +last_feedback_time: Instant
}

class EventSink {
  <<interface>>
  +on_event(message: ReplicationMessage) -> Result<()>
}

class HttpEventSink {
  +config: HttpSinkConfig
  +event_processor: EventProcessor
  +http_client: HttpClient
  +on_event(message: ReplicationMessage) -> Result<()>
}

class Hook0EventSink {
  +config: Hook0SinkConfig
  +hook0_client: Hook0Client
  +on_event(message: ReplicationMessage) -> Result<()>
}

class EventSinkConfig {
  <<enum>>
  Http(HttpSinkConfig)
  Hook0(Hook0SinkConfig)
}

class HttpSinkConfig {
  +url: String
  +method: String
  +timeout: u64
  +retries: u32
  +auth_header: Option<String>
  +event_format: String
}

class Hook0SinkConfig {
  +api_url: String
  +api_key: String
  +workspace_id: String
  +project_id: String
  +event_format: String
}

ReplicationServer --> ReplicationConfig : uses
ReplicationServer --> ReplicationState : manages
ReplicationServer --> EventSink : registers multiple
HttpEventSink --|> EventSink : implements
Hook0EventSink --|> EventSink : implements
EventSinkConfig --> HttpSinkConfig : contains
EventSinkConfig --> Hook0SinkConfig : contains
```

## 3. Implementation Plan

### Phase 1: Define Event Sink Interface

1. **Create EventSink trait**
   - Define a common interface for all event sinks
   - Include `on_event` method that takes a `ReplicationMessage`

2. **Update ReplicationServer**
   - Add support for multiple event sinks
   - Store event sinks as `Vec<Box<dyn EventSink>>`
   - Modify `process_replication_message` to notify all registered sinks

### Phase 2: Refactor HTTP Event Sink

1. **Extract Event Processor**
   - Move event processing logic to a separate struct
   - Make it reusable for other event sinks

2. **Implement HttpEventSink**
   - Implement the `EventSink` trait
   - Use the existing HTTP client and configuration

### Phase 3: Implement Hook0 Event Sink

1. **Add hook0-client dependency**
   - Add the Rust crate to Cargo.toml

2. **Create Hook0EventSink**
   - Implement the `EventSink` trait
   - Use hook0-client for API calls
   - Map replication messages to hook0 events

### Phase 4: Update Configuration

1. **Extend ReplicationConfig**
   - Add support for multiple event sink configurations
   - Create `EventSinkConfig` enum

2. **Update configuration parsing**
   - Support both HTTP and Hook0 sink configurations
   - Maintain backward compatibility

### Phase 5: Testing

1. **Unit tests**
   - Test individual event sink implementations
   - Test event processing and formatting

2. **Integration tests**
   - Test multiple event sinks working together
   - Test configuration parsing and validation

## 4. File Structure Changes

```
src/
├── event_sinks/          # New directory for event sink implementations
│   ├── mod.rs            # EventSink trait and common utilities
│   ├── http_sink.rs     # HTTP event sink implementation
│   ├── hook0_sink.rs    # Hook0 event sink implementation
│   └── event_processor.rs # Common event processing logic
├── server.rs            # Updated ReplicationServer with event sink support
├── config.rs            # Updated configuration handling
└── types.rs             # EventSink trait and related types
```

## 5. Configuration Changes

### Current Configuration
```toml
# Current configuration
connection_string = "postgresql://user:pass@localhost/db"
publication_name = "my_publication"
slot_name = "my_slot"
http_endpoint_url = "http://localhost:3000/events"
```

### New Configuration
```toml
# New configuration with multiple event sinks
connection_string = "postgresql://user:pass@localhost/db"
publication_name = "my_publication"
slot_name = "my_slot"

[event_sinks]
http = { url = "http://localhost:3000/events" }
hook0 = {
    api_url = "https://api.hook0.com"
    api_key = "sk_test_..."
    workspace_id = "ws_..."
    project_id = "proj_..."
}
```

## 6. Dependency Updates

Add `hook0-client` crate to Cargo.toml:
```toml
[dependencies]
hook0-client = "0.1"  # Add appropriate version
```

## 7. Backward Compatibility

- Maintain support for the existing `http_endpoint_url` configuration
- Provide migration path for users to update to the new configuration format
- Ensure existing HTTP event sink functionality remains unchanged

## 8. Implementation Steps

1. Define the `EventSink` trait and common types
2. Extract event processing logic to a common module
3. Implement `HttpEventSink` that implements the `EventSink` trait
4. Add `hook0-client` dependency and implement `Hook0EventSink`
5. Update `ReplicationServer` to support multiple event sinks
6. Update configuration parsing to support multiple event sinks
7. Add comprehensive tests for the new architecture
8. Document the changes and provide migration guide
```

<line_count>357</line_count>
</write_to_file>