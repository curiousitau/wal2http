# Modular Event Sink System Implementation Plan

## 1. Introduction

This document outlines the implementation plan for creating a modular event sink system in the `wal2http` project. The goal is to:

1. Define a common event sink trait/interface
2. Allow multiple event sink implementations (HTTP, Hook0, etc.)
3. Maintain backward compatibility
4. Keep core replication logic separate from event sinks

## 2. High-Level Architecture

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
  +get_relation(oid: Oid) -> Option<&RelationInfo>
}

class EventSink {
  <<interface>>
  +on_event(message: ReplicationMessage) -> Result<()>
}

class EventSinkConfig {
  <<enum>>
  Http(HttpSinkConfig)
  Hook0(Hook0SinkConfig)
}

class HttpSinkConfig {
  +enabled: bool
  +url: String
  +method: String
  +timeout: u64
  +retries: u32
  +auth_header: Option<String>
  +event_format: String
}

class Hook0SinkConfig {
  +enabled: bool
  +api_url: String
  +api_key: String
  +workspace_id: String
  +project_id: String
  +event_format: String
}

class HttpEventSink {
  +config: HttpSinkConfig
  +event_processor: EventProcessor
  +http_client: HttpClient
  +on_event(message: ReplicationMessage) -> Result<()>
}

class Hook0EventSink {
  +config: Hook0SinkConfig
  +event_processor: EventProcessor
  +hook0_client: Hook0Client
  +on_event(message: ReplicationMessage) -> Result<()>
}

class EventProcessor {
  +process_event(message: ReplicationMessage) -> Result<Event>
  +format_payload(event: Event) -> String
}

class HttpClient {
  +send_request(url: String, method: String, payload: String, headers: Option<HashMap<String, String>>) -> Result<String>
}

class Hook0Client {
  +send_event(event: Event, config: &Hook0SinkConfig) -> Result<String>
}

class ReplicationMessage {
  <<enumeration>>
  Begin { final_lsn: u64, timestamp: i64, xid: Xid }
  Commit { flags: u8, commit_lsn: u64, end_lsn: u64, timestamp: i64 }
  Relation { relation: RelationInfo }
  Insert { relation_id: Oid, tuple_data: TupleData, is_stream: bool, xid: Option<Xid> }
  Update { relation_id: Oid, key_type: Option<char>, old_tuple_data: Option<TupleData>, new_tuple_data: TupleData, is_stream: bool, xid: Option<Xid> }
  Delete { relation_id: Oid, key_type: char, tuple_data: TupleData, is_stream: bool, xid: Option<Xid> }
  Truncate { relation_ids: Vec<Oid>, flags: i8, is_stream: bool, xid: Option<Xid> }
  StreamStart { xid: Xid, first_segment: bool }
  StreamStop
  StreamCommit { xid: Xid, flags: u8, commit_lsn: u64, end_lsn: u64, timestamp: i64 }
  StreamAbort { xid: Xid, subtransaction_xid: Xid }
}

ReplicationServer --> ReplicationConfig : uses
ReplicationServer --> ReplicationState : manages
ReplicationServer --> EventSink : registers multiple
HttpEventSink --|> EventSink : implements
Hook0EventSink --|> EventSink : implements
EventSinkConfig --> HttpSinkConfig : contains
EventSinkConfig --> Hook0SinkConfig : contains
EventProcessor --> ReplicationState : accesses
EventProcessor --> Event : creates
HttpClient --> ReplicationMessage : processes
Hook0Client --> ReplicationMessage : processes
```

## 3. Component Descriptions

### 3.1 ReplicationServer

The core component that handles PostgreSQL replication. It will be modified to:
- Support multiple event sinks via the `EventSink` trait
- Register event sinks during initialization
- Notify all registered event sinks of replication events

### 3.2 EventSink Trait

A common interface that all event sinks must implement:
```rust
pub trait EventSink: Send + Sync {
    fn on_event(&self, message: ReplicationMessage) -> Result<()>;
}
```

### 3.3 EventSinkConfig Enum

A configuration enum that supports multiple event sink types:
```rust
pub enum EventSinkConfig {
    Http(HttpSinkConfig),
    Hook0(Hook0SinkConfig),
}
```

### 3.4 HttpEventSink

Handles sending replication events to an HTTP endpoint:
- Implements the `EventSink` trait
- Uses `HttpClient` for HTTP communication
- Processes events via `EventProcessor`

### 3.5 Hook0EventSink

Handles sending replication events to Hook0 API:
- Implements the `EventSink` trait
- Uses `Hook0Client` for API communication
- Processes events via `EventProcessor`

### 3.6 EventProcessor

A shared component that processes replication messages into a common event format:
- Maps `ReplicationMessage` to a generic `Event` struct
- Formats events for different sink types
- Provides a common interface for event processing

### 3.7 HttpClient and Hook0Client

Separate clients for HTTP and Hook0 API communication:
- `HttpClient`: Handles HTTP requests with configurable method, timeout, retries, etc.
- `Hook0Client`: Handles Hook0 API requests with authentication and event formatting

## 4. Planned File Structure Changes

```
src/
├── event_sinks/
│   ├── mod.rs
│   ├── event_processor.rs
│   ├── http_sink.rs
│   ├── hook0_sink.rs
│   └── traits.rs
├── config.rs
├── server.rs
└── types.rs
```

## 5. Implementation Steps

1. **Define EventSink Trait**: Create a common interface for all event sinks
2. **Create EventProcessor**: Extract and generalize event processing logic
3. **Implement HttpEventSink**: Create HTTP event sink implementation
4. **Implement Hook0EventSink**: Create Hook0 event sink implementation
5. **Update ReplicationServer**: Modify to support multiple event sinks
6. **Update Configuration**: Extend `ReplicationConfig` to support multiple sinks
7. **Add Dependency for Hook0**: Add `hook0-client` crate to `Cargo.toml`
8. **Create Tests**: Add unit and integration tests for the new architecture

## 6. Configuration Changes

Update the configuration system to support multiple event sinks:
```toml
# Example configuration
[event_sinks]
http = { url = "http://localhost:3000/events", enabled = true }
hook0 = {
    api_url = "https://api.hook0.com",
    api_key = "sk_test_...",
    workspace_id = "ws_...",
    project_id = "proj_...",
    enabled = true
}
```

## 7. Dependency Updates

Add the `hook0-client` crate to `Cargo.toml`:
```toml
[dependencies]
hook0-client = "0.12"
```

## 8. Backward Compatibility

- HTTP event sink will remain the default implementation
- Existing HTTP configuration will be supported via the new configuration format
- Event sinks are optional and disabled by default
- Core replication logic remains unchanged

## 9. Testing Plan

1. **Unit Tests**: Test individual event sink implementations
2. **Integration Tests**: Test multiple event sinks working together
3. **Performance Tests**: Measure impact on replication performance
4. **Configuration Tests**: Test various configuration scenarios

## 10. Documentation

- Update `README.md` with new configuration options
- Add documentation for Hook0 event sink
- Provide examples of multiple event sink configurations