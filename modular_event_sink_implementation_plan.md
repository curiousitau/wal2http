# Modular Event Sink System Implementation Plan

## 1. Introduction

This document outlines the implementation plan for creating a modular event sink system in the `wal2http` project.

**Status: ✅ COMPLETED**

The modular event sink system has been successfully implemented with the following achievements:

1. ✅ Defined a common event sink trait/interface
2. ✅ Implemented multiple event sink types (HTTP, Hook0, STDOUT)
3. ✅ Maintained backward compatibility
4. ✅ Kept core replication logic separate from event sinks
5. ✅ Added email notification capabilities
6. ✅ Implemented comprehensive error handling and retry logic

## 2. Implemented Architecture

```mermaid
classDiagram
class ReplicationServer {
  +config: ReplicationConfig
  +state: ReplicationState
  +event_sink: Option<Arc<dyn EventSink + Send + Sync>>
  +process_replication_message(message: ReplicationMessage) -> Result<()>
  +new(config: ReplicationConfig) -> Result<()>
}

class ReplicationConfig {
  +connection_string: String
  +publication_name: String
  +slot_name: String
  +feedback_interval_secs: u64
  +http_endpoint_url: Option<String>
  +hook0_api_url: Option<String>
  +hook0_application_id: Option<Uuid>
  +hook0_api_token: Option<String>
}

class ReplicationState {
  +relations: HashMap<Oid, RelationInfo>
  +received_lsn: u64
  +flushed_lsn: u64
  +applied_lsn: u64
  +last_feedback_time: Instant
  +get_relation(oid: Oid) -> Option<&RelationInfo>
  +update_applied_lsn(lsn: u64)
}

class EventSink {
  <<interface>>
  +send_event(event: &ReplicationMessage) -> Result<()>
}

class EmailConfig {
  +smtp_host: String
  +smtp_port: u16
  +smtp_username: String
  +smtp_password: String
  +from_email: String
  +to_email: String
}

class HttpEventSinkConfig {
  +endpoint_url: String
}

class Hook0EventSinkConfig {
  +api_url: String
  +application_id: Uuid
  +api_token: String
}

class HttpEventSink {
  +config: HttpEventSinkConfig
  +http_client: Arc<Mutex<Client>>
  +email_config: Option<EmailConfig>
  +send_event(event: &ReplicationMessage) -> Result<()>
  +send_email_notification(message: &str)
}

class Hook0EventSink {
  +config: Hook0EventSinkConfig
  +send_event(event: &ReplicationMessage) -> Result<()>
}

class StdoutEventSink {
  +send_event(event: &ReplicationMessage) -> Result<()>
}

class EventFormatter {
  +format(event: &ReplicationMessage) -> serde_json::Value
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
ReplicationServer --> EventSink : selects one
HttpEventSink --|> EventSink : implements
Hook0EventSink --|> EventSink : implements
StdoutEventSink --|> EventSink : implements
HttpEventSink --> EmailConfig : uses for notifications
HttpEventSink --> EventFormatter : uses
Hook0EventSink --> EventFormatter : uses
StdoutEventSink --> EventFormatter : uses
```

## 3. Implemented Components

### 3.1 ReplicationServer ✅

The core component that handles PostgreSQL replication. Successfully modified to:
- Support single event sink selection via the `EventSink` trait
- Select appropriate sink based on configuration priority
- Process events sequentially and asynchronously
- Track applied LSN based on sink success/failure

### 3.2 EventSink Trait ✅

A common async interface that all event sinks implement:
```rust
#[async_trait]
pub trait EventSink: Send + Sync {
    async fn send_event(&self, event: &ReplicationMessage) -> ReplicationResult<()>;
}
```

### 3.3 ReplicationState Enhanced ✅

Enhanced with applied LSN tracking:
- Added `applied_lsn` field for successful event delivery tracking
- Only updated after successful sink delivery
- Prevents data loss during sink failures

### 3.4 HttpEventSink ✅

Handles sending replication events to HTTP endpoints:
- Implements the `EventSink` trait with async support
- Uses `reqwest::Client` for HTTP communication
- Implements retry logic with exponential backoff (5 retries, 1s-30s)
- Sends email notifications when all retries are exhausted
- Processes events via `EventFormatter`

### 3.5 Hook0EventSink ✅

Handles sending replication events to Hook0 API:
- Implements the `EventSink` trait with async support
- Uses `hook0-client` library for API communication
- Handles custom event table schema decoding
- Simplifies metadata and labels processing
- Processes events via `EventFormatter`

### 3.6 StdoutEventSink ✅

Default fallback sink for development and debugging:
- Implements the `EventSink` trait
- Prints formatted events to console
- Always available as ultimate fallback

### 3.7 EventFormatter ✅

A shared component that formats replication messages into JSON:
- Converts all `ReplicationMessage` variants to JSON
- Handles PostgreSQL type conversions
- Maintains event sequence and metadata
- Used by all sink implementations

### 3.8 EmailConfig ✅

Configuration for email notifications:
- Loads SMTP settings from environment variables
- Validates email configuration
- Used by HttpEventSink for failure notifications

## 4. Implemented File Structure

```
src/
├── event_sink/
│   ├── mod.rs                    # EventSink trait definition
│   ├── event_formatter.rs        # JSON event formatting
│   ├── http.rs                   # HTTP event sink implementation
│   ├── hook0.rs                  # Hook0 event sink implementation
│   ├── hook0_error.rs            # Hook0-specific error handling
│   ├── pg_type_conversion.rs     # PostgreSQL type conversions
│   └── stdout.rs                 # STDOUT event sink implementation
├── email_config.rs               # Email notification configuration
├── types.rs                      # Enhanced with event sink config
├── server.rs                     # Updated with sink selection logic
├── main.rs                       # Environment variable configuration
└── buffer.rs, errors.rs, parser.rs, utils.rs  # Updated for integration
```

## 5. Completed Implementation Steps

✅ **1. Define EventSink Trait**: Created async trait interface for all event sinks
✅ **2. Create EventFormatter**: Implemented JSON formatting for all replication messages
✅ **3. Implement HttpEventSink**: Created HTTP event sink with retry logic and email notifications
✅ **4. Implement Hook0EventSink**: Created Hook0 event sink with API integration
✅ **5. Implement StdoutEventSink**: Created fallback STDOUT sink
✅ **6. Update ReplicationServer**: Modified to support single sink selection with applied LSN tracking
✅ **7. Update Configuration**: Extended `ReplicationConfig` with environment variable support
✅ **8. Add Dependencies**: Added `reqwest`, `lettre`, `async-trait`, `uuid`, `hook0-client`
✅ **9. Create Test Infrastructure**: Added test servers and scripts for validation

## 6. Implemented Configuration System

Environment variable-based configuration for sink selection:

**Required Configuration:**
```bash
DATABASE_URL=postgresql://user:password@localhost:5432/dbname
SLOT_NAME=replication_slot_name
PUB_NAME=publication_name
```

**HTTP Sink Configuration:**
```bash
HTTP_ENDPOINT_URL=http://localhost:3000/events
```

**Hook0 Sink Configuration:**
```bash
HOOK0_API_URL=https://api.hook0.com
HOOK0_APPLICATION_ID=550e8400-e29b-41d4-a716-446655440000
HOOK0_API_TOKEN=sk_test_...
```

**Email Notification Configuration:**
```bash
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USERNAME=your-email@gmail.com
EMAIL_SMTP_PASSWORD=your-app-password
EMAIL_FROM=your-email@gmail.com
EMAIL_TO=admin@yourcompany.com
```

**Sink Selection Priority:**
1. Hook0 (if all Hook0 variables configured)
2. HTTP (if HTTP_ENDPOINT_URL configured)
3. STDOUT (default fallback)

## 7. Implemented Dependencies

Added to `Cargo.toml`:
```toml
[dependencies]
reqwest = { version = "0.12.23", features = ["json", "stream"] }
lettre = { version = "0.11", features = ["smtp-transport", "builder"] }
async-trait = "0.1.88"
uuid = { version = "1.18.0", features = ["v4"] }
hook0-client = { rev = "d7642e4ed32e9851eb67114053afcbd9cf9ab614", git = "https://github.com/hook0/hook0", features = ["producer"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

## 8. Backward Compatibility ✅

- All event sinks are optional and disabled by default
- Core replication logic remains unchanged when sinks not configured
- STDOUT sink always available as fallback
- Environment variable configuration allows gradual adoption
- No breaking changes to existing replication functionality

## 9. Implemented Testing Infrastructure ✅

**Test Files Added:**
- `test_http_server.js` - Node.js HTTP server for testing HTTP sink
- `test_hook0_server.js` - Mock Hook0 API server for testing
- `test_email_server.js` - Email server for testing notifications
- `test_http_sink.sh` - Automated test script for HTTP sink
- `test_hook0_sink.sh` - Automated test script for Hook0 sink

**Testing Capabilities:**
1. **End-to-End Testing**: Complete event flow validation
2. **Sink-Specific Testing**: Individual sink validation
3. **Error Scenario Testing**: Failure handling and retry validation
4. **Integration Testing**: Sink selection and configuration testing

## 10. Documentation Status ✅

**Documentation Files Created/Updated:**
- `README.md` - Updated with comprehensive architecture and sequence diagrams
- `http_event_sink_architecture.md` - Detailed architecture documentation
- `modular_event_sink_implementation_plan.md` - Implementation status and details
- Additional design documents for planning and reference

**Documentation Includes:**
- Environment variable configuration examples
- Sink selection priority explanation
- Error handling and retry logic details
- Performance and security considerations
- Testing infrastructure setup instructions

## 11. Summary

The modular event sink system has been **successfully implemented** with the following key achievements:

### ✅ Core Features Delivered
- **Async EventSink Trait**: Common interface for all sink implementations
- **Multiple Sink Types**: HTTP, Hook0, and STDOUT sinks fully implemented
- **Sink Selection Logic**: Automatic selection based on configuration priority
- **Applied LSN Tracking**: Reliable event delivery tracking
- **Retry Logic**: Exponential backoff with email notifications
- **Email Notifications**: Configurable failure alerts

### ✅ Architecture Benefits
- **Modular Design**: Easy to add new sink types
- **Backward Compatible**: No breaking changes to existing functionality
- **Graceful Degradation**: Replication continues even if sinks fail
- **Configuration-Driven**: Environment variable based configuration
- **Production Ready**: Comprehensive error handling and logging

### ✅ Testing & Documentation
- **Complete Test Infrastructure**: Test servers and scripts for validation
- **Comprehensive Documentation**: Architecture, implementation, and usage guides
- **Examples**: Configuration examples and setup instructions

The implementation provides a robust, extensible foundation for event processing in the wal2http PostgreSQL replication system while maintaining reliability and performance.