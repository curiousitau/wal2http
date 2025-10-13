# Event Sink Implementation Plan

## Phase 1: Define Event Sink Interface

### Task 1: Create EventSink Trait
- Define a common interface for all event sinks
- Include `on_event` method that takes a `ReplicationMessage`
- Add to `src/event_sinks/mod.rs`

### Task 2: Update ReplicationServer
- Add support for multiple event sinks
- Store event sinks as `Vec<Box<dyn EventSink>>`
- Modify `process_replication_message` to notify all registered sinks
- Update `src/server.rs`

### Task 3: Create Common Event Processor
- Extract event processing logic from HTTP sink
- Create reusable `EventProcessor` struct
- Add to `src/event_sinks/event_processor.rs`

## Phase 2: Refactor HTTP Event Sink

### Task 4: Implement HttpEventSink
- Create new `HttpEventSink` struct that implements `EventSink` trait
- Move HTTP-specific logic from current implementation
- Add to `src/event_sinks/http_sink.rs`

### Task 5: Update HTTP Configuration
- Create `HttpSinkConfig` struct
- Update configuration parsing to use new format
- Maintain backward compatibility with existing `http_endpoint_url`

## Phase 3: Implement Hook0 Event Sink

### Task 6: Add hook0-client Dependency
- Add `hook0-client` crate to Cargo.toml
- Add appropriate version

### Task 7: Create Hook0EventSink
- Create new `Hook0EventSink` struct that implements `EventSink` trait
- Implement hook0 API integration
- Add to `src/event_sinks/hook0_sink.rs`

### Task 8: Create Hook0 Configuration
- Create `Hook0SinkConfig` struct
- Add configuration parsing for hook0 settings

## Phase 4: Update Configuration

### Task 9: Extend ReplicationConfig
- Add support for multiple event sink configurations
- Create `EventSinkConfig` enum
- Update `src/config.rs`

### Task 10: Update Configuration Parsing
- Support both HTTP and Hook0 sink configurations
- Maintain backward compatibility
- Update environment variable parsing

## Phase 5: Testing

### Task 11: Unit Tests
- Test individual event sink implementations
- Test event processing and formatting
- Add tests to `src/event_sinks/tests.rs`

### Task 12: Integration Tests
- Test multiple event sinks working together
- Test configuration parsing and validation
- Add integration tests

### Task 13: Performance Testing
- Test performance impact of multiple event sinks
- Measure latency of event processing
- Test with high event volume

## Phase 6: Documentation

### Task 14: Update Documentation
- Document the new event sink architecture
- Provide examples of configuration for both HTTP and Hook0 sinks
- Update `README.md` and other documentation files

### Task 15: Create Migration Guide
- Provide migration path for users to update to new configuration format
- Document backward compatibility considerations
- Add to `docs/migration_guide.md`

## Phase 7: Review and Final Touches

### Task 16: Code Review
- Review all changes for code quality and consistency
- Ensure proper error handling and logging
- Verify backward compatibility

### Task 17: Final Testing
- Run full test suite to ensure all tests pass
- Verify backward compatibility
- Test with real-world scenarios

### Task 18: Clean Up
- Remove any deprecated code or configuration options
- Clean up any remaining code
- Ensure all tests pass