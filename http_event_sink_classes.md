classDiagram
class ReplicationServer {
  +config: ReplicationConfig
  +state: ReplicationState
  +process_replication_message(message: ReplicationMessage) -> Result<()>
  +register_event_sink(sink: Box<dyn EventSink>) -> Result<()>
}

class ReplicationConfig {
  +connection_string: String
  +publication_name: String
  +slot_name: String
  +feedback_interval_secs: u64
  +http_sink_enabled: bool
  +http_sink_url: String
  +http_sink_method: String
  +http_sink_timeout: u64
  +http_sink_retries: u32
  +http_sink_auth_header: Option<String>
  +http_sink_event_format: String
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

class HttpEventSink {
  +config: HttpSinkConfig
  +event_processor: EventProcessor
  +http_client: HttpClient
  +on_event(message: ReplicationMessage) -> Result<()>
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

class EventProcessor {
  +process_event(message: ReplicationMessage) -> Result<HttpEvent>
  +format_payload(event: HttpEvent) -> String
}

class HttpEvent {
  +event_type: String
  +relation_id: Oid
  +table_name: String
  +data: String
  +timestamp: i64
  +xid: Option<Xid>
}

class HttpClient {
  +send_request(url: String, method: String, payload: String, headers: Option<HashMap<String, String>>) -> Result<String>
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
ReplicationServer --> EventSink : registers
HttpEventSink --|> EventSink : implements
HttpEventSink --> HttpSinkConfig : uses
HttpEventSink --> EventProcessor : uses
HttpEventSink --> HttpClient : uses
EventProcessor --> ReplicationState : accesses
EventProcessor --> HttpEvent : creates
HttpClient --> ReplicationMessage : processes

class RelationInfo {
  +oid: Oid
  +namespace: String
  +relation_name: String
  +replica_identity: char
  +column_count: i16
  +columns: Vec<ColumnInfo>
}

class ColumnInfo {
  +key_flag: i8
  +column_name: String
  +column_type: Oid
  +atttypmod: i32
}

class TupleData {
  +column_count: i16
  +columns: Vec<ColumnData>
  +processed_length: usize
}

class ColumnData {
  +data_type: char
  +length: i32
  +data: String
}