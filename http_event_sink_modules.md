```mermaid
classDiagram fef
direction LR

class src {
}

src --> main.rs
src --> buffer.rs
src --> errors.rs
src --> parser.rs
src --> server.rs
src --> types.rs
src --> utils.rs
src --> http_sink.rs : new

class main.rs {
  +Args : struct
  +main() -> Result<()>
  +run_replication_server(config: ReplicationConfig) -> Result<()>
}

class buffer.rs {
  +BufferReader : struct
  +BufferWriter : struct
}

class errors.rs {
  +ReplicationError : enum
  +Result : type alias
}

class parser.rs {
  +MessageParser : struct
  +parse_wal_message(buffer: &[u8]) -> Result<ReplicationMessage>
}

class server.rs {
  +ReplicationServer : struct
  +new(config: ReplicationConfig) -> Result<Self>
  +identify_system() -> Result<()>
  +create_replication_slot_and_start() -> Result<()>
  +start_replication() -> Result<()>
  +replication_loop() -> Result<()>
  +process_keepalive_message(data: &[u8]) -> Result<()>
  +process_wal_message(data: &[u8]) -> Result<()>
  +process_replication_message(message: ReplicationMessage) -> Result<()>
  +send_feedback() -> Result<()>
  +check_and_send_feedback() -> Result<()>
}

class types.rs {
  +ReplicationConfig : struct
  +ReplicationState : struct
  +ReplicationMessage : enum
  +RelationInfo : struct
  +ColumnInfo : struct
  +TupleData : struct
  +ColumnData : struct
}

class utils.rs {
  +PGConnection : struct
  +Oid : type alias
  +Xid : type alias
}

class http_sink.rs : new {
  +HttpEventSink : struct
  +HttpSinkConfig : struct
  +EventProcessor : struct
  +HttpClient : struct
  +HttpEvent : struct
  +EventSink : trait
  +process_event(message: ReplicationMessage) -> Result<HttpEvent>
  +format_payload(event: HttpEvent) -> String
  +send_request(url: String, method: String, payload: String, headers: Option<HashMap<String, String>>) -> Result<String>
}

main.rs --> server.rs : uses
server.rs --> types.rs : uses
server.rs --> parser.rs : uses
server.rs --> buffer.rs : uses
server.rs --> errors.rs : uses
server.rs --> http_sink.rs : uses (optional)

types.rs --> http_sink.rs : uses
parser.rs --> http_sink.rs : uses (optional)
errors.rs --> http_sink.rs : uses
http_sink.rs --> types.rs : uses
http_sink.rs --> errors.rs : uses
http_sink.rs --> buffer.rs : uses
http_sink.rs --> utils.rs : uses
```