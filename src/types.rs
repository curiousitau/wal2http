//! Data structures for PostgreSQL logical replication
//!
//! This module contains the core data types used throughout the wal2http application.
//! These types represent:
//! - Database schema information (tables, columns)
//! - Row data and changes (inserts, updates, deletes)
//! - Replication protocol messages
//! - Configuration and state management
//!
//! All types are designed to be serializable to JSON for easy integration with web services
//! and other external systems.

use crate::{
    buffer::{BufferReader, BufferWriter},
    errors::ReplicationError,
    utils::{Oid, Xid},
};
use serde::Serialize;           // For JSON serialization
use std::collections::HashMap;  // For storing relation information by OID

/// Information about a table column
///
/// This structure represents metadata about a column in a PostgreSQL table.
/// It's used to understand the structure of data being replicated.
///
/// # Fields
///
/// * `key_flag` - Indicates if this column is part of the primary key (-1 for key, 0 for non-key)
/// * `column_name` - The name of the column as defined in the database
/// * `column_type` - PostgreSQL Object ID (OID) representing the column's data type
/// * `atttypmod` - Type modifier containing additional information (like varchar length)
#[derive(Debug, Clone, Serialize)]
pub struct ColumnInfo {
    pub key_flag: i8,
    pub column_name: String,
    pub column_type: Oid,
    pub atttypmod: i32,
}

/// Information about a relation (table)
///
/// This structure represents metadata about a PostgreSQL table (relation) that is being
/// replicated. It contains the schema information needed to understand and interpret
/// the data changes flowing through the replication stream.
///
/// # Fields
///
/// * `oid` - PostgreSQL Object ID (OID) that uniquely identifies this table
/// * `namespace` - The schema name where this table resides
/// * `relation_name` - The name of the table
/// * `replica_identity` - How row changes are identified ('d' for default, 'f' for full, 'i' for index)
/// * `column_count` - Number of columns in the table
/// * `columns` - Vector of column information structures
#[derive(Debug, Clone, Serialize)]
pub struct RelationInfo {
    pub oid: Oid,
    pub namespace: String,
    pub relation_name: String,
    pub replica_identity: char,
    pub column_count: i16,
    pub columns: Vec<ColumnInfo>,
}

/// Data for a single column in a tuple (row)
///
/// This structure represents the actual data value for a single column in a row
/// that has been changed. It includes type information and the value itself.
///
/// # Fields
///
/// * `data_type` - Type indicator character: 'n' for NULL, 't' for text, 'u' for unchanged TOAST
/// * `length` - Length of the data in bytes (0 for NULL values)
/// * `data` - The actual data as a string (empty for NULL values)
#[derive(Debug, Clone, Serialize)]
pub struct ColumnData {
    pub data_type: char,
    pub length: i32,
    pub data: String,
}

/// Data for a complete row/tuple
///
/// This structure represents all the column data for a single row (tuple) in the database.
/// It's used for INSERT operations and the NEW version of UPDATE operations.
///
/// # Fields
///
/// * `column_count` - Number of columns in this tuple
/// * `columns` - Vector of column data structures
/// * `processed_length` - Total bytes processed when parsing this tuple from the wire protocol
#[derive(Debug, Clone, Serialize)]
pub struct TupleData {
    pub column_count: i16,
    pub columns: Vec<ColumnData>,
    pub processed_length: usize,
}

/// Types of logical replication messages
///
/// This enum represents all possible message types that can be received from PostgreSQL's
/// logical replication protocol. Each variant represents a different type of database
/// change or control message.
///
/// ## Message Flow
///
/// A typical transaction flows as:
/// 1. `Begin` - Transaction starts
/// 2. `Relation` - Table schema information (once per table)
/// 3. `Insert`/`Update`/`Delete` - Data changes
/// 4. `Commit` - Transaction completes
///
/// For streaming transactions (large transactions):
/// - `StreamStart` begins the streaming
/// - Changes are sent as they occur
/// - `StreamCommit` or `StreamAbort` ends the streaming
#[derive(Debug, Clone, Serialize)]
pub enum ReplicationMessage {
    /// Transaction start message
    ///
    /// Marks the beginning of a new transaction. All subsequent messages
    /// belong to this transaction until a Commit message is received.
    Begin {
        final_lsn: u64,
        timestamp: i64,
        xid: Xid,
    },

    /// Transaction commit message
    ///
    /// Marks the successful completion of a transaction. All changes
    /// in this transaction are now durable and visible.
    Commit {
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        timestamp: i64,
    },

    /// Table schema information message
    ///
    /// Provides metadata about a table that will be referenced by
    /// subsequent Insert/Update/Delete messages. Sent once per table
    /// when first referenced in a replication session.
    Relation {
        relation: RelationInfo,
    },

    /// Row insertion message
    ///
    /// Represents a new row being inserted into a table.
    Insert {
        relation_id: Oid,
        tuple_data: TupleData,
        is_stream: bool,
        xid: Option<Xid>,
    },

    /// Row update message
    ///
    /// Represents an existing row being modified. May include both
    /// old and new versions of the data depending on replica identity settings.
    Update {
        relation_id: Oid,
        key_type: Option<char>,
        old_tuple_data: Option<TupleData>,
        new_tuple_data: TupleData,
        is_stream: bool,
        xid: Option<Xid>,
    },

    /// Row deletion message
    ///
    /// Represents a row being deleted from a table. Includes either
    /// the full old tuple or just the replica identity key.
    Delete {
        relation_id: Oid,
        key_type: char,
        tuple_data: TupleData,
        is_stream: bool,
        xid: Option<Xid>,
    },

    /// Table truncate message
    ///
    /// Represents all rows being deleted from one or more tables.
    /// This is more efficient than sending individual Delete messages.
    Truncate {
        relation_ids: Vec<Oid>,
        flags: i8,
        is_stream: bool,
        xid: Option<Xid>,
    },

    /// Start of streaming transaction message
    ///
    /// Marks the beginning of a large transaction that will be streamed
    /// incrementally rather than waiting for completion.
    StreamStart {
        xid: Xid,
        first_segment: bool,
    },

    /// End of streaming segment message
    ///
    /// Marks the end of the current streaming segment. More segments
    /// may follow for the same transaction.
    StreamStop,

    /// Streaming transaction commit message
    ///
    /// Marks the successful completion of a streaming transaction.
    StreamCommit {
        xid: Xid,
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        timestamp: i64,
    },

    /// Streaming transaction abort message
    ///
    /// Indicates that a streaming transaction was rolled back.
    StreamAbort {
        xid: Xid,
        subtransaction_xid: Xid,
    },
}

/// State for managing logical replication
///
/// Tracks the current state of the replication connection, including schema information,
/// LSN positions, and feedback timing. This is used to maintain replication consistency
/// and provide proper feedback to the PostgreSQL server.
#[derive(Debug)]
pub struct ReplicationState {
    /// Table schema information indexed by table OID
    pub relations: HashMap<Oid, RelationInfo>,
    /// Highest LSN received from the server
    pub received_lsn: u64,
    /// Highest LSN flushed to disk (currently unused)
    #[allow(unused)]
    pub flushed_lsn: u64,
    /// When we last sent feedback to the server
    pub last_feedback_time: std::time::Instant,
    /// Highest LSN successfully processed by event sink
    pub applied_lsn: u64,
}

impl ReplicationState {
    /// Creates a new replication state with default values
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
            received_lsn: 0,
            flushed_lsn: 0,
            last_feedback_time: std::time::Instant::now(),
            applied_lsn: 0,
        }
    }

    /// Stores table schema information for later use
    pub fn add_relation(&mut self, relation: RelationInfo) {
        self.relations.insert(relation.oid, relation);
    }

    /// Retrieves table schema information by OID
    pub fn get_relation(&self, oid: Oid) -> Option<&RelationInfo> {
        self.relations.get(&oid)
    }

    /// Updates the received LSN if the new value is higher
    pub fn update_lsn(&mut self, lsn: u64) {
        if lsn > 0 {
            self.received_lsn = std::cmp::max(self.received_lsn, lsn);
        }
    }

    /// Updates the applied LSN if the new value is higher
    pub fn update_applied_lsn(&mut self, lsn: u64) {
        if lsn > 0 {
            self.applied_lsn = std::cmp::max(self.applied_lsn, lsn);
        }
    }
}

impl ReplicationMessage {
    /// Get the message type as a string for logging and tracing
    pub fn message_type(&self) -> &'static str {
        match self {
            ReplicationMessage::Begin { .. } => "Begin",
            ReplicationMessage::Commit { .. } => "Commit",
            ReplicationMessage::Relation { .. } => "Relation",
            ReplicationMessage::Insert { .. } => "Insert",
            ReplicationMessage::Update { .. } => "Update",
            ReplicationMessage::Delete { .. } => "Delete",
            ReplicationMessage::Truncate { .. } => "Truncate",
            ReplicationMessage::StreamStart { .. } => "StreamStart",
            ReplicationMessage::StreamStop => "StreamStop",
            ReplicationMessage::StreamCommit { .. } => "StreamCommit",
            ReplicationMessage::StreamAbort { .. } => "StreamAbort",
        }
    }
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

use uuid::Uuid;

/// Configuration for the replication checker with validation
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub connection_string: String,
    pub publication_name: String,
    pub slot_name: String,
    pub feedback_interval_secs: u64,
    pub event_sink: Option<String>,
    pub http_endpoint_url: Option<String>,
    pub hook0_api_url: Option<String>,
    pub hook0_application_id: Option<Uuid>,
    pub hook0_api_token: Option<String>,
}

impl ReplicationConfig {
    /// Create a new ReplicationConfig with validation
    pub fn new(
        connection_string: String,
        publication_name: String,
        slot_name: String,
        event_sink: Option<String>,
        http_endpoint_url: Option<String>,
        hook0_api_url: Option<String>,
        hook0_application_id: Option<Uuid>,
        hook0_api_token: Option<String>,
    ) -> crate::errors::ReplicationResult<Self> {
        // Basic validation
        if connection_string.trim().is_empty() {
            return Err(crate::errors::ReplicationError::config(
                "Connection string cannot be empty",
            ));
        }

        if publication_name.trim().is_empty() {
            return Err(crate::errors::ReplicationError::config(
                "Publication name cannot be empty",
            ));
        }

        if slot_name.trim().is_empty() {
            return Err(crate::errors::ReplicationError::config(
                "Slot name cannot be empty",
            ));
        }

        // Validate slot name format (PostgreSQL naming rules)
        if !slot_name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return Err(crate::errors::ReplicationError::config(
                "Slot name can only contain alphanumeric characters and underscores",
            ));
        }

        if slot_name.len() > 63 {
            // PostgreSQL identifier length limit
            return Err(crate::errors::ReplicationError::config(
                "Slot name cannot be longer than 63 characters",
            ));
        }

        // Validate HTTP endpoint URL if provided
        if let Some(ref url) = http_endpoint_url
            && !url.trim().is_empty()
            && !url.starts_with("http://")
            && !url.starts_with("https://")
        {
            return Err(crate::errors::ReplicationError::config(
                "HTTP endpoint URL must start with http:// or https://",
            ));
        }

        // Validate event sink if provided
        if let Some(ref service) = event_sink {
            let service_lower = service.to_lowercase();
            if !service_lower.is_empty()
                && service_lower != "http"
                && service_lower != "hook0"
                && service_lower != "stdout" {
                return Err(crate::errors::ReplicationError::config(
                    "Event sink must be one of: 'http', 'hook0', or 'stdout'",
                ));
            }
        }

        // Validate Hook0 configuration if provided
        if let Some(ref url) = hook0_api_url
            && !url.trim().is_empty()
            && !url.starts_with("http://")
            && !url.starts_with("https://")
        {
            return Err(crate::errors::ReplicationError::config(
                "Hook0 API URL must start with http:// or https://",
            ));
        }

        if let (Some(ref app_id), Some(ref token)) = (hook0_application_id, hook0_api_token.clone())
        {
            if app_id.to_string().trim().is_empty() {
                return Err(crate::errors::ReplicationError::config(
                    "Hook0 application ID cannot be empty",
                ));
            }
            if token.trim().is_empty() {
                return Err(crate::errors::ReplicationError::config(
                    "Hook0 API token cannot be empty",
                ));
            }
        }

        Ok(Self {
            connection_string,
            publication_name,
            slot_name,
            feedback_interval_secs: 1, // Send feedback every second
            event_sink,
            http_endpoint_url,
            hook0_api_url,
            hook0_application_id,
            hook0_api_token,
        })
    }
}

/*



XLogData (B)

    Byte1('w')

        Identifies the message as WAL data.
    Int64

        The starting point of the WAL data in this message.
    Int64

        The current end of WAL on the server.
    Int64

        The server's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    Byten

        A section of the WAL data stream.

        A single WAL record is never split across two XLogData messages. When a WAL record crosses a WAL page boundary, and is therefore already split using continuation records, it can be split at the page boundary. In other words, the first main WAL record and its continuation records can be sent in different XLogData messages.

Primary keepalive message (B)

    Byte1('k')

        Identifies the message as a sender keepalive.
    Int64

        The current end of WAL on the server.
    Int64

        The server's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    Byte1

        1 means that the client should reply to this message as soon as possible, to avoid a timeout disconnect. 0 otherwise.

The receiving process can send replies back to the sender at any time, using one of the following message formats (also in the payload of a CopyData message):

Standby status update (F)

    Byte1('r')

        Identifies the message as a receiver status update.
    Int64

        The location of the last WAL byte + 1 received and written to disk in the standby.
    Int64

        The location of the last WAL byte + 1 flushed to disk in the standby.
    Int64

        The location of the last WAL byte + 1 applied in the standby.
    Int64

        The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    Byte1

        If 1, the client requests the server to reply to this message immediately. This can be used to ping the server, to test if the connection is still healthy.

Hot standby feedback message (F)

    Byte1('h')

        Identifies the message as a hot standby feedback message.
    Int64

        The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    Int32

        The standby's current global xmin, excluding the catalog_xmin from any replication slots. If both this value and the following catalog_xmin are 0, this is treated as a notification that hot standby feedback will no longer be sent on this connection. Later non-zero messages may reinitiate the feedback mechanism.
    Int32

        The epoch of the global xmin xid on the standby.
    Int32

        The lowest catalog_xmin of any replication slots on the standby. Set to 0 if no catalog_xmin exists on the standby or if hot standby feedback is being disabled.
    Int32

        The epoch of the catalog_xmin xid on the standby.


*/
// https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-PRIMARY-KEEPALIVE-MESSAGE
pub struct KeepaliveMessage {
    pub message_type: char,
    pub log_pos: u64,
    pub timestamp: u64,
    pub reply_requested: bool,
}

pub struct XLogDataMessage {
    pub message_type: char,
    pub data_start: u64,
    pub wal_end: u64,
    pub send_time: u64,
    pub data: Vec<u8>,
}

pub struct StandbyStatusUpdateMessage {
    pub message_type: char,
    pub reply_requested: u8,
    pub last_lsn: u64,
    pub flush_lsn: u64,
    pub apply_lsn: u64,
    pub send_time: u64,
}

pub struct HotStandbyFeedbackMessage {
    pub message_type: char,
    pub send_time: u64,
    pub xmin: u32,
    pub epoch: u32,
    pub catalog_xmin: u32,
    pub catalog_epoch: u32,
}

trait FromBufferReader {
    fn parse(data: &[u8]) -> Result<Self, ReplicationError>
    where
        Self: Sized;
}

trait ToBufferWriter {
    fn write(&self, writer: &mut BufferWriter) -> Result<(), ReplicationError>;
}

// https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-XLOGDATA-MESSAGE
impl FromBufferReader for XLogDataMessage {
    fn parse(data: &[u8]) -> Result<Self, ReplicationError> {
        if data.len() < 25 {
            return Err(ReplicationError::protocol("WAL message too short"));
        }

        let mut reader = BufferReader::new(data);
        let message_type = reader.read_char()?;
        let data_start = reader.read_u64()?;
        let wal_end = reader.read_u64()?;
        let send_time = reader.read_u64()?;
        let data = reader.read_bytes(reader.remaining())?;

        Ok(XLogDataMessage {
            message_type,
            data_start,
            wal_end,
            send_time,
            data,
        })
    }
}

// https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-STANDBY-STATUS-UPDATE
impl FromBufferReader for StandbyStatusUpdateMessage {
    fn parse(data: &[u8]) -> Result<Self, ReplicationError> {
        if data.len() < 33 {
            return Err(ReplicationError::protocol(
                "Status update message too short",
            ));
        }

        let mut reader = BufferReader::new(data);
        let message_type = reader.read_char()?;
        let last_lsn = reader.read_u64()?;
        let flush_lsn = reader.read_u64()?;
        let apply_lsn = reader.read_u64()?;

        // The send_time is the last field in the message
        let send_time = reader.read_u64()?;
        let reply_requested = reader.read_u8()?;

        Ok(StandbyStatusUpdateMessage {
            message_type,
            last_lsn,
            flush_lsn,
            apply_lsn,
            send_time,
            reply_requested,
        })
    }
}

// https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-HOT-STANDBY-FEEDBACK-MESSAGE
impl FromBufferReader for HotStandbyFeedbackMessage {
    fn parse(data: &[u8]) -> Result<Self, ReplicationError> {
        if data.len() < 25 {
            return Err(ReplicationError::protocol(
                "Hot standby feedback message too short",
            ));
        }

        let mut reader = BufferReader::new(data);
        let message_type = reader.read_char()?;
        let send_time = reader.read_u64()?;
        let xmin = reader.read_u32()?;
        let epoch = reader.read_u32()?;
        let catalog_xmin = reader.read_u32()?;

        // The catalog_epoch is the last field in the message
        let catalog_epoch = reader.read_u32()?;

        Ok(HotStandbyFeedbackMessage {
            message_type,
            send_time,
            xmin,
            epoch,
            catalog_xmin,
            catalog_epoch,
        })
    }
}

impl ToBufferWriter for KeepaliveMessage {
    fn write(&self, writer: &mut BufferWriter) -> Result<(), ReplicationError> {
        writer.write_char(self.message_type)?;
        writer.write_u64(self.log_pos)?;
        writer.write_u64(self.timestamp)?;
        writer.write_u8(if self.reply_requested { 1 } else { 0 })?;
        Ok(())
    }
}

impl ToBufferWriter for StandbyStatusUpdateMessage {
    fn write(&self, writer: &mut BufferWriter) -> Result<(), ReplicationError> {
        writer.write_u8(self.message_type as u8)?;
        writer.write_u64(self.last_lsn)?;
        writer.write_u64(self.flush_lsn)?;
        writer.write_u64(self.apply_lsn)?;
        writer.write_u64(self.send_time)?;
        writer.write_u8(self.reply_requested)?;
        Ok(())
    }
}

impl ToBufferWriter for HotStandbyFeedbackMessage {
    fn write(&self, writer: &mut BufferWriter) -> Result<(), ReplicationError> {
        writer.write_char(self.message_type)?;
        writer.write_u64(self.send_time)?;
        writer.write_u32(self.xmin)?;
        writer.write_u32(self.epoch)?;
        writer.write_u32(self.catalog_xmin)?;
        writer.write_u32(self.catalog_epoch)?;
        Ok(())
    }
}

impl TryFrom<BufferReader<'_>> for KeepaliveMessage {
    type Error = ReplicationError;

    fn try_from(reader: BufferReader<'_>) -> Result<Self, Self::Error> {
        if !reader.has_bytes(18) {
            return Err(ReplicationError::protocol("Keepalive message too short"));
        }

        let mut reader = reader;

        let message_type = reader.read_char()?;
        let log_pos = reader.read_u64()?;
        let timestamp = reader.read_u64()?;
        let reply_requested = reader.read_u8()? != 0;

        Ok(KeepaliveMessage {
            message_type,
            log_pos,
            timestamp,
            reply_requested,
        })
    }
}

impl TryFrom<BufferReader<'_>> for StandbyStatusUpdateMessage {
    type Error = ReplicationError;

    fn try_from(reader: BufferReader<'_>) -> Result<Self, Self::Error> {
        if !reader.has_bytes(33) {
            return Err(ReplicationError::protocol(
                "Status update message too short",
            ));
        }

        let mut reader = reader;

        let message_type = reader.read_char()?;
        let last_lsn = reader.read_u64()?;
        let flush_lsn = reader.read_u64()?;
        let apply_lsn = reader.read_u64()?;
        let send_time = reader.read_u64()?;
        let reply_requested = reader.read_u8()?;

        Ok(StandbyStatusUpdateMessage {
            message_type,
            last_lsn,
            flush_lsn,
            apply_lsn,
            send_time,
            reply_requested,
        })
    }
}

impl TryFrom<BufferReader<'_>> for HotStandbyFeedbackMessage {
    type Error = ReplicationError;

    fn try_from(reader: BufferReader<'_>) -> Result<Self, Self::Error> {
        if !reader.has_bytes(25) {
            return Err(ReplicationError::protocol(
                "Hot standby feedback message too short",
            ));
        }

        let mut reader = reader;

        let message_type = reader.read_char()?;
        let send_time = reader.read_u64()?;
        let xmin = reader.read_u32()?;
        let epoch = reader.read_u32()?;
        let catalog_xmin = reader.read_u32()?;

        // The catalog_epoch is the last field in the message
        let catalog_epoch = reader.read_u32()?;

        Ok(HotStandbyFeedbackMessage {
            message_type,
            send_time,
            xmin,
            epoch,
            catalog_xmin,
            catalog_epoch,
        })
    }
}

// https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-XLOGDATA-MESSAGE
impl TryFrom<BufferReader<'_>> for XLogDataMessage {
    type Error = ReplicationError;

    fn try_from(reader: BufferReader<'_>) -> Result<Self, Self::Error> {
        if !reader.has_bytes(25) {
            return Err(ReplicationError::protocol("WAL message too short"));
        }

        let mut reader = reader;

        let message_type = reader.read_char()?;
        let data_start = reader.read_u64()?;
        let wal_end = reader.read_u64()?;
        let send_time = reader.read_u64()?;
        let data = reader.read_bytes(reader.remaining())?;

        Ok(XLogDataMessage {
            message_type,
            data_start,
            wal_end,
            send_time,
            data,
        })
    }
}
