//! PostgreSQL logical replication protocol messages
//!
//! Contains all the data structures for representing PostgreSQL logical replication
//! protocol messages. These represent the different types of database changes
//! and control messages that can be received during replication.

use serde::Serialize;
use std::collections::HashMap;
use crate::utils::binary::{Oid, Xid};

/// Information about a table column
///
/// This structure represents metadata about a column in a PostgreSQL table.
/// It's used to understand the structure of data being replicated.
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

    /// Updates the last feedback time to current time
    pub fn update_feedback_time(&mut self) {
        self.last_feedback_time = std::time::Instant::now();
    }
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

// PostgreSQL protocol message structures for replication

/// Keepalive message from PostgreSQL server
pub struct KeepaliveMessage {
    pub message_type: char,
    pub log_pos: u64,
    pub timestamp: u64,
    pub reply_requested: bool,
}

/// WAL data message from PostgreSQL server
pub struct XLogDataMessage {
    pub message_type: char,
    pub data_start: u64,
    pub wal_end: u64,
    pub send_time: u64,
    pub data: Vec<u8>,
}

/// Standby status update message sent to PostgreSQL
pub struct StandbyStatusUpdateMessage {
    pub message_type: char,
    pub reply_requested: u8,
    pub last_lsn: u64,
    pub flush_lsn: u64,
    pub apply_lsn: u64,
    pub send_time: u64,
}

/// Hot standby feedback message sent to PostgreSQL
pub struct HotStandbyFeedbackMessage {
    pub message_type: char,
    pub send_time: u64,
    pub xmin: u32,
    pub epoch: u32,
    pub catalog_xmin: u32,
    pub catalog_epoch: u32,
}

// Trait implementations for protocol message parsing

/// Trait for parsing protocol messages from buffer readers
pub trait FromBufferReader {
    fn parse(data: &[u8]) -> Result<Self, crate::core::errors::ReplicationError>
    where
        Self: Sized;
}

/// Trait for writing protocol messages to buffer writers
pub trait ToBufferWriter {
    fn write(&self, writer: &mut super::buffer::BufferWriter) -> Result<(), crate::core::errors::ReplicationError>;
}

// Implement parsing for protocol messages

impl FromBufferReader for XLogDataMessage {
    fn parse(data: &[u8]) -> Result<Self, crate::core::errors::ReplicationError> {
        if data.len() < 25 {
            return Err(crate::core::errors::ReplicationError::protocol("WAL message too short"));
        }

        let mut reader = super::buffer::BufferReader::new(data);
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

impl FromBufferReader for StandbyStatusUpdateMessage {
    fn parse(data: &[u8]) -> Result<Self, crate::core::errors::ReplicationError> {
        if data.len() < 33 {
            return Err(crate::core::errors::ReplicationError::protocol(
                "Status update message too short",
            ));
        }

        let mut reader = super::buffer::BufferReader::new(data);
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

impl FromBufferReader for HotStandbyFeedbackMessage {
    fn parse(data: &[u8]) -> Result<Self, crate::core::errors::ReplicationError> {
        if data.len() < 25 {
            return Err(crate::core::errors::ReplicationError::protocol(
                "Hot standby feedback message too short",
            ));
        }

        let mut reader = super::buffer::BufferReader::new(data);
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

// Implement TryFrom for buffer readers

impl TryFrom<super::buffer::BufferReader<'_>> for KeepaliveMessage {
    type Error = crate::core::errors::ReplicationError;

    fn try_from(reader: super::buffer::BufferReader<'_>) -> Result<Self, Self::Error> {
        if !reader.has_bytes(18) {
            return Err(crate::core::errors::ReplicationError::protocol("Keepalive message too short"));
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

impl TryFrom<super::buffer::BufferReader<'_>> for StandbyStatusUpdateMessage {
    type Error = crate::core::errors::ReplicationError;

    fn try_from(reader: super::buffer::BufferReader<'_>) -> Result<Self, Self::Error> {
        if !reader.has_bytes(33) {
            return Err(crate::core::errors::ReplicationError::protocol(
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

impl TryFrom<super::buffer::BufferReader<'_>> for HotStandbyFeedbackMessage {
    type Error = crate::core::errors::ReplicationError;

    fn try_from(reader: super::buffer::BufferReader<'_>) -> Result<Self, Self::Error> {
        if !reader.has_bytes(25) {
            return Err(crate::core::errors::ReplicationError::protocol(
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

impl TryFrom<super::buffer::BufferReader<'_>> for XLogDataMessage {
    type Error = crate::core::errors::ReplicationError;

    fn try_from(reader: super::buffer::BufferReader<'_>) -> Result<Self, Self::Error> {
        if !reader.has_bytes(25) {
            return Err(crate::core::errors::ReplicationError::protocol("WAL message too short"));
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

// Implement writing for protocol messages

impl ToBufferWriter for KeepaliveMessage {
    fn write(&self, writer: &mut super::buffer::BufferWriter) -> Result<(), crate::core::errors::ReplicationError> {
        writer.write_char(self.message_type)?;
        writer.write_u64(self.log_pos)?;
        writer.write_u64(self.timestamp)?;
        writer.write_u8(if self.reply_requested { 1 } else { 0 })?;
        Ok(())
    }
}

impl ToBufferWriter for StandbyStatusUpdateMessage {
    fn write(&self, writer: &mut super::buffer::BufferWriter) -> Result<(), crate::core::errors::ReplicationError> {
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
    fn write(&self, writer: &mut super::buffer::BufferWriter) -> Result<(), crate::core::errors::ReplicationError> {
        writer.write_char(self.message_type)?;
        writer.write_u64(self.send_time)?;
        writer.write_u32(self.xmin)?;
        writer.write_u32(self.epoch)?;
        writer.write_u32(self.catalog_xmin)?;
        writer.write_u32(self.catalog_epoch)?;
        Ok(())
    }
}