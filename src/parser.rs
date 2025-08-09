//! PostgreSQL logical replication protocol message parser
//! Handles parsing of various message types from the replication stream

use crate::types::*;
use crate::utils::{buf_recv_u64, buf_recv_i64, buf_recv_u32, buf_recv_i32, buf_recv_i16, buf_recv_i8, Oid, Xid, XLogRecPtr, TimestampTz};
use anyhow::{Result, anyhow};
use tracing::{debug, warn, error};

/// Parse logical replication messages from a buffer
pub struct MessageParser;

impl MessageParser {
    pub fn parse_wal_message(buffer: &[u8]) -> Result<ReplicationMessage> {
        if buffer.is_empty() {
            return Err(anyhow!("Empty message buffer"));
        }
        
        let message_type = buffer[0] as char;
        debug!("Parsing message type: {}", message_type);
        
        match message_type {
            'B' => Self::parse_begin_message(buffer),
            'C' => Self::parse_commit_message(buffer),
            'R' => Self::parse_relation_message(buffer),
            'I' => Self::parse_insert_message(buffer),
            'U' => Self::parse_update_message(buffer),
            'D' => Self::parse_delete_message(buffer),
            'T' => Self::parse_truncate_message(buffer),
            'S' => Self::parse_stream_start_message(buffer),
            'E' => Self::parse_stream_stop_message(buffer),
            'c' => Self::parse_stream_commit_message(buffer),
            'A' => Self::parse_stream_abort_message(buffer),
            _ => {
                warn!("Unknown message type: {}", message_type);
                Err(anyhow!("Unknown message type: {}", message_type))
            }
        }
    }
    
    fn parse_begin_message(buffer: &[u8]) -> Result<ReplicationMessage> {
        if buffer.len() < 21 { // 1 + 8 + 8 + 4
            return Err(anyhow!("Begin message too short"));
        }
        
        let mut offset = 1; // Skip 'B'
        
        let final_lsn = buf_recv_u64(&buffer[offset..]);
        offset += 8;
        
        let timestamp = buf_recv_i64(&buffer[offset..]);
        offset += 8;
        
        let xid = buf_recv_u32(&buffer[offset..]);
        
        Ok(ReplicationMessage::Begin {
            final_lsn,
            timestamp,
            xid,
        })
    }
    
    fn parse_commit_message(buffer: &[u8]) -> Result<ReplicationMessage> {
        if buffer.len() < 26 { // 1 + 1 + 8 + 8 + 8
            return Err(anyhow!("Commit message too short"));
        }
        
        let mut offset = 1; // Skip 'C'
        
        let flags = buffer[offset];
        offset += 1;
        
        let commit_lsn = buf_recv_u64(&buffer[offset..]);
        offset += 8;
        
        let end_lsn = buf_recv_u64(&buffer[offset..]);
        offset += 8;
        
        let timestamp = buf_recv_i64(&buffer[offset..]);
        
        Ok(ReplicationMessage::Commit {
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
    }
    
    fn parse_relation_message(buffer: &[u8]) -> Result<ReplicationMessage> {
        if buffer.len() < 7 { // Minimum size check
            return Err(anyhow!("Relation message too short"));
        }
        
        let mut offset = 1; // Skip 'R'
        
        let oid = buf_recv_u32(&buffer[offset..]);
        offset += 4;
        
        // Parse namespace (null-terminated string)
        let namespace_start = offset;
        while offset < buffer.len() && buffer[offset] != 0 {
            offset += 1;
        }
        if offset >= buffer.len() {
            return Err(anyhow!("Invalid namespace in relation message"));
        }
        let namespace = String::from_utf8_lossy(&buffer[namespace_start..offset]).into_owned();
        offset += 1; // Skip null terminator
        
        // Parse relation name (null-terminated string)
        let relation_name_start = offset;
        while offset < buffer.len() && buffer[offset] != 0 {
            offset += 1;
        }
        if offset >= buffer.len() {
            return Err(anyhow!("Invalid relation name in relation message"));
        }
        let relation_name = String::from_utf8_lossy(&buffer[relation_name_start..offset]).into_owned();
        offset += 1; // Skip null terminator
        
        if offset >= buffer.len() {
            return Err(anyhow!("Relation message truncated"));
        }
        
        let replica_identity = buffer[offset] as char;
        offset += 1;
        
        if offset + 2 > buffer.len() {
            return Err(anyhow!("Relation message truncated"));
        }
        
        let column_count = buf_recv_i16(&buffer[offset..]);
        offset += 2;
        
        let mut columns = Vec::new();
        for _ in 0..column_count {
            if offset >= buffer.len() {
                return Err(anyhow!("Column data truncated"));
            }
            
            let key_flag = buf_recv_i8(&buffer[offset..]);
            offset += 1;
            
            // Parse column name (null-terminated string)
            let column_name_start = offset;
            while offset < buffer.len() && buffer[offset] != 0 {
                offset += 1;
            }
            if offset >= buffer.len() {
                return Err(anyhow!("Invalid column name"));
            }
            let column_name = String::from_utf8_lossy(&buffer[column_name_start..offset]).into_owned();
            offset += 1; // Skip null terminator
            
            if offset + 8 > buffer.len() {
                return Err(anyhow!("Column data truncated"));
            }
            
            let column_type = buf_recv_u32(&buffer[offset..]);
            offset += 4;
            
            let atttypmod = buf_recv_i32(&buffer[offset..]);
            offset += 4;
            
            columns.push(ColumnInfo {
                key_flag,
                column_name,
                column_type,
                atttypmod,
            });
        }
        
        let relation = RelationInfo {
            oid,
            namespace,
            relation_name,
            replica_identity,
            column_count,
            columns,
        };
        
        Ok(ReplicationMessage::Relation { relation })
    }
    
    fn parse_insert_message(buffer: &[u8]) -> Result<ReplicationMessage> {
        if buffer.len() < 6 { // Minimum size
            return Err(anyhow!("Insert message too short"));
        }
        
        let mut offset = 1; // Skip 'I'
        
        let transaction_id_or_oid = buf_recv_u32(&buffer[offset..]);
        offset += 4;
        
        let (relation_id, is_stream, xid) = if offset < buffer.len() && buffer[offset] == b'N' {
            // Not a streaming transaction
            (transaction_id_or_oid, false, None)
        } else {
            // Streaming transaction
            let relation_id = buf_recv_u32(&buffer[offset..]);
            offset += 4;
            (relation_id, true, Some(transaction_id_or_oid))
        };
        
        if offset >= buffer.len() || buffer[offset] != b'N' {
            return Err(anyhow!("Expected 'N' marker in insert message"));
        }
        offset += 1;
        
        let tuple_data = Self::parse_tuple_data(&buffer[offset..])?;
        
        Ok(ReplicationMessage::Insert {
            relation_id,
            tuple_data,
            is_stream,
            xid,
        })
    }
    
    fn parse_update_message(buffer: &[u8]) -> Result<ReplicationMessage> {
        if buffer.len() < 6 {
            return Err(anyhow!("Update message too short"));
        }
        
        let mut offset = 1; // Skip 'U'
        
        let transaction_id_or_oid = buf_recv_u32(&buffer[offset..]);
        offset += 4;
        
        let (relation_id, is_stream, xid) = if offset < buffer.len() && 
            (buffer[offset] == b'K' || buffer[offset] == b'O' || buffer[offset] == b'N') {
            // Not a streaming transaction
            (transaction_id_or_oid, false, None)
        } else {
            // Streaming transaction
            let relation_id = buf_recv_u32(&buffer[offset..]);
            offset += 4;
            (relation_id, true, Some(transaction_id_or_oid))
        };
        
        if offset >= buffer.len() {
            return Err(anyhow!("Update message truncated"));
        }
        
        let marker = buffer[offset] as char;
        offset += 1;
        
        let (key_type, old_tuple_data) = match marker {
            'K' | 'O' => {
                let tuple_data = Self::parse_tuple_data(&buffer[offset..])?;
                offset += tuple_data.processed_length;
                
                if offset >= buffer.len() || buffer[offset] != b'N' {
                    return Err(anyhow!("Expected 'N' marker after old tuple data"));
                }
                offset += 1;
                
                (Some(marker), Some(tuple_data))
            }
            'N' => (None, None),
            _ => return Err(anyhow!("Invalid marker in update message: {}", marker)),
        };
        
        let new_tuple_data = Self::parse_tuple_data(&buffer[offset..])?;
        
        Ok(ReplicationMessage::Update {
            relation_id,
            key_type,
            old_tuple_data,
            new_tuple_data,
            is_stream,
            xid,
        })
    }
    
    fn parse_delete_message(buffer: &[u8]) -> Result<ReplicationMessage> {
        if buffer.len() < 6 {
            return Err(anyhow!("Delete message too short"));
        }
        
        let mut offset = 1; // Skip 'D'
        
        let transaction_id_or_oid = buf_recv_u32(&buffer[offset..]);
        offset += 4;
        
        let (relation_id, is_stream, xid, key_type) = if offset < buffer.len() && 
            (buffer[offset] == b'K' || buffer[offset] == b'O') {
            // Not a streaming transaction
            let key_type = buffer[offset] as char;
            offset += 1;
            (transaction_id_or_oid, false, None, key_type)
        } else {
            // Streaming transaction
            let relation_id = buf_recv_u32(&buffer[offset..]);
            offset += 4;
            
            if offset >= buffer.len() {
                return Err(anyhow!("Delete message truncated"));
            }
            let key_type = buffer[offset] as char;
            offset += 1;
            
            (relation_id, true, Some(transaction_id_or_oid), key_type)
        };
        
        let tuple_data = Self::parse_tuple_data(&buffer[offset..])?;
        
        Ok(ReplicationMessage::Delete {
            relation_id,
            key_type,
            tuple_data,
            is_stream,
            xid,
        })
    }
    
    fn parse_truncate_message(buffer: &[u8]) -> Result<ReplicationMessage> {
        if buffer.len() < 10 {
            return Err(anyhow!("Truncate message too short"));
        }
        
        let mut offset = 1; // Skip 'T'
        
        let xid_or_num_relations = buf_recv_u32(&buffer[offset..]);
        offset += 4;
        
        let possible_relation_num = buf_recv_u32(&buffer[offset..]);
        offset += 4;
        
        let remaining = buffer.len() - offset;
        let expected_size = 1 + (possible_relation_num as usize * 4); // flags + relation IDs
        
        let (is_stream, xid, num_relations) = if remaining == expected_size {
            // Streaming transaction
            (true, Some(xid_or_num_relations), possible_relation_num)
        } else {
            // Not a streaming transaction, go back 4 bytes
            offset -= 4;
            (false, None, xid_or_num_relations)
        };
        
        if offset >= buffer.len() {
            return Err(anyhow!("Truncate message truncated"));
        }
        
        let flags = buf_recv_i8(&buffer[offset..]);
        offset += 1;
        
        let mut relation_ids = Vec::new();
        for _ in 0..num_relations {
            if offset + 4 > buffer.len() {
                return Err(anyhow!("Truncate relation IDs truncated"));
            }
            let relation_id = buf_recv_u32(&buffer[offset..]);
            offset += 4;
            relation_ids.push(relation_id);
        }
        
        Ok(ReplicationMessage::Truncate {
            relation_ids,
            flags,
            is_stream,
            xid,
        })
    }
    
    fn parse_stream_start_message(buffer: &[u8]) -> Result<ReplicationMessage> {
        if buffer.len() < 6 {
            return Err(anyhow!("Stream start message too short"));
        }
        
        let mut offset = 1; // Skip 'S'
        
        let xid = buf_recv_u32(&buffer[offset..]);
        offset += 4;
        
        let first_segment = if offset < buffer.len() {
            buffer[offset] == 1
        } else {
            false
        };
        
        Ok(ReplicationMessage::StreamStart { xid, first_segment })
    }
    
    fn parse_stream_stop_message(_buffer: &[u8]) -> Result<ReplicationMessage> {
        Ok(ReplicationMessage::StreamStop)
    }
    
    fn parse_stream_commit_message(buffer: &[u8]) -> Result<ReplicationMessage> {
        if buffer.len() < 26 {
            return Err(anyhow!("Stream commit message too short"));
        }
        
        let mut offset = 1; // Skip 'c'
        
        let xid = buf_recv_u32(&buffer[offset..]);
        offset += 4;
        
        let flags = buffer[offset];
        offset += 1;
        
        let commit_lsn = buf_recv_u64(&buffer[offset..]);
        offset += 8;
        
        let end_lsn = buf_recv_u64(&buffer[offset..]);
        offset += 8;
        
        let timestamp = buf_recv_i64(&buffer[offset..]);
        
        Ok(ReplicationMessage::StreamCommit {
            xid,
            flags,
            commit_lsn,
            end_lsn,
            timestamp,
        })
    }
    
    fn parse_stream_abort_message(buffer: &[u8]) -> Result<ReplicationMessage> {
        if buffer.len() < 9 {
            return Err(anyhow!("Stream abort message too short"));
        }
        
        let mut offset = 1; // Skip 'A'
        
        let xid = buf_recv_u32(&buffer[offset..]);
        offset += 4;
        
        let subtransaction_xid = buf_recv_u32(&buffer[offset..]);
        
        Ok(ReplicationMessage::StreamAbort {
            xid,
            subtransaction_xid,
        })
    }
    
    fn parse_tuple_data(buffer: &[u8]) -> Result<TupleData> {
        if buffer.len() < 2 {
            return Err(anyhow!("Tuple data too short"));
        }
        
        let mut offset = 0;
        let column_count = buf_recv_i16(&buffer[offset..]);
        offset += 2;
        
        let mut columns = Vec::new();
        
        for _ in 0..column_count {
            if offset >= buffer.len() {
                return Err(anyhow!("Tuple data truncated"));
            }
            
            let data_type = buffer[offset] as char;
            offset += 1;
            
            let column_data = match data_type {
                'n' => ColumnData {
                    data_type: 'n',
                    length: 0,
                    data: String::new(),
                },
                'u' => {
                    debug!("Unchanged TOAST value encountered");
                    ColumnData {
                        data_type: 'u',
                        length: 0,
                        data: String::new(),
                    }
                },
                't' => {
                    if offset + 4 > buffer.len() {
                        return Err(anyhow!("Text data length truncated"));
                    }
                    
                    let text_len = buf_recv_i32(&buffer[offset..]);
                    offset += 4;
                    
                    if offset + text_len as usize > buffer.len() {
                        return Err(anyhow!("Text data truncated"));
                    }
                    
                    let text_data = String::from_utf8_lossy(&buffer[offset..offset + text_len as usize]).into_owned();
                    offset += text_len as usize;
                    
                    ColumnData {
                        data_type: 't',
                        length: text_len,
                        data: text_data,
                    }
                },
                _ => {
                    error!("Unknown tuple data type: {}", data_type);
                    return Err(anyhow!("Unknown tuple data type: {}", data_type));
                }
            };
            
            columns.push(column_data);
        }
        
        Ok(TupleData {
            column_count,
            columns,
            processed_length: offset,
        })
    }
}
