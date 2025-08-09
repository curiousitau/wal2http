//! PostgreSQL replication server implementation
//! Main server that handles connection, replication slot management, and message processing

use crate::types::*;
use crate::utils::{PGConnection, system_time_to_postgres_timestamp, buf_send_u64, buf_send_i64, buf_recv_u64, INVALID_XLOG_REC_PTR};
use crate::parser::MessageParser;
use anyhow::{Result, anyhow};
use tracing::{info, debug, warn, error};
use std::time::{Instant, Duration};

pub struct ReplicationServer {
    connection: PGConnection,
    config: ReplicationConfig,
    state: ReplicationState,
}

impl ReplicationServer {
    pub fn new(config: ReplicationConfig) -> Result<Self> {
        info!("Connecting to database: {}", config.connection_string);
        let connection = PGConnection::connect(&config.connection_string)?;
        info!("Successfully connected to database server");
        
        Ok(Self {
            connection,
            config,
            state: ReplicationState::new(),
        })
    }
    
    pub fn identify_system(&self) -> Result<()> {
        debug!("Identifying system");
        match self.connection.exec("IDENTIFY_SYSTEM") {
            Ok(result) => {
                info!("IDENTIFY_SYSTEM succeeded: {:?}", result.status());
            }
            Err(err) => {
                return Err(anyhow!("IDENTIFY_SYSTEM failed: {}", err));
            }
        }
        
        info!("System identification successful");
        Ok(())
    }
    
    pub fn create_replication_slot_and_start(&mut self) -> Result<()> {
        self.create_replication_slot()?;
        self.start_replication()?;
        Ok(())
    }
    
    fn create_replication_slot(&self) -> Result<()> {
        let create_slot_sql = format!(
            "CREATE_REPLICATION_SLOT \"{}\" LOGICAL pgoutput (SNAPSHOT 'nothing');",
            self.config.slot_name
        );
        
        info!("Creating replication slot: {}", self.config.slot_name);
        let result = self.connection.exec(&create_slot_sql)?;
        
        if !result.is_ok() {
            warn!("Replication slot creation may have failed, but continuing");
        } else {
            info!("Replication slot created successfully");
        }
        
        Ok(())
    }
    
    fn start_replication(&mut self) -> Result<()> {
        let start_replication_sql = format!(
            "START_REPLICATION SLOT \"{}\" LOGICAL 0/0 (proto_version '3', streaming 'on', publication_names '\"{}\"');",
            self.config.slot_name,
            self.config.publication_name
        );
        
        info!("Starting replication with publication: {}, SQL:{}", self.config.publication_name,start_replication_sql);
        let _result = self.connection.exec(&start_replication_sql)?;
        
        info!("Started receiving data from database server");
        self.replication_loop()?;
        Ok(())
    }
    
    fn replication_loop(&mut self) -> Result<()> {
        loop {
            self.check_and_send_feedback()?;
            
            match self.connection.get_copy_data(0)? {
                None => {
                    info!("No data received, continuing");
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Some(data) => {
                    if data.is_empty() {
                        continue;
                    }
                    debug!("PQgetCopyData returned: {}, data len: {}",data[0] as char, data.len());
                    match data[0] as char {
                        'k' => {
                            self.process_keepalive_message(&data)?;
                        }
                        'w' => {
                            self.process_wal_message(&data)?;
                        }
                        _ => {
                            warn!("Received unknown message type: {}", data[0] as char);
                        }
                    }
                }
            }
        }
    }
    
    fn process_keepalive_message(&mut self, data: &[u8]) -> Result<()> {
        if data.len() < 18 { // 'k' + 8 bytes LSN + 8 bytes timestamp + 1 byte reply flag
            return Err(anyhow!("Keepalive message too short"));
        }
        
        debug!("Processing keepalive message");
        
        let offset = 1; // Skip 'k'
        let log_pos = buf_recv_u64(&data[offset..]);
        self.state.update_lsn(log_pos);
        
        self.send_feedback()?;
        Ok(())
    }
    
    fn process_wal_message(&mut self, data: &[u8]) -> Result<()> {
        if data.len() < 25 { // 'w' + 8 + 8 + 8 + at least 1 byte data
            return Err(anyhow!("WAL message too short"));
        }
        
        let mut offset = 1; // Skip 'w'
        
        // Parse WAL message header
        let data_start = buf_recv_u64(&data[offset..]);
        offset += 8;
        
        let _wal_end = buf_recv_u64(&data[offset..]);
        offset += 8;
        
        let _send_time = crate::utils::buf_recv_i64(&data[offset..]);
        offset += 8;
        
        if data_start > 0 {
            self.state.update_lsn(data_start);
        }
        
        if offset >= data.len() {
            return Err(anyhow!("WAL message has no data"));
        }
        
        // Parse the actual logical replication message
        let message_data = &data[offset..];
        match MessageParser::parse_wal_message(message_data) {
            Ok(message) => {
                self.process_replication_message(message)?;
            }
            Err(e) => {
                error!("Failed to parse replication message: {}", e);
                return Err(e);
            }
        }
        
        self.send_feedback()?;
        Ok(())
    }
    
    fn process_replication_message(&mut self, message: ReplicationMessage) -> Result<()> {
        match message {
            ReplicationMessage::Begin { xid, .. } => {
                info!("BEGIN: Xid {}", xid);
            }
            
            ReplicationMessage::Commit { .. } => {
                info!("COMMIT\n");
            }
            
            ReplicationMessage::Relation { relation } => {
                info!("Received relation info for {}.{}", relation.namespace, relation.relation_name);
                self.state.add_relation(relation);
            }
            
            ReplicationMessage::Insert { relation_id, tuple_data, is_stream, xid } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    if is_stream {
                        if let Some(xid) = xid {
                            info!("Streaming, Xid: {} ", xid);
                        }
                    }
                    info!("table {}.{}: INSERT: ", relation.namespace, relation.relation_name);
                    self.info_tuple_data(relation, &tuple_data)?;
                } else {
                    error!("Received INSERT for unknown relation: {}", relation_id);
                }
            }
            
            ReplicationMessage::Update { relation_id, key_type, old_tuple_data, new_tuple_data, is_stream, xid } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    if is_stream {
                        if let Some(xid) = xid {
                            info!("Streaming, Xid: {} ", xid);
                        }
                    }
                    info!("table {}.{} UPDATE ", relation.namespace, relation.relation_name);
                    
                    if let Some(old_data) = old_tuple_data {
                        let key_info = match key_type {
                            Some('K') => "INDEX: ",
                            Some('O') => "REPLICA IDENTITY: ",
                            _ => "",
                        };
                        info!("Old {}: ", key_info);
                        self.info_tuple_data(relation, &old_data)?;
                        info!(" New Row: ");
                    } else {
                        info!("New Row: ");
                    }
                    
                    self.info_tuple_data(relation, &new_tuple_data)?;
                } else {
                    error!("Received UPDATE for unknown relation: {}", relation_id);
                }
            }
            
            ReplicationMessage::Delete { relation_id, key_type, tuple_data, is_stream, xid } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    if is_stream {
                        if let Some(xid) = xid {
                            info!("Streaming, Xid: {} ", xid);
                        }
                    }
                    let key_info = match key_type {
                        'K' => "INDEX",
                        'O' => "REPLICA IDENTITY",
                        _ => "UNKNOWN",
                    };
                    info!("table {}.{}: DELETE: ({}): ", relation.namespace, relation.relation_name, key_info);
                    self.info_tuple_data(relation, &tuple_data)?;
                } else {
                    error!("Received DELETE for unknown relation: {}", relation_id);
                }
            }
            
            ReplicationMessage::Truncate { relation_ids, flags, is_stream, xid } => {
                if is_stream {
                    if let Some(xid) = xid {
                        info!("Streaming, Xid: {} ", xid);
                    }
                }
                
                let flag_info = match flags {
                    1 => "CASCADE ",
                    2 => "RESTART IDENTITY ",
                    _ => "",
                };
                
                info!("TRUNCATE {}", flag_info);
                for relation_id in relation_ids {
                    if let Some(relation) = self.state.get_relation(relation_id) {
                        info!("{}.{} ", relation.namespace, relation.relation_name);
                    } else {
                        info!("UNKNOWN_RELATION({}) ", relation_id);
                    }
                }
            }
            
            ReplicationMessage::StreamStart { xid, .. } => {
                info!("Opening a streamed block for transaction {}", xid);
            }
            
            ReplicationMessage::StreamStop => {
                info!("Stream Stop");
            }
            
            ReplicationMessage::StreamCommit { xid, .. } => {
                info!("Committing streamed transaction {}\n", xid);
            }
            
            ReplicationMessage::StreamAbort { xid, .. } => {
                info!("Aborting streamed transaction {}", xid);
            }
        }
        
        Ok(())
    }
    
    fn info_tuple_data(&self, relation: &RelationInfo, tuple_data: &TupleData) -> Result<()> {
        for (i, column_data) in tuple_data.columns.iter().enumerate() {
            if column_data.data_type == 'n' {
                continue; // Skip NULL values
            }
            
            if i < relation.columns.len() {
                info!("{}: {} ", relation.columns[i].column_name, column_data.data);
            }
        }
        Ok(())
    }
    
    fn send_feedback(&mut self) -> Result<()> {
        if self.state.received_lsn == 0 {
            return Ok(());
        }
        
        let now = std::time::SystemTime::now();
        let timestamp = system_time_to_postgres_timestamp(now);
        
        let mut reply_buf = [0u8; 34]; // 1 + 8 + 8 + 8 + 8 + 1
        let mut offset = 0;
        
        reply_buf[offset] = b'r';
        offset += 1;
        
        buf_send_u64(self.state.received_lsn, &mut reply_buf[offset..]);
        offset += 8;
        
        buf_send_u64(self.state.received_lsn, &mut reply_buf[offset..]);
        offset += 8;
        
        buf_send_u64(INVALID_XLOG_REC_PTR, &mut reply_buf[offset..]);
        offset += 8;
        
        buf_send_i64(timestamp, &mut reply_buf[offset..]);
        offset += 8;
        
        reply_buf[offset] = 0; // Don't request reply for now
        
        self.connection.put_copy_data(&reply_buf)?;
        self.connection.flush()?;
        
        debug!("Sent feedback with LSN: {}", self.state.received_lsn);
        Ok(())
    }
    
    fn check_and_send_feedback(&mut self) -> Result<()> {
        let now = Instant::now();
        if now.duration_since(self.state.last_feedback_time) > Duration::from_secs(self.config.feedback_interval_secs) {
            self.send_feedback()?;
            self.state.last_feedback_time = now;
        }
        Ok(())
    }
}
