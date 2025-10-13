//! PostgreSQL replication server implementation
//! Main server that handles connection, replication slot management, and message processing

use crate::buffer::{BufferReader, BufferWriter};
use crate::errors::{ReplicationError, Result};
use crate::parser::MessageParser;
use crate::types::*;
use crate::utils::{format_timestamp_from_pg, system_time_to_postgres_timestamp, PGConnection, INVALID_XLOG_REC_PTR};
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, error, info, warn};

/// Helper function to identify transient errors that might be retried
fn is_transient_error(err: &ReplicationError) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("network") ||
    msg.contains("timeout") ||
    msg.contains("temporarily unavailable") ||
    msg.contains("connection") ||
    msg.contains("broken pipe") ||
    msg.contains("resource temporarily unavailable")
}

/// Simple metrics tracker for replication operations
#[derive(Debug, Default)]
pub struct MetricsTracker {
    pub messages: u64,
    pub bytes: u64,
    pub errors: u64,
    pub last_msg: Option<SystemTime>,
}

impl MetricsTracker {
    pub fn record_bytes(&mut self, len: usize) {
        self.bytes += len as u64;
        self.last_msg = Some(SystemTime::now());
    }

    pub fn record_message(&mut self) {
        self.messages += 1;
    }

    pub fn record_error(&mut self) {
        self.errors += 1;
    }

    pub fn should_validate(&self, no_data_cycles: usize, threshold: usize) -> bool {
        no_data_cycles >= threshold
    }

    pub fn is_healthy(&self) -> bool {
        let now = SystemTime::now();
        let recent = self.last_msg
            .map(|time| match now.duration_since(time) {
                Ok(duration) => duration.as_secs() < 30,
                Err(_) => false, // If there's a clock error, treat as unhealthy
            })
            .unwrap_or(false);

        let total = (self.messages + self.errors).max(1);
        let error_rate = self.errors as f64 / total as f64;

        recent && error_rate < 0.01
    }
}

/// Simple shutdown flag for graceful shutdown management
#[derive(Debug, Default)]
pub struct ShutdownFlag(bool);

impl ShutdownFlag {
    pub fn request(&mut self) {
        self.0 = true;
    }

    pub fn is_requested(&self) -> bool {
        self.0
    }

    pub fn reset(&mut self) {
        self.0 = false;
    }
}

pub struct ReplicationServer {
    connection: PGConnection,
    config: ReplicationConfig,
    state: ReplicationState,
    metrics: MetricsTracker,
    shutdown_flag: ShutdownFlag,
    error_count_threshold: usize,
    error_rate_threshold: f64,
    max_no_data_cycles: usize,
}

/// Metrics for monitoring replication performance and health
#[derive(Debug, Default)]
pub struct ReplicationMetrics {
    pub messages_processed: u64,
    pub bytes_received: u64,
    pub errors_count: u64,
    pub last_message_time: Option<SystemTime>,
    pub connection_attempts: u32,
}

impl ReplicationServer {
    pub fn new(config: ReplicationConfig) -> Result<Self> {
        Self::new_with_thresholds(config, 100, 0.1)
    }

    pub fn new_with_thresholds(
        config: ReplicationConfig,
        error_count_threshold: usize,
        error_rate_threshold: f64,
    ) -> Result<Self> {
        Self::new_with_full_config(config, error_count_threshold, error_rate_threshold, 100)
    }

    pub fn new_with_full_config(
        config: ReplicationConfig,
        error_count_threshold: usize,
        error_rate_threshold: f64,
        max_no_data_cycles: usize,
    ) -> Result<Self> {
        info!("Connecting to database: {}", config.connection_string);
        let connection = PGConnection::connect(&config.connection_string)?;
        info!("Successfully connected to database server");

        Ok(Self {
            connection,
            config,
            state: ReplicationState::new(),
            metrics: MetricsTracker::default(),
            shutdown_flag: ShutdownFlag::default(),
            error_count_threshold,
            error_rate_threshold,
            max_no_data_cycles,
        })
    }

    pub fn identify_system(&self) -> Result<()> {
        debug!("Identifying system");
        match self.connection.exec("IDENTIFY_SYSTEM") {
            Ok(result) => {
                let status = result.status();
                if result.is_ok() && result.ntuples() > 0 {
                    let system_id = result.getvalue(0, 0);
                    let timeline = result.getvalue(0, 1); 
                    let xlogpos = result.getvalue(0, 2);
                    let dbname = result.getvalue(0, 3);
                    info!("IDENTIFY_SYSTEM succeeded: status: {:?}, system_id: {:?}, timeline: {:?}, xlogpos: {:?}, dbname: {:?}", 
                        status, system_id, timeline, xlogpos, dbname);
                } else {
                    return Err(crate::errors::ReplicationError::protocol(format!(
                        "IDENTIFY_SYSTEM failed: status: {:?}, rows: {}, columns: {}. This usually means the connection is not in replication mode or lacks replication privileges.",
                        status, result.ntuples(), result.nfields()
                    )));
                }
            }
            Err(err) => {
                return Err(crate::errors::ReplicationError::protocol(format!(
                    "IDENTIFY_SYSTEM command failed: {}",
                    err
                )));
            }
        }

        info!("System identification successful");
        Ok(())
    }

    pub async fn create_replication_slot_and_start(&mut self) -> Result<()> {
        self.create_replication_slot()?;
        self.start_replication().await?;
        Ok(())
    }

    fn create_replication_slot(&self) -> Result<()> {
        // https://www.postgresql.org/docs/14/protocol-replication.html
        let create_slot_sql = format!(
            "CREATE_REPLICATION_SLOT \"{}\" LOGICAL pgoutput NOEXPORT_SNAPSHOT;",
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

    async fn start_replication(&mut self) -> Result<()> {
        /*
        proto_version
            Protocol version. Currently versions 1, 2, 3, and 4 are supported. A valid version is required.
            Version 2 is supported only for server version 14 and above, and it allows streaming of large in-progress transactions.
            Version 3 is supported only for server version 15 and above, and it allows streaming of two-phase commits.
            Version 4 is supported only for server version 16 and above, and it allows streams of large in-progress transactions to be applied in parallel.
        https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS
        */
        let start_replication_sql = format!(
            "START_REPLICATION SLOT \"{}\" LOGICAL 0/0 (proto_version '2', streaming 'on', publication_names '\"{}\"');",
            self.config.slot_name,
            self.config.publication_name
        );

        info!(
            "Starting replication with publication: {}, executing SQL: {}",
            self.config.publication_name, start_replication_sql
        );
        let _ = self.connection.exec(&start_replication_sql)?;

        info!("Started receiving data from database server");
        self.replication_loop().await?;
        Ok(())
    }

    async fn replication_loop(&mut self) -> Result<()> {
        let mut no_data_count = 0;

        loop {
            // Check for shutdown request
            if self.shutdown_flag.is_requested() {
                info!("Shutdown requested, gracefully stopping replication loop");
                break;
            }

            self.check_and_send_feedback()?;

            match self.connection.get_copy_data(0)? {
                None => {
                    no_data_count += 1;
                    if self.metrics.should_validate(no_data_count, self.max_no_data_cycles) {
                        debug!("No data received for {} cycles, checking connection health", no_data_count);
                        self.validate_connection()?;
                        no_data_count = 0;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                Some(data) => {
                    no_data_count = 0; // Reset counter when we receive data

                    if data.is_empty() {
                        continue;
                    }

                    // Update metrics
                    self.metrics.record_bytes(data.len());

                    debug!(
                        "PQgetCopyData returned: {}, data len: {}",
                        data[0] as char,
                        data.len()
                    );

                    // please refer to https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-XLOGDATA
                    match data[0] as char {
                        'k' => {
                            self.process_keepalive_message(&data)?;
                        }
                        'w' => {
                            if let Err(e) = self.process_wal_message(&data) {
                                self.metrics.record_error();

                                // Extract context for diagnostics
                                let message_type = data[0] as char;
                                // Extract LSN from bytes 1..9 (u64, big-endian) per protocol
                                let lsn = if data.len() >= 9 {
                                    u64::from_be_bytes(data[1..9].try_into().unwrap_or_default())
                                } else {
                                    0
                                };
                                let connection_status = format!(
                                    "messages_processed: {}, errors_count: {}, lsn: {}",
                                    self.metrics.messages,
                                    self.metrics.errors,
                                    self.state.received_lsn
                                );

                                error!(
                                    "Error processing WAL message: {} | type: '{}' | LSN: {} | connection_status: {}",
                                    e, message_type, lsn, connection_status
                                );

                                // For certain errors, we might want to continue
                                if self.should_continue_after_error(&e) {
                                    warn!("Continuing after recoverable error: {}", e);
                                    continue;
                                } else {
                                    return Err(e);
                                }
                            } else {
                                self.metrics.record_message();
                            }
                        }
                        _ => {
                            warn!("Received unknown message type: {}", data[0] as char);
                            self.metrics.record_error();
                        }
                    }
                }
            }
        }

        // Send final feedback before shutdown with retry logic
        const MAX_FEEDBACK_RETRIES: u8 = 3;
        let mut feedback_attempts = 0;
        loop {
            match self.send_feedback() {
                Ok(_) => {
                    info!("Successfully sent final feedback during shutdown");
                    break;
                }
                Err(e) => {
                    feedback_attempts += 1;
                    if feedback_attempts < MAX_FEEDBACK_RETRIES && is_transient_error(&e) {
                        warn!(
                            "Transient error sending final feedback (attempt {}/{}): {}. Retrying...",
                            feedback_attempts, MAX_FEEDBACK_RETRIES, e
                        );
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    } else {
                        warn!(
                            "Failed to send final feedback during shutdown after {} attempts: {}",
                            feedback_attempts, e
                        );
                        break;
                    }
                }
            }
        }

        info!("Replication loop stopped gracefully");
        Ok(())
    }

    fn process_keepalive_message(&mut self, data: &[u8]) -> Result<()> {
        if data.len() < 18 {
            // 'k' + 8 bytes LSN + 8 bytes timestamp + 1 byte reply flag
            return Err(crate::errors::ReplicationError::protocol(
                "Keepalive message too short",
            ));
        }

        debug!("Processing keepalive message");

        let mut reader = BufferReader::new(data);
        let _msg_type = reader.skip_message_type()?; // Skip 'k'
        let log_pos = reader.read_u64()?;

        self.state.update_lsn(log_pos);

        self.send_feedback()?;
        Ok(())
    }

    fn process_wal_message(&mut self, data: &[u8]) -> Result<()> {
        if data.len() < 25 {
            // 'w' + 8 + 8 + 8 + at least 1 byte data
            return Err(crate::errors::ReplicationError::protocol(
                "WAL message too short",
            ));
        }

        let mut reader = BufferReader::new(data);
        let _msg_type = reader.skip_message_type()?; // Skip 'w'

        // Parse WAL message header
        let data_start = reader.read_u64()?;
        let _wal_end = reader.read_u64()?;
        let _send_time = reader.read_i64()?;

        if data_start > 0 {
            self.state.update_lsn(data_start);
        }

        if reader.remaining() == 0 {
            return Err(crate::errors::ReplicationError::protocol(
                "WAL message has no data",
            ));
        }

        // Parse the actual logical replication message
        let message_data = &data[reader.position()..];
        match MessageParser::parse_wal_message(message_data) {
            Ok(message) => {
                self.process_replication_message(message)?;
                self.metrics.record_message();
            }
            Err(e) => {
                error!("Failed to parse replication message: {}", e);
                return Err(e);
            }
        }

        self.send_feedback()?;
        Ok(())
    }

    /// Determine if replication should continue after an error
    fn should_continue_after_error(&self, error: &ReplicationError) -> bool {
        match error {
            // Continue after parsing errors that might be recoverable
            crate::errors::ReplicationError::MessageParsing { .. } => true,
            // Continue after protocol errors that don't break the connection
            crate::errors::ReplicationError::Protocol { .. } => true,
            // Don't continue after connection or buffer errors
            crate::errors::ReplicationError::Connection { .. } => false,
            crate::errors::ReplicationError::BufferOperation { .. } => false,
            // Use judgment for other error types
            _ => false,
        }
    }

    /// Get current replication metrics
    pub fn get_metrics(&self) -> &MetricsTracker {
        &self.metrics
    }

    /// Check replication health status
    pub fn is_healthy(&self) -> bool {
        self.metrics.is_healthy()
    }

    fn process_replication_message(&mut self, message: ReplicationMessage) -> Result<()> {
        match message {
            ReplicationMessage::Begin { xid, .. } => {
                info!("BEGIN: Xid {}", xid);
            }

            ReplicationMessage::Commit { 
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
             } => {
                info!("COMMIT: flags: {}, lsn: {}, end_lsn: {}, commit_time: {}", flags, commit_lsn, end_lsn, format_timestamp_from_pg(timestamp));
            }

            ReplicationMessage::Relation { relation } => {
                // info!(
                //     "Received relation info for {}.{}",
                //     relation.namespace, relation.relation_name
                // );
                self.state.add_relation(relation);
            }

            ReplicationMessage::Insert {
                relation_id,
                tuple_data,
                is_stream,
                xid,
            } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    if is_stream {
                        if let Some(xid) = xid {
                            info!("Streaming, Xid: {} ", xid);
                        }
                    }
                    info!(
                        "table {}.{}: INSERT: ",
                        relation.namespace, relation.relation_name
                    );
                    self.info_tuple_data(relation, &tuple_data)?;
                } else {
                    error!("Received INSERT for unknown relation: {}", relation_id);
                }
            }

            ReplicationMessage::Update {
                relation_id,
                key_type,
                old_tuple_data,
                new_tuple_data,
                is_stream,
                xid,
            } => {
                if let Some(relation) = self.state.get_relation(relation_id) {
                    if is_stream {
                        if let Some(xid) = xid {
                                                        info!("Streaming, Xid: {} ", xid);
                        }
                    }
                    info!(
                        "table {}.{} UPDATE ",
                        relation.namespace, relation.relation_name
                    );

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

            ReplicationMessage::Delete {
                relation_id,
                key_type,
                tuple_data,
                is_stream,
                xid,
            } => {
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
                    info!(
                        "table {}.{}: DELETE: ({}): ",
                        relation.namespace, relation.relation_name, key_info
                    );
                    self.info_tuple_data(relation, &tuple_data)?;
                } else {
                    error!("Received DELETE for unknown relation: {}", relation_id);
                }
            }

            ReplicationMessage::Truncate {
                relation_ids,
                flags,
                is_stream,
                xid,
            } => {
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
            if column_data.data_type == crate::parser::COLUMN_TYPE_NULL {
                continue; // Skip NULL values
            }

            if i < relation.columns.len() {
                let column_name = &relation.columns[i].column_name;

                // Handle different data types more gracefully
                match column_data.data_type {
                    crate::parser::COLUMN_TYPE_TEXT => {
                        // Limit data length to prevent log flooding with safe UTF-8 truncation
                        let display_data = if column_data.data.len() > 200 {
                            // Find safe UTF-8 character boundary to truncate
                            let safe_truncate_pos = column_data.data
                                .char_indices()
                                .find(|(pos, _)| *pos > 200)
                                .map(|(pos, _)| pos)
                                .unwrap_or(200);
                            format!("{}... (truncated)", &column_data.data[..safe_truncate_pos])
                        } else {
                            column_data.data.clone()
                        };
                        info!("{}: {} ", column_name, display_data);
                    }
                    crate::parser::COLUMN_TYPE_UNCHANGED_TOAST => {
                        info!("{}: <TOASTED> ", column_name);
                    }
                    _ => {
                        info!("{}: <UNKNOWN_TYPE> ", column_name);
                    }
                }
            } else {
                warn!("Column index {} exceeds relation column count {}", i, relation.columns.len());
            }
        }
        Ok(())
    }

    fn send_feedback(&mut self) -> Result<()> {
        if self.state.received_lsn == 0 {
            return Ok(());
        }

        let now = SystemTime::now();
        let timestamp = system_time_to_postgres_timestamp(now);
        let mut reply_buf = [0u8; 34]; // 1 + 8 + 8 + 8 + 8 + 1
        let bytes_written = {
            let mut writer = BufferWriter::new(&mut reply_buf);

            writer.write_u8(b'r')?;
            writer.write_u64(self.state.received_lsn)?; // Received LSN
            writer.write_u64(self.state.received_lsn)?; // Flushed LSN (same as received)
            writer.write_u64(INVALID_XLOG_REC_PTR)?; // Applied LSN (not tracking)
            writer.write_i64(timestamp)?; // Timestamp
            writer.write_u8(0)?; // Don't request reply

            writer.bytes_written()
        };

        self.send_feedback_data(&reply_buf[..bytes_written])
    }

    /// Helper function to send feedback data with consolidated error handling
    fn send_feedback_data(&mut self, data: &[u8]) -> Result<()> {
        if let Err(e) = self.connection.put_copy_data(data) {
            error!("Failed to send feedback data: {}", e);
            return Err(e);
        }

        if let Err(e) = self.connection.flush() {
            error!("Failed to flush feedback: {}", e);
            return Err(e);
        }

        debug!("Sent feedback with LSN: {}", self.state.received_lsn);
        Ok(())
    }

    fn check_and_send_feedback(&mut self) -> Result<()> {
        let now = Instant::now();
        if now.duration_since(self.state.last_feedback_time)
            > Duration::from_secs(self.config.feedback_interval_secs)
        {
            if let Err(e) = self.send_feedback() {
                warn!("Failed to send periodic feedback: {}", e);
                // Don't return error here, as we'll try again next time
            }
            self.state.last_feedback_time = now;
        }
        Ok(())
    }

    /// Validate connection health
    fn validate_connection(&self) -> Result<()> {
        // Simple validation by checking if we can get connection status
        // This is a basic check - more sophisticated checks could be added
        if self.metrics.errors > self.error_count_threshold as u64 {
            let total_ops = self.metrics.messages + self.metrics.errors;
            if total_ops > 0 {
                let error_rate = self.metrics.errors as f64 / total_ops as f64;
                if error_rate > self.error_rate_threshold {
                    return Err(crate::errors::ReplicationError::protocol(
                        format!("High error rate detected: {:.2}%", error_rate * 100.0)
                    ));
                }
            }
        }
        Ok(())
    }

    /// Request graceful shutdown of the replication server
    pub fn request_shutdown(&mut self) {
        info!("Graceful shutdown requested for replication server");
        self.shutdown_flag.request();
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_flag.is_requested()
    }

    /// Reset the shutdown flag (useful for restarting)
    pub fn reset_shutdown_flag(&mut self) {
        self.shutdown_flag.reset();
    }

    /// Get a summary of replication status
    pub fn get_status_summary(&self) -> String {
        format!(
            "Replication Status - Messages: {}, Bytes: {}, Errors: {}, Healthy: {}, Shutdown: {}",
            self.metrics.messages,
            self.metrics.bytes,
            self.metrics.errors,
            self.is_healthy(),
            self.shutdown_flag.is_requested()
        )
    }
}
