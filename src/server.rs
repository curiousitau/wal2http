//! PostgreSQL replication server implementation
//!
//! Main server that handles the complete logical replication lifecycle:
//! - Database connection and validation
//! - Replication slot and publication verification
//! - WAL streaming and message processing
//! - Event delivery to configured sinks

use crate::buffer::{BufferReader, BufferWriter};
use crate::errors::ReplicationResult;
use crate::event_sink::EventSink;
use crate::event_sink::hook0::{self, Hook0EventSinkConfig};
use crate::event_sink::http::{HttpEventSink, HttpEventSinkConfig};
use crate::parser::MessageParser;
use crate::types::*;
use crate::utils::{
    PGConnection, system_time_to_postgres_timestamp,
};
use libpq_sys::ExecStatusType;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, error, info, warn};

/// Main replication server that manages the logical replication connection
///
/// This struct coordinates all aspects of the replication process, maintaining
/// the connection state, processing messages, and ensuring reliable delivery
/// of database changes to the configured event sink.
pub struct ReplicationServer {
    connection: PGConnection,
    config: ReplicationConfig,
    state: ReplicationState,
    event_sink: Option<Arc<dyn EventSink + Send + Sync>>,
}

impl ReplicationServer {
    pub fn new(config: ReplicationConfig) -> ReplicationResult<Self> {
        info!("Connecting to database: {}", config.connection_string);
        let connection = PGConnection::connect(&config.connection_string)?;
        info!("Successfully connected to database server");

        let event_sink = if let (Some(api_url), Some(app_id), Some(api_token)) = (
            config.hook0_api_url.as_ref(),
            config.hook0_application_id,
            config.hook0_api_token.as_ref(),
        ) {
            info!("Initializing Hook0 event sink with URL: {}", api_url);
            let hook0_config = Hook0EventSinkConfig {
                api_url: api_url.clone(),
                application_id: app_id,
                api_token: api_token.clone(),
            };
            match hook0::Hook0EventSink::new(hook0_config) {
                Ok(sink) => {
                    info!("Successfully initialized Hook0 event sink");
                    Some(Arc::new(sink) as Arc<dyn EventSink + Send + Sync>)
                }
                Err(e) => {
                    error!("Failed to initialize Hook0 event sink: {}", e);
                    return Err(crate::errors::ReplicationError::protocol(e));
                }
            }
        } else if let Some(url) = config.http_endpoint_url.as_ref() {
            info!("Initializing HTTP event sink with URL: {}", url);
            let http_config = HttpEventSinkConfig {
                endpoint_url: url.clone(),
            };
            match HttpEventSink::new(http_config) {
                Ok(sink) => {
                    info!("Successfully initialized HTTP event sink");
                    Some(Arc::new(sink) as Arc<dyn EventSink + Send + Sync>)
                }
                Err(e) => {
                    error!("Failed to initialize HTTP event sink: {}", e);
                    return Err(crate::errors::ReplicationError::protocol(e));
                }
            }
        } else {
            info!("No event sink configured, events will be sent to STDOUT");

            Some(Arc::new(crate::event_sink::stdout::StdoutEventSink {})
                as Arc<dyn EventSink + Send + Sync>)
        };

        Ok(Self {
            connection,
            config,
            state: ReplicationState::new(),
            event_sink,
        })
    }

    /// Verifies that PostgreSQL is configured for logical replication
    ///
    /// Checks that the wal_level setting is 'logical', which is required
    /// for logical replication to function.
    pub fn check_wal_level(&self) -> ReplicationResult<()> {
        info!("Checking wal_level setting");

        let result = self.connection.exec("SHOW wal_level;")?;
        if !result.is_ok() {
            warn!("Failed to check wal_level, status: {:?}", result.status());
            return Err(crate::errors::ReplicationError::protocol(
                "Failed to check wal_level",
            ));
        }

        let wal_level = result.getvalue(0, 0);
        match wal_level {
            Some(level) => {
                info!("Current wal_level: {}", level);
                if level == "logical" {
                    info!("wal_level is correctly set to 'logical'");
                    Ok(())
                } else {
                    Err(crate::errors::ReplicationError::protocol(
                        "wal_level is not set to 'logical'. Please set wal_level to 'logical' in postgresql.conf and restart the PostgreSQL server.",
                    ))
                }
            }
            None => {
                warn!("Could not retrieve wal_level value");
                Err(crate::errors::ReplicationError::protocol(
                    "Could not retrieve wal_level value",
                ))
            }
        }
    }

    /// Identifies the PostgreSQL system and verifies replication support
    ///
    /// Executes IDENTIFY_SYSTEM to verify the connection supports replication
    /// and retrieves system information including timeline and WAL position.
    pub fn identify_system(&self) -> ReplicationResult<()> {
        debug!("Identifying system");
        match self.connection.exec("IDENTIFY_SYSTEM") {
            Ok(result) => {
                if !result.is_ok() {
                    return Err(crate::errors::ReplicationError::protocol(format!(
                        "IDENTIFY_SYSTEM failed: {:?}",
                        result.status()
                    )));
                }

                info!(
                    "IDENTIFY_SYSTEM succeeded: {:?}, system_id: {:?}, timeline: {:?}, xlogpos: {:?}, dbname: {:?}",
                    result.status(),
                    result.getvalue(0, 0),
                    result.getvalue(0, 1),
                    result.getvalue(0, 2),
                    result.getvalue(0, 3)
                );
            }
            Err(err) => {
                return Err(crate::errors::ReplicationError::protocol(format!(
                    "IDENTIFY_SYSTEM failed: {}",
                    err
                )));
            }
        }

        info!("System identification successful");
        Ok(())
    }

    /// Orchestrates the complete replication setup process
    ///
    /// Performs all necessary validation and setup before starting replication:
    /// 1. Verifies wal_level is 'logical'
    /// 2. Checks replication slot exists
    /// 3. Verifies publication exists
    /// 4. Starts the replication stream
    pub async fn create_replication_slot_and_start(&mut self) -> ReplicationResult<()> {
        self.check_wal_level()?;
        self.check_replication_slot()?;
        self.check_publication()?;

        self.start_replication().await?;

        Ok(())
    }

    fn check_replication_slot(&self) -> ReplicationResult<()> {
        // Check if the replication slot already exists
        let check_slot_sql = format!(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = '{}';",
            self.config.slot_name
        );

        let result = self.connection.exec(&check_slot_sql)?;
        if !result.is_ok() {
            return Err(crate::errors::ReplicationError::protocol(format!(
                "Failed to check existing replication slots: {:?}",
                result.status()
            )));
        }

        if result.ntuples() == 0 {
            return Err(crate::errors::ReplicationError::protocol(format!(
                "Replication slot '{}' does not exist. Please create it manually using the following SQL command:\n\nCREATE_REPLICATION_SLOT \"{}\" LOGICAL pgoutput NOEXPORT_SNAPSHOT;\nn",
                self.config.slot_name, self.config.slot_name
            )));
        }

        Ok(())
    }

    fn check_publication(&self) -> ReplicationResult<()> {
        // Check if the publication already exists and is for the correct table if specified
        debug!(
            "Checking if publication '{}' exists",
            self.config.publication_name
        );
        let check_pub_sql = format!(
            "SELECT * FROM pg_publication WHERE pubname = '{}';",
            self.config.publication_name
        );
        let result = self.connection.exec(&check_pub_sql)?;
        if !result.is_ok() {
            return Err(crate::errors::ReplicationError::protocol(format!(
                "Failed to check existing publications: {:?}",
                result.status()
            )));
        }

        if result.ntuples() == 0 {
            return Err(crate::errors::ReplicationError::protocol(format!(
                "Publication '{}' does not exist. Please create it manually using the following SQL command:\n\nCREATE PUBLICATION \"{}\" FOR TABLE {};\n\nor\n\nCREATE PUBLICATION \"{}\" FOR ALL TABLES;\n\ndepending on your configuration.",
                self.config.publication_name,
                self.config.publication_name,
                "<your_table_name>",
                self.config.publication_name
            )));
        }

        Ok(())
    }

    async fn start_replication(&mut self) -> ReplicationResult<()> {
        /*
        START_REPLICATION
            SLOT slot_name
            LOGICAL start_lsn
            (option_name 'value' [, ...])

        On success, the server responds with a CopyBothResponse message, and then starts to stream WAL to the frontend.

        https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-START-REPLICATION

        proto_version
            Protocol version. Currently versions 1, 2, 3, and 4 are supported. A valid version is required.
            Version 2 is supported only for server version 14 and above, and it allows streaming of large in-progress transactions.
            Version 3 is supported only for server version 15 and above, and it allows streaming of two-phase commits.
            Version 4 is supported only for server version 16 and above, and it allows streams of large in-progress transactions to be applied in parallel.
        https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS
        */
        let start_replication_sql = format!(
            "START_REPLICATION SLOT \"{}\" LOGICAL 0/0 (proto_version '2', streaming 'on', publication_names '{}');",
            self.config.slot_name, self.config.publication_name
        );

        info!(
            "Starting replication with publication: {}, executing SQL: {}",
            self.config.publication_name, start_replication_sql
        );

        let result = self.connection.exec(&start_replication_sql)?;
        if result.status() != ExecStatusType::PGRES_COPY_BOTH {
            return Err(crate::errors::ReplicationError::protocol(format!(
                "Failed to start replication: {:?}",
                result.status()
            )));
        }

        info!("Started receiving data from database server");
        self.replication_loop().await?;

        Ok(())
    }

    async fn replication_loop(&mut self) -> ReplicationResult<()> {
        loop {
            self.check_and_send_feedback()?;

            match self.connection.get_copy_data()? {
                None => {
                    info!("No data received, continuing");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                Some(data) => {
                    if data.is_empty() {
                        continue;
                    }
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
                            self.process_wal_message(&data).await?;
                        }
                        _ => {
                            warn!("Received unknown message type: {}", data[0] as char);
                        }
                    }
                }
            }
        }
    }

    /// Process a keepalive message from the replication stream
    ///
    /// This function handles keepalive messages which are sent periodically by the server
    /// to indicate that the replication connection is still alive. It updates the LSN tracking
    /// and sends feedback to the server if requested.
    ///
    /// Reference: https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-PRIMARY-KEEPALIVE-MESSAGE
    ///
    /// # Arguments
    /// * `data` - The raw keepalive message data from the replication stream
    ///
    /// # Returns
    /// A Result indicating success or failure of processing
    ///
    /// # Errors
    /// Returns an error if the message is malformed or cannot be processed
    ///
    fn process_keepalive_message(&mut self, data: &[u8]) -> ReplicationResult<()> {
        if data.len() < 18 {
            // 'k' + 8 bytes LSN + 8 bytes timestamp + 1 byte reply flag
            return Err(crate::errors::ReplicationError::protocol(
                "Keepalive message too short",
            ));
        }

        // Validate message type byte before accessing
        if data.is_empty() || data[0] != b'k' {
            return Err(crate::errors::ReplicationError::protocol(
                "Invalid keepalive message type",
            ));
        }

        debug!("Processing keepalive message");

        let reader = BufferReader::new(data);

        let k: KeepaliveMessage = reader.try_into()?;

        if k.reply_requested {
            debug!("Server requested feedback in keepalive");
            self.send_feedback()?;
            self.connection.flush()?;
        }
        Ok(())
    }

    /// Process a WAL message from the replication stream
    ///
    /// This function handles WAL data messages, which contain the actual logical replication data.
    /// It parses the WAL message header and delegates to the parser to extract the logical replication message.
    ///
    /// Reference: https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-XLOGDATA
    ///
    /// # Arguments
    /// * `data` - The raw WAL message data from the replication stream
    ///
    /// # Returns
    /// A Result indicating success or failure of processing
    ///
    /// # Errors
    /// Returns an error if the message is malformed or cannot be parsed
    async fn process_wal_message(&mut self, data: &[u8]) -> ReplicationResult<()> {
        let reader = BufferReader::new(data);

        let w = XLogDataMessage::try_from(reader)?;

        if w.data.is_empty() {
            return Err(crate::errors::ReplicationError::protocol(
                "WAL message has no data",
            ));
        }

        if w.data_start > 0 {
            self.state.update_lsn(w.data_start);
        }

        // Parse the actual logical replication message
        match MessageParser::parse_wal_message(&w.data) {
            Ok(message) => {
                self.process_replication_message(message).await?;
            }
            Err(e) => {
                error!("Failed to parse replication message: {}", e);
                return Err(e);
            }
        }

        self.send_feedback()?;
        Ok(())
    }

    async fn process_replication_message(
        &mut self,
        message: ReplicationMessage,
    ) -> ReplicationResult<()> {
        // Send event to configured sink if available
        if let Some(ref event_sink) = self.event_sink {
            debug!("Sending event to event sink: {:?}", message);

            // Process event sequentially - wait for successful delivery before continuing
                match event_sink.send_event(&message).await {
                Ok(()) => {
                    debug!(
                        "Successfully sent event to sink for LSN: {:x}",
                        self.state.received_lsn
                    );
                    // Only update applied LSN after successful event delivery
                      self.state.update_applied_lsn(self.state.received_lsn);
                }
                Err(e) => {
                    error!("Failed to send event to event sink: {}", e);
                    return Err(crate::errors::ReplicationError::protocol(format!(
                        "Event sink failed: {}",
                        e
                    )));
                }
            }
        } else {
            // No event sink configured, so we can consider this immediately "applied"
              self.state.update_applied_lsn(self.state.received_lsn);
        }

        // self.print_replication_message(message)?;

        Ok(())
    }

    fn send_feedback(&mut self) -> ReplicationResult<()> {
        debug!("Sending feedback to server");

        let now = SystemTime::now();
        let timestamp = system_time_to_postgres_timestamp(now);
        let mut reply_buf = [0u8; 34]; // 1 + 8 + 8 + 8 + 8 + 1
        let bytes_written = {
            let mut writer = BufferWriter::new(&mut reply_buf);

            writer.write_u8(b'r')?;
            writer.write_u8(0)?; // Don't request reply
            writer.write_u64(self.state.received_lsn)?;
            writer.write_u64(self.state.received_lsn)?;
            writer.write_u64(self.state.applied_lsn)?;
            writer.write_i64(timestamp)?;

            writer.bytes_written()
        };

        if bytes_written != reply_buf.len() {
            return Err(crate::errors::ReplicationError::protocol(
                "Failed to write feedback data".to_string(),
            ));
        }

        self.connection.put_copy_data(&reply_buf)?;

          debug!(
            "Sent feedback with received LSN: {:x}, applied LSN: {:x}",
            self.state.received_lsn, self.state.applied_lsn
        );
        Ok(())
    }

    fn check_and_send_feedback(&mut self) -> ReplicationResult<()> {
        let now = Instant::now();
        if now.duration_since(self.state.last_feedback_time)
            > Duration::from_secs(self.config.feedback_interval_secs)
        {
            self.send_feedback()?;
            self.state.last_feedback_time = now;
        }
        Ok(())
    }
}
