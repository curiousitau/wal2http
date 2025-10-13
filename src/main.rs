//! PostgreSQL Replication Checker - Rust Edition
//!
//! A Rust implementation of a PostgreSQL logical replication client that connects to a database,
//! creates replication slots, and displays changes in real-time.
//!
//! Based on the C++ implementation: https://github.com/fkfk000/replication_checker

mod buffer;
mod errors;
mod logging;
mod event_sink;
mod parser;
mod server;
mod types;
mod utils;

use crate::logging::LoggingConfig;
use crate::server::ReplicationServer;
use crate::types::ReplicationConfig;
use errors::Result;
use std::env;
use tokio::signal;
use tracing::{error, info, warn};


#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging from environment variables
    let logging_config = LoggingConfig::from_env()
        .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;
    
    logging_config.init_logging()
        .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;

    // Check for required environment variables
    let slot_name = env::var("slot_name").unwrap_or_else(|_| "sub".to_string());
    let publication_name = env::var("pub_name").unwrap_or_else(|_| "pub".to_string());

    info!("Slot name: {}", slot_name);
    info!("Publication name: {}", publication_name);

    // Get connection string from environment variable
    let connection_string = env::var("DB_CONNECTION_STRING")
        .map_err(|_| crate::errors::ReplicationError::MissingEnvVar("DB_CONNECTION_STRING".into()))?;
    
    // Create configuration with validation
    let config = ReplicationConfig::new(connection_string, publication_name, slot_name)?;

    // Create and run the replication server
    match run_replication_server(config).await {
        Ok(()) => {
            info!("Replication server completed successfully");
            Ok(())
        }
        Err(e) => {
            error!("Replication server failed: {}", e);
            Err(e)
        }
    }
}

async fn run_replication_server(config: ReplicationConfig) -> Result<()> {
    let mut server = ReplicationServer::new(config)?;

    // Set up graceful shutdown handling
    let shutdown_signal = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");
        warn!("Received interrupt signal, shutting down gracefully...");
    };

    // Run the server with shutdown handling
    tokio::select! {
        result = async {
            server.identify_system()
                .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;
            server.create_replication_slot_and_start().await
                .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;
            Ok(())
        } => result,
        _ = shutdown_signal => {
            info!("Graceful shutdown completed");
            Ok(())
        }
    }
}
