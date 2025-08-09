
//! PostgreSQL Replication Checker - Rust Edition
//! 
//! A Rust implementation of a PostgreSQL logical replication client that connects to a database,
//! creates replication slots, and displays changes in real-time.
//! 
//! Based on the C++ implementation: https://github.com/fkfk000/replication_checker

mod utils;
mod types;
mod parser;
mod server;
mod errors;

use errors::Result;
use clap::Parser;
use tracing::{info, error};
use tracing_subscriber::{fmt, EnvFilter};
use std::env;
use crate::types::ReplicationConfig;
use crate::server::ReplicationServer;

#[derive(Parser, Debug)]
#[command(
    name = "pg_replica_rs",
    about = "PostgreSQL Logical Replication Checker in Rust",
    version = "0.1.0"
)]
struct Args {
    /// Database connection parameters (space-separated key=value pairs)
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    connection_params: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    
    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .init();

    let args = Args::parse();

    // Check for required environment variables
    let slot_name = env::var("SlotName").unwrap_or_else(|_| "sub".to_string());
    let publication_name = env::var("PubName").unwrap_or_else(|_| "pub".to_string());

    info!("Slot name: {}", slot_name);
    info!("Publication name: {}", publication_name);

    // Parse connection parameters
    let connection_string = crate::utils::parse_connection_args(args.connection_params);
    info!("Connection string: {}", connection_string);

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
    // Run the server in a blocking task since libpq is synchronous
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut server = ReplicationServer::new(config)?;
        
        server.identify_system()
            .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;
        
        server.create_replication_slot_and_start()
            .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;
        
        Ok(())
    }).await?
}
