//! PostgreSQL Replication Checker - Rust Edition
//!
//! A Rust implementation of a PostgreSQL logical replication client that connects to a database,
//! creates replication slots, and streams database changes to HTTP endpoints or other sinks in real-time.
//!
//! ## Architecture Overview
//!
//! This application implements the PostgreSQL logical replication protocol to:
//! 1. Connect to a PostgreSQL database as a replication client
//! 2. Create or use existing replication slots and publications
//! 3. Stream WAL (Write-Ahead Log) changes as they happen
//! 4. Parse and convert database changes into structured events
//! 5. Send events to configured sinks (HTTP endpoints, Hook0, or STDOUT)
//!
//! ## Key Concepts
//!
//! - **Logical Replication**: PostgreSQL's mechanism for replicating data at the row level
//! - **Replication Slot**: A mechanism to track which changes have been consumed
//! - **Publication**: Defines which tables/changes are published for replication
//! - **WAL**: Write-Ahead Log, PostgreSQL's transaction log containing all changes
//! - **LSN**: Log Sequence Number, a unique identifier for positions in the WAL
//!
//! Based on the C++ implementation: https://github.com/fkfk000/replication_checker

// Module declarations - each file contains a specific component of the replication system
mod buffer;        // Binary buffer reading/writing utilities for protocol messages
mod email_config;  // Configuration for email notifications (not currently used)
mod errors;        // Comprehensive error types for the application
mod event_sink;    // Output handlers for sending events to various destinations
mod parser;        // Protocol message parser for decoding WAL data
mod server;        // Main replication server implementation
mod types;         // Data structures and type definitions
mod utils;         // Utility functions for PostgreSQL integration

// Import the core types and functionality we need
use crate::types::ReplicationConfig;
use crate::{errors::ReplicationError, server::ReplicationServer};
use clap::Parser;              // Command line argument parsing
use errors::ReplicationResult; // Result type alias for error handling
use std::env;                  // Environment variable access
use tracing::{error, info};    // Structured logging
use tracing_subscriber::{EnvFilter, fmt}; // Log formatting and filtering
use uuid::Uuid;                // UUID handling for Hook0 integration

/// Command line arguments structure using clap for parsing
///
/// This structure defines the command-line interface for the application.
/// Currently, it only accepts database connection parameters, but most
/// configuration is done through environment variables for better
/// containerization and security.
#[derive(Parser, Debug)]
#[command(
    name = "wal2http",
    about = "PostgreSQL Logical Replication Checker in Rust",
    version = "0.1.0"
)]
struct Args {
    /// Database connection parameters (space-separated key=value pairs)
    ///
    /// This accepts traditional PostgreSQL connection string parameters.
    /// However, in practice, most users should set the DATABASE_URL
    /// environment variable instead for consistency.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    connection_params: Vec<String>,
}

/// Application entry point
///
/// This is the main function that initializes the application and starts the replication server.
/// It uses the `tokio` async runtime to handle asynchronous operations.
///
/// # Configuration
///
/// The application is configured primarily through environment variables:
/// - `DATABASE_URL`: PostgreSQL connection string (required)
/// - `SLOT_NAME`: Replication slot name (defaults to "sub")
/// - `PUB_NAME`: Publication name (defaults to "pub")
/// - `HTTP_ENDPOINT_URL`: URL for HTTP event sink (optional)
/// - `HOOK0_API_URL`: Hook0 API URL (optional)
/// - `HOOK0_APPLICATION_ID`: Hook0 application UUID (optional)
/// - `HOOK0_API_TOKEN`: Hook0 API token (optional)
///
/// # Returns
///
/// Returns `Ok(())` on successful completion or an error if replication fails.
#[tokio::main]
async fn main() -> ReplicationResult<()> {
    // Initialize structured logging with tracing
    // This sets up logging levels and output formatting
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)      // Use the filter we just configured
        .with_target(false)           // Don't show module targets in logs
        .with_thread_ids(false)       // Don't show thread IDs
        .with_thread_names(false)     // Don't show thread names
        .init();

    // Load replication configuration from environment variables
    // These control which replication slot and publication we use
    let slot_name = env::var("SLOT_NAME").unwrap_or_else(|_| "sub".to_string());
    let publication_name = env::var("PUB_NAME").unwrap_or_else(|_| "pub".to_string());

    info!("Slot name: {}", slot_name);
    info!("Publication name: {}", publication_name);

    // Get the database connection URL from environment
    // This is required for connecting to PostgreSQL
    let database_url = env::var("DATABASE_URL").ok();

    let connection_string = if let Some(url) = database_url {
        url
    } else {
        // If no DATABASE_URL is provided, we can't proceed
        Err(ReplicationError::Configuration {
            message: "Missing DATABASE_URL environment variable".to_string(),
        })?
    };

    info!("Connection string: {}", connection_string);

    // Load optional sink configuration from environment variables
    // These determine where replication events are sent
    let http_endpoint_url = env::var("HTTP_ENDPOINT_URL").ok();
    info!("HTTP endpoint URL from env: {:?}", http_endpoint_url);
    let hook0_api_url = env::var("HOOK0_API_URL").ok();
    info!("Hook0 API URL from env: {:?}", hook0_api_url);

    // Parse Hook0 application ID as a UUID if provided
    let hook0_application_id = env::var("HOOK0_APPLICATION_ID")
        .ok()
        .and_then(|s| Uuid::parse_str(&s).ok());
    info!("Hook0 application ID from env: {:?}", hook0_application_id);

    // Get Hook0 API token from environment (sensitive information)
    let hook0_api_token = env::var("HOOK0_API_TOKEN").ok();
    info!("Hook0 API token from env: {:?}", hook0_api_token);

    // Create the main configuration object with validation
    // This performs various checks on the provided configuration
    let config = ReplicationConfig::new(
        connection_string,
        publication_name,
        slot_name,
        http_endpoint_url,
        hook0_api_url,
        hook0_application_id,
        hook0_api_token,
    )?;

    // Create and run the replication server
    // This is the main execution loop that handles replication
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

/// Helper function to run the replication server
///
/// This function encapsulates the core replication logic:
/// 1. Creates a new ReplicationServer instance with the provided configuration
/// 2. Identifies the PostgreSQL system (verifies connection and gets system info)
/// 3. Creates replication slot and starts the replication process
///
/// # Arguments
///
/// * `config` - The replication configuration containing connection details and settings
///
/// # Returns
///
/// Returns `Ok(())` when replication completes or an error if any step fails
async fn run_replication_server(config: ReplicationConfig) -> ReplicationResult<()> {
    // Create the replication server instance
    // This establishes the database connection and sets up event sinks
    let mut server = ReplicationServer::new(config)?;

    // Identify the PostgreSQL system we're connecting to
    // This verifies the connection supports replication and gets system information
    server
        .identify_system()
        .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;

    // Start the replication process
    // This creates/validates replication slots and begins streaming changes
    server
        .create_replication_slot_and_start()
        .await
        .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;

    Ok(())
}
