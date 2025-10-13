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
mod config;        // Configuration loading from environment variables
mod email_config;  // Configuration for email notifications (not currently used)
mod errors;        // Comprehensive error types for the application
mod event_sink;    // Output handlers for sending events to various destinations
mod parser;        // Protocol message parser for decoding WAL data
mod server;        // Main replication server implementation
mod types;         // Data structures and type definitions
mod utils;         // Utility functions for PostgreSQL integration

// Import the core types and functionality we need
use crate::types::ReplicationConfig;
use crate::server::ReplicationServer;
use clap::Parser;              // Command line argument parsing
use errors::ReplicationResult; // Result type alias for error handling
use std::sync::Arc;            // Arc for shared ownership
use std::sync::atomic::{AtomicBool, Ordering}; // Atomic flag for shutdown signaling
use tracing::{error, info, warn};    // Structured logging
use tracing_subscriber::{EnvFilter, fmt}; // Log formatting and filtering

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
/// - `EVENT_SINK`: Event sink type - "http", "hook0", or "stdout" (optional, defaults to "stdout")
/// - `HTTP_ENDPOINT_URL`: URL for HTTP event sink (optional, required when using "http" service)
/// - `HOOK0_API_URL`: Hook0 API URL (optional, required when using "hook0" service)
/// - `HOOK0_APPLICATION_ID`: Hook0 application UUID (optional, required when using "hook0" service)
/// - `HOOK0_API_TOKEN`: Hook0 API token (optional, required when using "hook0" service)
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
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .init();

    // Create a shutdown signal that can be shared across the application
    let shutdown_signal = Arc::new(AtomicBool::new(false));

    // Set up signal handling for graceful shutdown
    let signal_handler_shutdown = shutdown_signal.clone();
    tokio::spawn(async move {
        // Wait for SIGTERM or SIGINT signals
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
            let mut sigint = signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");
            
            tokio::select! {
                _ = sigterm.recv() => {
                    warn!("Received SIGTERM signal, initiating graceful shutdown");
                    signal_handler_shutdown.store(true, Ordering::SeqCst);
                }
                _ = sigint.recv() => {
                    warn!("Received SIGINT signal, initiating graceful shutdown");
                    signal_handler_shutdown.store(true, Ordering::SeqCst);
                }
            }
        }
        
        #[cfg(not(unix))]
        {
            // For Windows, we'll use Ctrl-C
            tokio::signal::ctrl_c().await.expect("Failed to setup Ctrl-C handler");
            warn!("Received Ctrl-C, initiating graceful shutdown");
            signal_handler_shutdown.store(true, Ordering::SeqCst);
        }
    });

    // Load configuration from environment variables
    let config = config::load_config_from_env()?;

    match run_replication_server(config, shutdown_signal).await {
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

// Include test module for graceful shutdown functionality
#[cfg(test)]
mod test_graceful_shutdown;

/// Helper function to run the replication server
///
/// This function encapsulates the core replication logic:
/// 1. Creates a new ReplicationServer instance with the provided configuration
/// 2. Identifies the PostgreSQL system (verifies connection and gets system info)
/// 3. Creates replication slot and starts the replication process
/// 4. Handles graceful shutdown when signaled
///
/// # Arguments
///
/// * `config` - The replication configuration containing connection details and settings
/// * `shutdown_signal` - Shared atomic flag to signal shutdown
///
/// # Returns
///
/// Returns `Ok(())` when replication completes or an error if any step fails
async fn run_replication_server(
    config: ReplicationConfig,
    shutdown_signal: Arc<AtomicBool>,
) -> ReplicationResult<()> {
    let mut server = ReplicationServer::new(config, shutdown_signal)?;

    server
        .identify_system()
        .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;

    server
        .create_replication_slot_and_start()
        .await
        .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;

    Ok(())
}
