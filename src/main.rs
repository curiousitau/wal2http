//! PostgreSQL Replication Checker - Rust Edition
//!
//! A Rust implementation of a PostgreSQL logical replication client that connects to a database,
//! creates replication slots, and displays changes in real-time.
//!
//! Based on the C++ implementation: https://github.com/fkfk000/replication_checker

mod buffer;
mod email_config;
mod errors;
mod event_sink;
mod parser;
mod server;
mod types;
mod utils;

use crate::types::ReplicationConfig;
use crate::{errors::ReplicationError, server::ReplicationServer};
use clap::Parser;
use errors::ReplicationResult;
use std::env;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt};

use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(
    name = "wal2http",
    about = "PostgreSQL Logical Replication Checker in Rust",
    version = "0.1.0"
)]
struct Args {
    /// Database connection parameters (space-separated key=value pairs)
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
    // Initialize tracing
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
            use std::sync::atomic::Ordering;

            use tokio::signal::unix::{SignalKind, signal};
            use tracing::warn;
            let mut sigterm =
                signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
            let mut sigint =
                signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");

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
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to setup Ctrl-C handler");
            warn!("Received Ctrl-C, initiating graceful shutdown");
            signal_handler_shutdown.store(true, Ordering::SeqCst);
        }
    });

    // Load replication configuration from environment variables
    // These control which replication slot and publication we use
    let slot_name = env::var("SLOT_NAME").unwrap_or_else(|_| "sub".to_string());
    let publication_name = env::var("PUB_NAME").unwrap_or_else(|_| "pub".to_string());

    info!("Slot name: {}", slot_name);
    info!("Publication name: {}", publication_name);

    // If DATABASE_URL exists in env vars and no connection params were passed,
    // we'll create a param from it to maintain backward compatibility with existing Docker setup.
    // But for consistent approach, we should make the DATABASE_URL be directly read as an env var
    let database_url = env::var("DATABASE_URL").ok();

    let connection_string = if let Some(url) = database_url {
        url
    } else {
        Err(ReplicationError::Configuration {
            message: "Missing DATABASE_URL environment variable".to_string(),
        })?
    };

    info!("Connection string: {}", connection_string);

    // Create configuration with validation
    let http_endpoint_url = env::var("HTTP_ENDPOINT_URL").ok();
    info!("HTTP endpoint URL from env: {:?}", http_endpoint_url);
    let hook0_api_url = env::var("HOOK0_API_URL").ok();
    info!("Hook0 API URL from env: {:?}", hook0_api_url);

    // Attempt to parse Hook0 application ID from environment
    let hook0_application_id = env::var("HOOK0_APPLICATION_ID")
        .ok()
        .and_then(|s| Uuid::parse_str(&s).ok());
    info!("Hook0 application ID from env: {:?}", hook0_application_id);

    // Get Hook0 API token from environment
    let hook0_api_token = env::var("HOOK0_API_TOKEN").ok();
    info!("Hook0 API token from env: {:?}", hook0_api_token);

    let config = ReplicationConfig::new(
        connection_string,
        publication_name,
        slot_name,
        http_endpoint_url,
        hook0_api_url,
        hook0_application_id,
        hook0_api_token,
    )?;

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
