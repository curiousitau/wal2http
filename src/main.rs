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

    // Check for required environment variables
    let slot_name = env::var("SLOT_NAME").unwrap_or_else(|_| "sub".to_string());
    let publication_name = env::var("PUB_NAME").unwrap_or_else(|_| "pub".to_string());

    info!("Slot name: {}", slot_name);
    info!("Publication name: {}", publication_name);

    // If DATABASE_URL exists in env vars and no connection params were passed,
    // we'll create a param from it to maintain backward compatibility with existing Docker setup.
    // But for consistent approach, we should make the DATABASE_URL be directly read as an env var
    let database_url = env::var("DATABASE_URL").ok();

    let connection_string = if let Some(url) = database_url {
        url // Use DATABASE_URL directly from environment
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

async fn run_replication_server(config: ReplicationConfig) -> ReplicationResult<()> {
    let mut server = ReplicationServer::new(config)?;

    server
        .identify_system()
        .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;
    server
        .create_replication_slot_and_start()
        .await
        .map_err(|e| crate::errors::ReplicationError::Other(e.into()))?;

    Ok(())
}
