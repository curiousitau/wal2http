//! Configuration module for PostgreSQL logical replication
//!
//! This module handles loading configuration from environment variables
//! and creating a ReplicationConfig struct with proper validation.

use crate::errors::ReplicationError;
use crate::types::ReplicationConfig;
use std::env;
use tracing::info;
use uuid::Uuid;

/// Loads replication configuration from environment variables
///
/// This function reads all necessary configuration from environment variables
/// and returns a validated ReplicationConfig. It handles default values
/// and performs validation on all inputs.
///
/// # Environment Variables
///
/// - `DATABASE_URL`: PostgreSQL connection string (required)
/// - `SLOT_NAME`: Replication slot name (defaults to "sub")
/// - `PUB_NAME`: Publication name (defaults to "pub")
/// - `EVENT_SINK`: Event sink type - "http", "hook0", or "stdout" (optional)
/// - `HTTP_ENDPOINT_URL`: URL for HTTP event sink (optional, required when using "http" service)
/// - `HOOK0_API_URL`: Hook0 API URL (optional, required when using "hook0" service)
/// - `HOOK0_APPLICATION_ID`: Hook0 application UUID (optional, required when using "hook0" service)
/// - `HOOK0_API_TOKEN`: Hook0 API token (optional, required when using "hook0" service)
///
/// # Returns
///
/// Returns a `ReplicationResult<ReplicationConfig>` containing the validated configuration
/// or an error if required variables are missing or invalid.
pub fn load_config_from_env() -> Result<ReplicationConfig, ReplicationError> {
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
        return Err(ReplicationError::Configuration {
            message: "Missing DATABASE_URL environment variable".to_string(),
        });
    };

    info!("Connection string: {}", connection_string);

    // Load event sink specification from environment variable
    // This determines which event sink to use
    let event_sink = env::var("EVENT_SINK").ok();
    info!("Event sink from env: {:?}", event_sink);

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

    // Create and return the configuration
    ReplicationConfig::new(
        connection_string,
        publication_name,
        slot_name,
        event_sink,
        http_endpoint_url,
        hook0_api_url,
        hook0_application_id,
        hook0_api_token,
    )
}