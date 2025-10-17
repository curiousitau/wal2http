//! Event sink implementations for PostgreSQL logical replication
//!
//! Provides various event sink implementations for sending replication events
//! to different destinations including HTTP endpoints, Hook0, and STDOUT.

use crate::core::errors::ReplicationResult;
use crate::protocol::messages::ReplicationMessage;
use async_trait::async_trait;

pub mod event_formatter;
pub mod hook0;
pub mod hook0_error;
pub mod http;
pub mod pg_type_conversion;
pub mod stdout;

/// Registry for managing and creating event sinks
pub struct EventSinkRegistry;

impl EventSinkRegistry {
    /// Create an event sink based on configuration
    pub fn create_sink(
        sink_type: &str,
        config: &crate::core::config::ReplicationConfig,
    ) -> ReplicationResult<Box<dyn super::EventSink + Send + Sync>> {
        match sink_type.to_lowercase().as_str() {
            "http" => {
                if let Some(ref url) = config.http_endpoint_url {
                    let http_config = http::HttpEventSinkConfig {
                        endpoint_url: url.clone(),
                    };
                    let sink = http::HttpEventSink::new(http_config)?;
                    Ok(Box::new(sink) as Box<dyn super::EventSink + Send + Sync>)
                } else {
                    Err(crate::core::errors::ReplicationError::config(
                        "HTTP endpoint URL required for HTTP sink",
                    ))
                }
            }
            "hook0" => {
                if let (Some(ref api_url), Some(app_id), Some(ref api_token)) = (
                    config.hook0_api_url.as_ref(),
                    config.hook0_application_id,
                    config.hook0_api_token.as_ref(),
                ) {
                    let hook0_config = hook0::Hook0EventSinkConfig {
                        api_url: api_url.clone(),
                        application_id: *app_id,
                        api_token: api_token.clone(),
                    };
                    let sink = hook0::Hook0EventSink::new(hook0_config)?;
                    Ok(Box::new(sink) as Box<dyn super::EventSink + Send + Sync>)
                } else {
                    Err(crate::core::errors::ReplicationError::config(
                        "Hook0 API URL, application ID, and token required for Hook0 sink",
                    ))
                }
            }
            "stdout" | _ => {
                // Default to stdout for unknown or empty sink type
                let sink = stdout::StdoutEventSink {};
                Ok(Box::new(sink) as Box<dyn super::EventSink + Send + Sync>)
            }
        }
    }
}