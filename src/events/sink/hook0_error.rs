//! Hook0 error types
//!
//! Custom error types for Hook0 event sink operations.

use thiserror::Error;

/// Errors that can occur during Hook0 operations
#[derive(Error, Debug)]
pub enum Hook0Error {
    #[error("Hook0 API error: {0}")]
    ApiError(String),

    #[error("Hook0 authentication failed")]
    AuthenticationError,

    #[error("Hook0 configuration error: {0}")]
    ConfigurationError(String),

    #[error("Hook0 network error: {0}")]
    NetworkError(String),
}