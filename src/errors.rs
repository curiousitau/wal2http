//! Comprehensive error types for PostgreSQL replication checker
//! Provides structured error handling using thiserror for better error reporting

use thiserror::Error;

/// Main error type for the PostgreSQL replication checker application
#[derive(Error, Debug)]
pub enum ReplicationError {
    /// Database connection related errors
    #[error("Database connection error: {message}")]
    Connection {
        message: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Configuration related errors
    #[error("Configuration error: {message}")]
    Configuration { message: String },

    /// Message parsing errors
    #[error("Message parsing error: {message}")]
    MessageParsing {
        message: String,
        context: Option<String>,
    },

    /// Protocol errors
    #[error("Protocol error: {message}")]
    Protocol {
        message: String,
        context: Option<String>,
    },

    /// Buffer operation errors
    #[error("Buffer operation error: {message}")]
    BufferOperation { message: String },

    /// Network/IO related errors
    #[error("Network IO error")]
    NetworkIO(#[from] std::io::Error),

    /// String conversion errors
    #[error("String conversion error")]
    StringConversion(#[from] std::string::FromUtf8Error),

    /// C string conversion errors
    #[error("C string conversion error")]
    CStringConversion(#[from] std::ffi::NulError),

    /// Task execution errors for async operations
    #[error("Task execution error")]
    TaskExecution(#[from] tokio::task::JoinError),

    #[error("Sink error")]
    Sink { message: String, sink: String },

    /// Generic error for compatibility
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Result type alias for convenience
pub type ReplicationResult<T> = std::result::Result<T, ReplicationError>;

impl ReplicationError {
    /// Create a connection error with context
    pub fn connection<S: Into<String>>(message: S) -> Self {
        Self::Connection {
            message: message.into(),
            source: None,
        }
    }

    /// Create a configuration error
    pub fn config<S: Into<String>>(message: S) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    /// Create a message parsing error
    pub fn parse<S: Into<String>>(message: S) -> Self {
        Self::MessageParsing {
            message: message.into(),
            context: None,
        }
    }

    /// Create a message parsing error with context
    pub fn parse_with_context<S: Into<String>, C: Into<String>>(message: S, context: C) -> Self {
        Self::MessageParsing {
            message: message.into(),
            context: Some(context.into()),
        }
    }

    /// Create a protocol error
    pub fn protocol<S: Into<String>>(message: S) -> Self {
        Self::Protocol {
            message: message.into(),
            context: None,
        }
    }

    /// Create a buffer operation error
    pub fn buffer<S: Into<String>>(message: S) -> Self {
        Self::BufferOperation {
            message: message.into(),
        }
    }
}
