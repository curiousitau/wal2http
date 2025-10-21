//! Core module containing fundamental types and configurations
//!
//! This module provides the basic building blocks for the walpipe application,
//! including configuration management, error handling, and common types.

pub mod config;
pub mod email_config;
pub mod errors;

// Re-export for convenience
pub use config::ReplicationConfig;
pub use errors::{ReplicationError, ReplicationResult};