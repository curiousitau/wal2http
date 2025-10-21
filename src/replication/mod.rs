//! Replication module for PostgreSQL logical replication
//!
//! This module contains the core replication server implementation that manages
//! the complete logical replication lifecycle, including database connection,
//! replication slot management, WAL streaming, and event processing.

pub mod server;
pub mod state;

// Re-export for convenience
pub use server::ReplicationServer;
