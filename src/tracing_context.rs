//! Correlation ID and tracing context management
//!
//! This module provides utilities for generating and managing correlation IDs
//! that allow tracing of requests and events throughout the system.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use tracing::{Span, instrument};
use uuid::Uuid;

/// Global counter for generating sequential correlation IDs
static CORRELATION_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A correlation ID that uniquely identifies a request or transaction chain
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CorrelationId(String);

impl CorrelationId {
    /// Generate a new correlation ID using timestamp and counter
    pub fn new() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let counter = CORRELATION_COUNTER.fetch_add(1, Ordering::SeqCst);

        // Format: timestamp-counter for readability and uniqueness
        let id = format!("{}-{}", timestamp, counter);
        CorrelationId(id)
    }

    /// Generate a UUID-based correlation ID for distributed systems
    pub fn new_uuid() -> Self {
        CorrelationId(Uuid::new_v4().to_string())
    }

    /// Create a correlation ID from a string (useful for received IDs)
    pub fn from_string(id: String) -> Self {
        CorrelationId(id)
    }

    /// Get the correlation ID as a string reference
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume and return the inner string
    pub fn into_string(self) -> String {
        self.0
    }
}

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A tracing context that holds correlation and span information
#[derive(Debug, Clone)]
pub struct TracingContext {
    pub correlation_id: CorrelationId,
    pub span: Span,
}

impl TracingContext {
    /// Create a new tracing context with a generated correlation ID
    pub fn new() -> Self {
        let correlation_id = CorrelationId::new();
        let span = tracing::info_span!(
            "replication_context",
            correlation_id = %correlation_id,
            component = "wal2http"
        );

        Self {
            correlation_id,
            span,
        }
    }

    /// Create a tracing context with a specific correlation ID
    pub fn with_correlation_id(correlation_id: CorrelationId) -> Self {
        let span = tracing::info_span!(
            "replication_context",
            correlation_id = %correlation_id,
            component = "wal2http"
        );

        Self {
            correlation_id,
            span,
        }
    }

    /// Create a child context for a specific operation
    pub fn child_context(&self, operation: &str) -> Self {
        let span = tracing::info_span!(
            "replication_operation",
            correlation_id = %self.correlation_id,
            operation = operation,
            component = "wal2http"
        );

        Self {
            correlation_id: self.correlation_id.clone(),
            span,
        }
    }

    /// Enter the span and execute a function
    pub fn with_span<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let _guard = self.span.enter();
        f()
    }

    /// Enter the span and execute an async function
    pub async fn with_span_async<F, R>(&self, f: F) -> R
    where
        F: std::future::Future<Output = R>,
    {
        let _guard = self.span.enter();
        f.await
    }
}

/// A trait for types that can be associated with a tracing context
pub trait WithTracingContext {
    fn get_tracing_context(&self) -> Option<&TracingContext>;
    fn set_tracing_context(&mut self, context: TracingContext);
}

/// Extension trait for adding correlation ID information to tracing events
pub trait TracingExt {
    /// Add correlation ID to the current span
    fn with_correlation_id(&self, correlation_id: &CorrelationId) -> &Self;

    /// Add event-specific fields to the current span
    fn with_event_fields(&self, fields: &[(&str, &str)]) -> &Self;
}

impl TracingExt for tracing::Span {
    fn with_correlation_id(&self, correlation_id: &CorrelationId) -> &Self {
        self.record("correlation_id", &correlation_id.as_str());
        self
    }

    fn with_event_fields(&self, fields: &[(&str, &str)]) -> &Self {
        for (key, value) in fields {
            self.record(*key, value);
        }
        self
    }
}

/// Create a new tracing context and execute a function within it
#[instrument(skip_all)]
pub async fn with_tracing_context<F, R>(f: F) -> R
where
    F: FnOnce(&TracingContext) -> R,
{
    let context = TracingContext::new();
    context.with_span(|| f(&context))
}

/// Create a tracing context with a specific correlation ID and execute a function
#[instrument(skip_all)]
pub async fn with_tracing_context_id<F, R>(correlation_id: CorrelationId, f: F) -> R
where
    F: FnOnce(&TracingContext) -> R,
{
    let context = TracingContext::with_correlation_id(correlation_id);
    context.with_span(|| f(&context))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_correlation_id_generation() {
        let id1 = CorrelationId::new();
        let id2 = CorrelationId::new();

        assert_ne!(id1, id2);
        assert!(!id1.as_str().is_empty());
        assert!(!id2.as_str().is_empty());
    }

    #[test]
    fn test_correlation_id_uuid() {
        let id1 = CorrelationId::new_uuid();
        let id2 = CorrelationId::new_uuid();

        assert_ne!(id1, id2);
        assert!(!id1.as_str().is_empty());
        assert!(!id2.as_str().is_empty());
    }

    #[test]
    fn test_correlation_id_from_string() {
        let test_id = "test-123";
        let id = CorrelationId::from_string(test_id.to_string());

        assert_eq!(id.as_str(), test_id);
    }

    #[test]
    fn test_tracing_context() {
        let context = TracingContext::new();
        let child_context = context.child_context("test_operation");

        assert_eq!(context.correlation_id, child_context.correlation_id);
    }
}