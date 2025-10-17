//! Hook0 error types
//!
//! Provides error handling for Hook0 API responses and operations.

/// Error IDs that can be returned by the Hook0 API
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Hook0ErrorId {
    /// Event type does not exist or was deactivated
    EventTypeDoesNotExist,
    /// Event has already been ingested
    EventAlreadyIngested,
    /// Invalid event ID provided
    InvalidEventId,
    /// Invalid payload provided
    InvalidPayload,
    /// Unauthorized access
    Unauthorized,
    /// Rate limit exceeded
    RateLimitExceeded,
    /// Internal server error
    InternalServerError,
}

impl Hook0ErrorId {
    /// Get the string representation of the error ID
    pub fn as_str(&self) -> &'static str {
        match self {
            Hook0ErrorId::EventTypeDoesNotExist => "EventTypeDoesNotExist",
            Hook0ErrorId::EventAlreadyIngested => "EventAlreadyIngested",
            Hook0ErrorId::InvalidEventId => "InvalidEventId",
            Hook0ErrorId::InvalidPayload => "InvalidPayload",
            Hook0ErrorId::Unauthorized => "Unauthorized",
            Hook0ErrorId::RateLimitExceeded => "RateLimitExceeded",
            Hook0ErrorId::InternalServerError => "InternalServerError",
        }
    }
}

impl From<&str> for Hook0ErrorId {
    fn from(s: &str) -> Self {
        match s {
            "EventTypeDoesNotExist" => Hook0ErrorId::EventTypeDoesNotExist,
            "EventAlreadyIngested" => Hook0ErrorId::EventAlreadyIngested,
            "InvalidEventId" => Hook0ErrorId::InvalidEventId,
            "InvalidPayload" => Hook0ErrorId::InvalidPayload,
            "Unauthorized" => Hook0ErrorId::Unauthorized,
            "RateLimitExceeded" => Hook0ErrorId::RateLimitExceeded,
            "InternalServerError" => Hook0ErrorId::InternalServerError,
            _ => {
                // For unknown error IDs, we'll treat them as a generic internal error
                Hook0ErrorId::InternalServerError
            }
        }
    }
}