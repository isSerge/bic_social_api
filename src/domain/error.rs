use thiserror::Error;
use uuid::Uuid;

use crate::repository::error::RepoError;

/// Domain-level errors that can occur during like/unlike operations
#[derive(Debug, Error)]
pub enum DomainError {
    /// Represents a case where a requested content item does not exist in the system.
    #[error("Content item {content_id} of type {content_type} does not exist")]
    ContentNotFound { content_type: String, content_id: Uuid },

    /// Wraps repository layer errors that occur during like operations.
    #[error(transparent)]
    Repository(#[from] RepoError),

    /// Represents an error when a batch request exceeds the maximum allowed size.
    #[error("Batch request size {size} exceeds maximum allowed {max}")]
    BatchTooLarge { size: usize, max: usize },

    /// Represents an error when an invalid time window parameter is provided.
    #[error("Invalid time window specified: {0}")]
    InvalidTimeWindow(String),

    /// Represents an error when a pagination cursor is invalid or cannot be decoded.
    #[error("Invalid pagination cursor: {0}")]
    InvalidCursor(String),
}
