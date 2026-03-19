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
    // TODO: add more domain-specific errors
}
