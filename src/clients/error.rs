use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    /// Circuit breaker is open for the target service
    #[error("Dependency unavailable: {0}")]
    DependencyUnavailable(String),

    /// Content not found (e.g., 404 from external service)
    #[error("Content not found")]
    NotFound,

    /// HTTP client errors (e.g., timeouts, connection issues)
    #[error("HTTP client error: {0}")]
    Http(#[from] reqwest::Error),
}
