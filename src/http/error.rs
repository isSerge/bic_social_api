use axum::response::{IntoResponse, Response};
use reqwest::StatusCode;
use thiserror::Error;
use uuid::Uuid;

use crate::clients::error::ClientError;
use crate::config::ContentTypeRegistryError;
use crate::domain::DomainError;
use crate::repository::error::RepoError;

#[derive(Debug, Error)]
pub enum ApiError {
    /* --- HTTP Specific --- */
    #[error("Missing, malformed, or invalid token")]
    Unauthorized,

    #[error("Rate limit exceeded")]
    RateLimited { retry_after_secs: u64 },

    #[error(transparent)]
    ContentTypeUnknown(#[from] ContentTypeRegistryError),

    #[error("Content item {content_id} of type {content_type} does not exist")]
    ContentNotFound { content_type: String, content_id: Uuid },

    /* --- Layer Compositions --- */
    #[error(transparent)]
    Domain(#[from] DomainError),

    #[error(transparent)]
    Repo(#[from] RepoError),

    #[error(transparent)]
    Client(#[from] ClientError),
}

// Map the layered errors to the spec's exact JSON shape
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            // HTTP specific mappings
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "UNAUTHORIZED", self.to_string()),
            ApiError::RateLimited { .. } => {
                (StatusCode::TOO_MANY_REQUESTS, "RATE_LIMITED", self.to_string())
            }
            ApiError::ContentTypeUnknown(_) => {
                (StatusCode::BAD_REQUEST, "CONTENT_TYPE_UNKNOWN", self.to_string())
            }
            ApiError::ContentNotFound { .. } => {
                (StatusCode::NOT_FOUND, "CONTENT_NOT_FOUND", self.to_string())
            }

            // TODO: double check if these belong to domain
            // Domain layer mappings
            ApiError::Domain(DomainError::ContentNotFound { content_type, content_id }) => (
                StatusCode::NOT_FOUND,
                "CONTENT_NOT_FOUND",
                format!("Content item {} of type {} does not exist", content_id, content_type),
            ),

            ApiError::Domain(DomainError::InvalidTimeWindow(window)) => (
                StatusCode::BAD_REQUEST,
                "INVALID_TIME_WINDOW",
                format!("Invalid time window parameter: {}", window),
            ),

            ApiError::Domain(DomainError::BatchTooLarge { size, max }) => (
                StatusCode::BAD_REQUEST,
                "BATCH_TOO_LARGE",
                format!("Batch size {} exceeds maximum of {}", size, max),
            ),

            ApiError::Domain(DomainError::Repository(_)) => {
                tracing::error!(error = %self, "Repository error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Unexpected internal error".to_string(),
                )
            }

            ApiError::Domain(DomainError::InvalidCursor(cursor)) => (
                StatusCode::BAD_REQUEST,
                "INVALID_CURSOR",
                format!("Invalid pagination cursor: {}", cursor),
            ),

            // Client layer mappings
            ApiError::Client(ClientError::DependencyUnavailable(_)) => {
                (StatusCode::SERVICE_UNAVAILABLE, "DEPENDENCY_UNAVAILABLE", self.to_string())
            }

            // 500 Internal Server Errors (DB, Redis, unknown Reqwest errors)
            ApiError::Repo(_) | ApiError::Client(_) => {
                tracing::error!(error = %self, "Internal server error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Unexpected internal error".to_string(),
                )
            }
        };

        let body = axum::Json(serde_json::json!({
            "error": {
                "code": code,
                "message": message,
            }
        }));

        let mut response = (status, body).into_response();

        if let ApiError::RateLimited { retry_after_secs } = self {
            response.headers_mut().insert(reqwest::header::RETRY_AFTER, retry_after_secs.into());
        }

        response
    }
}
