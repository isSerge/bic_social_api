use axum::response::{IntoResponse, Response};
use reqwest::StatusCode;
use serde_json::{Value, json};
use thiserror::Error;

use crate::clients::error::ClientError;
use crate::config::ContentTypeRegistryError;
use crate::domain::DomainError;
use crate::http::observability::ReadinessReport;
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

    #[error("One or more required dependencies are unavailable")]
    ReadinessFailed { report: ReadinessReport },

    #[error("Failed to encode Prometheus metrics")]
    MetricsEncoding,

    /* --- Layer Compositions --- */
    #[error(transparent)]
    Domain(#[from] DomainError),

    #[error(transparent)]
    Repo(#[from] RepoError),

    #[error(transparent)]
    Client(#[from] ClientError),
}

// TODO: add unit tests
// Map the layered errors to the spec's exact JSON shape
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message, details) = match &self {
            // HTTP specific mappings
            ApiError::Unauthorized => {
                (StatusCode::UNAUTHORIZED, "UNAUTHORIZED", self.to_string(), json!({}))
            }
            ApiError::RateLimited { retry_after_secs } => (
                StatusCode::TOO_MANY_REQUESTS,
                "RATE_LIMITED",
                self.to_string(),
                json!({ "retry_after_secs": retry_after_secs }),
            ),
            ApiError::ContentTypeUnknown(ContentTypeRegistryError::UnknownContentType(raw)) => (
                StatusCode::BAD_REQUEST,
                "CONTENT_TYPE_UNKNOWN",
                self.to_string(),
                json!({ "content_type": raw }),
            ),
            ApiError::ReadinessFailed { report } => (
                StatusCode::SERVICE_UNAVAILABLE,
                "DEPENDENCY_UNAVAILABLE",
                self.to_string(),
                json!({ "failures": report.failures }),
            ),
            ApiError::MetricsEncoding => {
                tracing::error!(error = %self, "Metrics encoding error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Unexpected internal error".to_string(),
                    json!({}),
                )
            }

            // TODO: double check if these belong to domain
            // Domain layer mappings
            ApiError::Domain(DomainError::ContentNotFound { content_type, content_id }) => (
                StatusCode::NOT_FOUND,
                "CONTENT_NOT_FOUND",
                format!("Content item {} of type {} does not exist", content_id, content_type),
                json!({
                    "content_type": content_type,
                    "content_id": content_id,
                }),
            ),

            ApiError::Domain(DomainError::InvalidTimeWindow(window)) => (
                StatusCode::BAD_REQUEST,
                "INVALID_TIME_WINDOW",
                format!("Invalid time window parameter: {}", window),
                json!({ "window": window }),
            ),

            ApiError::Domain(DomainError::BatchTooLarge { size, max }) => (
                StatusCode::BAD_REQUEST,
                "BATCH_TOO_LARGE",
                format!("Batch size {} exceeds maximum of {}", size, max),
                json!({ "size": size, "max": max }),
            ),

            ApiError::Domain(DomainError::Repository(_)) => {
                tracing::error!(error = %self, "Repository error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Unexpected internal error".to_string(),
                    json!({}),
                )
            }

            ApiError::Domain(DomainError::InvalidCursor(cursor)) => (
                StatusCode::BAD_REQUEST,
                "INVALID_CURSOR",
                format!("Invalid pagination cursor: {}", cursor),
                json!({ "cursor": cursor }),
            ),

            ApiError::Domain(DomainError::Client(ClientError::DependencyUnavailable(_))) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "DEPENDENCY_UNAVAILABLE",
                self.to_string(),
                json!({}),
            ),

            ApiError::Domain(DomainError::Client(ClientError::NotFound)) => {
                (StatusCode::NOT_FOUND, "CONTENT_NOT_FOUND", self.to_string(), json!({}))
            }

            ApiError::Domain(DomainError::Client(ClientError::Http(_))) => {
                tracing::error!(error = %self, "HTTP client error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Unexpected internal error".to_string(),
                    json!({}),
                )
            }

            // Client layer mappings
            ApiError::Client(ClientError::DependencyUnavailable(_)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "DEPENDENCY_UNAVAILABLE",
                self.to_string(),
                json!({}),
            ),

            // 500 Internal Server Errors (DB, Redis, unknown Reqwest errors)
            ApiError::Repo(_) | ApiError::Client(_) => {
                tracing::error!(error = %self, "Internal server error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Unexpected internal error".to_string(),
                    json!({}),
                )
            }
        };

        let body = axum::Json(json!({
            "error": {
                "code": code,
                "message": message,
                "details": normalize_details(details),
            }
        }));

        let mut response = (status, body).into_response();

        if let ApiError::RateLimited { retry_after_secs } = self {
            response.headers_mut().insert(reqwest::header::RETRY_AFTER, retry_after_secs.into());
        }

        response
    }
}

fn normalize_details(details: Value) -> Value {
    match details {
        Value::Object(_) => details,
        _ => json!({}),
    }
}

// TODO: add tets
