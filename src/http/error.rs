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

// Map the layered errors to the spec's exact JSON shape
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        /// Internal struct to define the shape of the error response
        struct ErrorResponseSpec {
            status: StatusCode,
            code: &'static str,
            message: String,
            details: Option<Value>,
        }

        // Helper closures to reduce boilerplate when creating response specs with or without details
        let with_details = |status, code, message, details| ErrorResponseSpec {
            status,
            code,
            message,
            details: Some(details),
        };
        let without_details =
            |status, code, message| ErrorResponseSpec { status, code, message, details: None };

        let spec = match &self {
            // HTTP specific mappings
            ApiError::Unauthorized => {
                without_details(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", self.to_string())
            }
            ApiError::RateLimited { retry_after_secs } => with_details(
                StatusCode::TOO_MANY_REQUESTS,
                "RATE_LIMITED",
                self.to_string(),
                json!({ "retry_after_secs": retry_after_secs }),
            ),
            ApiError::ContentTypeUnknown(ContentTypeRegistryError::UnknownContentType(raw)) => {
                with_details(
                    StatusCode::BAD_REQUEST,
                    "CONTENT_TYPE_UNKNOWN",
                    self.to_string(),
                    json!({ "content_type": raw }),
                )
            }
            ApiError::ReadinessFailed { report } => with_details(
                StatusCode::SERVICE_UNAVAILABLE,
                "DEPENDENCY_UNAVAILABLE",
                self.to_string(),
                json!({ "failures": report.failures }),
            ),
            ApiError::MetricsEncoding => {
                tracing::error!(error = %self, "Metrics encoding error");
                without_details(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Unexpected internal error".to_string(),
                )
            }

            // Domain layer mappings
            ApiError::Domain(DomainError::ContentNotFound { content_type, content_id }) => {
                with_details(
                    StatusCode::NOT_FOUND,
                    "CONTENT_NOT_FOUND",
                    format!("Content item {} of type {} does not exist", content_id, content_type),
                    json!({
                        "content_type": content_type,
                        "content_id": content_id,
                    }),
                )
            }

            ApiError::Domain(DomainError::InvalidTimeWindow(window)) => with_details(
                StatusCode::BAD_REQUEST,
                "INVALID_TIME_WINDOW",
                format!("Invalid time window parameter: {}", window),
                json!({ "window": window }),
            ),

            ApiError::Domain(DomainError::BatchTooLarge { size, max }) => with_details(
                StatusCode::BAD_REQUEST,
                "BATCH_TOO_LARGE",
                format!("Batch size {} exceeds maximum of {}", size, max),
                json!({ "size": size, "max": max }),
            ),

            ApiError::Domain(DomainError::Repository(_)) => {
                tracing::error!(error = %self, "Repository error");
                without_details(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Unexpected internal error".to_string(),
                )
            }

            ApiError::Domain(DomainError::InvalidCursor(cursor)) => with_details(
                StatusCode::BAD_REQUEST,
                "INVALID_CURSOR",
                format!("Invalid pagination cursor: {}", cursor),
                json!({ "cursor": cursor }),
            ),

            ApiError::Domain(DomainError::Client(ClientError::DependencyUnavailable(_))) => {
                without_details(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "DEPENDENCY_UNAVAILABLE",
                    self.to_string(),
                )
            }

            ApiError::Domain(DomainError::Client(ClientError::NotFound)) => {
                without_details(StatusCode::NOT_FOUND, "CONTENT_NOT_FOUND", self.to_string())
            }

            ApiError::Domain(DomainError::Client(ClientError::Http(_))) => {
                tracing::error!(error = %self, "HTTP client error");
                without_details(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Unexpected internal error".to_string(),
                )
            }

            // Client layer mappings
            ApiError::Client(ClientError::DependencyUnavailable(_)) => without_details(
                StatusCode::SERVICE_UNAVAILABLE,
                "DEPENDENCY_UNAVAILABLE",
                self.to_string(),
            ),

            // 500 Internal Server Errors (DB, Redis, unknown Reqwest errors)
            ApiError::Repo(_) | ApiError::Client(_) => {
                tracing::error!(error = %self, "Internal server error");
                without_details(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Unexpected internal error".to_string(),
                )
            }
        };

        let body = axum::Json(json!({
            "error": {
                "code": spec.code,
                "message": spec.message,
                "details": normalize_details(spec.details.unwrap_or_else(|| json!({}))),
            }
        }));

        let mut response = (spec.status, body).into_response();

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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use serde_json::Value;
    use uuid::Uuid;

    async fn response_json(response: Response) -> Value {
        let body_bytes = to_bytes(response.into_body(), usize::MAX).await.expect("body bytes");
        serde_json::from_slice(&body_bytes).expect("valid json body")
    }

    #[tokio::test]
    async fn unauthorized_error_returns_expected_response() {
        let response = ApiError::Unauthorized.into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(response.headers().get(reqwest::header::RETRY_AFTER).is_none());

        let body = response_json(response).await;
        assert_eq!(
            body,
            json!({
                "error": {
                    "code": "UNAUTHORIZED",
                    "message": "Missing, malformed, or invalid token",
                    "details": {}
                }
            })
        );
    }

    #[tokio::test]
    async fn rate_limited_error_sets_retry_after_header_and_details() {
        let response = ApiError::RateLimited { retry_after_secs: 42 }.into_response();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get(reqwest::header::RETRY_AFTER).unwrap(), "42");

        let body = response_json(response).await;
        assert_eq!(
            body,
            json!({
                "error": {
                    "code": "RATE_LIMITED",
                    "message": "Rate limit exceeded",
                    "details": {
                        "retry_after_secs": 42
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn content_not_found_error_returns_expected_details() {
        let content_id = Uuid::new_v4();
        let response = ApiError::Domain(DomainError::ContentNotFound {
            content_type: "post".to_string(),
            content_id,
        })
        .into_response();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = response_json(response).await;
        assert_eq!(
            body,
            json!({
                "error": {
                    "code": "CONTENT_NOT_FOUND",
                    "message": format!("Content item {} of type post does not exist", content_id),
                    "details": {
                        "content_type": "post",
                        "content_id": content_id,
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn readiness_failed_error_includes_failures() {
        let mut report = ReadinessReport::default();
        report.record_failure("redis", "redis probe failed: timeout");
        report.record_failure("content_api", "no content API upstreams configured");

        let response = ApiError::ReadinessFailed { report }.into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = response_json(response).await;
        assert_eq!(
            body,
            json!({
                "error": {
                    "code": "DEPENDENCY_UNAVAILABLE",
                    "message": "One or more required dependencies are unavailable",
                    "details": {
                        "failures": [
                            {
                                "dependency": "redis",
                                "message": "redis probe failed: timeout"
                            },
                            {
                                "dependency": "content_api",
                                "message": "no content API upstreams configured"
                            }
                        ]
                    }
                }
            })
        );
    }

    #[tokio::test]
    async fn metrics_encoding_error_returns_generic_internal_response() {
        let response = ApiError::MetricsEncoding.into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = response_json(response).await;
        assert_eq!(
            body,
            json!({
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "Unexpected internal error",
                    "details": {}
                }
            })
        );
    }
}
