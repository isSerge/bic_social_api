use axum::{http::StatusCode, response::IntoResponse};

/// GET /metrics
pub async fn metrics() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}
