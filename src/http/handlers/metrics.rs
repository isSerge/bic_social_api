use axum::response::IntoResponse;
use reqwest::StatusCode;

/// GET /metrics
pub async fn metrics() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}
