use axum::response::IntoResponse;
use reqwest::StatusCode;

/// GET /health/live
pub async fn live() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// GET /health/ready
pub async fn ready() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}
