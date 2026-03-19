use axum::response::IntoResponse;
use reqwest::StatusCode;

/// POST /v1/likes
pub async fn like() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// DELETE /v1/likes/{content_type}/{content_id}
pub async fn unlike() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// GET /v1/likes/{content_type}/{content_id}/count
pub async fn get_count() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// GET /v1/likes/{content_type}/{content_id}/status
pub async fn get_status() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// GET /v1/likes/user
pub async fn get_user_likes() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// POST /v1/likes/batch/counts
pub async fn batch_counts() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// POST /v1/likes/batch/statuses
pub async fn batch_statuses() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// GET /v1/likes/top
pub async fn top_liked() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// GET /v1/likes/stream
pub async fn sse_stream() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}
