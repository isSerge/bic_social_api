use std::os::macos::raw::stat;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    domain::DomainError,
    http::{AppState, error::ApiError},
};

/// Like request DTO
#[derive(Deserialize)]
pub struct LikeRequest {
    content_type: String,
    content_id: Uuid,
}

/// Like response DTO
#[derive(Serialize)]
pub struct LikeResponse {
    liked: bool,
    already_existed: bool,
    count: i64,
    liked_at: DateTime<Utc>,
}

/// POST /v1/likes
pub async fn like(
    State(state): State<AppState>,
    Extension(user_id): Extension<Uuid>,
    Json(payload): Json<LikeRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let content_type = state.content_type_registry.validate(&payload.content_type)?;

    let (already_existed, count, liked_at) =
        state.like_service.like(user_id, content_type, payload.content_id).await?;

    let response = LikeResponse { liked: true, already_existed, count, liked_at };

    Ok((StatusCode::CREATED, Json(response)))
}

/// Unlike response DTO
#[derive(Serialize)]
pub struct UnlikeResponse {
    liked: bool,
    was_liked: bool,
    count: i64,
}

/// DELETE /v1/likes/{content_type}/{content_id}
pub async fn unlike(
    State(state): State<AppState>,
    Extension(user_id): Extension<Uuid>,
    Path((raw_type, content_id)): Path<(String, Uuid)>,
) -> Result<impl IntoResponse, ApiError> {
    let content_type = state.content_type_registry.validate(&raw_type)?;

    let (was_liked, count) = state.like_service.unlike(user_id, content_type, content_id).await?;

    let response = UnlikeResponse { liked: false, was_liked, count };

    Ok((StatusCode::OK, Json(response)))
}

/// Count response DTO
#[derive(Serialize)]
pub struct CountResponse {
    content_type: String,
    content_id: Uuid,
    count: i64,
}

/// GET /v1/likes/{content_type}/{content_id}/count
pub async fn get_count(
    State(state): State<AppState>,
    Path((raw_type, content_id)): Path<(String, Uuid)>,
) -> Result<impl IntoResponse, ApiError> {
    let content_type = state.content_type_registry.validate(&raw_type)?;

    let count = state.like_service.get_count(content_type, content_id).await?;

    let response = CountResponse { content_type: raw_type, content_id, count };

    Ok((StatusCode::OK, Json(response)))
}

/// Status response DTO
#[derive(Serialize)]
pub struct StatusResponse {
    liked: bool,
    liked_at: Option<DateTime<Utc>>,
}

/// GET /v1/likes/{content_type}/{content_id}/status
pub async fn get_status(
    State(state): State<AppState>,
    Extension(user_id): Extension<Uuid>,
    Path((raw_type, content_id)): Path<(String, Uuid)>,
) -> Result<impl IntoResponse, ApiError> {
    let content_type = state.content_type_registry.validate(&raw_type)?;

    let liked_at = state.like_service.get_status(user_id, content_type, content_id).await?;

    let response = StatusResponse { liked: liked_at.is_some(), liked_at };

    Ok((StatusCode::OK, Json(response)))
}

/// GET /v1/likes/user
pub async fn get_user_likes() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// Batch request DTO
#[derive(Deserialize)]
pub struct BatchRequest {
    items: Vec<LikeRequest>,
}

/// Batch count result DTO
#[derive(Serialize)]
pub struct BatchCountResult {
    content_type: String,
    content_id: Uuid,
    count: i64,
}

/// POST /v1/likes/batch/counts
pub async fn batch_counts(
    State(state): State<AppState>,
    Json(payload): Json<BatchRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Validate batch size against configured maximum
    let max = state.config.max_batch_pairs;

    if payload.items.len() > max {
        // TODO: think where this error belongs
        return Err(DomainError::BatchTooLarge { size: payload.items.len(), max }.into());
    }

    // Validate content types and prepare list of (ContentType, content_id) pairs
    let mut validated_items = Vec::with_capacity(payload.items.len());
    for item in &payload.items {
        let ct = state.content_type_registry.validate(&item.content_type)?;
        validated_items.push((ct, item.content_id));
    }

    // Fetch counts from the like service
    let counts = state.like_service.batch_get_counts(&validated_items).await?;

    // Zip together the original requests with their corresponding counts to form the response
    let results: Vec<BatchCountResult> = payload
        .items
        .into_iter()
        .zip(counts.into_iter())
        .map(|(req, count)| BatchCountResult {
            content_type: req.content_type,
            content_id: req.content_id,
            count,
        })
        .collect();

    Ok((StatusCode::OK, Json(serde_json::json!({ "results": results }))))
}

/// Batch status result DTO
#[derive(Serialize)]
pub struct BatchStatusResult {
    content_type: String,
    content_id: Uuid,
    liked: bool,
    liked_at: Option<DateTime<Utc>>,
}

/// POST /v1/likes/batch/statuses
pub async fn batch_statuses(
    State(state): State<AppState>,
    Extension(user_id): Extension<Uuid>,
    Json(payload): Json<BatchRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Validate batch size against configured maximum
    let max = state.config.max_batch_pairs;

    if payload.items.len() > max {
        // TODO: think where this error belongs
        return Err(DomainError::BatchTooLarge { size: payload.items.len(), max }.into());
    }

    // Validate content types and prepare list of (ContentType, content_id) pairs
    let mut validated_items = Vec::with_capacity(payload.items.len());
    for item in &payload.items {
        let ct = state.content_type_registry.validate(&item.content_type)?;
        validated_items.push((ct, item.content_id));
    }

    // Fetch statuses from the like service
    let statuses = state.like_service.batch_get_statuses(user_id, &validated_items).await?;

    // Zip together the original requests with their corresponding statuses to form the response
    let results: Vec<BatchStatusResult> = payload
        .items
        .into_iter()
        .zip(statuses.into_iter())
        .map(|(req, liked_at)| BatchStatusResult {
            content_type: req.content_type,
            content_id: req.content_id,
            liked: liked_at.is_some(),
            liked_at,
        })
        .collect();

    Ok((StatusCode::OK, Json(serde_json::json!({ "results": results }))))
}

/// GET /v1/likes/top
pub async fn top_liked() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

/// GET /v1/likes/stream
pub async fn sse_stream() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}
