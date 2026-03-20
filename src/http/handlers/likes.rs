use axum::{
    Extension, Json,
    extract::{Path, Query, State},
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

/// Top liked query parameters DTO
#[derive(Deserialize)]
pub struct TopLikedQuery {
    content_type: Option<String>,
    window: Option<String>,
    limit: Option<i64>,
}

/// Top liked response DTO
#[derive(Serialize)]
pub struct TopLikedResponse {
    window: String,
    content_type: Option<String>,
    items: Vec<BatchCountResult>, // Reuse same shape
}

/// GET /v1/likes/top
pub async fn top_liked(
    State(state): State<AppState>,
    Query(query): Query<TopLikedQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Validate time window parameter and convert to a timestamp cutoff
    let window_str = query.window.unwrap_or_else(|| "all".to_string());

    let since = match window_str.as_str() {
        "24h" => Some(Utc::now() - chrono::Duration::hours(24)),
        "7d" => Some(Utc::now() - chrono::Duration::days(7)),
        "30d" => Some(Utc::now() - chrono::Duration::days(30)),
        "all" => None,
        _ => return Err(DomainError::InvalidTimeWindow(window_str).into()),
    };

    // Validate content type if provided
    let content_type = match &query.content_type {
        Some(raw) => Some(state.content_type_registry.validate(raw)?),
        None => None,
    };

    // Validate and clamp limit parameter
    let max = state.config.max_top_liked_limit as i64;
    let limit = query.limit.unwrap_or(max).clamp(1, max);

    // Fetch top liked content from the like service based on the provided filters
    let top = state.like_service.get_top_liked(content_type, since, limit).await?;

    // Transform the service results into the response DTO format
    let items: Vec<BatchCountResult> = top
        .into_iter()
        .map(|(ct, id, count)| BatchCountResult {
            content_type: ct.0.to_string(),
            content_id: id,
            count,
        })
        .collect();

    let response = TopLikedResponse { window: window_str, content_type: query.content_type, items };

    Ok((StatusCode::OK, Json(response)))
}

/// GET /v1/likes/stream
pub async fn sse_stream() -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use axum::{
        Router,
        body::Body,
        http::{self, Request},
        routing::{delete, get, post},
    };
    use tower::ServiceExt;

    use crate::{
        clients::profile::ProfileClient,
        config::{AppConfig, ContentTypeRegistry},
        http::AppState,
        like_service::LikeService,
        repository::like_repo::MockLikeRepository,
    };

    // --- Helper: Setup Test App ---
    fn app_for_test(mock_repo: MockLikeRepository, test_user_id: Uuid) -> Router {
        // Use a dummy config and a manually seeded registry to prevent flaky env-var tests
        let config = Arc::new(AppConfig::default());
        let registry = Arc::new(ContentTypeRegistry::default());
        let like_service = Arc::new(LikeService::new(Arc::new(mock_repo)));
        let profile_client =
            Arc::new(ProfileClient::new(reqwest::Client::new(), "http://mock-profile"));

        let state =
            AppState { config, content_type_registry: registry, like_service, profile_client };

        Router::new()
            .route("/v1/likes", post(like))
            .route("/v1/likes/{content_type}/{content_id}", delete(unlike))
            .route("/v1/likes/{content_type}/{content_id}/count", get(get_count))
            .route("/v1/likes/{content_type}/{content_id}/status", get(get_status))
            .route("/v1/likes/user", get(get_user_likes))
            .route("/v1/likes/batch/counts", post(batch_counts))
            .route("/v1/likes/batch/statuses", post(batch_statuses))
            .route("/v1/likes/top", get(top_liked))
            .with_state(state)
            .layer(Extension(test_user_id))
    }

    // Helper to parse JSON responses in tests
    async fn parse_json<T: serde::de::DeserializeOwned>(response: axum::response::Response) -> T {
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("Failed to read response body");

        serde_json::from_slice(&body_bytes).expect("Failed to parse JSON")
    }

    #[tokio::test]
    async fn test_like_handler_success() {
        let test_user_id = Uuid::new_v4();
        let content_id = Uuid::new_v4();
        let timestamp = Utc::now();

        // Simulate a new like with count 42
        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_insert_like()
            .times(1)
            .returning(move |_, _, _| Ok((false, 42, timestamp)));

        let app = app_for_test(mock_repo, test_user_id);
        let payload = serde_json::json!({ "content_type": "post", "content_id": content_id });

        let request = Request::builder()
            .method(http::Method::POST)
            .uri("/v1/likes")
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_string(&payload).unwrap()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let body: serde_json::Value = parse_json(response).await;
        assert_eq!(body["liked"], true);
        assert_eq!(body["already_existed"], false);
        assert_eq!(body["count"], 42);
    }

    #[tokio::test]
    async fn test_like_handler_returns_400_for_unknown_content_type() {
        let test_user_id = Uuid::new_v4();
        let content_id = Uuid::new_v4();

        // Repo should not be called
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_insert_like().times(0);

        let app = app_for_test(mock_repo, test_user_id);

        let payload = serde_json::json!({
            "content_type": "invalid_type",
            "content_id": content_id
        });

        let request = Request::builder()
            .method(http::Method::POST)
            .uri("/v1/likes")
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_string(&payload).unwrap()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        // Assert HTTP Status mapped correctly by `error.rs`
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Assert specific JSON error structure from Assignment Spec
        let body: serde_json::Value = parse_json(response).await;
        assert_eq!(body["error"]["code"], "CONTENT_TYPE_UNKNOWN");
    }

    #[tokio::test]
    async fn test_unlike_handler_success() {
        let test_user_id = Uuid::new_v4();
        let content_id = Uuid::new_v4();

        // Simulate a content item that was previously liked and now has 41 likes after unliking
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_delete_like().times(1).returning(|_, _, _| Ok((true, 41)));

        let app = app_for_test(mock_repo, test_user_id);
        let request = Request::builder()
            .method(http::Method::DELETE)
            .uri(format!("/v1/likes/post/{}", content_id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body: serde_json::Value = parse_json(response).await;
        assert_eq!(body["liked"], false);
        assert_eq!(body["was_liked"], true);
        assert_eq!(body["count"], 41);
    }

    #[tokio::test]
    async fn test_get_count_handler_success() {
        let test_user_id = Uuid::new_v4();
        let content_id = Uuid::new_v4();

        // Simulate a content item with 150 likes
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_get_count().times(1).returning(|_, _| Ok(150));

        let app = app_for_test(mock_repo, test_user_id);

        let request = Request::builder()
            .method(http::Method::GET)
            .uri(format!("/v1/likes/post/{}/count", content_id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body: serde_json::Value = parse_json(response).await;
        assert_eq!(body["content_type"], "post");
        assert_eq!(body["content_id"], content_id.to_string());
        assert_eq!(body["count"], 150);
    }

    #[tokio::test]
    async fn test_get_status_handler_liked() {
        let test_user_id = Uuid::new_v4();
        let content_id = Uuid::new_v4();
        let timestamp = Utc::now();

        // Simulate a content item that the user has liked with a specific timestamp
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_get_status().times(1).returning(move |_, _, _| Ok(Some(timestamp)));

        let app = app_for_test(mock_repo, test_user_id);
        let request = Request::builder()
            .method(http::Method::GET)
            .uri(format!("/v1/likes/post/{}/status", content_id))
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body: serde_json::Value = parse_json(response).await;
        assert_eq!(body["liked"], true);
        assert!(body["liked_at"].is_string());
    }

    // TODO: add test for get user likes handler with pagination once implemented

    #[tokio::test]
    async fn test_batch_counts_success() {
        let test_user_id = Uuid::new_v4();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        // Simulate batch counts for two content items with counts 10 and 20 respectively
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_batch_get_counts().times(1).returning(|_| Ok(vec![10, 20]));

        let app = app_for_test(mock_repo, test_user_id);
        let payload = serde_json::json!({
            "items": [
                { "content_type": "post", "content_id": id1 },
                { "content_type": "bonus_hunter", "content_id": id2 }
            ]
        });

        let request = Request::builder()
            .method(http::Method::POST)
            .uri("/v1/likes/batch/counts")
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_string(&payload).unwrap()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body: serde_json::Value = parse_json(response).await;
        let results = body["results"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["count"], 10);
        assert_eq!(results[1]["count"], 20);
    }

    #[tokio::test]
    async fn test_batch_counts_enforces_100_item_limit() {
        let test_user_id = Uuid::new_v4();

        // Repo should not be called when batch size exceeds limit
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_batch_get_counts().times(0);

        let app = app_for_test(mock_repo, test_user_id);

        // Generate 101 items
        let items: Vec<_> = (0..101)
            .map(|_| serde_json::json!({ "content_type": "post", "content_id": Uuid::new_v4() }))
            .collect();
        let payload = serde_json::json!({ "items": items });

        let request = Request::builder()
            .method(http::Method::POST)
            .uri("/v1/likes/batch/counts")
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_string(&payload).unwrap()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body: serde_json::Value = parse_json(response).await;
        assert_eq!(body["error"]["code"], "BATCH_TOO_LARGE");
    }

    #[tokio::test]
    async fn test_top_liked_valid_window() {
        let test_user_id = Uuid::new_v4();
        let mut mock_repo = MockLikeRepository::new();

        // Simulate top liked content with no specific content type and a 7-day window, returning an empty list for simplicity
        mock_repo.expect_get_top_liked().times(1).returning(|_, _, _| Ok(vec![]));

        let app = app_for_test(mock_repo, test_user_id);
        let request = Request::builder()
            .method(http::Method::GET)
            .uri("/v1/likes/top?window=7d")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body: serde_json::Value = parse_json(response).await;
        assert_eq!(body["window"], "7d");
    }

    #[tokio::test]
    async fn test_top_liked_invalid_window() {
        let test_user_id = Uuid::new_v4();

        // Repo should not be called when window parameter is invalid
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_get_top_liked().times(0);

        let app = app_for_test(mock_repo, test_user_id);
        let request = Request::builder()
            .method(http::Method::GET)
            .uri("/v1/likes/top?window=5m") // invalid
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body: serde_json::Value = parse_json(response).await;
        assert_eq!(body["error"]["code"], "INVALID_TIME_WINDOW");
    }
}
