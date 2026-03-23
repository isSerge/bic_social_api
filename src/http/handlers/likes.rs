use std::{
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Sse, sse::Event},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_stream::{
    Stream, StreamExt,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};
use uuid::Uuid;

use crate::{
    domain::{DomainError, PaginationCursor},
    http::{AppState, error::ApiError, observability::AppMetrics},
};

// TODO: consider moving into separate module
/// Custom stream wrapper to track active SSE connections for metrics purposes
struct SseConnectionStream<S> {
    inner: Pin<Box<S>>,
    metrics: Arc<AppMetrics>,
}

impl<S> SseConnectionStream<S> {
    fn new(stream: S, metrics: Arc<AppMetrics>) -> Self {
        Self { inner: Box::pin(stream), metrics }
    }
}

impl<S> Stream for SseConnectionStream<S>
where
    S: Stream<Item = Result<Event, Infallible>>,
{
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl<S> Drop for SseConnectionStream<S> {
    fn drop(&mut self) {
        self.metrics.decrement_sse_connections();
    }
}

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

/// User likes query parameters DTO
#[derive(Deserialize)]
pub struct UserLikesQuery {
    content_type: Option<String>,
    cursor: Option<String>,
    limit: Option<i64>,
}

/// User likes item DTO
#[derive(Serialize)]
pub struct UserLikesItem {
    content_type: String,
    content_id: Uuid,
    liked_at: DateTime<Utc>,
}

/// User likes response DTO
#[derive(Serialize)]
pub struct UserLikesResponse {
    items: Vec<UserLikesItem>,
    next_cursor: Option<String>,
    has_more: bool,
}

/// GET /v1/likes/user
pub async fn get_user_likes(
    State(state): State<AppState>,
    Extension(user_id): Extension<Uuid>,
    Query(query): Query<UserLikesQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Validate content type if provided
    let content_type = match &query.content_type {
        Some(raw) => Some(state.content_type_registry.validate(raw)?),
        None => None,
    };

    // Validate and parse cursor if provided
    let cursor = query.cursor.as_deref().map(PaginationCursor::try_from).transpose()?;

    // Validate and clamp limit parameter
    // TODO: add max limit to config and use it here
    let limit = query.limit.unwrap_or(20).clamp(1, 100);

    // Fetch user likes from the like service with pagination parameters
    let mut records =
        state.like_service.get_user_likes(user_id, content_type.clone(), cursor, limit + 1).await?;

    // Determine if there are more records for pagination and prepare the next cursor if needed
    let has_more = records.len() as i64 > limit;
    if has_more {
        records.pop();
    }

    // Prepare the next cursor based on the last record in the current page, if it exists
    let next_cursor = records.last().map(|last| {
        String::from(&PaginationCursor { created_at: last.created_at, id: last.content_id })
    });

    // Transform the like records into the response DTO format
    let items: Vec<UserLikesItem> = records
        .into_iter()
        .map(|r| UserLikesItem {
            content_type: r.content_type.0.to_string(),
            content_id: r.content_id,
            liked_at: r.created_at,
        })
        .collect();

    let response = UserLikesResponse { items, next_cursor, has_more };

    Ok((StatusCode::OK, Json(response)))
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
    let max = state.config.limits.max_batch_pairs;

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
    let max = state.config.limits.max_batch_pairs;

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
    let max = state.config.limits.max_top_liked_limit as i64;
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

/// SSE stream query parameters DTO
#[derive(Deserialize)]
pub struct StreamQuery {
    content_type: String,
    content_id: Uuid,
}

/// GET /v1/likes/stream
pub async fn sse_stream(
    State(state): State<AppState>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    // Validate content type
    let content_type = state.content_type_registry.validate(&query.content_type)?;

    // Subscribe to the channel for this content type
    let rx = state.broadcaster.subscribe(content_type, &query.content_id);

    let heartbeat_interval = Duration::from_secs(state.config.app.heartbeat_interval_secs);
    let requested_content_type = query.content_type.clone();
    let requested_content_id = query.content_id;
    state.metrics.increment_sse_connections();

    // Create a stream
    let stream = BroadcastStream::new(rx).timeout(heartbeat_interval).filter_map(move |result| {
        match result {
            // On a new like event, serialize it to JSON and send as SSE data
            Ok(Ok(like_event)) => {
                if let Ok(json) = serde_json::to_string(&like_event) {
                    Some(Ok(Event::default().data(json)))
                } else {
                    None
                }
            }
            // On a lagged event, log a warning and skip
            Ok(Err(BroadcastStreamRecvError::Lagged(_))) => {
                tracing::warn!(
                    "SSE stream lagged behind for content {}/{}",
                    requested_content_type,
                    requested_content_id
                );
                None
            }
            // On timeout (no events in the interval), send a heartbeat event to keep the connection alive
            Err(_) => {
                let heartbeat_json = serde_json::json!({
                    "type": "heartbeat",
                    "timestamp": Utc::now(),
                })
                .to_string();

                Some(Ok(Event::default().data(heartbeat_json)))
            }
        }
    });

    Ok(Sse::new(SseConnectionStream::new(stream, Arc::clone(&state.metrics))))
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::{
        Router,
        body::Body,
        http::{self, Request},
        routing::{delete, get, post},
    };
    use chrono::TimeZone;
    use mockall::predicate::eq;
    use tower::ServiceExt;

    use crate::{
        clients::{content::MockContentValidationClient, profile::ProfileClient},
        config::{AppConfig, ContentTypeRegistry},
        domain::{ContentType, LikeRecord},
        http::{
            AppState,
            observability::{AppMetrics, StaticReadinessProbe},
        },
        repository::{cache_repo::MockCacheRepository, like_repo::MockLikeRepository},
        service::{broadcast::Broadcaster, like_service::LikeService},
    };

    // --- Helper: Setup Test App ---
    fn app_for_test(
        mock_repo: MockLikeRepository,
        mock_cache_repo: MockCacheRepository,
        test_user_id: Uuid,
    ) -> Router {
        // Use a dummy config and a manually seeded registry to prevent flaky env-var tests
        let config = Arc::new(AppConfig::default());
        let registry = Arc::new(ContentTypeRegistry::default());
        let mock_like_repo = Arc::new(mock_repo);
        let mock_cache_repo = Arc::new(mock_cache_repo);
        let broadcaster = Arc::new(Broadcaster::new(config.server.sse_channel_capacity));
        let metrics = Arc::new(AppMetrics::new());
        let like_service = Arc::new(LikeService::new(
            config.cache,
            mock_like_repo,
            mock_cache_repo.clone(),
            Arc::new(MockContentValidationClient::new()),
            Arc::clone(&broadcaster),
            Arc::clone(&metrics),
        ));
        let profile_client = Arc::new(ProfileClient::new(
            reqwest::Client::new(),
            "http://mock-profile",
            config.circuit_breaker,
            Arc::clone(&metrics),
        ));

        let state = AppState {
            config,
            content_type_registry: registry,
            like_service,
            profile_client,
            cache: mock_cache_repo,
            broadcaster,
            readiness: Arc::new(StaticReadinessProbe::ready()),
            metrics,
        };

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

    // TODO: move it somewhere common
    // Helper to create a dummy ContentType
    fn content_type(raw: &str) -> ContentType {
        ContentType(Arc::from(raw.to_string()))
    }

    const CACHE_TTL_LIKE_COUNTS_SECS: u64 = 300;

    #[tokio::test]
    async fn test_like_handler_success() {
        let test_user_id = Uuid::new_v4();
        let content_id = Uuid::new_v4();
        let timestamp = Utc::now();
        let content_type = content_type("post");
        let like_count = 42;

        // Simulate a new like with count 42
        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_insert_like()
            .times(1)
            .returning(move |_, _, _| Ok((false, like_count, timestamp)));

        let mut mock_cache = MockCacheRepository::new();
        // Cache repo should call get_content_exists to check if content exists before liking
        mock_cache.expect_get_content_exists().times(1).returning(|_, _| Ok(Some(true)));
        // Cache repo should call set_count with the new count
        mock_cache
            .expect_set_count()
            .with(
                eq(content_type.clone()),
                eq(content_id),
                eq(like_count),
                eq(CACHE_TTL_LIKE_COUNTS_SECS),
            )
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let app = app_for_test(mock_repo, mock_cache, test_user_id);
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
        assert_eq!(body["count"], like_count);
    }

    #[tokio::test]
    async fn test_like_handler_returns_400_for_unknown_content_type() {
        let test_user_id = Uuid::new_v4();
        let content_id = Uuid::new_v4();

        // Repo should not be called
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_insert_like().times(0);

        // Cache repo should not be called
        let mock_cache = MockCacheRepository::new();

        let app = app_for_test(mock_repo, mock_cache, test_user_id);

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
        let content_type = content_type("post");
        let like_count_after = 41;

        // Simulate a content item that was previously liked and now has 41 likes after unliking
        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_delete_like()
            .times(1)
            .returning(move |_, _, _| Ok((true, like_count_after)));

        // Cache repo should call set_count with the new count after unlike
        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_set_count()
            .with(
                eq(content_type.clone()),
                eq(content_id),
                eq(like_count_after),
                eq(CACHE_TTL_LIKE_COUNTS_SECS),
            )
            .times(1)
            .returning(|_, _, _, _| Ok(()));

        let app = app_for_test(mock_repo, mock_cache, test_user_id);

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
        assert_eq!(body["count"], like_count_after);
    }

    #[tokio::test]
    async fn test_get_count_handler_success() {
        let test_user_id = Uuid::new_v4();
        let content_id = Uuid::new_v4();
        let like_count = 150;

        // Simulate a content item with 150 likes
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_get_count().times(1).returning(move |_, _| Ok(like_count));

        let mut mock_cache = MockCacheRepository::new();
        // Cache repo should call get_count and return None to indicate cache miss
        mock_cache.expect_get_count().times(1).returning(|_, _| Ok(None));
        mock_cache
            .expect_try_acquire_count_lock()
            .with(eq(content_type("post")), eq(content_id), eq(1u64))
            .times(1)
            .returning(|_, _, _| Ok(true));
        // Cache repo should call set_count with the count from repo after cache miss
        mock_cache
            .expect_set_count()
            .with(
                eq(content_type("post")),
                eq(content_id),
                eq(like_count),
                eq(CACHE_TTL_LIKE_COUNTS_SECS),
            )
            .times(1)
            .returning(|_, _, _, _| Ok(()));
        mock_cache
            .expect_release_count_lock()
            .with(eq(content_type("post")), eq(content_id))
            .times(1)
            .returning(|_, _| Ok(()));

        let app = app_for_test(mock_repo, mock_cache, test_user_id);

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
        assert_eq!(body["count"], like_count);
    }

    #[tokio::test]
    async fn test_get_status_handler_liked() {
        let test_user_id = Uuid::new_v4();
        let content_id = Uuid::new_v4();
        let timestamp = Utc::now();

        // Simulate a content item that the user has liked with a specific timestamp
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_get_status().times(1).returning(move |_, _, _| Ok(Some(timestamp)));

        let mock_cache = MockCacheRepository::new();

        let app = app_for_test(mock_repo, mock_cache, test_user_id);

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

    #[tokio::test]
    async fn test_batch_counts_success() {
        let test_user_id = Uuid::new_v4();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        // Simulate batch counts for two content items with counts 10 and 20 respectively
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_batch_get_counts().times(1).returning(|_| Ok(vec![10, 20]));

        let mock_cache = MockCacheRepository::new();

        let app = app_for_test(mock_repo, mock_cache, test_user_id);

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

        let mock_cache = MockCacheRepository::new();

        let app = app_for_test(mock_repo, mock_cache, test_user_id);

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

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_get_leaderboard()
            .with(eq(None), eq("7d"), eq(50))
            .times(1)
            .returning(|_, _, _| Ok(vec![]));

        let app = app_for_test(mock_repo, mock_cache, test_user_id);

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

        let mock_cache = MockCacheRepository::new();

        let app = app_for_test(mock_repo, mock_cache, test_user_id);

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

    #[tokio::test]
    async fn test_get_user_likes_pagination_has_more() {
        let test_user_id = Uuid::new_v4();
        let ct = content_type("post");
        let ts1 = Utc.with_ymd_and_hms(2026, 2, 2, 17, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2026, 2, 2, 16, 0, 0).unwrap();
        let ts3 = Utc.with_ymd_and_hms(2026, 2, 2, 15, 0, 0).unwrap();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_get_user_likes()
            // We request limit=2, so the service asks the repo for 3 (limit + 1)
            .with(eq(test_user_id), eq(None), eq(None), eq(3))
            .times(1)
            .returning(move |_, _, _, _| {
                Ok(vec![
                    LikeRecord {
                        user_id: test_user_id,
                        content_type: ct.clone(),
                        content_id: id1,
                        created_at: ts1,
                    },
                    LikeRecord {
                        user_id: test_user_id,
                        content_type: ct.clone(),
                        content_id: id2,
                        created_at: ts2,
                    },
                    LikeRecord {
                        user_id: test_user_id,
                        content_type: ct.clone(),
                        content_id: id3,
                        created_at: ts3,
                    },
                ])
            });

        let mock_cache = MockCacheRepository::new();

        let app = app_for_test(mock_repo, mock_cache, test_user_id);

        let request = Request::builder()
            .method(http::Method::GET)
            .uri("/v1/likes/user?limit=2")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body: serde_json::Value = parse_json(response).await;
        let items = body["items"].as_array().unwrap();

        // Assert it returns only 2 items
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["content_id"], id1.to_string());
        assert_eq!(items[1]["content_id"], id2.to_string());

        // Assert pagination metadata
        assert_eq!(body["has_more"], true);
        assert!(body["next_cursor"].is_string());

        // Verify the generated cursor actually decodes back to the last item (id2)
        let cursor_str = body["next_cursor"].as_str().unwrap();
        let decoded_cursor = PaginationCursor::try_from(cursor_str).expect("Valid base64 cursor");
        assert_eq!(decoded_cursor.id, id2);
        assert_eq!(decoded_cursor.created_at, ts2);
    }

    #[tokio::test]
    async fn test_get_user_likes_invalid_cursor_returns_400() {
        let test_user_id = Uuid::new_v4();

        // Should not call the repo if the cursor is invalid
        let mut mock_repo = MockLikeRepository::new();
        mock_repo.expect_get_user_likes().times(0);

        let mock_cache = MockCacheRepository::new();

        let app = app_for_test(mock_repo, mock_cache, test_user_id);

        let request = Request::builder()
            .method(http::Method::GET)
            .uri("/v1/likes/user?cursor=this_is_not_base64_json")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body: serde_json::Value = parse_json(response).await;
        assert_eq!(body["error"]["code"], "INVALID_CURSOR");
    }

    #[tokio::test]
    async fn test_get_user_likes_with_content_type_filter() {
        let test_user_id = Uuid::new_v4();
        let filter_ct = content_type("bonus_hunter");

        // Simulate a request with content_type filter and verify that the service correctly passes it down to the repo
        let mut mock_repo = MockLikeRepository::new();
        mock_repo
            .expect_get_user_likes()
            .with(eq(test_user_id), eq(Some(filter_ct)), eq(None), eq(21)) // default limit 20 + 1
            .times(1)
            .returning(|_, _, _, _| Ok(vec![])); // Return empty for simplicity

        let mock_cache = MockCacheRepository::new();

        let app = app_for_test(mock_repo, mock_cache, test_user_id);

        let request = Request::builder()
            .method(http::Method::GET)
            .uri("/v1/likes/user?content_type=bonus_hunter")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body: serde_json::Value = parse_json(response).await;
        let items = body["items"].as_array().unwrap();

        assert_eq!(items.len(), 0);
        assert_eq!(body["has_more"], false);
        assert!(body["next_cursor"].is_null());
    }
}
