use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{ConnectInfo, Request, State},
    http::{HeaderValue, header::HeaderName},
    middleware::Next,
    response::IntoResponse,
    response::Response,
};
use reqwest::Method;
use uuid::Uuid;

use crate::http::{AppState, error::ApiError};
use crate::repository::cache_repo::RateLimitStatus;

const RATE_LIMIT_LIMIT_HEADER: HeaderName = HeaderName::from_static("x-ratelimit-limit");
const RATE_LIMIT_REMAINING_HEADER: HeaderName = HeaderName::from_static("x-ratelimit-remaining");
const RATE_LIMIT_RESET_HEADER: HeaderName = HeaderName::from_static("x-ratelimit-reset");

/// Middleware to enforce rate limits based on IP for reads and User ID for writes.
/// Reads (GET) are limited per IP, while writes (POST, PUT, DELETE, PATCH) are limited per authenticated user.
pub async fn rate_limiter(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // Determine if the request is a write operation
    let is_write_method =
        matches!(req.method(), &Method::POST | &Method::DELETE | &Method::PUT | &Method::PATCH);

    // Determine the key and limit based on request type.
    let (key, limit) = if is_write_method {
        let user_id = req.extensions().get::<Uuid>().ok_or(ApiError::Unauthorized)?; // Unauthorized should never happen if routing is correct
        (format!("rate:write:user:{}", user_id), state.config.limits.write_per_minute)
    } else {
        // Public reads - use IP address as key
        let key = format!("rate:read:ip:{}", addr.ip());
        (key, state.config.limits.read_per_minute)
    };

    // Check the rate limit for this key (IP or user token)
    let rate_limit = state
        .cache
        .check_rate_limit(&key, limit, state.config.limits.rate_limit_window_secs)
        .await?;

    if !rate_limit.allowed {
        let mut response =
            ApiError::RateLimited { retry_after_secs: rate_limit.retry_after_secs }.into_response();
        apply_rate_limit_headers(&mut response, limit, rate_limit);
        return Ok(response);
    }

    let mut response = next.run(req).await;
    apply_rate_limit_headers(&mut response, limit, rate_limit);

    Ok(response)
}

/// Helper function to apply rate limit headers to the response based on the current rate limit status. This includes the total limit, remaining requests, and reset time for the client to know when they can make their next request without being rate limited.
fn apply_rate_limit_headers(response: &mut Response, limit: u32, rate_limit: RateLimitStatus) {
    let remaining = limit.saturating_sub(rate_limit.current_count);
    let reset_at = SystemTime::now()
        .checked_add(Duration::from_secs(rate_limit.retry_after_secs))
        .unwrap_or(SystemTime::now())
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    response.headers_mut().insert(RATE_LIMIT_LIMIT_HEADER.clone(), HeaderValue::from(limit));
    response
        .headers_mut()
        .insert(RATE_LIMIT_REMAINING_HEADER.clone(), HeaderValue::from(remaining));
    if let Ok(value) = HeaderValue::from_str(&reset_at.to_string()) {
        response.headers_mut().insert(RATE_LIMIT_RESET_HEADER.clone(), value);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        sync::Arc,
    };

    use axum::{
        Extension, Router, body::Body, http, middleware, response::IntoResponse, routing::any,
    };
    use mockall::predicate::eq;
    use tower::ServiceExt;

    use crate::{
        clients::{content::MockContentValidationClient, profile::MockProfileValidationClient},
        config::{AppConfig, ContentTypeRegistry},
        http::observability::{AppMetrics, StaticReadinessProbe},
        repository::{cache_repo::MockCacheRepository, like_repo::MockLikeRepository},
        service::{broadcast::Broadcaster, like_service::LikeService},
    };

    use super::*;

    /// Helper function to extract the rate limit reset value from a response
    fn rate_limit_reset_value(response: &Response) -> u64 {
        response
            .headers()
            .get(RATE_LIMIT_RESET_HEADER)
            .expect("x-ratelimit-reset header")
            .to_str()
            .expect("valid x-ratelimit-reset value")
            .parse::<u64>()
            .expect("numeric x-ratelimit-reset value")
    }

    /// Helper function to create a test socket address
    fn test_socket_addr() -> SocketAddr {
        SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3000))
    }

    /// Helper function to create an Extension for user_id to simulate authenticated requests in tests
    fn user_id_extension(user_id: Uuid) -> Extension<Uuid> {
        Extension(user_id)
    }

    /// Helper function to create an app with the rate limiter middleware and a mock cache repository for testing
    fn app_for_test(mock_cache: Arc<MockCacheRepository>) -> Router {
        let config = Arc::new(AppConfig::default());
        let broadcaster = Arc::new(Broadcaster::new(config.server.sse_channel_capacity));
        let metrics = Arc::new(AppMetrics::new());
        let like_service = Arc::new(LikeService::new(
            config.cache,
            Arc::new(MockLikeRepository::new()),
            mock_cache.clone(),
            Arc::new(MockContentValidationClient::new()),
            Arc::clone(&broadcaster),
            Arc::clone(&metrics),
        ));

        let state = AppState {
            config,
            content_type_registry: Arc::new(ContentTypeRegistry::default()),
            like_service,
            profile_client: Arc::new(MockProfileValidationClient::new()),
            cache: mock_cache,
            broadcaster,
            readiness: Arc::new(StaticReadinessProbe::ready()),
            metrics,
        };

        async fn dummy_handler() -> impl IntoResponse {
            (http::StatusCode::OK, "ok")
        }

        Router::new()
            .route("/test", any(dummy_handler)) // Use any method to test both read and write scenarios
            .layer(middleware::from_fn_with_state(state.clone(), rate_limiter))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_read_request_uses_ip_and_allows_traffic() {
        let addr = test_socket_addr();
        let expected_key = format!("rate:read:ip:{}", addr.ip());

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_check_rate_limit()
            .with(
                eq(expected_key),
                eq(AppConfig::default().limits.read_per_minute),
                eq(AppConfig::default().limits.rate_limit_window_secs),
            )
            .times(1)
            .returning(|_, _, _| {
                Ok(RateLimitStatus { allowed: true, current_count: 1, retry_after_secs: 60 })
            });

        let app = app_for_test(Arc::new(mock_cache));

        let mut request =
            Request::builder().method(http::Method::GET).uri("/test").body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(addr));

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response.headers().get(RATE_LIMIT_LIMIT_HEADER).unwrap(),
            &AppConfig::default().limits.read_per_minute.to_string()
        );
        assert_eq!(
            response.headers().get(RATE_LIMIT_REMAINING_HEADER).unwrap(),
            &(AppConfig::default().limits.read_per_minute - 1).to_string()
        );
        assert!(rate_limit_reset_value(&response) > 0);
    }

    #[tokio::test]
    async fn test_write_request_uses_user_id_and_allows_traffic() {
        let addr = test_socket_addr();
        let user_id = Uuid::new_v4();
        let expected_key = format!("rate:write:user:{}", user_id);

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_check_rate_limit()
            .with(
                eq(expected_key),
                eq(AppConfig::default().limits.write_per_minute),
                eq(AppConfig::default().limits.rate_limit_window_secs),
            )
            .times(1)
            .returning(|_, _, _| {
                Ok(RateLimitStatus { allowed: true, current_count: 1, retry_after_secs: 60 })
            });

        let app = app_for_test(Arc::new(mock_cache)).layer(user_id_extension(user_id));

        let mut request =
            Request::builder().method(http::Method::POST).uri("/test").body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(addr));

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response.headers().get(RATE_LIMIT_LIMIT_HEADER).unwrap(),
            &AppConfig::default().limits.write_per_minute.to_string()
        );
        assert_eq!(
            response.headers().get(RATE_LIMIT_REMAINING_HEADER).unwrap(),
            &(AppConfig::default().limits.write_per_minute - 1).to_string()
        );
        assert!(rate_limit_reset_value(&response) > 0);
    }

    #[tokio::test]
    async fn test_allowed_request_at_limit_sets_zero_remaining() {
        let addr = test_socket_addr();
        let limit = AppConfig::default().limits.read_per_minute;
        let expected_key = format!("rate:read:ip:{}", addr.ip());

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_check_rate_limit()
            .with(
                eq(expected_key),
                eq(limit),
                eq(AppConfig::default().limits.rate_limit_window_secs),
            )
            .times(1)
            .returning(move |_, _, _| {
                Ok(RateLimitStatus { allowed: true, current_count: limit, retry_after_secs: 60 })
            });

        let app = app_for_test(Arc::new(mock_cache));

        let mut request =
            Request::builder().method(http::Method::GET).uri("/test").body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(addr));

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(response.headers().get(RATE_LIMIT_LIMIT_HEADER).unwrap(), &limit.to_string());
        assert_eq!(response.headers().get(RATE_LIMIT_REMAINING_HEADER).unwrap(), "0");
        assert!(rate_limit_reset_value(&response) > 0);
    }

    #[tokio::test]
    async fn test_rate_limit_exceeded_returns_429_with_retry_header() {
        let addr = test_socket_addr();
        let expected_key = format!("rate:read:ip:{}", addr.ip());
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let limit = AppConfig::default().limits.read_per_minute;

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_check_rate_limit()
            .with(
                eq(expected_key),
                eq(limit),
                eq(AppConfig::default().limits.rate_limit_window_secs),
            )
            .times(1)
            .returning(move |_, _, _| {
                Ok(RateLimitStatus {
                    allowed: false,
                    current_count: limit + 1,
                    retry_after_secs: 42,
                }) // Simulate limit exceeded with 42 seconds until reset
            });

        let app = app_for_test(Arc::new(mock_cache));

        let mut request =
            Request::builder().method(http::Method::GET).uri("/test").body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(addr));

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), http::StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get(reqwest::header::RETRY_AFTER).unwrap(), "42");
        assert_eq!(response.headers().get(RATE_LIMIT_LIMIT_HEADER).unwrap(), &limit.to_string());
        assert_eq!(response.headers().get(RATE_LIMIT_REMAINING_HEADER).unwrap(), "0");
        assert!(rate_limit_reset_value(&response) >= before + 42);
    }

    #[tokio::test]
    async fn test_fail_open_still_sets_rate_limit_headers() {
        let addr = test_socket_addr();
        let expected_key = format!("rate:read:ip:{}", addr.ip());
        let limit = AppConfig::default().limits.read_per_minute;
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_check_rate_limit()
            .with(
                eq(expected_key),
                eq(limit),
                eq(AppConfig::default().limits.rate_limit_window_secs),
            )
            .times(1)
            .returning(|_, _, _| {
                Ok(RateLimitStatus { allowed: true, current_count: 0, retry_after_secs: 0 })
            });

        let app = app_for_test(Arc::new(mock_cache));

        let mut request =
            Request::builder().method(http::Method::GET).uri("/test").body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(addr));

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(response.headers().get(RATE_LIMIT_LIMIT_HEADER).unwrap(), &limit.to_string());
        assert_eq!(
            response.headers().get(RATE_LIMIT_REMAINING_HEADER).unwrap(),
            &limit.to_string()
        );
        assert!(rate_limit_reset_value(&response) >= before);
    }

    #[tokio::test]
    async fn test_write_request_without_auth_fails_fast() {
        let addr = test_socket_addr();

        let mut mock_cache = MockCacheRepository::new();
        mock_cache.expect_check_rate_limit().times(0);

        let app = app_for_test(Arc::new(mock_cache));

        let mut request = Request::builder()
            .method(http::Method::DELETE)
            .uri("/test")
            .body(Body::empty())
            .unwrap();
        request.extensions_mut().insert(ConnectInfo(addr));

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
    }
}
