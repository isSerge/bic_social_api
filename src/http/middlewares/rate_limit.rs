use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, Request, State},
    middleware::Next,
    response::Response,
};
use reqwest::Method;
use uuid::Uuid;

use crate::http::{AppState, error::ApiError};

// TODO: consider adding to config
const ONE_MINUTE_WINDOW: u64 = 60; // Window size in seconds

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
    let (allowed, retry_after_secs) =
        state.cache.check_rate_limit(&key, limit, ONE_MINUTE_WINDOW).await?;

    if !allowed {
        // Return a 429 Too Many Requests with the retry_after info
        return Err(ApiError::RateLimited { retry_after_secs });
    }

    Ok(next.run(req).await)
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
        repository::{cache_repo::MockCacheRepository, like_repo::MockLikeRepository},
        service::like_service::LikeService,
    };

    use super::*;

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
        let like_service = Arc::new(LikeService::new(
            Arc::new(MockLikeRepository::new()),
            mock_cache.clone(),
            Arc::new(MockContentValidationClient::new()),
            config.cache,
        ));

        let state = AppState {
            config,
            content_type_registry: Arc::new(ContentTypeRegistry::default()),
            like_service,
            profile_client: Arc::new(MockProfileValidationClient::new()),
            cache: mock_cache,
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
                eq(ONE_MINUTE_WINDOW),
            )
            .times(1)
            .returning(|_, _, _| Ok((true, 0)));

        let app = app_for_test(Arc::new(mock_cache));

        let mut request =
            Request::builder().method(http::Method::GET).uri("/test").body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(addr));

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), http::StatusCode::OK);
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
                eq(ONE_MINUTE_WINDOW),
            )
            .times(1)
            .returning(|_, _, _| Ok((true, 0)));

        let app = app_for_test(Arc::new(mock_cache)).layer(user_id_extension(user_id));

        let mut request =
            Request::builder().method(http::Method::POST).uri("/test").body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(addr));

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), http::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rate_limit_exceeded_returns_429_with_retry_header() {
        let addr = test_socket_addr();
        let expected_key = format!("rate:read:ip:{}", addr.ip());

        let mut mock_cache = MockCacheRepository::new();
        mock_cache
            .expect_check_rate_limit()
            .with(
                eq(expected_key),
                eq(AppConfig::default().limits.read_per_minute),
                eq(ONE_MINUTE_WINDOW),
            )
            .times(1)
            .returning(|_, _, _| Ok((false, 42)));

        let app = app_for_test(Arc::new(mock_cache));

        let mut request =
            Request::builder().method(http::Method::GET).uri("/test").body(Body::empty()).unwrap();
        request.extensions_mut().insert(ConnectInfo(addr));

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), http::StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get(reqwest::header::RETRY_AFTER).unwrap(), "42");
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
