use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};

use crate::{
    clients::error::ClientError,
    http::{AppState, error::ApiError},
};

/// Middleware to require authentication on protected routes. Validates the Bearer token using the Profile API and injects the user ID into request extensions for handlers to use.
pub async fn require_auth(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // Extract auth header
    let auth_header =
        req.headers().get(axum::http::header::AUTHORIZATION).and_then(|value| value.to_str().ok());

    // Extract the token by stripping the "Bearer " prefix
    let token = match auth_header {
        Some(header) if header.starts_with("Bearer ") => {
            header.strip_prefix("Bearer ").ok_or(ApiError::Unauthorized)?
        }
        _ => {
            return Err(ApiError::Unauthorized);
        }
    };

    // Validate token with Profile API
    let result = state.profile_client.validate_token(token).await;

    match result {
        Ok(user_id) => {
            // Inject user_id into request extensions for handlers to use
            req.extensions_mut().insert(user_id);

            Ok(next.run(req).await)
        }
        Err(ClientError::NotFound) => Err(ApiError::Unauthorized), // Token invalid
        Err(ClientError::DependencyUnavailable(service)) => {
            Err(ApiError::Client(ClientError::DependencyUnavailable(service)))
        } // Profile API down
        Err(e) => {
            tracing::error!("Unexpected error during token validation: {:?}", e);
            Err(ApiError::Unauthorized) // Default to unauthorized on unexpected errors for security
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        Extension, Router, body::Body, http, middleware, response::IntoResponse, routing::get,
    };
    use reqwest::StatusCode;
    use tower::ServiceExt;
    use uuid::Uuid;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };

    use crate::{
        clients::profile::ProfileClient,
        config::{AppConfig, ContentTypeRegistry},
        like_service::LikeService,
        repository::like_repo::MockLikeRepository,
    };

    use super::*;

    // --- Helper: Setup Test App with Real Middleware ---
    async fn setup_app_with_mock_profile() -> (Router, MockServer, Uuid) {
        let mock_server = MockServer::start().await;

        let expected_uuid = Uuid::new_v4();
        let mock_user_id = format!("usr_{}", expected_uuid);

        // Configure mock Profile API to accept exactly one specific token
        Mock::given(method("GET"))
            .and(path("/v1/auth/validate"))
            .and(header("Authorization", "Bearer valid_token_123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "valid": true,
                "user_id": mock_user_id,
            })))
            .mount(&mock_server)
            .await;

        let config = Arc::new(AppConfig::default());
        let registry = Arc::new(ContentTypeRegistry::default());
        let like_service = Arc::new(LikeService::new(Arc::new(MockLikeRepository::new())));
        let profile_client =
            Arc::new(ProfileClient::new(reqwest::Client::new(), mock_server.uri()));

        let state =
            AppState { config, content_type_registry: registry, like_service, profile_client };

        // A dummy handler to test the middleware - just returns the user_id from extensions if auth succeeds
        async fn dummy_handler(Extension(user_id): Extension<Uuid>) -> impl IntoResponse {
            (StatusCode::OK, user_id.to_string())
        }

        let app = Router::new()
            .route("/protected", get(dummy_handler))
            .layer(middleware::from_fn_with_state(state.clone(), require_auth))
            .with_state(state);

        (app, mock_server, expected_uuid)
    }

    #[tokio::test]
    async fn auth_middleware_allows_valid_token_and_injects_user_id() {
        let (app, _server, expected_uuid) = setup_app_with_mock_profile().await;

        let request = Request::builder()
            .method(http::Method::GET)
            .uri("/protected")
            .header(http::header::AUTHORIZATION, "Bearer valid_token_123")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        // Must return 200 OK from the dummy handler
        assert_eq!(response.status(), StatusCode::OK);

        // Must contain the exact UUID injected into the extensions
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert_eq!(body_str, expected_uuid.to_string());
    }

    #[tokio::test]
    async fn auth_middleware_rejects_missing_header() {
        let (app, _server, _) = setup_app_with_mock_profile().await;

        let request = Request::builder()
            .method(http::Method::GET)
            .uri("/protected")
            // NO Authorization header
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        // Must be intercepted and return 401 Unauthorized
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_middleware_rejects_malformed_header() {
        let (app, _server, _) = setup_app_with_mock_profile().await;

        let request = Request::builder()
            .method(http::Method::GET)
            .uri("/protected")
            .header(http::header::AUTHORIZATION, "Basic some_string") // Not Bearer
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn middleware_rejects_invalid_token_from_profile_api() {
        let (app, mock_server, _) = setup_app_with_mock_profile().await;

        // Configure the mock server to reject "bad_token"
        Mock::given(method("GET"))
            .and(path("/v1/auth/validate"))
            .and(header("Authorization", "Bearer bad_token"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "valid": false,
                "error": "invalid_token"
            })))
            .mount(&mock_server)
            .await;

        let request = Request::builder()
            .method(http::Method::GET)
            .uri("/protected")
            .header(http::header::AUTHORIZATION, "Bearer bad_token")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
