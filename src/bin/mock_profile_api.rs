//! Mock Profile API service.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::{Json, Router, routing::get};
use serde::Serialize;

#[path = "../server_utils.rs"]
mod server_utils;

#[derive(Clone)]
struct AppState {
    // Maps token string -> (user_id, display_name)
    tokens: Arc<HashMap<String, UserProfile>>,
}

#[derive(Clone)]
struct UserProfile {
    user_id: String, // Note: The spec explicitly uses a "usr_" prefix in the JSON
    display_name: String,
}

#[derive(Serialize)]
struct ValidResponse {
    valid: bool,
    user_id: String,
    display_name: String,
}

#[derive(Serialize)]
struct InvalidResponse {
    valid: bool,
    error: String,
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    let state = AppState { tokens: Arc::new(seed_tokens()) };

    let app = create_app(state);

    // Spec sets PROFILE_API_URL to port 8084
    server_utils::serve_mock(app, 8084, "Mock Profile API").await;
}

fn create_app(state: AppState) -> Router {
    Router::new()
        .route("/v1/auth/validate", get(validate_token))
        .route("/health", get(|| async { StatusCode::OK }))
        .with_state(state)
}

/// Handler for GET /v1/auth/validate
async fn validate_token(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    // Extract the Authorization header safely
    let auth_header = headers.get(axum::http::header::AUTHORIZATION).and_then(|h| h.to_str().ok());

    // Extract the token by stripping the "Bearer " prefix
    let token = auth_header.and_then(|h| h.strip_prefix("Bearer "));

    // Look up the token
    match token.and_then(|t| state.tokens.get(t)) {
        Some(profile) => {
            let payload = ValidResponse {
                valid: true,
                user_id: profile.user_id.clone(),
                display_name: profile.display_name.clone(),
            };
            (StatusCode::OK, Json(payload)).into_response()
        }
        None => {
            let payload = InvalidResponse { valid: false, error: "invalid_token".to_string() };
            (StatusCode::UNAUTHORIZED, Json(payload)).into_response()
        }
    }
}

/// Seeds the in-memory token -> profile mapping with the 5 entries specified in the assignment spec
fn seed_tokens() -> HashMap<String, UserProfile> {
    let mut map = HashMap::new();

    // From the assignment spec exactly:
    let seed_data = vec![
        ("tok_user_1", "usr_550e8400-e29b-41d4-a716-446655440001", "Test User 1"),
        ("tok_user_2", "usr_550e8400-e29b-41d4-a716-446655440002", "Test User 2"),
        ("tok_user_3", "usr_550e8400-e29b-41d4-a716-446655440003", "Test User 3"),
        ("tok_user_4", "usr_550e8400-e29b-41d4-a716-446655440004", "Test User 4"),
        ("tok_user_5", "usr_550e8400-e29b-41d4-a716-446655440005", "Test User 5"),
    ];

    for (token, user_id, display_name) in seed_data {
        map.insert(
            token.to_string(),
            UserProfile { user_id: user_id.to_string(), display_name: display_name.to_string() },
        );
    }

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn spawn_test_server() -> String {
        let app = create_app(AppState { tokens: Arc::new(seed_tokens()) });
        server_utils::spawn_test_server(app).await
    }

    #[tokio::test]
    async fn valid_token_returns_200() {
        let base = spawn_test_server().await;
        let client = reqwest::Client::new();

        let response = client
            .get(format!("{}/v1/auth/validate", base))
            .header("Authorization", "Bearer tok_user_1")
            .send()
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::OK);

        let json: serde_json::Value = response.json().await.unwrap();
        assert_eq!(json["valid"], true);
        assert_eq!(json["user_id"], "usr_550e8400-e29b-41d4-a716-446655440001");
        assert_eq!(json["display_name"], "Test User 1");
    }

    #[tokio::test]
    async fn invalid_token_returns_401() {
        let base = spawn_test_server().await;
        let client = reqwest::Client::new();

        let response = client
            .get(format!("{}/v1/auth/validate", base))
            .header("Authorization", "Bearer bad_token")
            .send()
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let json: serde_json::Value = response.json().await.unwrap();
        assert_eq!(json["valid"], false);
        assert_eq!(json["error"], "invalid_token");
    }

    #[tokio::test]
    async fn missing_header_returns_401() {
        let base = spawn_test_server().await;
        let client = reqwest::Client::new();

        let response =
            client.get(format!("{}/v1/auth/validate", base)).send().await.expect("request failed");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn malformed_bearer_returns_401() {
        let base = spawn_test_server().await;
        let client = reqwest::Client::new();

        let response = client
            .get(format!("{}/v1/auth/validate", base))
            .header("Authorization", "tok_user_1") // Missing "Bearer "
            .send()
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
