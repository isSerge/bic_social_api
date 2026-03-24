//! Mock Content API service.

use axum::{
    Json, Router, extract::Path, extract::State, http::StatusCode, response::IntoResponse,
    routing::get,
};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;

#[path = "../server_utils.rs"]
mod server_utils;

#[derive(Clone)]
struct AppState {
    // Maps content_type -> set(content_id)
    known_content: Arc<HashMap<String, HashSet<Uuid>>>,
}

#[derive(Debug, Serialize)]
struct ContentResponse {
    id: Uuid,
    title: String,
    content_type: String,
}

#[tokio::main]
async fn main() {
    // Init tracing
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    let state = AppState { known_content: Arc::new(seed_content()) };

    let app = create_app(state);
    server_utils::serve_mock(app, 8081, "Mock Content API").await;
}

fn create_app(state: AppState) -> Router {
    Router::new()
        .route("/v1/{content_type}/{content_id}", get(get_content))
        .route("/health", get(|| async { StatusCode::OK }))
        .with_state(state)
}

/// Handler for GET /v1/{content_type}/{content_id}
async fn get_content(
    State(state): State<AppState>,
    Path((content_type, content_id)): Path<(String, Uuid)>,
) -> impl IntoResponse {
    let normalized_type = content_type.to_lowercase();

    let Some(known_ids) = state.known_content.get(&normalized_type) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    if !known_ids.contains(&content_id) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let payload = ContentResponse {
        id: content_id,
        title: format!("Mock {normalized_type} content"),
        content_type: normalized_type,
    };

    (StatusCode::OK, Json(payload)).into_response()
}

/// Seeds the mock content store with known content IDs for testing.
fn seed_content() -> HashMap<String, HashSet<Uuid>> {
    let mut map = HashMap::new();

    map.insert(
        "post".to_string(),
        [
            "731b0395-4888-4822-b516-05b4b7bf2089",
            "9601c044-6130-4ee5-a155-96570e05a02f",
            "933dde0f-4744-4a66-9a38-bf5cb1f67553",
            "ea0f2020-0509-45fd-adb9-24b8843055ee",
            "bd27f926-0a00-41fd-b085-a7491e6d0902",
            "2a656157-5284-48b5-9d76-ede492933347",
            "4f884e5e-2f1d-4965-b0f1-16922acd91a2",
            "ad1d9238-622c-4875-9881-5f8e19997783",
            "c34ee1e3-7224-4a97-ba44-0993eb7a6ed8",
            "c2b7f212-6162-4ae6-837b-16ee34cc9a50",
        ]
        .into_iter()
        .map(|s| Uuid::parse_str(s).unwrap())
        .collect(),
    );

    map.insert(
        "bonus_hunter".to_string(),
        [
            "c3d4e5f6-a7b8-9012-cdef-123456789012", // ID from the spec example
            // Additional IDs to meet "at least 5" requirement
            "d89063c4-4d83-48b4-9279-d5c64c740dd6",
            "f61c3ea0-5d6b-4e0d-8517-56e691b0f545",
            "1c4558ea-5367-4d7a-b5e1-558e801ab1a1",
            "88ea4fa2-1672-4d7e-9276-809bb396ab9a",
        ]
        .into_iter()
        .map(|s| Uuid::parse_str(s).unwrap())
        .collect(),
    );

    map.insert(
        "top_picks".to_string(),
        // 5 random IDs
        [
            "e8b15d9a-1153-4e8c-8c63-42eb43ccbce3",
            "c97f2231-6453-41ea-bf51-1bfa49cce824",
            "3b591b68-6c8c-4a1e-81f1-0a6042de11ee",
            "99d421eb-98c4-4869-aa57-3fdd0bbab420",
            "07b22294-b1eb-47eb-ba04-b9d9df1dc425",
        ]
        .into_iter()
        .map(|s| Uuid::parse_str(s).unwrap())
        .collect(),
    );

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn spawn_test_server() -> String {
        let app = create_app(AppState { known_content: Arc::new(seed_content()) });
        server_utils::spawn_test_server(app).await
    }

    #[tokio::test]
    async fn known_uuid_returns_200() {
        let base = spawn_test_server().await;
        let client = reqwest::Client::new();

        // Updated to use the smoke test ID
        let response = client
            .get(format!("{base}/v1/post/731b0395-4888-4822-b516-05b4b7bf2089"))
            .send()
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn unknown_uuid_returns_404() {
        let base = spawn_test_server().await;
        let client = reqwest::Client::new();

        let response = client
            .get(format!("{base}/v1/post/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))
            .send()
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn wrong_content_type_returns_404() {
        let base = spawn_test_server().await;
        let client = reqwest::Client::new();

        let response = client
            .get(format!("{base}/v1/invalid_type/731b0395-4888-4822-b516-05b4b7bf2089"))
            .send()
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn health_check_returns_200() {
        let base = spawn_test_server().await;
        let client = reqwest::Client::new();

        let response = client.get(format!("{base}/health")).send().await.expect("request failed");

        assert_eq!(response.status(), StatusCode::OK);
    }
}
