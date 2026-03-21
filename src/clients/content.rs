use reqwest::StatusCode;
use serde::Deserialize;
use uuid::Uuid;

use crate::clients::error::ClientError;

/// Response structure for content validation
#[derive(Debug, Deserialize)]
struct ContentResponse {
    id: Uuid,
    content_type: String,
    title: String,
}

/// Client for interacting with the Content API, specifically for content existence validation.
pub struct ContentClient {
    http_client: reqwest::Client,
    base_url: String,
}

impl ContentClient {
    pub fn new(http_client: reqwest::Client, base_url: impl Into<String>) -> Self {
        ContentClient { http_client, base_url: base_url.into() }
    }

    /// Validates that a content item exists by calling the Content API.
    /// Returns `Ok(())` if the item exists, `ClientError::NotFound` if it does not,
    /// or `ClientError::DependencyUnavailable` for any other failure.
    pub async fn validate_content(
        &self,
        content_type: &str,
        content_id: Uuid,
    ) -> Result<(), ClientError> {
        let url = format!(
            "{}/v1/{}/{}",
            self.base_url.trim_end_matches('/'),
            content_type,
            content_id,
        );

        let response =
            self.http_client.get(&url).send().await.map_err(ClientError::Http)?;

        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(ClientError::NotFound),
            _ => Err(ClientError::DependencyUnavailable("Content API".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn validate_content_success() {
        let mock_server = MockServer::start().await;
        let content_id = Uuid::new_v4();

        Mock::given(method("GET"))
            .and(path(format!("/v1/post/{}", content_id)))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": content_id,
                "content_type": "post",
                "title": "Mock post content"
            })))
            .mount(&mock_server)
            .await;

        let client = ContentClient::new(reqwest::Client::new(), mock_server.uri());
        let result = client.validate_content("post", content_id).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn validate_content_not_found_returns_not_found() {
        let mock_server = MockServer::start().await;
        let content_id = Uuid::new_v4();

        Mock::given(method("GET"))
            .and(path(format!("/v1/post/{}", content_id)))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let client = ContentClient::new(reqwest::Client::new(), mock_server.uri());
        let result = client.validate_content("post", content_id).await;

        assert!(matches!(result.unwrap_err(), ClientError::NotFound));
    }

    #[tokio::test]
    async fn validate_content_server_error_returns_dependency_unavailable() {
        let mock_server = MockServer::start().await;
        let content_id = Uuid::new_v4();

        Mock::given(method("GET"))
            .and(path(format!("/v1/post/{}", content_id)))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let client = ContentClient::new(reqwest::Client::new(), mock_server.uri());
        let result = client.validate_content("post", content_id).await;

        assert!(matches!(result.unwrap_err(), ClientError::DependencyUnavailable(_)));
    }

    #[tokio::test]
    async fn validate_content_wrong_content_type_returns_not_found() {
        let mock_server = MockServer::start().await;
        let content_id = Uuid::new_v4();

        Mock::given(method("GET"))
            .and(path(format!("/v1/invalid_type/{}", content_id)))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let client = ContentClient::new(reqwest::Client::new(), mock_server.uri());
        let result = client.validate_content("invalid_type", content_id).await;

        assert!(matches!(result.unwrap_err(), ClientError::NotFound));
    }
}
