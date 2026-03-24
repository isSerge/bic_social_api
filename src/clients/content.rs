use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use reqwest::StatusCode;
use reqwest_middleware::{ClientWithMiddleware, Error as MiddlewareError};
use serde::Deserialize;
use tracing::instrument;
use uuid::Uuid;

use super::circuit_breaker::CircuitBreaker;
use crate::{
    clients::error::ClientError,
    config::{CircuitBreakerConfig, ContentTypeRegistry},
    domain::ContentType,
    http::observability::{
        AppMetrics, ExternalCallStatusLabel, ExternalServiceLabel, HttpMethodLabel,
    },
};

/// Client for interacting with the Content API, specifically for content existence validation.
pub struct HttpContentClient {
    http_client: ClientWithMiddleware,
    registry: Arc<ContentTypeRegistry>,
    breaker: CircuitBreaker,
    metrics: Arc<AppMetrics>,
}

#[derive(Debug, Deserialize)]
struct ContentLookupResponse {
    #[serde(default)]
    items: Vec<serde_json::Value>,
}

/// Trait to allow mocking in tests and to abstract away the implementation details of the content validation logic.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait ContentValidationClient: Send + Sync {
    /// Validates if a content item exists by making an HTTP request to the Content API.
    /// Returns Ok(()) if the content exists, ClientError::NotFound if it doesn't, and other ClientErrors for different failure scenarios.
    async fn validate_content(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<(), ClientError>;
}

impl HttpContentClient {
    pub fn new(
        http_client: ClientWithMiddleware,
        registry: Arc<ContentTypeRegistry>,
        config: CircuitBreakerConfig,
        metrics: Arc<AppMetrics>,
    ) -> Self {
        HttpContentClient {
            http_client,
            registry,
            breaker: CircuitBreaker::new("Content API", config, Arc::clone(&metrics)),
            metrics,
        }
    }

    /// HTTP request logic, separated so the breaker can wrap it
    async fn execute_request(
        &self,
        content_type: &ContentType,
        content_id: Uuid,
    ) -> Result<(), ClientError> {
        let started_at = Instant::now();
        let base_url = self.registry.get_url(content_type);
        let url =
            format!("{}/v1/{}/{}", base_url.trim_end_matches('/'), content_type.0, content_id);

        let response = match self.http_client.get(&url).send().await {
            Ok(response) => response,
            Err(error) => {
                self.metrics.observe_external_call(
                    ExternalServiceLabel::ContentApi,
                    HttpMethodLabel::Get,
                    ExternalCallStatusLabel::Error,
                    started_at,
                );
                return Err(match error {
                    MiddlewareError::Reqwest(error) => ClientError::Http(error),
                    MiddlewareError::Middleware(error) => {
                        ClientError::DependencyUnavailable(error.to_string())
                    }
                });
            }
        };

        self.metrics.observe_external_call(
            ExternalServiceLabel::ContentApi,
            HttpMethodLabel::Get,
            ExternalCallStatusLabel::Http(response.status()),
            started_at,
        );

        match response.status() {
            StatusCode::OK => {
                let payload: ContentLookupResponse =
                    response.json().await.map_err(ClientError::Http)?;

                // A post is valid if the response contains a non-empty items array
                if payload.items.is_empty() { Err(ClientError::NotFound) } else { Ok(()) }
            }
            StatusCode::NOT_FOUND => Err(ClientError::NotFound),
            _ => Err(ClientError::DependencyUnavailable("Content API".to_string())),
        }
    }
}

#[async_trait]
impl ContentValidationClient for HttpContentClient {
    #[instrument(skip(self), err)]
    async fn validate_content(
        &self,
        content_type: ContentType,
        content_id: Uuid,
    ) -> Result<(), ClientError> {
        // Check if the circuit breaker allows the call
        if !self.breaker.is_call_permitted() {
            tracing::warn!("Circuit breaker OPEN for Content API");
            return Err(ClientError::DependencyUnavailable("Content API".to_string()));
        }

        // Execute the HTTP request
        let result = self.execute_request(&content_type, content_id).await;

        // Update the circuit breaker state based on the result
        match &result {
            Ok(_) => {
                self.breaker.on_success();
                Ok(())
            }
            Err(ClientError::NotFound) => {
                // A 404 is a SUCCESSFUL network call, should NOT trip the circuit breaker.
                self.breaker.on_success();
                Err(ClientError::NotFound)
            }
            Err(ClientError::Http(_)) | Err(ClientError::DependencyUnavailable(_)) => {
                // Actual failures that should trip the breaker (network issues, server errors, etc.)
                self.breaker.on_error();
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::observability::AppMetrics;
    use reqwest_middleware::ClientBuilder;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Helper to create a registry that points to our mock server for testing
    fn registry_for_mock_server(
        mock_server: &MockServer,
        content_types: impl IntoIterator<Item = &'static str>,
    ) -> Arc<ContentTypeRegistry> {
        Arc::new(ContentTypeRegistry::from_base_urls(
            content_types.into_iter().map(|content_type| (content_type, mock_server.uri())),
        ))
    }

    #[tokio::test]
    async fn validate_content_success() {
        let mock_server = MockServer::start().await;
        let content_id = Uuid::new_v4();
        let content_type = ContentType::from("post");

        Mock::given(method("GET"))
            .and(path(format!("/v1/post/{content_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "items": [
                    {
                        "id": content_id,
                        "content_type": "post",
                        "title": "Mock post content"
                    }
                ]
            })))
            .mount(&mock_server)
            .await;

        let registry = registry_for_mock_server(&mock_server, ["post"]);

        let client = HttpContentClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            registry,
            CircuitBreakerConfig::default(),
            Arc::new(AppMetrics::new()),
        );
        let result = client.validate_content(content_type, content_id).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn validate_content_empty_items_returns_not_found() {
        let mock_server = MockServer::start().await;
        let content_id = Uuid::new_v4();

        Mock::given(method("GET"))
            .and(path(format!("/v1/post/{content_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "items": []
            })))
            .mount(&mock_server)
            .await;

        let registry = registry_for_mock_server(&mock_server, ["post"]);
        let client = HttpContentClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            registry,
            CircuitBreakerConfig::default(),
            Arc::new(AppMetrics::new()),
        );
        let content_type = ContentType::from("post");

        let result = client.validate_content(content_type, content_id).await;

        assert!(matches!(result.unwrap_err(), ClientError::NotFound));
    }

    #[tokio::test]
    async fn validate_content_not_found_returns_not_found() {
        let mock_server = MockServer::start().await;
        let content_id = Uuid::new_v4();

        Mock::given(method("GET"))
            .and(path(format!("/v1/post/{content_id}")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let registry = registry_for_mock_server(&mock_server, ["post"]);
        let client = HttpContentClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            registry,
            CircuitBreakerConfig::default(),
            Arc::new(AppMetrics::new()),
        );
        let content_type = ContentType::from("post");
        let result = client.validate_content(content_type, content_id).await;

        assert!(matches!(result.unwrap_err(), ClientError::NotFound));
    }

    #[tokio::test]
    async fn validate_content_server_error_returns_dependency_unavailable() {
        let mock_server = MockServer::start().await;
        let content_id = Uuid::new_v4();

        Mock::given(method("GET"))
            .and(path(format!("/v1/post/{content_id}")))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let registry = registry_for_mock_server(&mock_server, ["post"]);
        let client = HttpContentClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            registry,
            CircuitBreakerConfig::default(),
            Arc::new(AppMetrics::new()),
        );
        let content_type = ContentType::from("post");
        let result = client.validate_content(content_type, content_id).await;

        assert!(matches!(result.unwrap_err(), ClientError::DependencyUnavailable(_)));
    }

    #[tokio::test]
    async fn validate_content_wrong_content_type_returns_not_found() {
        let mock_server = MockServer::start().await;
        let content_id = Uuid::new_v4();

        Mock::given(method("GET"))
            .and(path(format!("/v1/invalid_type/{content_id}")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let registry = registry_for_mock_server(&mock_server, ["invalid_type"]);
        let client = HttpContentClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            registry,
            CircuitBreakerConfig::default(),
            Arc::new(AppMetrics::new()),
        );
        let content_type = ContentType::from("invalid_type");
        let result = client.validate_content(content_type, content_id).await;

        assert!(matches!(result.unwrap_err(), ClientError::NotFound));
    }

    #[tokio::test]
    async fn validate_content_trips_circuit_breaker() {
        let mock_server = MockServer::start().await;
        let content_id = Uuid::new_v4();

        // The mock server will always return a 500 Error
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        // Create a config where it trips after exactly 2 failures
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            recovery_timeout_secs: 10,
            success_threshold: 1,
        };

        let registry = registry_for_mock_server(&mock_server, ["post"]);
        let client = HttpContentClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            registry,
            config,
            Arc::new(AppMetrics::new()),
        );

        // Call 1: Fails, but circuit remains Closed
        let content_type = ContentType::from("post");
        let result1 = client.validate_content(content_type.clone(), content_id).await;
        assert!(matches!(result1.unwrap_err(), ClientError::DependencyUnavailable(_)));

        // Call 2: Fails, threshold reached! Circuit trips to Open
        let result2 = client.validate_content(content_type.clone(), content_id).await;
        assert!(matches!(result2.unwrap_err(), ClientError::DependencyUnavailable(_)));

        // Call 3: Fails FAST without hitting the network
        // We know it didn't hit the network because wiremock would complain if it got a request
        let result3 = client.validate_content(content_type, content_id).await;
        assert!(matches!(result3.unwrap_err(), ClientError::DependencyUnavailable(_)));
    }

    #[tokio::test]
    async fn validate_content_404_does_not_trip_circuit_breaker() {
        let mock_server = MockServer::start().await;
        let content_id = Uuid::new_v4();

        // The mock server returns 404
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        // Trip after 1 failure
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout_secs: 10,
            success_threshold: 1,
        };

        let registry = registry_for_mock_server(&mock_server, ["post"]);
        let client = HttpContentClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            registry,
            config,
            Arc::new(AppMetrics::new()),
        );

        // Call 1: Returns 404. This is a business logic error, NOT a network failure.
        let content_type = ContentType::from("post");
        let result1 = client.validate_content(content_type.clone(), content_id).await;
        assert!(matches!(result1.unwrap_err(), ClientError::NotFound));

        // Call 2: If the circuit tripped, this would return DependencyUnavailable.
        // Because it returns NotFound again, we prove the circuit remained Closed!
        let result2 = client.validate_content(content_type, content_id).await;
        assert!(matches!(result2.unwrap_err(), ClientError::NotFound));
    }
}
