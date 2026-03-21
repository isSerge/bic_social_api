use async_trait::async_trait;
use reqwest::StatusCode;
use uuid::Uuid;

use super::circuit_breaker::CircuitBreaker;
use crate::{clients::error::ClientError, config::CircuitBreakerConfig};

/// Client for interacting with the Content API, specifically for content existence validation.
pub struct HttpContentClient {
    http_client: reqwest::Client,
    base_url: String,
    breaker: CircuitBreaker,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait ContentValidationClient: Send + Sync {
    async fn validate_content(
        &self,
        content_type: &str,
        content_id: Uuid,
    ) -> Result<(), ClientError>;
}

impl HttpContentClient {
    pub fn new(
        http_client: reqwest::Client,
        base_url: impl Into<String>,
        config: CircuitBreakerConfig,
    ) -> Self {
        HttpContentClient {
            http_client,
            base_url: base_url.into(),
            breaker: CircuitBreaker::new("Content API", config),
        }
    }

    /// HTTP request logic, separated so the breaker can wrap it
    async fn execute_request(
        &self,
        content_type: &str,
        content_id: Uuid,
    ) -> Result<(), ClientError> {
        let url =
            format!("{}/v1/{}/{}", self.base_url.trim_end_matches('/'), content_type, content_id);

        let response = self.http_client.get(&url).send().await.map_err(ClientError::Http)?;

        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::NOT_FOUND => Err(ClientError::NotFound),
            _ => Err(ClientError::DependencyUnavailable("Content API".to_string())),
        }
    }
}

#[async_trait]
impl ContentValidationClient for HttpContentClient {
    async fn validate_content(
        &self,
        content_type: &str,
        content_id: Uuid,
    ) -> Result<(), ClientError> {
        // Check if the circuit breaker allows the call
        if !self.breaker.is_call_permitted() {
            tracing::warn!("Circuit breaker OPEN for Content API");
            return Err(ClientError::DependencyUnavailable("Content API".to_string()));
        }

        // Execute the HTTP request
        let result = self.execute_request(content_type, content_id).await;

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

        let client = HttpContentClient::new(
            reqwest::Client::new(),
            mock_server.uri(),
            CircuitBreakerConfig::default(),
        );
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

        let client = HttpContentClient::new(
            reqwest::Client::new(),
            mock_server.uri(),
            CircuitBreakerConfig::default(),
        );
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

        let client = HttpContentClient::new(
            reqwest::Client::new(),
            mock_server.uri(),
            CircuitBreakerConfig::default(),
        );
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

        let client = HttpContentClient::new(
            reqwest::Client::new(),
            mock_server.uri(),
            CircuitBreakerConfig::default(),
        );
        let result = client.validate_content("invalid_type", content_id).await;

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

        let client = HttpContentClient::new(reqwest::Client::new(), mock_server.uri(), config);

        // Call 1: Fails, but circuit remains Closed
        let result1 = client.validate_content("post", content_id).await;
        assert!(matches!(result1.unwrap_err(), ClientError::DependencyUnavailable(_)));

        // Call 2: Fails, threshold reached! Circuit trips to Open
        let result2 = client.validate_content("post", content_id).await;
        assert!(matches!(result2.unwrap_err(), ClientError::DependencyUnavailable(_)));

        // Call 3: Fails FAST without hitting the network
        // We know it didn't hit the network because wiremock would complain if it got a request
        let result3 = client.validate_content("post", content_id).await;
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

        let client = HttpContentClient::new(reqwest::Client::new(), mock_server.uri(), config);

        // Call 1: Returns 404. This is a business logic error, NOT a network failure.
        let result1 = client.validate_content("post", content_id).await;
        assert!(matches!(result1.unwrap_err(), ClientError::NotFound));

        // Call 2: If the circuit tripped, this would return DependencyUnavailable.
        // Because it returns NotFound again, we prove the circuit remained Closed!
        let result2 = client.validate_content("post", content_id).await;
        assert!(matches!(result2.unwrap_err(), ClientError::NotFound));
    }
}
