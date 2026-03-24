use std::time::Instant;

use std::sync::Arc;

use async_trait::async_trait;
use axum::http;
use reqwest::StatusCode;
use reqwest_middleware::{ClientWithMiddleware, Error as MiddlewareError};
use serde::Deserialize;
use tracing::instrument;
use uuid::Uuid;

use super::circuit_breaker::CircuitBreaker;
use crate::{
    clients::error::ClientError,
    config::CircuitBreakerConfig,
    http::observability::{
        AppMetrics, ExternalCallStatusLabel, ExternalServiceLabel, HttpMethodLabel,
    },
};

/// Response structure for profile validation
#[derive(Debug, Deserialize)]
struct ProfileValidationResponse {
    /// Indicates if the token is valid
    valid: bool,
    /// User ID associated with the token, if valid. Spec uses "usr_" prefix.
    user_id: Option<String>,
}

/// Client for interacting with the Profile API, specifically for token validation.
pub struct ProfileClient {
    http_client: ClientWithMiddleware,
    base_url: String,
    breaker: CircuitBreaker,
    metrics: Arc<AppMetrics>,
}

/// Trait to allow mocking in tests and to abstract away the implementation details of the profile validation logic.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait ProfileValidationClient: Send + Sync {
    /// Validates a token by making an HTTP request to the Profile API.
    /// Returns the associated user ID if valid, ClientError::NotFound if invalid, and other ClientErrors for different failure scenarios.
    async fn validate_token(&self, token: &str) -> Result<Uuid, ClientError>;
}

impl ProfileClient {
    pub fn new(
        http_client: ClientWithMiddleware,
        base_url: impl Into<String>,
        config: CircuitBreakerConfig,
        metrics: Arc<AppMetrics>,
    ) -> Self {
        ProfileClient {
            http_client,
            base_url: base_url.into(),
            breaker: CircuitBreaker::new("Profile API", config, Arc::clone(&metrics)),
            metrics,
        }
    }

    /// Internal method to execute the HTTP request for token validation. Separated from the public validate_token method to allow for better error handling and circuit breaker logic.
    async fn execute_request(&self, token: &str) -> Result<Uuid, ClientError> {
        let started_at = Instant::now();
        let url = format!("{}/v1/auth/validate", self.base_url.trim_end_matches('/'));

        let response = match self
            .http_client
            .get(&url)
            .header(http::header::AUTHORIZATION, format!("Bearer {}", token))
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.metrics.observe_external_call(
                    ExternalServiceLabel::ProfileApi,
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
            ExternalServiceLabel::ProfileApi,
            HttpMethodLabel::Get,
            ExternalCallStatusLabel::Http(response.status()),
            started_at,
        );

        match response.status() {
            StatusCode::OK => {
                let payload: ProfileValidationResponse =
                    response.json().await.map_err(ClientError::Http)?;

                if !payload.valid {
                    return Err(ClientError::NotFound);
                }

                let user_id_str = payload.user_id.ok_or(ClientError::NotFound)?;
                let normalized = user_id_str.strip_prefix("usr_").unwrap_or(&user_id_str);

                Uuid::parse_str(normalized).map_err(|_| ClientError::NotFound)
            }
            StatusCode::UNAUTHORIZED => Err(ClientError::NotFound),
            _ => Err(ClientError::DependencyUnavailable("Profile API".to_string())),
        }
    }
}

#[async_trait]
impl ProfileValidationClient for ProfileClient {
    #[instrument(skip(self), err)]
    async fn validate_token(&self, token: &str) -> Result<Uuid, ClientError> {
        if !self.breaker.is_call_permitted() {
            tracing::warn!("Circuit breaker OPEN for Profile API");
            return Err(ClientError::DependencyUnavailable("Profile API".to_string()));
        }

        let result = self.execute_request(token).await;

        match &result {
            Ok(_) => {
                self.breaker.on_success();
                result
            }
            Err(ClientError::NotFound) => {
                self.breaker.on_success();
                result
            }
            Err(ClientError::Http(_)) | Err(ClientError::DependencyUnavailable(_)) => {
                self.breaker.on_error();
                result
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest_middleware::ClientBuilder;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn validate_token_success() {
        let mock_server = MockServer::start().await;

        let expected_uuid = Uuid::new_v4();
        let mock_user_id = format!("usr_{}", expected_uuid);

        // Expected request and mock response for a valid token
        Mock::given(method("GET"))
            .and(path("/v1/auth/validate"))
            .and(header("Authorization", "Bearer valid_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "valid": true,
                "user_id": mock_user_id,
                "display_name": "Test User"
            })))
            .mount(&mock_server)
            .await;

        let client = ProfileClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            mock_server.uri(),
            CircuitBreakerConfig::default(),
            Arc::new(AppMetrics::new()),
        );

        // Act & Assert
        let result = client.validate_token("valid_token").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_uuid);
    }

    #[tokio::test]
    async fn validate_token_invalid_returns_not_found() {
        let mock_server = MockServer::start().await;

        // Expected request and mock response for an invalid token
        Mock::given(method("GET"))
            .and(path("/v1/auth/validate"))
            .and(header("Authorization", "Bearer invalid_token"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "valid": false,
                "error": "invalid_token"
            })))
            .mount(&mock_server)
            .await;

        let client = ProfileClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            mock_server.uri(),
            CircuitBreakerConfig::default(),
            Arc::new(AppMetrics::new()),
        );

        let result = client.validate_token("invalid_token").await;

        assert!(matches!(result.unwrap_err(), ClientError::NotFound));
    }

    #[tokio::test]
    async fn validate_token_server_error_returns_dependency_unavailable() {
        let mock_server = MockServer::start().await;

        // Simulate Profile API being down or returning an unexpected error
        Mock::given(method("GET"))
            .and(path("/v1/auth/validate"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let client = ProfileClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            mock_server.uri(),
            CircuitBreakerConfig::default(),
            Arc::new(AppMetrics::new()),
        );

        let result = client.validate_token("any_token").await;

        assert!(matches!(result.unwrap_err(), ClientError::DependencyUnavailable(_)));
    }

    #[tokio::test]
    async fn validate_token_malformed_uuid_returns_not_found() {
        let mock_server = MockServer::start().await;

        // Simulate Profile API returning a valid response but with a malformed user_id that cannot be parsed as UUID
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "valid": true,
                "user_id": "usr_not_a_uuid",
            })))
            .mount(&mock_server)
            .await;

        let client = ProfileClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            mock_server.uri(),
            CircuitBreakerConfig::default(),
            Arc::new(AppMetrics::new()),
        );
        let result = client.validate_token("valid_token").await;

        // Should fail gracefully and return NotFound
        assert!(matches!(result.unwrap_err(), ClientError::NotFound));
    }

    #[tokio::test]
    async fn validate_token_trips_circuit_breaker() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            recovery_timeout_secs: 10,
            success_threshold: 1,
        };

        let client = ProfileClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            mock_server.uri(),
            config,
            Arc::new(AppMetrics::new()),
        );

        let result1 = client.validate_token("valid_token").await;
        assert!(matches!(result1.unwrap_err(), ClientError::DependencyUnavailable(_)));

        let result2 = client.validate_token("valid_token").await;
        assert!(matches!(result2.unwrap_err(), ClientError::DependencyUnavailable(_)));

        let result3 = client.validate_token("valid_token").await;
        assert!(matches!(result3.unwrap_err(), ClientError::DependencyUnavailable(_)));
    }

    #[tokio::test]
    async fn validate_token_unauthorized_does_not_trip_circuit_breaker() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&mock_server)
            .await;

        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout_secs: 10,
            success_threshold: 1,
        };

        let client = ProfileClient::new(
            ClientBuilder::new(reqwest::Client::new()).build(),
            mock_server.uri(),
            config,
            Arc::new(AppMetrics::new()),
        );

        let result1 = client.validate_token("invalid_token").await;
        assert!(matches!(result1.unwrap_err(), ClientError::NotFound));

        let result2 = client.validate_token("invalid_token").await;
        assert!(matches!(result2.unwrap_err(), ClientError::NotFound));
    }
}
