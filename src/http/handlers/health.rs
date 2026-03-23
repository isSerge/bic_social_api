use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};

use crate::http::{AppState, error::ApiError};

/// GET /health/live
pub async fn live() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({ "status": "alive" })))
}

/// GET /health/ready
pub async fn ready(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let report = state.readiness.probe().await;

    if report.is_ready() {
        return Ok((StatusCode::OK, Json(serde_json::json!({ "status": "ready" }))));
    }

    Err(ApiError::ReadinessFailed { report })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{Router, body::Body, http::Request, routing::get};
    use tower::ServiceExt;

    use crate::{
        clients::{content::MockContentValidationClient, profile::MockProfileValidationClient},
        config::{AppConfig, ContentTypeRegistry},
        http::{
            AppState,
            observability::{AppMetrics, ReadinessReport, StaticReadinessProbe},
        },
        repository::{cache_repo::MockCacheRepository, like_repo::MockLikeRepository},
        service::{broadcast::Broadcaster, like_service::LikeService},
    };

    use super::*;

    fn app_with_report(report: ReadinessReport) -> Router {
        let config = Arc::new(AppConfig::default());
        let cache = Arc::new(MockCacheRepository::new());
        let broadcaster = Arc::new(Broadcaster::new(config.server.sse_channel_capacity));
        let metrics = Arc::new(AppMetrics::new());
        let like_service = Arc::new(LikeService::new(
            config.cache,
            Arc::new(MockLikeRepository::new()),
            cache.clone(),
            Arc::new(MockContentValidationClient::new()),
            Arc::clone(&broadcaster),
            Arc::clone(&metrics),
        ));

        let state = AppState {
            config,
            content_type_registry: Arc::new(ContentTypeRegistry::default()),
            like_service,
            profile_client: Arc::new(MockProfileValidationClient::new()),
            cache,
            broadcaster,
            readiness: Arc::new(StaticReadinessProbe::with_report(report)),
            metrics,
        };

        Router::new()
            .route("/health/live", get(live))
            .route("/health/ready", get(ready))
            .with_state(state)
    }

    #[tokio::test]
    async fn live_returns_200() {
        let response = app_with_report(ReadinessReport::default())
            .oneshot(Request::builder().uri("/health/live").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn ready_returns_503_with_failures_when_dependency_down() {
        let mut report = ReadinessReport::default();
        report.record_failure("redis", "redis ping failed");
        report.record_failure("content_api", "http://content/health returned 500");

        let response = app_with_report(report)
            .oneshot(Request::builder().uri("/health/ready").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body_bytes =
            axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json body");
        let failures = body["error"]["details"]["failures"].as_array().expect("failures");
        assert_eq!(failures.len(), 2);
        assert_eq!(failures[0]["dependency"], "redis");
    }
}
