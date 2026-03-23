use axum::{
    extract::State,
    http::{StatusCode, header::CONTENT_TYPE},
    response::IntoResponse,
};

use crate::http::{AppState, error::ApiError};

/// GET /metrics
pub async fn metrics(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let body = state.metrics.render().map_err(|error| {
        tracing::error!(error = %error, "Failed to encode Prometheus metrics");
        ApiError::MetricsEncoding
    })?;

    Ok((StatusCode::OK, [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], body))
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
            observability::{AppMetrics, HttpMethodLabel, StaticReadinessProbe},
        },
        repository::{cache_repo::MockCacheRepository, like_repo::MockLikeRepository},
        service::{broadcast::Broadcaster, like_service::LikeService},
    };

    use super::*;

    fn app() -> Router {
        let config = Arc::new(AppConfig::default());
        let cache = Arc::new(MockCacheRepository::new());
        let broadcaster = Arc::new(Broadcaster::new(config.server.sse_channel_capacity));
        let app_metrics = Arc::new(AppMetrics::new());
        app_metrics.observe_http_request(
            HttpMethodLabel::Get,
            "/metrics",
            200,
            std::time::Instant::now(),
        );
        let like_service = Arc::new(LikeService::new(
            config.cache,
            Arc::new(MockLikeRepository::new()),
            cache.clone(),
            Arc::new(MockContentValidationClient::new()),
            Arc::clone(&broadcaster),
            Arc::clone(&app_metrics),
        ));

        let state = AppState {
            config,
            content_type_registry: Arc::new(ContentTypeRegistry::default()),
            like_service,
            profile_client: Arc::new(MockProfileValidationClient::new()),
            cache,
            broadcaster,
            readiness: Arc::new(StaticReadinessProbe::ready()),
            metrics: app_metrics,
        };

        Router::new().route("/metrics", get(metrics)).with_state(state)
    }

    #[tokio::test]
    async fn metrics_returns_prometheus_text() {
        let response = app()
            .oneshot(Request::builder().uri("/metrics").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).and_then(|value| value.to_str().ok()),
            Some("text/plain; version=0.0.4; charset=utf-8")
        );

        let body_bytes =
            axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let body = String::from_utf8(body_bytes.to_vec()).expect("utf-8");
        assert!(body.contains("social_api_http_requests_total"));
        assert!(body.contains("social_api_http_request_duration_seconds"));
    }
}
