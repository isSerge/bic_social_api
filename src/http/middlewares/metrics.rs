use std::time::Instant;

use axum::{
    extract::{MatchedPath, Request, State},
    middleware::Next,
    response::Response,
};

use crate::http::AppState;

/// Middleware to record HTTP metrics for each request, including method, path, status code, and duration. Uses the AppMetrics instance from state to record metrics in a Prometheus-compatible format.
pub async fn record_http_metrics(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let started_at = Instant::now();
    let method = request.method().clone();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());

    let response = next.run(request).await;

    state.metrics.observe_http_request(
        method.as_str(),
        &path,
        response.status().as_u16(),
        started_at,
    );

    response
}
