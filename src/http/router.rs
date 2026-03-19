use axum::{
    Router,
    middleware::{self},
    routing::{delete, get, post},
};
use tower_http::trace::TraceLayer;

use super::{AppState, handlers, middlewares};

/// Constructs the main application router with all routes and middleware.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        // Likes
        .route("/v1/likes", post(handlers::likes::like))
        .route("/v1/likes/{content_type}/{content_id}", delete(handlers::likes::unlike))
        .route("/v1/likes/{content_type}/{content_id}/count", get(handlers::likes::get_count))
        .route("/v1/likes/{content_type}/{content_id}/status", get(handlers::likes::get_status))
        .route("/v1/likes/user", get(handlers::likes::get_user_likes))
        .route("/v1/likes/batch/counts", post(handlers::likes::batch_counts))
        .route("/v1/likes/batch/statuses", post(handlers::likes::batch_statuses))
        .route("/v1/likes/top", get(handlers::likes::top_liked))
        .route("/v1/likes/stream", get(handlers::likes::sse_stream))
        // Health
        .route("/health/live", get(handlers::health::live))
        .route("/health/ready", get(handlers::health::ready))
        // Metrics
        .route("/metrics", get(handlers::metrics::metrics))
        .with_state(state)
        .layer(middleware::from_fn(middlewares::request_id))
        // TODO: add other middlewares
        .layer(TraceLayer::new_for_http())
}
