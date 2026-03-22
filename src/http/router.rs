use axum::{
    Router,
    middleware::{self},
    routing::{delete, get, post},
};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use super::{AppState, handlers, middlewares};

/// Constructs the main application router with all routes and middleware.
pub fn create_router(state: AppState) -> Router {
    // Protected Like routes that require authentication
    let protected = Router::new()
        .route("/v1/likes", post(handlers::likes::like))
        .route("/v1/likes/{content_type}/{content_id}", delete(handlers::likes::unlike))
        .route("/v1/likes/{content_type}/{content_id}/status", get(handlers::likes::get_status))
        .route("/v1/likes/user", get(handlers::likes::get_user_likes))
        .route("/v1/likes/batch/statuses", post(handlers::likes::batch_statuses))
        .route_layer(
            ServiceBuilder::new()
                .layer(middleware::from_fn_with_state(
                    state.clone(),
                    middlewares::auth::require_auth,
                ))
                .layer(middleware::from_fn_with_state(state.clone(), middlewares::rate_limiter)),
        );

    // Public Like routes that do not require authentication
    let public = Router::new()
        .route("/v1/likes/{content_type}/{content_id}/count", get(handlers::likes::get_count))
        .route("/v1/likes/batch/counts", post(handlers::likes::batch_counts))
        .route("/v1/likes/top", get(handlers::likes::top_liked))
        .route("/v1/likes/stream", get(handlers::likes::sse_stream))
        .route_layer(middleware::from_fn_with_state(state.clone(), middlewares::rate_limiter));

    // Diagnostic routes for health checks and metrics - no auth or rate limiting, intended only for internal communication
    let diagnostic = Router::new()
        .route("/health/live", get(handlers::health::live))
        .route("/health/ready", get(handlers::health::ready))
        .route("/metrics", get(handlers::metrics::metrics));

    Router::new().merge(protected).merge(public).merge(diagnostic).with_state(state).layer(
        ServiceBuilder::new()
            .layer(middleware::from_fn(middlewares::request_id))
            .layer(TraceLayer::new_for_http()),
    )
}
