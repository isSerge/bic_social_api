use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, Request, State},
    middleware::Next,
    response::Response,
};
use reqwest::Method;
use uuid::Uuid;

use crate::http::{AppState, error::ApiError};

const ONE_MINUTE_WINDOW: u64 = 60; // Window size in seconds

/// Middleware to enforce rate limits based on IP for reads and User ID for writes.
/// Reads (GET) are limited per IP, while writes (POST, PUT, DELETE, PATCH) are limited per authenticated user.
pub async fn rate_limiter(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // Determine if the request is a write operation
    let is_write_method =
        matches!(req.method(), &Method::POST | &Method::DELETE | &Method::PUT | &Method::PATCH);

    // Determine the key and limit based on request type.
    let (key, limit) = if is_write_method {
        let user_id = req.extensions().get::<Uuid>().ok_or(ApiError::Unauthorized)?; // Unauthorized should never happen if routing is correct
        (format!("rate:write:user:{}", user_id), state.config.limits.write_per_minute)
    } else {
        // Public reads - use IP address as key
        let key = format!("rate:read:ip:{}", addr.ip());
        (key, state.config.limits.read_per_minute)
    };

    // Check the rate limit for this key (IP or user token)
    let (allowed, retry_after_secs) =
        state.cache.check_rate_limit(&key, limit, ONE_MINUTE_WINDOW).await?;

    if !allowed {
        // Return a 429 Too Many Requests with the retry_after info
        return Err(ApiError::RateLimited { retry_after_secs });
    }

    Ok(next.run(req).await)
}
