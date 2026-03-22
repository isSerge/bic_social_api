use std::sync::Arc;

use crate::{
    clients::profile::ProfileValidationClient,
    config::{AppConfig, ContentTypeRegistry},
    repository::cache_repo::CacheRepository,
    service::{broadcast::Broadcaster, like_service::LikeService},
};

pub mod error;
mod handlers;
mod middlewares;
pub mod router;

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    /// Application configuration loaded from environment variables.
    pub config: Arc<AppConfig>,
    /// Registry for content types and their associated base URLs.
    pub content_type_registry: Arc<ContentTypeRegistry>,
    /// Service for managing likes, backed by a PostgreSQL repository.
    pub like_service: Arc<LikeService>,
    /// Profile API client for validating user tokens
    pub profile_client: Arc<dyn ProfileValidationClient>,
    /// Cache repository for managing cached data
    pub cache: Arc<dyn CacheRepository>,
    /// Broadcaster for real-time updates (SSE)
    pub broadcaster: Arc<Broadcaster>,
}
