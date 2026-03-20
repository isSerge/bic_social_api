use std::sync::Arc;

use crate::{
    config::{AppConfig, ContentTypeRegistry},
    like_service::LikeService,
    repository::like_repo::PgLikeRepository,
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
    pub like_service: Arc<LikeService<PgLikeRepository>>,
}
