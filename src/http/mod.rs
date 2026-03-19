use std::sync::Arc;

use crate::config::{AppConfig, ContentTypeRegistry};

mod error;
mod handlers;
mod middlewares;
pub mod router;

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub content_type_registry: Arc<ContentTypeRegistry>,
}
