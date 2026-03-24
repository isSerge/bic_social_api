mod error;
mod like;

pub use error::DomainError;
pub use like::{LikeEvent, LikeEventKind, LikeRecord, PaginationCursor};
use serde::Serialize;

use std::sync::Arc;

/// Domain-specific types and logic for the Social API application.
///
/// We wrap the content type identifier in an `Arc<str>` instead of a `String`.
/// This allows us to share the exact same string allocation across the Registry,
/// Database queries, Cache keys, and HTTP JSON Responses via cheap atomic reference
/// counting, eliminating hundreds of heap allocations per request.
#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::Type, Serialize)]
#[sqlx(transparent, no_pg_array)]
pub struct ContentType(pub Arc<str>);

impl From<&str> for ContentType {
    fn from(s: &str) -> Self {
        ContentType(Arc::from(s))
    }
}
