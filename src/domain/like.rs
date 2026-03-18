use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::ContentType;

/// Persistent Like record stored in the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikeRecord {
    pub user_id: Uuid,
    pub content_type: ContentType,
    pub content_id: Uuid,
    pub created_at: DateTime<Utc>,
}

/// Event kind emitted to SSE subscribers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LikeEventKind {
    Like,
    Unlike,
}

/// Event payload broadcast over SSE after like/unlike operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikeEvent {
    pub event: LikeEventKind,
    pub user_id: Uuid,
    pub content_type: ContentType,
    pub content_id: Uuid,
    pub count: i64,
    pub timestamp: DateTime<Utc>,
}
